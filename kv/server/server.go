package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, keys)

	for _, mutation := range req.Mutations {
		key := mutation.Key

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
			continue
		}

		// A commit of another transaction with a timestamp newer than our start
		// timestamp means our snapshot is out of date.
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback && commitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: commitTs,
				Key:        key,
				Primary:    req.PrimaryLock,
			}})
			continue
		}

		var kind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, mutation.Value)
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
			kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
			continue
		default:
			continue
		}
		txn.PutLock(key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	if len(txn.Writes()) > 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, req.Keys)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// The lock is already gone: either this transaction has been
			// committed before (a repeated commit is a no-op) or it has been
			// rolled back.
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "the transaction has been rolled back"}
			}
			continue
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "the lock belongs to another transaction"}
			continue
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}

	if len(txn.Writes()) > 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	for uint32(len(resp.Pairs)) < req.Limit {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock != nil && lock.Ts == req.LockTs {
		// The lock belongs to the transaction we are asked about: roll it back if
		// its TTL has expired, otherwise report that it is still alive.
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			resp.LockTtl = lock.Ttl
			resp.Action = kvrpcpb.Action_NoAction
		}
	} else {
		// No live lock: the transaction has either committed or been rolled back.
		// Otherwise the prewrite never made it to this store, so roll it back.
		write, commitTs, err := txn.MostRecentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write != nil && write.StartTS == req.LockTs {
			if write.Kind != mvcc.WriteKindRollback {
				resp.CommitVersion = commitTs
			}
			resp.Action = kvrpcpb.Action_NoAction
		} else {
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_LockNotExistRollback
		}
	}

	if len(txn.Writes()) > 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := &kvrpcpb.BatchRollbackResponse{}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, req.Keys)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			if lock.Ts == req.StartVersion {
				txn.DeleteValue(key)
				txn.DeleteLock(key)
			}
			// The lock belongs to another transaction, leave it alone but still
			// make sure our own transaction cannot write this key later.
		} else {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil {
				if write.Kind != mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{Abort: "the transaction is already committed"}
				}
				continue
			}
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
	}

	if len(txn.Writes()) > 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}
	for _, pair := range locks {
		if req.CommitVersion == 0 {
			txn.DeleteValue(pair.Key)
			txn.DeleteLock(pair.Key)
			txn.PutWrite(pair.Key, pair.Lock.Ts, &mvcc.Write{StartTS: pair.Lock.Ts, Kind: mvcc.WriteKindRollback})
			continue
		}
		txn.PutWrite(pair.Key, req.CommitVersion, &mvcc.Write{StartTS: pair.Lock.Ts, Kind: pair.Lock.Kind})
		txn.DeleteLock(pair.Key)
	}

	if len(txn.Writes()) > 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
