package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// toRegionError converts a storage error into a region error when the request was
// rejected because of region metadata (only RaftStorage produces those).
func toRegionError(err error) *errorpb.Error {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		return regionErr.RequestErr
	}
	return nil
}

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{RegionError: toRegionError(err)}, nil
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{Value: val, NotFound: len(val) == 0}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value},
	}})
	if err != nil {
		return &kvrpcpb.RawPutResponse{RegionError: toRegionError(err)}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{Cf: req.Cf, Key: req.Key},
	}})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{RegionError: toRegionError(err)}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{RegionError: toRegionError(err)}, nil
	}
	defer reader.Close()

	it := reader.IterCF(req.Cf)
	defer it.Close()

	resp := &kvrpcpb.RawScanResponse{}
	limit := int(req.Limit)
	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		if limit > 0 && len(resp.Kvs) >= limit {
			break
		}
		item := it.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		})
	}
	return resp, nil
}
