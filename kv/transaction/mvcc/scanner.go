package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	startKey []byte
	txn      *MvccTxn
	iter     engine_util.DBIterator
	// The last user key returned, used to skip the older versions of a key.
	lastKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfDefault)
	// Data keys are ordered by user key ascending and by timestamp descending,
	// so seeking to (startKey, StartTS) lands on the newest version of startKey
	// that is visible to this transaction.
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		startKey: startKey,
		txn:      txn,
		iter:     iter,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		userKey := DecodeUserKey(item.Key())
		if len(scan.lastKey) > 0 && string(userKey) == string(scan.lastKey) {
			// Older versions of the key we already returned.
			continue
		}
		scan.lastKey = userKey

		value, err := scan.txn.GetValue(userKey)
		if err != nil {
			return nil, nil, err
		}
		if value == nil {
			// The key was deleted before the transaction's start timestamp.
			continue
		}
		return userKey, value, nil
	}
	return nil, nil, nil
}
