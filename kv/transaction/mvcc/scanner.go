package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey []byte
	txn      *MvccTxn
	it       engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iterator := txn.Reader.IterCF(engine_util.CfDefault)
	iterator.Seek(startKey)
	return &Scanner{startKey: startKey, txn: txn, it: iterator}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	// do nothing
	scan.it.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.it.Valid() {
		return nil, nil, nil
	}
	item := scan.it.Item()
	keyWithTs := item.Key()
	userKey := DecodeUserKey(keyWithTs)
	value, err := scan.txn.GetValue(userKey)
	if err != nil {
		return nil, nil, err
	}

	for {
		scan.it.Next()
		if !scan.it.Valid() {
			break
		}
		nextItem := scan.it.Item()
		if bytes.Compare(DecodeUserKey(nextItem.Key()), userKey) != 0 {
			break
		}
	}
	if value == nil {
		return scan.Next()
	}
	return userKey, value, nil
}
