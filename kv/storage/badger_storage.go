package storage

import (
	"bytes"
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// BadgerStorage 是基于 BadgerDB 实现的一个列式存储引擎
type BadgerStorage struct {
	badgerInstance *badger.DB
	config         *config.Config
}

func NewBadgerStorage(conf *config.Config) *BadgerStorage {
	return &BadgerStorage{
		config: conf,
	}
}

func (s *BadgerStorage) Start() error {
	dir := s.config.DBPath
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	// 打开BadgerDB数据库
	db, err := badger.Open(opts)
	if err != nil {
		return err
	} else {
		s.badgerInstance = db
		return nil
	}
}

func (s *BadgerStorage) Stop() error {
	return s.badgerInstance.Close()

}

func (s *badgerReader) Close() {
	// TODO: fill this code (1)
}

func (s *BadgerStorage) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	db := s.badgerInstance
	txn := db.NewTransaction(true)
	// Use the transaction...
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			cfKey := toCFKey(data.Cf, data.Key)
			err := txn.Set(cfKey, data.Value)
			if err != nil {
				return err
			}
		case Delete:
			cfKey := toCFKey(data.Cf, data.Key)
			err := txn.Delete(cfKey)
			if err != nil {
				return err
			}
		}
	}
	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	} else {
		return nil
	}
}

func (s *BadgerStorage) Reader(ctx *kvrpcpb.Context) (StorageReader, error) {
	return &badgerReader{s}, nil
}

type badgerReader struct {
	inner *BadgerStorage
}

func toCFKey(cf string, key []byte) []byte {
	return []byte(cf + "_" + string(key))
}

type badgerIterator struct {
	it  *badger.Iterator
	cf  []byte
	txn *badger.Txn
}

func (br *badgerReader) IterCF(cf string) engine_util.DBIterator {
	db := br.inner.badgerInstance
	res := badgerIterator{}
	txn := db.NewTransaction(false)
	res.it = txn.NewIterator(badger.DefaultIteratorOptions)
	res.cf = []byte(cf)
	res.Seek(res.cf)
	res.txn = txn
	return &res
}

// GetCF 从指定的 Column Family 中读取指定 Key 的值
func (br *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 使用配置和 CF 信息打开 Badger 数据库
	db := br.inner.badgerInstance

	cfKey := toCFKey(cf, key)
	var result []byte

	// Start a writable transaction.
	txn := db.NewTransaction(false)
	// 读 Key Value
	item, err := txn.Get(cfKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	// 读 Value
	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	result = append(result, val...)
	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

// extractCFAndKey 从带有 CF 前缀的键中提取原始的 Column Family 和 Key
func extractCFAndKey(cfKey []byte) (string, []byte, error) {
	parts := bytes.SplitN(cfKey, []byte("_"), 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid CF key format: %s", cfKey)
	}

	cf := string(parts[0])
	key := parts[1]

	return cf, key, nil
}

// Item returns pointer to the current key-value pair.
func (it *badgerIterator) Item() engine_util.DBItem {
	keyWithCF := it.it.Item().Key()
	_, key, _ := extractCFAndKey(keyWithCF)
	val, _ := it.it.Item().Value()
	return badgerItem{key, val, true}
}

// // Valid returns false when iteration is done.
func (it *badgerIterator) Valid() bool {
	return it.it.Valid()
}

// // Next would advance the iterator by one. Always check it.Valid() after a Next()
// // to ensure you have access to a valid it.Item().
func (it *badgerIterator) Next() {
	it.it.Next()
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided.
func (it *badgerIterator) Seek([]byte) {
	it.it.Seek(it.cf)
}

// Close the iterator
func (it *badgerIterator) Close() {
	it.it.Close()
	it.txn.Commit()

}

type badgerItem struct {
	key   []byte
	value []byte
	fresh bool
}

func (it badgerItem) Key() []byte {
	return it.key
}
func (it badgerItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it badgerItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it badgerItem) ValueSize() int {
	return len(it.value)
}
func (it badgerItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}

func (it badgerItem) Less(than llrb.Item) bool {
	other := than.(badgerItem)
	return bytes.Compare(it.key, other.key) < 0
}
