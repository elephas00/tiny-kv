package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *storage.BadgerStorage
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// 创建一个 StandAloneStorage 对象 res
	res := &StandAloneStorage{
		engine: storage.NewBadgerStorage(conf),
	}
	return res
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	err := s.engine.Start()
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engine.Stop()
	if err != nil {
		return err
	} else {
		return nil
	}

}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader, err := s.engine.Reader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.engine.Write(ctx, batch)
}
