package storage_test

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"

	// 替换为你的实际包导入路径
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	// 其他需要的导入...
)

func TestBadger_Write_And_Read(t *testing.T) {
	dir, err := ioutil.TempDir("", "engine_util")
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	key := []byte("k1")
	val := []byte("v1")
	err = txn.Set(key, val)
	require.Nil(t, err)
	err = txn.Commit()
	require.Nil(t, err)

	txn1 := db.NewTransaction(false)
	item, err := txn1.Get(key)
	require.Nil(t, err)
	getValue, err := item.Value()
	require.Nil(t, err)
	require.Equal(t, val, getValue)

}

func TestBadgerStorage_Read(t *testing.T) {
	dbPath := "badger-storage"
	dir, _ := ioutil.TempDir("", dbPath)
	conf := &config.Config{
		DBPath: dir, // 替换为你的临时目录
		// 其他配置...
	}

	storageInstance := storage.NewBadgerStorage(conf)
	defer func() {
		err := storageInstance.Stop()
		if err != nil {
			t.Fatalf("Error stopping BadgerStorage: %v", err)
		}
	}()

}

func TestBadgerStorage_WriteAndRead(t *testing.T) {
	// 在这里创建一个临时目录，用于存储测试数据
	// 注意：如果你使用了临时目录，请确保在测试完成后删除它
	dbPath := "badger-storage"
	dir, _ := ioutil.TempDir("", dbPath)
	conf := &config.Config{
		DBPath: dir, // 替换为你的临时目录
		// 其他配置...
	}

	storageInstance := storage.NewBadgerStorage(conf)
	defer func() {
		err := storageInstance.Stop()
		if err != nil {
			t.Fatalf("Error stopping BadgerStorage: %v", err)
		}
	}()

	err := storageInstance.Start()
	if err != nil {
		t.Fatalf("Error starting BadgerStorage: %v", err)
	}

	// 写入测试数据
	key := []byte("k1")
	value := []byte("v1")
	testBatch := []storage.Modify{
		// 创建你的测试数据..
		{storage.Put{Cf: "test", Key: key, Value: value}},
	}

	err = storageInstance.Write(&kvrpcpb.Context{}, testBatch)
	if err != nil {
		t.Fatalf("Error writing to BadgerStorage: %v", err)
	}

	// 读取测试数据
	reader, err := storageInstance.Reader(&kvrpcpb.Context{})
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}
	val, err := reader.GetCF("test", []byte("k1"))
	require.Equal(t, value, val)
	if err != nil {
		return
	}
	defer reader.Close()

	// 创建你的读取测试逻辑...
}

// 可以添加更多的测试用例和辅助函数
