package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(),
		}, err
	}
	val, err := reader.GetCF(string(req.Cf), req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(),
		}, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       "",  // No error, set to empty string
			Value:       val,
			NotFound:    true,
		}, nil
	} else {
		return &kvrpcpb.RawGetResponse{

			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       "",  // No error, set to empty string
			Value:       val,
		}, nil
	}

}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	batch := []storage.Modify{{put}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(),
		}, err
	} else {
		return &kvrpcpb.RawPutResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       "",  // No error, set to empty string
		}, nil
	}
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Your Code Here (1).
	put := storage.Put{Key: req.Key, Cf: req.Cf}
	batch := []storage.Modify{{put}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(),
		}, err
	} else {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: nil, // Assuming RegionError is a field in RawGetResponse
			Error:       "",  // No error, set to empty string
		}, nil
	}
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			RegionError: nil,         // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(), // No error, set to empty string
		}, err
	}
	kvs, err := scanIterator(req, reader)

	if err != nil {
		return &kvrpcpb.RawScanResponse{
			RegionError: nil,         // Assuming RegionError is a field in RawGetResponse
			Error:       err.Error(), // No error, set to empty string
		}, err

	}
	reader.Close()

	return &kvrpcpb.RawScanResponse{
		RegionError: nil, // Assuming RegionError is a field in RawGetResponse
		Kvs:         kvs,
	}, err

}

func scanIterator(req *kvrpcpb.RawScanRequest, reader storage.StorageReader) (kvs []*kvrpcpb.KvPair, err error) {
	var res []*kvrpcpb.KvPair
	idx := 0
	log.Infof("request: %+v", req)
	it := reader.IterCF(req.Cf)
	for ; it.Valid() && uint32(idx) < req.Limit; it.Next() {
		item := it.Item()
		key := item.Key()
		//if bytes.Compare(key, req.StartKey) < 0 {
		//	continue
		//}
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		if value != nil && len(value) > 0 {
			res = append(res, &kvrpcpb.KvPair{Key: key, Value: value, Error: nil})
			log.Infof("item %d: %+v", idx, res[idx])
			idx++
		}

	}
	it.Close()
	return res, nil
}
