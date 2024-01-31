package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
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
	// Assuming RawGetResponse has a Value field for the result
	return &kvrpcpb.RawGetResponse{
		RegionError: nil, // Assuming RegionError is a field in RawGetResponse
		Error:       "",  // No error, set to empty string
		Value:       val,
	}, nil
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
	reader.IterCF(req.Cf)
	return nil, nil
}
