package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(
	_ context.Context,
	req *kvrpcpb.RawGetRequest,
) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(req.Context)
	result, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &response, nil
	}
	if result == nil {
		response.NotFound = true
	}
	response.Value = result
	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(
	_ context.Context,
	req *kvrpcpb.RawPutRequest,
) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := kvrpcpb.RawPutResponse{}
	put := storage.Put{}
	put.Key = req.Key
	put.Cf = req.Cf
	put.Value = req.Value
	modify := storage.Modify{
		Data: put,
	}
	modifies := []storage.Modify{}
	modifies = append(modifies, modify)
	err := server.storage.Write(req.Context, modifies)
	return &response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(
	_ context.Context,
	req *kvrpcpb.RawDeleteRequest,
) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := kvrpcpb.RawDeleteResponse{}
	del := storage.Delete{}
	del.Key = req.Key
	del.Cf = req.Cf
	modify := storage.Modify{
		Data: del,
	}
	modifies := []storage.Modify{}
	modifies = append(modifies, modify)
	err := server.storage.Write(req.Context, modifies)
	return &response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(
	_ context.Context,
	req *kvrpcpb.RawScanRequest,
) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := kvrpcpb.RawScanResponse{}
	reader, _ := server.storage.Reader(req.Context)
	iter := reader.IterCF(req.Cf)
	nums := 0
	var kvs []*kvrpcpb.KvPair
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if uint32(nums) >= req.GetLimit() {
			break
		}
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return &response, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		nums++
	}
	iter.Close()
	response.Kvs = kvs
	return &response, nil
}
