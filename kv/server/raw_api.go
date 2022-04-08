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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawGetResponse)
	resp.Value, err = reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
	}
	resp.NotFound = resp.Value == nil
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawScanResponse)

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	for i := uint64(0); iter.Valid() && i < uint64(req.Limit); i++ {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err == nil {
			resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
		}
		iter.Next()
	}
	return resp, err
}
