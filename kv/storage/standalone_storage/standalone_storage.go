package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	opts badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	sas := StandAloneStorage{}
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	sas.opts = opts
	return &sas
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db, _ = badger.Open(s.opts)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := StorageReader{}
	reader.txn = s.db.NewTransaction(false)
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			key := put.Key
			value := put.Value
			cf := put.Cf
			err := engine_util.PutCF(s.db, cf, key, value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := b.Data.(storage.Delete)
			key := del.Key
			cf := del.Cf
			err := engine_util.DeleteCF(s.db, cf, key)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

type StorageReader struct {
	txn *badger.Txn
}

func (s *StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StorageReader) Close() {
	s.txn.Discard()
}
