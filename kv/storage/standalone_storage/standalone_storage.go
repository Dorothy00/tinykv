package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	s.db.Update(func(txn *badger.Txn) error {
		for _, kv := range batch {
			switch kv.Data.(type){
			case storage.Put:
				return engine_util.PutCF(s.db, kv.Cf(), kv.Key(), kv.Value())
			case storage.Delete:
				return engine_util.DeleteCF(s.db, kv.Cf(), kv.Key())
			}
		}
		return nil
	})
	return nil
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCF(s.db, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneStorage)IterCF(cf string) engine_util.DBIterator {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	it := engine_util.NewCFIterator(cf, txn)
	defer it.Close()
	return it
}

func (s *StandAloneStorage)Close() {
	s.db.Close()
}
