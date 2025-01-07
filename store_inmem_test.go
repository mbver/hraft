package hraft

import (
	"sync"
	"testing"
)

type inMemLogStore struct {
	l        sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	logs     map[uint64]*Log
}

func newInMemLogStore() *inMemLogStore {
	return &inMemLogStore{
		logs: map[uint64]*Log{},
	}
}

func (s *inMemLogStore) FirstIdx() (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	return s.firstIdx, nil
}

func (s *inMemLogStore) LastIdx() (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	return s.lastIdx, nil
}

func (s *inMemLogStore) GetLog(idx uint64, log *Log) error {
	s.l.Lock()
	defer s.l.Unlock()
	l, ok := s.logs[idx]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

func (s *inMemLogStore) StoreLog(log *Log) error {
	return s.StoreLogs([]*Log{log})
}

func (s *inMemLogStore) StoreLogs(logs []*Log) error {
	s.l.Lock()
	defer s.l.Unlock()
	for _, log := range logs {
		s.logs[log.Idx] = log
		if s.firstIdx == 0 {
			s.firstIdx = log.Idx
		}
		if s.firstIdx > log.Idx {
			s.firstIdx = log.Idx
		}
		if s.lastIdx < log.Idx {
			s.lastIdx = log.Idx
		}
	}
	return nil
}

func (s *inMemLogStore) DeleteRange(min, max uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	for idx := min; idx <= max; idx++ {
		delete(s.logs, idx)
	}
	if min <= s.firstIdx {
		s.firstIdx = max + 1
	}
	if max >= s.lastIdx {
		s.lastIdx = min - 1
	}
	if s.firstIdx > s.lastIdx {
		s.firstIdx = 0
		s.lastIdx = 0
	}
	return nil
}

func (s *inMemLogStore) Sync() error {
	return nil
}

func (s *inMemLogStore) Close() error {
	return nil
}

type inMemKVStore struct {
	l         sync.Mutex
	kvs       map[string][]byte
	kvsUint64 map[string]uint64
}

func newInMemKVStore() *inMemKVStore {
	return &inMemKVStore{
		kvs:       map[string][]byte{},
		kvsUint64: map[string]uint64{},
	}
}

func (s *inMemKVStore) Set(k, v []byte) error {
	s.l.Lock()
	defer s.l.Unlock()
	s.kvs[string(k)] = v
	return nil
}

func (s *inMemKVStore) Get(k []byte) ([]byte, error) {
	s.l.Lock()
	defer s.l.Unlock()
	v, ok := s.kvs[string(k)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (s *inMemKVStore) SetUint64(k []byte, val uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	s.kvsUint64[string(k)] = val
	return nil
}

func (s *inMemKVStore) GetUint64(k []byte) (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	v, ok := s.kvsUint64[string(k)]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return v, nil
}

func (s *inMemKVStore) Sync() error {
	return nil
}

func (s *inMemKVStore) Close() error {
	return nil
}

func TestInMemStore_Implement(t *testing.T) {
	var _ LogStore = (*inMemLogStore)(nil)
	var _ KVStore = (*inMemKVStore)(nil)
}
