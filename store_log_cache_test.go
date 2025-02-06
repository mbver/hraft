package hraft

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogCache(t *testing.T) {
	store := newInMemLogStore()
	c, _ := NewLogCache(16, store)

	for i := 0; i < 32; i++ {
		log := &Log{Idx: uint64(i) + 1}
		store.StoreLog(log)
	}
	idx, _ := c.FirstIdx()
	require.Equal(t, uint64(1), idx)

	idx, _ = c.LastIdx()
	require.Equal(t, uint64(32), idx)

	// Try get log with a miss
	var log Log
	err := c.GetLog(1, &log)
	require.Nil(t, err)
	require.Equal(t, uint64(1), log.Idx)

	// Store logs
	logs := []*Log{{Idx: 33}, {Idx: 34}}
	err = c.StoreLogs(logs)
	require.Nil(t, err)

	idx, _ = c.LastIdx()
	require.Equal(t, uint64(34), idx)

	err = store.GetLog(33, &log)
	require.Nil(t, err)
	err = store.GetLog(34, &log)
	require.Nil(t, err)

	// delete logs in underlying store
	err = store.DeleteRange(33, 34)
	require.Nil(t, err)

	// should be in the ring buffer
	err = c.GetLog(33, &log)
	require.Nil(t, err)
	err = c.GetLog(34, &log)
	require.Nil(t, err)

	// purge the ring buffer
	err = c.DeleteRange(33, 34)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// should not be in the ring buffer
	err = c.GetLog(33, &log)
	require.Equal(t, ErrLogNotFound, err)
	err = c.GetLog(34, &log)
	require.Equal(t, ErrLogNotFound, err)
}

type faultyStore struct {
	LogStore
	maxFailures  int
	failureCount int
}

func (f *faultyStore) setMaxFailures(n int) {
	f.maxFailures = n
}

func (f *faultyStore) StoreLogs(logs []*Log) error {
	if f.maxFailures > 0 {
		f.failureCount++
		if f.failureCount <= f.maxFailures {
			return errors.New("faulty store error")
		}
		// clear maxFailures. No failure until it is set again
		f.maxFailures = 0
	}
	return f.LogStore.StoreLogs(logs)
}

func TestLogCacheWithBackendStoreError(t *testing.T) {
	var err error
	store := newInMemLogStore()
	faulty := &faultyStore{LogStore: store}
	c, _ := NewLogCache(16, faulty)

	for i := 0; i < 4; i++ {
		log := &Log{Idx: uint64(i) + 1}
		store.StoreLog(log)
	}
	faulty.setMaxFailures(1)
	log := &Log{Idx: 5}
	err = c.StoreLog(log)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "faulty store error")

	log = &Log{}
	for i := 1; i < 5; i++ {
		err = c.GetLog(uint64(i), log)
		require.Nil(t, err)
	}
	err = c.GetLog(5, log)
	require.Equal(t, ErrLogNotFound, err)
}
