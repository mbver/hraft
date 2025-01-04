package hraft

import (
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/require"
)

func TestStore_CreateBoltStore(t *testing.T) {
	store, cleanup, err := newTestStoreWithOpts(nil)
	defer cleanup()
	require.Nil(t, err)
	logBucket := []byte("log")
	kvBucket := []byte("kv")

	err = store.setupBuckets([][]byte{logBucket, kvBucket})
	require.Nil(t, err)

	_, err = os.Stat(store.path)
	require.Nil(t, err)

	err = store.Close()
	require.Nil(t, err)

	db, err := bolt.Open(store.path, dbFileMode, nil)
	require.Nil(t, err)

	tx, err := db.Begin(true)
	require.Nil(t, err)

	_, err = tx.CreateBucket(logBucket)
	require.Equal(t, bolt.ErrBucketExists, err)

	_, err = tx.CreateBucket(kvBucket)
	require.Equal(t, bolt.ErrBucketExists, err)
}

func TestStore_BoltOptions_Timeout(t *testing.T) {
	opts := &bolt.Options{
		Timeout: 100 * time.Millisecond,
	}
	store, cleanup, err := newTestStoreWithOpts(opts)
	defer cleanup()
	require.Nil(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := NewBoltStore(store.path, opts, nil)
		errCh <- err
	}()
	select {
	case err = <-errCh:
		require.NotNil(t, err)
		require.Equal(t, "timeout", err.Error())
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expect no timeout")
	}
}

func TestStore_BoltOptions_ReadOnly(t *testing.T) {
	bolt1, cleanup1, err := newTestStoreWithOpts(nil)
	defer cleanup1()
	require.Nil(t, err)
	bucket := []byte("test_opts_read_only")
	logStore, err := NewLogStore(bolt1, bucket)
	require.Nil(t, err)
	defer logStore.Close()
	log := &Log{
		Idx:  1,
		Data: []byte("log1"),
	}

	err = logStore.StoreLog(log)
	require.Nil(t, err)
	logStore.Close()

	opts := &bolt.Options{
		Timeout:  100 * time.Millisecond,
		ReadOnly: true,
	}

	readOnlyBolt, err := NewBoltStore(bolt1.path, opts, nil)
	require.Nil(t, err)
	defer readOnlyBolt.Close()

	readOnlyStore, err := NewLogStore(readOnlyBolt, bucket)
	require.Nil(t, err)
	var gotLog Log
	err = readOnlyStore.GetLog(1, &gotLog)
	require.Nil(t, err)
	require.True(t, reflect.DeepEqual(log, &gotLog))

	err = readOnlyStore.StoreLog(log)
	require.Equal(t, bolt.ErrDatabaseReadOnly, err)
}

func TestLogStore_FirstIdx(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_first_idx"))
	require.Nil(t, err)

	idx, err := store.FirstIdx()
	require.Nil(t, err)
	require.Zero(t, idx)

	logs := []*Log{
		createTestLog(1, "log1"),
		createTestLog(2, "log2"),
		createTestLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	require.Nil(t, err)

	idx, err = store.FirstIdx()
	require.Nil(t, err)
	require.Equal(t, uint64(1), idx)
}

func TestLogStore_LastIdx(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_last_idx"))
	require.Nil(t, err)

	idx, err := store.LastIdx()
	require.Nil(t, err)
	require.Zero(t, idx)

	logs := []*Log{
		createTestLog(1, "log1"),
		createTestLog(2, "log2"),
		createTestLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	require.Nil(t, err)

	idx, err = store.LastIdx()
	require.Nil(t, err)
	require.Equal(t, uint64(3), idx)
}

func TestLogStore_GetLog(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_get_log"))
	require.Nil(t, err)

	var log Log

	err = store.GetLog(1, &log)
	require.Equal(t, ErrLogNotFound, err)

	logs := []*Log{
		createTestLog(1, "log1"),
		createTestLog(2, "log2"),
		createTestLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	require.Nil(t, err)

	err = store.GetLog(2, &log)
	require.Nil(t, err)
	require.True(t, reflect.DeepEqual(&log, logs[1]))
}

func TestLogStore_StoreLog(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_store_log"))
	require.Nil(t, err)

	log := createTestLog(1, "log1")
	err = store.StoreLog(log)
	require.Nil(t, err)

	var gotLog Log
	err = store.GetLog(1, &gotLog)
	require.Nil(t, err)
	require.True(t, reflect.DeepEqual(log, &gotLog))
}

func TestLogStore_SetLogs(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_store_logs"))
	require.Nil(t, err)

	logs := []*Log{
		createTestLog(1, "log1"),
		createTestLog(2, "log2"),
	}

	err = store.StoreLogs(logs)
	require.Nil(t, err)

	var gotLog1, gotLog2 Log
	err = store.GetLog(1, &gotLog1)
	require.Nil(t, err)
	require.True(t, reflect.DeepEqual(&gotLog1, logs[0]))

	err = store.GetLog(2, &gotLog2)
	require.Nil(t, err)
	require.True(t, reflect.DeepEqual(&gotLog2, logs[1]))
}

func TestLogStore_DeleteRange(t *testing.T) {
	store, err := NewLogStore(testBoltStore, []byte("test_log_store_delete_range"))
	require.Nil(t, err)

	logs := []*Log{
		createTestLog(1, "log1"),
		createTestLog(2, "log2"),
		createTestLog(3, "log3"),
	}

	err = store.StoreLogs(logs)
	require.Nil(t, err)

	err = store.DeleteRange(1, 2)
	require.Nil(t, err)
	var gotLog Log
	err = store.GetLog(1, &gotLog)
	require.Equal(t, ErrLogNotFound, err)

	err = store.GetLog(2, &gotLog)
	require.Equal(t, ErrLogNotFound, err)
}

func TestKVStore_Set_Get(t *testing.T) {
	store, err := NewKVStore(testBoltStore, []byte("test_kv_store_set_get"))
	require.Nil(t, err)

	k, v := []byte("hello"), []byte("world")

	_, err = store.Get([]byte(k))
	require.Equal(t, ErrKeyNotFound, err)

	err = store.Set(k, v)
	require.Nil(t, err)

	val, err := store.Get(k)
	require.Nil(t, err)
	require.True(t, bytes.Equal(v, val))
}

func TestKVStore_SetUint64_GetUint64(t *testing.T) {
	store, err := NewKVStore(testBoltStore, []byte("test_kv_store_setuint64_getuint64"))
	require.Nil(t, err)

	k, v := []byte("abc"), uint64(123)

	_, err = store.GetUint64([]byte(k))
	require.Equal(t, ErrKeyNotFound, err)

	err = store.SetUint64(k, v)
	require.Nil(t, err)

	val, err := store.GetUint64(k)
	require.Nil(t, err)
	require.Equal(t, v, val)
}
