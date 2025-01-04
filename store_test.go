package hraft

import (
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
	require.NotNil(t, err)
	require.Equal(t, bolt.ErrBucketExists, err)

	_, err = tx.CreateBucket(kvBucket)
	require.NotNil(t, err)
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
	bucket := []byte("test-opts-read-only")
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
	require.NotNil(t, err)
	require.Equal(t, bolt.ErrDatabaseReadOnly, err)
}
