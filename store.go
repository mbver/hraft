package hraft

import (
	"errors"

	"github.com/boltdb/bolt"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("key not found")
	ErrLogNotFound = errors.New("log not found")
)

type BoltStore struct {
	// conn is the underlying handle to the db.
	db *bolt.DB

	// The path to the Bolt database file
	path string
}

func isReadOnly(opts *bolt.Options) bool {
	return opts != nil && opts.ReadOnly
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string, opts *bolt.Options, buckets [][]byte) (*BoltStore, error) {
	db, err := bolt.Open(path, dbFileMode, opts)
	if err != nil {
		return nil, err
	}

	// Create the new store
	store := &BoltStore{
		db:   db,
		path: path,
	}

	if !isReadOnly(opts) {
		if err := store.setupBuckets(buckets); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

func (b *BoltStore) setupBuckets(buckets [][]byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, bucket := range buckets {
		if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *BoltStore) Close() error {
	return b.db.Close()
}

func (b *BoltStore) Sync() error {
	return b.db.Sync()
}

type LogStore struct {
	bucket []byte
	bolt   *BoltStore
}

func NewLogStore(bolt *BoltStore, bucket []byte) (*LogStore, error) {
	if !bolt.db.IsReadOnly() {
		err := bolt.setupBuckets([][]byte{bucket})
		if err != nil {
			return nil, err
		}
	}
	return &LogStore{
		bucket: bucket,
		bolt:   bolt,
	}, nil
}

func (s *LogStore) FirstIdx() (uint64, error) {
	tx, err := s.bolt.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(s.bucket).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return toUint64(first), nil
	}
}

func (s *LogStore) LastIdx() (uint64, error) {
	tx, err := s.bolt.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(s.bucket).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return toUint64(last), nil
	}
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (s *LogStore) GetLog(idx uint64, log *Log) error {
	tx, err := s.bolt.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.bucket)
	val := bucket.Get(toBytes(idx))

	if val == nil {
		return ErrLogNotFound
	}
	return decode(val, log)
}

// StoreLog is used to store a single raft log
func (s *LogStore) StoreLog(log *Log) error {
	return s.StoreLogs([]*Log{log})
}

// StoreLogs is used to store a set of raft logs
func (s *LogStore) StoreLogs(logs []*Log) error {
	tx, err := s.bolt.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := toBytes(log.Idx)
		val, err := encode(log)
		if err != nil {
			return err
		}

		bucket := tx.Bucket(s.bucket)
		if err := bucket.Put(key, val); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *LogStore) DeleteRange(min, max uint64) error {
	minKey := toBytes(min)

	tx, err := s.bolt.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(s.bucket).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if toUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *LogStore) Sync() error {
	return s.bolt.Sync()
}

func (s *LogStore) Close() error {
	return s.bolt.Close()
}

type KVStore struct {
	bucket []byte
	bolt   *BoltStore
}

func (kv *KVStore) Set(k, v []byte) error {
	tx, err := kv.bolt.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(kv.bucket)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

func (kv *KVStore) Get(k []byte) ([]byte, error) {
	tx, err := kv.bolt.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(kv.bucket)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

func (kv *KVStore) SetUint64(key []byte, val uint64) error {
	return kv.Set(key, toBytes(val))
}

func (kv *KVStore) GetUint64(key []byte) (uint64, error) {
	val, err := kv.Get(key)
	if err != nil {
		return 0, err
	}
	return toUint64(val), nil
}

func (kv *KVStore) Sync() error {
	return kv.bolt.Sync()
}

func (kv *KVStore) Close() error {
	return kv.bolt.Close()
}
