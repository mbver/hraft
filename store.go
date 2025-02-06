package hraft

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrLogNotFound = errors.New("log not found")
)

type LogStore interface {
	FirstIdx() (uint64, error)
	LastIdx() (uint64, error)
	GetLog(uint64, *Log) error
	StoreLog(*Log) error
	StoreLogs([]*Log) error
	DeleteRange(min, max uint64) error
}

type KVStore interface {
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	SetUint64(k []byte, val uint64) error
	GetUint64(k []byte) (uint64, error)
}
