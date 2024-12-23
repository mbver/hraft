package hraft

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/hashicorp/go-msgpack/codec"
)

type shutdown struct {
	done bool
	ch   chan struct{}
	l    sync.Mutex
}

func (s *shutdown) Shutdown() {
	s.l.Lock()
	defer s.l.Unlock()
	if s.done {
		return
	}
	s.done = true
	close(s.ch)
}

func (s *shutdown) Done() bool {
	s.l.Lock()
	defer s.l.Unlock()
	return s.done
}

func (s *shutdown) Ch() chan struct{} {
	return s.ch
}

func newShutdown() *shutdown {
	return &shutdown{
		ch: make(chan struct{}),
	}
}

func decode(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encode(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// Converts bytes to an integer
func toUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func toBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
