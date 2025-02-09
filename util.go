// Copyright (c) HashiCorp, Inc.
// Copyright (c) 2025 Phuoc Phi
// SPDX-License-Identifier: MPL-2.0
package hraft

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"math/rand"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
)

type ProtectedChan struct {
	closed bool
	ch     chan struct{}
	l      sync.Mutex
}

func (p *ProtectedChan) Close() {
	p.l.Lock()
	defer p.l.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.ch)
}

func (p *ProtectedChan) IsClosed() bool {
	p.l.Lock()
	defer p.l.Unlock()
	return p.closed
}

func (p *ProtectedChan) Ch() chan struct{} {
	p.l.Lock()
	defer p.l.Unlock()
	return p.ch
}

func newProtectedChan() *ProtectedChan {
	return &ProtectedChan{
		ch: make(chan struct{}),
	}
}

type ResetableProtectedChan struct {
	*ProtectedChan
}

func (r *ResetableProtectedChan) Reset() {
	r.l.Lock()
	defer r.l.Unlock()
	r.closed = false
	r.ch = make(chan struct{})
}

func newResetableProtectedChan() *ResetableProtectedChan {
	return &ResetableProtectedChan{newProtectedChan()}
}

type ProtectedWaitGroup struct {
	l       sync.Mutex
	wg      sync.WaitGroup
	blocked bool
}

func (w *ProtectedWaitGroup) Add(n int) bool {
	w.l.Lock()
	defer w.l.Unlock()
	if w.blocked {
		return false
	}
	w.wg.Add(n)
	return true
}

func (w *ProtectedWaitGroup) Done() {
	w.wg.Done()
}

func (w *ProtectedWaitGroup) Wait() {
	w.l.Lock()
	w.blocked = true
	w.l.Unlock()
	w.wg.Wait()
}

type ContactTime struct {
	l    sync.Mutex
	time time.Time
}

func newContactTime() *ContactTime {
	return &ContactTime{
		time: time.Now(),
	}
}

func (t *ContactTime) setNow() {
	t.l.Lock()
	defer t.l.Unlock()
	t.time = time.Now()
}

func (t *ContactTime) get() time.Time {
	t.l.Lock()
	defer t.l.Unlock()
	return t.time
}

func decode(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encode(in interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf.Bytes(), err
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

func tryNotify(ch chan struct{}) {
	trySend(ch, struct{}{})
}

func jitterTimeoutCh(interval time.Duration) <-chan time.Time {
	if interval == 0 {
		return nil
	}
	j := time.Duration(rand.Int63()) % interval
	return time.After(interval + j)
}

func getTimeoutCh(timeout time.Duration) <-chan time.Time {
	if timeout == 0 {
		return nil
	}
	return time.After(timeout)
}

type backoff struct {
	l     sync.Mutex
	value time.Duration
	base  time.Duration
	max   time.Duration
}

func newBackoff(base, max time.Duration) *backoff {
	return &backoff{
		base: base,
		max:  max,
	}
}

func (b *backoff) next() {
	b.l.Lock()
	defer b.l.Unlock()
	if b.value == 0 {
		b.value = b.base
		return
	}
	v := b.value * 2
	if v > b.max {
		v = b.max
	}
	b.value = v
}

func (b *backoff) getValue() time.Duration {
	b.l.Lock()
	defer b.l.Unlock()
	return b.value
}

func (b *backoff) reset() {
	b.l.Lock()
	defer b.l.Unlock()
	b.value = 0
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func trySend[T any](ch chan T, s T) {
	select {
	case ch <- s:
	default:
	}
}

func tryGetNotify(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// prevent calling wg.Add after wg.Wait

func logFinishTransition(logger hclog.Logger, trans *Transition, currentState RaftStateType, currentTerm uint64) {
	logger.Info(
		"finish transition",
		"transition", trans.String(),
		"current_state", currentState.String(),
		"current_term", currentTerm,
	)
}

func retry(n int, f func() (bool, string)) (success bool, msg string) {
	for i := 0; i < n; i++ {
		success, msg = f()
		if success {
			return
		}
	}
	return
}

func copyBytes(p []byte) []byte {
	q := make([]byte, len(p))
	copy(q, p)
	return q
}
