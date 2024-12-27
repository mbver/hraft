package hraft

import (
	"sync"
	"time"
)

type heartbeatTimeout struct {
	timeout time.Duration
	l       sync.Mutex
	ch      <-chan time.Time
	fresh   bool
}

func newHeartbeatTimeout(timeout time.Duration) *heartbeatTimeout {
	return &heartbeatTimeout{
		timeout: timeout,
		ch:      jitterTimeoutCh(timeout),
		fresh:   true,
	}
}

func (h *heartbeatTimeout) reset() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = jitterTimeoutCh(h.timeout)
	h.fresh = true
}

func (h *heartbeatTimeout) block() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = nil
	h.fresh = true
}

func (h *heartbeatTimeout) getCh() <-chan time.Time {
	h.l.Lock()
	defer h.l.Unlock()
	h.fresh = false
	return h.ch
}

func (h *heartbeatTimeout) isFresh() bool {
	h.l.Lock()
	defer h.l.Unlock()
	return h.fresh
}
