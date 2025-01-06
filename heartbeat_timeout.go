package hraft

import (
	"sync"
	"time"
)

type heartbeatTimeout struct {
	timeout       time.Duration
	l             sync.Mutex
	ch            <-chan time.Time
	resetNotifyCh chan struct{}
}

func newHeartbeatTimeout(timeout time.Duration) *heartbeatTimeout {
	return &heartbeatTimeout{
		timeout:       timeout,
		ch:            jitterTimeoutCh(timeout),
		resetNotifyCh: make(chan struct{}, 1),
	}
}

func (h *heartbeatTimeout) reset() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = jitterTimeoutCh(h.timeout)
	tryNotify(h.resetNotifyCh)
}

func (h *heartbeatTimeout) block() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = nil
}

func (h *heartbeatTimeout) getCh() <-chan time.Time {
	h.l.Lock()
	defer h.l.Unlock()
	return h.ch
}

func (h *heartbeatTimeout) getResetNotifyCh() chan struct{} {
	return h.resetNotifyCh
}
