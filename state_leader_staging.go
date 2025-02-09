package hraft

import (
	"sync"
	"time"
)

// all channel buffer is 1
// allow adding only 1 peer a time
type staging struct {
	l          sync.Mutex
	id         string
	stageCh    chan struct{}
	logSyncCh  chan struct{}
	promotedCh chan struct{}
}

func newStaging() *staging {
	return &staging{
		stageCh:    make(chan struct{}, 1),
		logSyncCh:  make(chan struct{}, 1),
		promotedCh: make(chan struct{}, 1),
	}
}

func (s *staging) getId() string {
	s.l.Lock()
	defer s.l.Unlock()
	return s.id
}

func (s *staging) stage(id string, shutdownCh, stepdownCh chan struct{}) {
	s.l.Lock()
	s.id = id
	s.l.Unlock()
	select {
	case s.stageCh <- struct{}{}:
	case <-stepdownCh:
	case <-shutdownCh:
	}
}

// the nested loop to ensure
// only one staging is handled at a time.
// goro is blocked until peer is promoted
func (l *Leader) receiveStaging() {
	if !l.raft.wg.Add(1) {
		return
	}
	defer l.raft.wg.Done()
	for {
		select {
		case <-l.staging.stageCh:
			l.receiveLogSynced()
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}

func (l *Leader) receiveLogSynced() {
	select {
	case <-l.staging.logSyncCh:
		for {
			if l.stepdown.IsClosed() || l.raft.shutdown.IsClosed() {
				return
			}
			m := newMembershipChangeRequest(l.staging.getId(), promotePeer)
			l.raft.membershipChangeCh <- m
			err := <-m.errCh
			if err == ErrMembershipUnstable { // keep promote peer until successful or stopped
				l.raft.logger.Warn("promote peer from staging: membership is unstable, retry", "peer", m.addr)
				time.Sleep(200 * time.Millisecond) // TODO: ADD TO CONFIG?
				continue
			}
			// ignore other err??
			return
		}
	case <-l.stepdown.Ch():
	case <-l.raft.shutdownCh():
	}
}
