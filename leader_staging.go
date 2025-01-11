package hraft

import (
	"sync"
	"time"
)

// all channel buffer is 1
// allow adding only 1 peer a time
type staging struct {
	l          sync.Mutex
	active     bool
	id         string
	stageCh    chan string
	logSyncCh  chan string
	promotedCh chan string
}

func newStaging() *staging {
	return &staging{
		stageCh:    make(chan string, 1),
		logSyncCh:  make(chan string, 1),
		promotedCh: make(chan string, 1),
	}
}

func (s *staging) isActive() bool {
	s.l.Lock()
	defer s.l.Unlock()
	return s.active
}

func (s *staging) getId() string {
	s.l.Lock()
	defer s.l.Unlock()
	return s.id
}

func (s *staging) stage(id string) {
	s.stageCh <- id // TODO: have a timeout?
}

// the nested loop to ensure
// only one staging is handled at a time.
// goro is blocked until peer is promoted
func (l *Leader) receiveStaging() {
	for {
		var id string
		select {
		case id = <-l.staging.stageCh:
			l.staging.l.Lock()
			l.staging.id = id
			l.staging.active = true
			l.staging.l.Unlock()
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
	case id := <-l.staging.logSyncCh:
		l.staging.l.Lock()
		l.staging.active = false
		l.staging.l.Unlock()
		for {
			m := newMembershipChange(id, promotePeer)
			l.raft.membershipChangeCh <- m
			err := <-m.errCh
			if err != nil { // keep promote peer until successful or stopped
				l.raft.logger.Warn("promote peer from staging: membership is unstable, retry", "peer", id)
				time.Sleep(200 * time.Millisecond) // TODO: ADD TO CONFIG?
				continue
			}
			return
		}
	case <-l.stepdown.Ch():
	case <-l.raft.shutdownCh():
	}
}
