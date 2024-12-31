package hraft

import "sync"

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
	s.l.Lock()
	defer s.l.Unlock()
	s.id = id
	s.active = true
	s.stageCh <- id // TODO: have a timeout?
}

func (s *staging) promote(id string) {
	s.l.Lock()
	defer s.l.Unlock()
	s.active = false
	s.promotedCh <- id
}

// the nested loop to ensure
// only one staging is handled at a time.
// goro is blocked until peer is promoted
func (l *Leader) receiveStaging() {
	for {
		var id string
		select {
		case id = <-l.staging.stageCh:
			l.receiveLogSynced(id)
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}

func (l *Leader) receiveLogSynced(stagingId string) {
	for {
		select {
		case id := <-l.staging.logSyncCh:
			if id != stagingId {
				panic("staging_id not match logsync_id")
			}
			l.raft.membershipChangeCh <- &membershipChange{id, promotePeer}
			l.receivePromoted(id)
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}

func (l *Leader) receivePromoted(syncId string) {
	for {
		select {
		case id := <-l.staging.promotedCh:
			if id != syncId {
				panic("promote_id not mach logsync_id")
			}
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}
