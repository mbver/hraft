package hraft

import (
	"sort"
	"sync"
)

type Commit struct{}
type commitControl struct {
	l              sync.Mutex
	commitNotifyCh chan struct{}
	matchIdxs      map[string]uint64
	commitIdx      uint64
	startIdx       uint64
}

func (c *commitControl) getCommitIdx() uint64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.commitIdx
}

func (c *commitControl) updateMatchIdx(id string, idx uint64) {
	c.l.Lock()
	defer c.l.Unlock()
	prev := c.matchIdxs[id]
	if idx <= prev {
		return
	}
	c.matchIdxs[id] = idx
	c.updateCommitIdx()
}

func (c *commitControl) updateCommitIdx() {
	if len(c.matchIdxs) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIdxs))
	for _, idx := range c.matchIdxs {
		matched = append(matched, idx)
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i] < matched[j]
	})

	commitIdx := matched[(len(matched)-1)/2]

	if commitIdx > c.commitIdx && commitIdx >= c.startIdx { // why care about startIdx?
		c.commitIdx = commitIdx
		tryNotify(c.commitNotifyCh)
	}
}

type Leader struct {
	l           sync.Mutex
	raft        *Raft
	active      bool
	commit      *commitControl
	stepdown    *ProtectedChan
	staleTermCh chan struct{}
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:     r,
		stepdown: newProtectedChan(),
	}
	l.stepdown.Close()
	return l
}

func (l *Leader) StepUp() {
	l.l.Lock()
	defer l.l.Unlock()
	if l.active {
		return
	}
	l.active = true // TODO: may not be needed?
	l.stepdown.Reset()
	l.staleTermCh = make(chan struct{}, 1)
	go l.receiveStaleTerm()
	// TODO: start replication
}

func (l *Leader) getStaleTermCh() chan struct{} {
	l.l.Lock()
	defer l.l.Unlock()
	return l.staleTermCh
}

func (l *Leader) receiveStaleTerm() {
	for {
		select {
		case <-l.getStaleTermCh():
			l.Stepdown()
			return
		case <-l.stepdown.Ch():
			return
		case <-l.raft.ShutdownCh():
			return
		}
	}
}

func (l *Leader) Stepdown() {
	l.l.Lock()
	defer l.l.Unlock()
	if !l.active {
		return
	}
	l.active = false
	l.stepdown.Close()
	l.staleTermCh = nil
	// TODO: transition to follower
}

func (l *Leader) HandleNewCommit() {
	commitIdx := l.commit.getCommitIdx()
	l.raft.setCommitIdx(commitIdx)
	l.processNewCommit(commitIdx)
}

func (l *Leader) processNewCommit(idx uint64) {}

type Apply struct{}

func (l *Leader) dispatchLogs(applies []*Apply)
