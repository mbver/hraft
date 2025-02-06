package hraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minObserveTimeout = time.Millisecond
)

var nextObserverId uint64 // to provide a unique id for each observer

type RaftEvent interface {
	IsRaftEvent()
}

type LeaderChangeEvent struct {
	OldLeader string
	OldTerm   string
	NewLeader string
	NewTerm   string
}

func (l LeaderChangeEvent) IsRaftEvent() {}

type StateTransitionEvent struct {
	OldState RaftStateType
	OldTerm  uint64
	NewState RaftStateType
	NewTerm  uint64
}

func (t StateTransitionEvent) IsRaftEvent() {}

type RequestVoteEvent struct {
	Term        uint64
	Candidate   []byte
	LastLogIdx  uint64
	LastLogTerm uint64
}

func newRequestVoteEvent(req *VoteRequest) RequestVoteEvent {
	return RequestVoteEvent{
		Term:        req.Term,
		Candidate:   copyBytes(req.Candidate),
		LastLogIdx:  req.LastLogIdx,
		LastLogTerm: req.LastLogTerm,
	}
}

func (v RequestVoteEvent) IsRaftEvent() {}

type PeerReplicationEvent struct {
	Peer   string
	Active bool
}

func (p PeerReplicationEvent) IsRaftEvent() {}

type HearbeatFailureEvent struct {
	Peer        string
	LastContact time.Time
}

func (f HearbeatFailureEvent) IsRaftEvent() {}

type HeartbeatResumedEvent struct {
	Peer string
}

func (r HeartbeatResumedEvent) IsRaftEvent() {}

// e is one of LeaderChangeEvent|PeerReplicationEvent|HeartbeatFailureEvent|HeartbeatResumedEvent
type EventFilterFn func(e RaftEvent) bool

type Observer struct {
	nObserved uint64
	nDropped  uint64
	// to wait for event sent down the channel.
	// set it to enormous value can block raft!
	ch       chan RaftEvent
	timeout  time.Duration
	acceptFn EventFilterFn
	id       uint64
}

func NewObserver(ch chan RaftEvent, timeout time.Duration, acceptFn EventFilterFn) (*Observer, error) {
	if ch == nil {
		return nil, fmt.Errorf("nil channel is not accepted")
	}
	if timeout == 0 {
		timeout = minObserveTimeout
	}
	if acceptFn == nil {
		acceptFn = func(e RaftEvent) bool { return true }
	}
	return &Observer{
		timeout:  timeout,
		ch:       ch,
		acceptFn: acceptFn,
		id:       atomic.AddUint64(&nextObserverId, 1),
	}, nil
}

func (o *Observer) observe(e RaftEvent) {
	if !o.acceptFn(e) {
		return
	}
	select {
	case o.ch <- e:
		atomic.AddUint64(&o.nObserved, 1)
	case <-time.After(o.timeout):
		atomic.AddUint64(&o.nDropped, 1)
	}
}

func (o *Observer) GetNumObserved() uint64 {
	return atomic.LoadUint64(&o.nObserved)
}

func (o *Observer) GetNumDropped() uint64 {
	return atomic.LoadUint64(&o.nDropped)
}

type observerManager struct {
	l         sync.RWMutex
	observers map[uint64]*Observer
}

func newObserverManager() *observerManager {
	return &observerManager{
		observers: map[uint64]*Observer{},
	}
}

func (m *observerManager) register(o *Observer) {
	m.l.Lock()
	defer m.l.Unlock()
	m.observers[o.id] = o
}

func (m *observerManager) deregister(id uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	delete(m.observers, id)
}

func (m *observerManager) observe(e RaftEvent) {
	m.l.RLock()
	defer m.l.RUnlock()
	for _, o := range m.observers {
		o.observe(e)
	}
}

func (r *Raft) RegisterObserver(o *Observer) {
	r.observers.register(o)
}

func (r *Raft) DeregisterObserver(id uint64) {
	r.observers.deregister(id)
}
