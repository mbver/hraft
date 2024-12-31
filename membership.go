package hraft

import (
	"fmt"
	"sync"
)

type peerRole int8

const (
	roleStaging peerRole = iota
	roleVoter
	roleNonVoter
	roleAbsent
)

func (r peerRole) String() string {
	switch r {
	case roleStaging:
		return "staging"
	case roleVoter:
		return "voter"
	case roleNonVoter:
		return "non-voter"
	case roleAbsent:
		return "absent"
	}
	return "unknown-role"
}

type peer struct {
	id   string
	role peerRole
}

func (p *peer) getId() string {
	return p.id
}

func (p *peer) isVoter() bool {
	return p.role == roleVoter
}

func (p *peer) isStaging() bool {
	return p.role == roleStaging
}

type membership struct {
	l            sync.Mutex
	local        *peer
	latest       []*peer
	lastestIdx   uint64
	commited     []*peer
	committedIdx uint64
}

func (m *membership) getLocal() *peer {
	m.l.Lock()
	defer m.l.Unlock()
	return m.local
}

func (m *membership) getVoters() []string {
	m.l.Lock()
	defer m.l.Unlock()
	voters := []string{}
	for _, p := range m.latest {
		if p.id == m.local.id || p.role != roleVoter {
			continue
		}
		voters = append(voters, p.id)
	}
	return voters
}

func (m *membership) validate() {
	numStaging := 0
	for _, p := range m.latest {
		if p.isStaging() {
			numStaging++
		}
	}
	if numStaging > 1 {
		panic(fmt.Errorf("only 1 staging peer is allowed, got %d", numStaging))
	}
	numStaging = 0
	for _, p := range m.commited {
		if p.isStaging() {
			numStaging++
		}
	}
	if numStaging > 1 {
		panic(fmt.Errorf("only 1 staging peer is allowed, got %d", numStaging))
	}
}

func (m *membership) getStaging() string {
	m.l.Lock()
	defer m.l.Unlock()
	for _, p := range m.latest {
		if p.role == roleStaging {
			return p.id
		}
	}
	return ""
}

func (m *membership) getPeer(id string) peerRole {
	m.l.Lock()
	defer m.l.Unlock()
	for _, p := range m.latest {
		if p.id == id {
			return p.role
		}
	}
	return roleAbsent
}

func (m *membership) peers() []string {
	m.l.Lock()
	defer m.l.Unlock()
	mems := make([]string, 0, len(m.latest)-1)
	for _, p := range m.latest {
		if p.id == m.local.id {
			continue
		}
		mems = append(mems, p.id)
	}
	return mems
}
