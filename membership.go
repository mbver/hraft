package hraft

import (
	"fmt"
	"sync"
)

type peerRole int8

const (
	roleStaging peerRole = iota
	roleNonVoter
	roleVoter
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

func validatePeers(peers []*peer) error {
	numStaging := 0
	for _, p := range peers {
		if p.id == "" {
			return fmt.Errorf("empty peer id")
		}
		if p.isStaging() {
			numStaging++
		}
	}
	if numStaging > 1 {
		return fmt.Errorf("number of staging peer exceeds 1: %d", numStaging)
	}
	return nil
}

type membership struct {
	l              sync.Mutex
	local          *peer
	latestPeers    []*peer
	lastestIdx     uint64
	committedPeers []*peer
	committedIdx   uint64
}

func newMembership(localID string, peerIDs []string) *membership {
	peers := make([]*peer, len(peerIDs))
	for i, id := range peerIDs {
		peers[i] = &peer{id, roleVoter}
	}
	return &membership{
		local:          &peer{localID, roleVoter},
		latestPeers:    peers,
		lastestIdx:     0,
		committedPeers: peers,
		committedIdx:   0,
	}
}

func (m *membership) getLocal() *peer {
	m.l.Lock()
	defer m.l.Unlock()
	return m.local
}

func (m *membership) isStable() bool {
	m.l.Lock()
	defer m.l.Unlock()
	return m.committedIdx == m.lastestIdx
}

func (m *membership) getVoters() []string {
	m.l.Lock()
	defer m.l.Unlock()
	voters := []string{}
	for _, p := range m.latestPeers {
		if p.role != roleVoter {
			continue
		}
		voters = append(voters, p.id)
	}
	return voters
}

func (m *membership) validate() {
	numStaging := 0
	for _, p := range m.latestPeers {
		if p.isStaging() {
			numStaging++
		}
	}
	if numStaging > 1 {
		panic(fmt.Errorf("only 1 staging peer is allowed, got %d", numStaging))
	}
	numStaging = 0
	for _, p := range m.committedPeers {
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
	for _, p := range m.latestPeers {
		if p.role == roleStaging {
			return p.id
		}
	}
	return ""
}

func (m *membership) getPeer(id string) peerRole {
	m.l.Lock()
	defer m.l.Unlock()
	for _, p := range m.latestPeers {
		if p.id == id {
			return p.role
		}
	}
	return roleAbsent
}

func (m *membership) peers() []string {
	m.l.Lock()
	defer m.l.Unlock()
	mems := make([]string, 0, len(m.latestPeers)-1)
	for _, p := range m.latestPeers {
		mems = append(mems, p.id)
	}
	return mems
}

func (m *membership) getLatest() []*peer {
	m.l.Lock()
	defer m.l.Unlock()
	return copyPeers(m.latestPeers)
}

func (m *membership) setLatest(in []*peer, idx uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	m.latestPeers = copyPeers(in)
	m.lastestIdx = idx
}

func copyPeers(in []*peer) []*peer {
	out := make([]*peer, len(in))
	for i, p := range in {
		out[i] = &peer{p.id, p.role}
	}
	return out
}

type membershipChageType int8

const (
	addStaging membershipChageType = iota
	addNonVoter
	promotePeer
	demotePeer
	removePeer
)

func (t membershipChageType) String() string {
	switch t {
	case addStaging:
		return "add_staging"
	case addNonVoter:
		return "add_non_voter"
	case promotePeer:
		return "promote_peer"
	case demotePeer:
		return "demote_peer"
	case removePeer:
		return "remove_peer"
	}
	return "unknown membershipt change"
}

type membershipChange struct {
	addr       string
	changeType membershipChageType
	errCh      chan error
}

func newMembershipChange(addr string, changeType membershipChageType) *membershipChange {
	return &membershipChange{
		addr:       addr,
		changeType: changeType,
		errCh:      make(chan error, 1),
	}
}

func (m *membership) newPeersFromChange(change *membershipChange) ([]*peer, error) {
	latest := m.getLatest()
	switch change.changeType {
	case addStaging:
		for _, p := range latest {
			if p.id == change.addr {
				return nil, fmt.Errorf("peer exists with role %s", p.role)
			}
		}
		latest = append(latest, &peer{change.addr, roleStaging})
		if err := validatePeers(latest); err != nil {
			return nil, err
		}
		return latest, nil
	case addNonVoter:
		for _, p := range latest {
			if p.id == change.addr {
				return nil, fmt.Errorf("peer exists with role %s", p.role)
			}
		}
		latest = append(latest, &peer{change.addr, roleNonVoter})
		if err := validatePeers(latest); err != nil {
			return nil, err
		}
		return latest, nil
	case promotePeer:
		for _, p := range latest {
			if p.id == change.addr && p.role == roleStaging {
				p.role = roleVoter
				return latest, nil
			}
		}
	case demotePeer:
		for _, p := range latest {
			if p.id == change.addr && p.role == roleVoter {
				p.role = roleVoter
				return latest, nil
			}
		}
	case removePeer:
		for i, p := range latest {
			if p.id == change.addr {
				n := len(latest)
				latest[i], latest[n-1] = latest[n-1], latest[i]
				latest = latest[:n-1]
				return latest, nil
			}
		}
	}
	return nil, fmt.Errorf("unable to find peer %s for %s change", change.addr, change.changeType)
}
