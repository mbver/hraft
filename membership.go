// Copyright (c) HashiCorp, Inc.
// Copyright (c) 2025 Phuoc Phi
// SPDX-License-Identifier: MPL-2.0
package hraft

import (
	"fmt"
	"sync"
)

type PeerRole int8

const (
	RoleStaging PeerRole = iota
	RoleNonVoter
	RoleVoter
	RoleAbsent
)

func (r PeerRole) String() string {
	switch r {
	case RoleStaging:
		return "staging"
	case RoleVoter:
		return "voter"
	case RoleNonVoter:
		return "non-voter"
	case RoleAbsent:
		return "absent"
	}
	return "unknown-role"
}

type Peer struct {
	ID   string
	Role PeerRole
}

func (p *Peer) getId() string {
	return p.ID
}

func (p *Peer) isVoter() bool {
	return p.Role == RoleVoter
}

func (p *Peer) isStaging() bool {
	return p.Role == RoleStaging
}

func validatePeers(peers []*Peer) error {
	numStaging := 0
	numVoters := 0
	exist := map[string]bool{}
	for _, p := range peers {
		if p.ID == "" {
			return fmt.Errorf("empty peer id")
		}
		if exist[p.ID] {
			return fmt.Errorf("duplicated peer %s", p.ID)
		}
		exist[p.ID] = true
		if p.isVoter() {
			numVoters++
		}
		if p.isStaging() {
			numStaging++
		}
	}
	if numVoters < 1 {
		return fmt.Errorf("require at least 1 voter")
	}
	if numStaging > 1 {
		return fmt.Errorf("number of staging peer exceeds 1: %d", numStaging)
	}
	return nil
}

type membership struct {
	l              sync.Mutex
	localID        string
	latestPeers    []*Peer
	lastestIdx     uint64
	committedPeers []*Peer
	committedIdx   uint64
}

func newMembership(localID string) *membership {
	return &membership{
		localID:        localID,
		latestPeers:    nil,
		lastestIdx:     0,
		committedPeers: nil,
		committedIdx:   0,
	}
}

func (m *membership) getLocalID() string {
	return m.localID
}

func (m *membership) isLocalVoter() bool {
	m.l.Lock()
	defer m.l.Unlock()
	for _, p := range m.latestPeers {
		if p.ID == m.localID {
			return p.Role == RoleVoter
		}
	}
	return false
}

func (m *membership) isActive() bool {
	m.l.Lock()
	defer m.l.Unlock()
	return len(m.latestPeers) > 0
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
		if p.Role != RoleVoter {
			continue
		}
		voters = append(voters, p.ID)
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
		if p.Role == RoleStaging {
			return p.ID
		}
	}
	return ""
}

func (m *membership) getPeer(id string) PeerRole {
	m.l.Lock()
	defer m.l.Unlock()
	for _, p := range m.latestPeers {
		if p.ID == id {
			return p.Role
		}
	}
	return RoleAbsent
}

func (m *membership) peerAddresses() []string {
	m.l.Lock()
	defer m.l.Unlock()
	mems := make([]string, 0, len(m.latestPeers)-1)
	for _, p := range m.latestPeers {
		mems = append(mems, p.ID)
	}
	return mems
}

func (m *membership) getCommitted() ([]*Peer, uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	return copyPeers(m.committedPeers), m.committedIdx
}

func (m *membership) rollbackToCommitted() {
	m.l.Lock()
	defer m.l.Unlock()
	m.lastestIdx = m.committedIdx
	m.latestPeers = copyPeers(m.committedPeers)
}

func (m *membership) getLatest() ([]*Peer, uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	return copyPeers(m.latestPeers), m.lastestIdx
}

func (m *membership) setLatest(in []*Peer, idx uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	m.latestPeers = copyPeers(in)
	m.lastestIdx = idx
}

func (m *membership) setCommitted(in []*Peer, idx uint64) {
	m.l.Lock()
	defer m.l.Unlock()
	m.committedPeers = copyPeers(in)
	m.committedIdx = idx
}

func copyPeers(in []*Peer) []*Peer {
	out := make([]*Peer, len(in))
	for i, p := range in {
		out[i] = &Peer{p.ID, p.Role}
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
	bootstrap
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
	case bootstrap:
		return "bootstrap"
	}
	return "unknown membershipt change"
}

type membershipChangeRequest struct {
	addr       string
	changeType membershipChageType
	errCh      chan error
}

func newMembershipChangeRequest(addr string, changeType membershipChageType) *membershipChangeRequest {
	return &membershipChangeRequest{
		addr:       addr,
		changeType: changeType,
		errCh:      make(chan error, 1),
	}
}

func (m *membership) newPeersFromChange(change *membershipChangeRequest) ([]*Peer, error) {
	latest, _ := m.getLatest()
	switch change.changeType {
	case addStaging:
		for _, p := range latest {
			if p.ID == change.addr {
				return nil, fmt.Errorf("peer exists with role %s", p.Role)
			}
		}
		latest = append(latest, &Peer{change.addr, RoleStaging})
		if err := validatePeers(latest); err != nil {
			return nil, err
		}
		return latest, nil
	case addNonVoter:
		for _, p := range latest {
			if p.ID == change.addr {
				return nil, fmt.Errorf("peer exists with role %s", p.Role)
			}
		}
		latest = append(latest, &Peer{change.addr, RoleNonVoter})
		if err := validatePeers(latest); err != nil {
			return nil, err
		}
		return latest, nil
	case promotePeer:
		for _, p := range latest {
			if p.ID == change.addr && p.Role == RoleStaging {
				p.Role = RoleVoter
				return latest, nil
			}
		}
	case demotePeer:
		for _, p := range latest {
			if p.ID == change.addr && p.Role == RoleVoter {
				p.Role = RoleNonVoter
				return latest, nil
			}
		}
		return nil, fmt.Errorf("peer %s is absent or not a voter", change.addr)
	case removePeer:
		for i, p := range latest {
			if p.ID == change.addr {
				n := len(latest)
				latest[i], latest[n-1] = latest[n-1], latest[i]
				latest = latest[:n-1]
				return latest, nil
			}
		}
	case bootstrap:
		return []*Peer{{m.localID, RoleVoter}}, nil
	}
	return nil, fmt.Errorf("unable to find peer %s for %s change", change.addr, change.changeType)
}
