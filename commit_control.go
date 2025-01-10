package hraft

import (
	"sort"
	"sync"
)

type commitControl struct {
	l              sync.Mutex
	commitNotifyCh chan struct{}
	matchIdxs      map[string]uint64
	commitIdx      uint64
}

func newCommitControl(startIdx uint64, commitNotifyCh chan struct{}) *commitControl {
	return &commitControl{
		commitNotifyCh: commitNotifyCh,
		commitIdx:      startIdx,
	}
}

func (c *commitControl) getCommitIdx() uint64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.commitIdx
}

func (c *commitControl) updateVoters(voters []string) {
	c.l.Lock()
	defer c.l.Unlock()
	oldMap := c.matchIdxs
	c.matchIdxs = make(map[string]uint64)
	for _, addr := range voters {
		c.matchIdxs[addr] = oldMap[addr]
	}
	c.updateCommitIdx()
}

func (c *commitControl) updateMatchIdx(id string, idx uint64) {
	c.l.Lock()
	defer c.l.Unlock()
	prev, ok := c.matchIdxs[id]
	if !ok || idx <= prev {
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
	if commitIdx > c.commitIdx {
		c.commitIdx = commitIdx
		tryNotify(c.commitNotifyCh)
	}
}
