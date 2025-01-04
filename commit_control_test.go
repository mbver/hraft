package hraft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitControl_UpdateVoters(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(10, commitCh)
	c.updateVoters([]string{"a", "b", "c"})
	c.updateMatchIdx("a", 10)
	c.updateMatchIdx("b", 20)
	c.updateMatchIdx("c", 30)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(20), c.getCommitIdx())
	c.updateVoters([]string{"c", "d", "e"})
	c.updateMatchIdx("e", 40)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(30), c.getCommitIdx())
}

func TestCommitControl_UpdateMatchIdx_NonVoters(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(4, commitCh)
	c.updateVoters([]string{"a", "b", "c", "d", "e"})
	c.updateMatchIdx("a", 8)
	c.updateMatchIdx("b", 8)
	c.updateMatchIdx("c", 8)

	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(8), c.getCommitIdx())

	c.updateMatchIdx("x", 10)
	c.updateMatchIdx("y", 10)
	c.updateMatchIdx("z", 10)

	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(8), c.getCommitIdx())
	require.Equal(t, 5, len(c.matchIdxs))
}

func TestCommitControl_UpdateCommitIdx(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(0, commitCh)
	c.updateVoters([]string{"a", "b", "c", "d", "e"})

	c.updateMatchIdx("a", 30)
	c.updateMatchIdx("b", 20)

	require.False(t, tryGetNotify(commitCh))
	require.Zero(t, c.getCommitIdx())

	c.updateMatchIdx("c", 10)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(10), c.getCommitIdx())

	c.updateMatchIdx("d", 15)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(15), c.getCommitIdx())

	c.updateVoters([]string{"a", "b", "c"})
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(20), c.getCommitIdx())

	c.updateVoters([]string{"a", "b", "c", "d"})
	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(20), c.getCommitIdx())
	c.updateMatchIdx("b", 25)
	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(20), c.getCommitIdx())

	c.updateMatchIdx("d", 23)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(23), c.getCommitIdx())
}

func TestCommitControl_StartIdx(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(4, commitCh)
	c.updateVoters([]string{"a", "b", "c", "d", "e"})

	c.updateMatchIdx("a", 3)
	c.updateMatchIdx("b", 3)
	c.updateMatchIdx("c", 3)

	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(4), c.getCommitIdx())

	c.updateMatchIdx("a", 5)
	c.updateMatchIdx("b", 5)
	c.updateMatchIdx("c", 5)

	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(5), c.getCommitIdx())
}

func TestCommitControl_NoVoters(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(4, commitCh)
	c.updateVoters([]string{})
	c.updateMatchIdx("a", 10)

	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(4), c.getCommitIdx())

	c.updateVoters([]string{"a"})
	c.updateMatchIdx("a", 15)
	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(15), c.getCommitIdx())

	c.updateVoters([]string{})
	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(15), c.getCommitIdx())
}

func TestCommitControl_SingleVoter(t *testing.T) {
	commitCh := make(chan struct{}, 1)
	c := newCommitControl(4, commitCh)
	c.updateVoters([]string{"a"})
	c.updateMatchIdx("a", 10)

	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(10), c.getCommitIdx())

	c.updateVoters([]string{"a"})
	require.False(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(10), c.getCommitIdx())
	c.updateMatchIdx("a", 12)

	require.True(t, tryGetNotify(commitCh))
	require.Equal(t, uint64(12), c.getCommitIdx())
}
