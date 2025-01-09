package hraft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCluster_StartStop(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, false)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
}

func TestCluster_NoElect_StartStop(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, true)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, 3, len(c.getNodesByState(followerStateType)))
	require.Equal(t, 0, len(c.getNodesByState(leaderStateType)))
}

func TestRaft_AfterShutdown(t *testing.T) {
	c, cleanup, err := createTestCluster(1, false)
	defer cleanup()
	require.Nil(t, err)
	c.close()

	raft := c.rafts[0]
	err = raft.Apply(nil, 0)
	require.Equal(t, ErrRaftShutdown, err)

	err = raft.AddVoter("127.0.0.1:7946", 0)
	require.Equal(t, ErrRaftShutdown, err)
}
