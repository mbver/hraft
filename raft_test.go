package hraft

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaft_NoBootstrap_StartStop(t *testing.T) {
	t.Parallel()
	raft, cleanup, err := createTestNode()
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.True(t, raft.getStateType() == followerStateType)
}

func TestRaft_AfterShutdown(t *testing.T) {
	c, cleanup, err := createTestCluster(1)
	defer cleanup()
	require.Nil(t, err)
	c.close()

	raft := c.rafts[0]
	err = raft.Apply(nil, 0)
	require.Equal(t, ErrRaftShutdown, err)

	err = raft.AddVoter("127.0.0.1:7946", 0)
	require.Equal(t, ErrRaftShutdown, err)
}

func TestCluster_StartStop(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
}
func TestCluster_SingleNode(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, len(c.getNodesByState(followerStateType)))
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	raft := c.getNodesByState(leaderStateType)[0]
	err = raft.Apply([]byte("test"), raft.config.HeartbeatTimeout)
	require.Nil(t, err)
	commands := getRecordCommandState(raft)
	require.Equal(t, 1, len(commands))
	require.True(t, bytes.Equal([]byte("test"), commands[0].Data))
	require.Equal(t, uint64(2), commands[0].Idx)
}
