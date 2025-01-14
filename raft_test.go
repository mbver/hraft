package hraft

import (
	"bytes"
	"fmt"
	"reflect"
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

func TestRaft_ApplyNonLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	raft := c.getNodesByState(followerStateType)[0]
	err = raft.Apply([]byte("test"), raft.config.HeartbeatTimeout)
	require.Equal(t, ErrNotLeader, err)
}

func TestRaft_Apply_Timeout(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	raft := c.getNodesByState(leaderStateType)[0]

	err = raft.Apply([]byte("test"), time.Microsecond)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	raft := c.getNodesByState(leaderStateType)[0]

	numApplies := 100
	errCh := make(chan error, numApplies)
	for i := 0; i < numApplies; i++ {
		go func(i int) {
			err := raft.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
			errCh <- err
		}(i)
	}
	for i := 0; i < numApplies; i++ {
		select {
		case err = <-errCh:
			require.Nil(t, err)
		case <-time.After(1 * time.Second):
			t.Fatalf("expect no timeout")
		}
	}
	success, msg := retry(5, func() (bool, string) {
		time.Sleep(100 * time.Millisecond)
		if !c.isConsistent() {
			return false, "cluster is inconsistent"
		}
		return true, ""
	})
	require.True(t, success, msg)
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

func TestRaft_RemoveFollower(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	follower0 := c.getNodesByState(followerStateType)[0]
	follower1 := c.getNodesByState(followerStateType)[1]

	err = leader.RemovePeer(follower0.ID(), 500*time.Millisecond)
	require.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, followerStateType, follower0.getStateType())

	require.Equal(t, 2, len(leader.Voters()))
	require.Equal(t, 2, len(follower0.Voters()))
	require.Equal(t, 2, len(follower1.Voters()))

	require.True(t, reflect.DeepEqual(leader.Voters(), follower0.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower1.Voters()))
	require.Equal(t, RoleAbsent, leader.membership.getPeer(follower0.ID()))

	require.True(t, c.isConsistent())
}

func TestRaft_RemoveLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))

	err = leader.RemovePeer(leader.ID(), 500*time.Millisecond)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, followerStateType, leader.getStateType(), fmt.Sprintf("wrong state type: %s", leader.getStateType()))

	success, msg := retry(10, func() (bool, string) {
		time.Sleep(100 * time.Millisecond)
		if len(c.getNodesByState(followerStateType)) != 2 {
			return false, "not enough followers"
		}
		if len(c.getNodesByState(leaderStateType)) != 1 {
			return false, "no leader"
		}
		return true, ""
	})
	require.True(t, success, msg)

	follower := c.getNodesByState(followerStateType)[1]
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	oldLeader := leader
	leader = c.getNodesByState(leaderStateType)[0]

	require.Equal(t, 2, len(follower.Voters()))
	require.Equal(t, 2, len(oldLeader.Voters()))
	require.Equal(t, 2, len(leader.Voters()))

	require.True(t, reflect.DeepEqual(leader.Voters(), oldLeader.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower.Voters()))

	require.True(t, c.isConsistent())
}

func TestRaft_RemoveLeader_AndApply(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))

	for i := 0; i < 100; i++ {
		if i == 80 {
			err = leader.RemovePeer(leader.ID(), 500*time.Millisecond)
			require.Nil(t, err, "failed to remove leader")
			continue
		}
		err = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if i < 80 {
			require.Nil(t, err)
			continue
		}
		require.Equal(t, ErrNotLeader, err)
	}

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, followerStateType, leader.getStateType(), fmt.Sprintf("wrong state type: %s", leader.getStateType()))

	success, msg := retry(10, func() (bool, string) {
		time.Sleep(100 * time.Millisecond)
		if len(c.getNodesByState(followerStateType)) != 2 {
			return false, "not enough followers"
		}
		if len(c.getNodesByState(leaderStateType)) != 1 {
			return false, "no leader"
		}
		return true, ""
	})
	require.True(t, success, msg)

	follower := c.getNodesByState(followerStateType)[1]
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	oldLeader := leader
	leader = c.getNodesByState(leaderStateType)[0]

	require.Equal(t, 2, len(follower.Voters()))
	require.Equal(t, 2, len(oldLeader.Voters()))
	require.Equal(t, 2, len(leader.Voters()))

	require.True(t, reflect.DeepEqual(leader.Voters(), oldLeader.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower.Voters()))

	require.True(t, c.isConsistent())
}
