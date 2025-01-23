package hraft

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sleep() {
	time.Sleep(200 * time.Millisecond)
}

func TestRaft_NoBootstrap_StartStop(t *testing.T) {
	t.Parallel()
	raft, cleanup, err := createTestNode(nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.True(t, raft.getStateType() == followerStateType)
}

func TestRaft_AfterShutdown(t *testing.T) {
	c, cleanup, err := createTestCluster(1, nil)
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
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	raft := c.getNodesByState(followerStateType)[0]
	err = raft.Apply([]byte("test"), raft.config.HeartbeatTimeout)
	require.Equal(t, ErrNotLeader, err)
}

func TestRaft_Apply_Timeout(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	raft := c.getNodesByState(leaderStateType)[0]

	err = raft.Apply([]byte("test"), time.Microsecond)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
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
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
}
func TestCluster_SingleNode(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
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
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	follower0 := c.getNodesByState(followerStateType)[0]
	follower1 := c.getNodesByState(followerStateType)[1]

	err = leader.RemovePeer(follower0.ID(), 500*time.Millisecond)
	require.Nil(t, err)

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 2, len(leader.Voters()))
	require.Equal(t, followerStateType, follower0.getStateType())

	require.True(t, reflect.DeepEqual(leader.Voters(), follower0.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower1.Voters()))
	require.Equal(t, RoleAbsent, leader.membership.getPeer(follower0.ID()))

	c.remove(follower0.ID())
	defer follower0.Shutdown()
	require.True(t, c.isConsistent())
}

func TestRaft_Remove_Rejoin_Follower(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
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

	require.True(t, reflect.DeepEqual(leader.Voters(), follower0.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower1.Voters()))
	require.Equal(t, RoleAbsent, leader.membership.getPeer(follower0.ID()))

	c.remove(follower0.ID())
	defer follower0.Shutdown()
	require.True(t, c.isConsistent())

	err = leader.AddVoter(follower0.ID(), 0)
	require.Nil(t, err)
	c.add(follower0)
	success, msg := retry(10, func() (bool, string) {
		time.Sleep(100 * time.Millisecond)
		if len(leader.Voters()) != 3 {
			return false, "not enough voters"
		}
		if len(follower0.Voters()) != 3 || len(follower1.Voters()) != 3 {
			return false, "not enough voters for follower"
		}
		return true, ""
	})
	require.True(t, success, msg)

	require.True(t, reflect.DeepEqual(leader.Voters(), follower0.Voters()), fmt.Sprintf("expect: %v, got: %v", leader.Voters(), follower0.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower1.Voters()), fmt.Sprintf("expect: %v, got: %v", leader.Voters(), follower1.Voters()))
	require.Equal(t, RoleVoter, leader.membership.getPeer(follower0.ID()))
	success, msg = retry(5, func() (bool, string) {
		time.Sleep(100 * time.Millisecond)
		if !c.isConsistent() {
			return false, "inconsistent state"
		}
		return true, ""
	})
	require.True(t, success, msg)
}

func TestRaft_RemoveFollower_SplitCluster(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(4, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	followers := c.getNodesByState(followerStateType)
	require.Equal(t, 3, len(followers))
	// split the cluster to two partitions
	c.partition(followers[0].ID(), followers[1].ID())
	// try remove a node
	err = leader.RemovePeer(followers[2].ID(), 500*time.Millisecond)
	// commit will not able to proceed
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "timeout draining error")
}

func TestRaft_RemoveLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
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
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
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

func TestRaft_AddKnownPeer(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	follower0 := c.getNodesByState(followerStateType)[0]
	err = leader.AddVoter(follower0.ID(), 0)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "peer exists with role voter")
}

func TestRaft_RemoveUnknownPeer(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))

	err = leader.RemovePeer("8.8.8.8:8888", 0)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find peer")
}

func TestRaft_VerifyLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	success := leader.VerifyLeader(500 * time.Millisecond)
	require.True(t, success)
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	success := leader.VerifyLeader(500 * time.Millisecond)
	require.True(t, success)
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(2, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]

	require.Equal(t, 1, len(c.getNodesByState(followerStateType)))
	follower := c.getNodesByState(followerStateType)[0]

	follower.setTerm(follower.getTerm() + 1)

	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	oldLeader := leader
	require.Equal(t, followerStateType, oldLeader.getStateType())
	require.Equal(t, 1, len(c.getNodesByState(followerStateType)))
	leader = follower
	require.Equal(t, leaderStateType, leader.getStateType())

	success := oldLeader.VerifyLeader(500 * time.Millisecond)
	require.False(t, success)
}

func TestRaft_VerifyLeader_PartialDisconnect(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()

	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]

	require.Equal(t, 2, len(c.getNodesByState(followerStateType)))
	follower0 := c.getNodesByState(followerStateType)[0]

	c.partition(follower0.ID())
	sleep()
	success := leader.VerifyLeader(500 * time.Millisecond)
	require.True(t, success)
}

func TestRaft_UserSnapshot(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()

	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]

	openSnapshot, err := leader.Snapshot(0)
	require.Nil(t, err)
	require.NotNil(t, openSnapshot)
	metas, err := leader.snapstore.List()
	require.Nil(t, err)
	require.Equal(t, 1, len(metas))

	meta, snapshot, err := openSnapshot()
	require.Nil(t, err)
	defer snapshot.Close()
	require.Zero(t, meta.Size)

	buf := bytes.Buffer{}
	n, err := io.Copy(&buf, snapshot)
	require.Nil(t, err)
	require.Zero(t, n)
	err = snapshot.Close()
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		err = leader.Apply([]byte(fmt.Sprintf("test_%d", i)), 0)
		require.Nil(t, err)
	}

	openSnapshot, err = leader.Snapshot(0)
	require.Nil(t, err)
	require.NotNil(t, openSnapshot)
	metas, err = leader.snapstore.List()
	require.Nil(t, err)
	require.Equal(t, 2, len(metas))

	meta, snapshot, err = openSnapshot()
	require.Nil(t, err)
	defer snapshot.Close()

	buf = bytes.Buffer{}
	n, err = io.Copy(&buf, snapshot)
	require.Nil(t, err)
	require.NotZero(t, n)
	require.Equal(t, meta.Size, n)
}

func TestRaft_AutoSnapshot(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	conf.SnapshotInterval = 2 * conf.CommitSyncInterval
	conf.SnapshotThreshold = 50
	conf.NumTrailingLogs = 10
	c, cleanup, err := createTestCluster(1, conf)
	defer cleanup()
	require.Nil(t, err)
	sleep()

	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]

	for i := 0; i < 100; i++ {
		err = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		require.Nil(t, err)
	}
	sleep()
	metas, err := leader.snapstore.List()
	require.Nil(t, err)
	require.NotZero(t, len(metas))
}

func fetchErr(errCh chan error, n int, timeout time.Duration) error {
	timeoutCh := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case <-timeoutCh:
			return fmt.Errorf("timeout")
		}
	}
	return nil
}

func TestRaft_UserRestore(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond

	c, cleanup, err := createTestCluster(3, conf)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]

	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			err = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
			errCh <- err
		}()
	}
	sleep()

	err = fetchErr(errCh, 10, 5*time.Second)
	require.Nil(t, err)

	openSnapshot, err := leader.Snapshot(0)
	require.Nil(t, err)

	errCh = make(chan error, 10)
	for i := 10; i < 20; i++ {
		go func() {
			err = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
			errCh <- err
		}()
	}
	err = fetchErr(errCh, 10, 5*time.Second)
	require.Nil(t, err)

	meta, source, err := openSnapshot()
	require.Nil(t, err)
	defer source.Close()
	meta.Idx += 30
	err = leader.Restore(meta, source, 5*time.Second)
	require.Nil(t, err)
}
