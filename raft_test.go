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

	require.Equal(t, 1, len(c.getNodesByState(leaderStateType)))
	leader := c.getNodesByState(leaderStateType)[0]
	err = <-leader.Apply(nil, 0)
	require.Equal(t, ErrRaftShutdown, err)
	err = leader.AddVoter("127.0.0.1:7946", 0)
	require.Equal(t, ErrRaftShutdown, err)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	raft := c.getNodesByState(followerStateType)[0]
	err = <-raft.Apply([]byte("test"), raft.config.HeartbeatTimeout)
	require.Equal(t, ErrNotLeader, err)
}

func TestRaft_Apply_Timeout(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	raft := c.getNodesByState(leaderStateType)[0]

	err = <-raft.Apply([]byte("test"), time.Microsecond)
	require.NotNil(t, err, fmt.Sprintf("%v", err))
	require.Contains(t, err.Error(), "timeout")
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	raft := c.getNodesByState(leaderStateType)[0]

	numApplies := 100
	errCh := make(chan error, numApplies)
	for i := 0; i < numApplies; i++ {
		go func(i int) {
			err := <-raft.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
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
	success, msg := retry(5, c.isConsistent)
	require.True(t, success, msg)
}

func TestCluster_StartStop(t *testing.T) {
	t.Parallel()
	_, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
}

func TestCluster_SingleNode(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)

	raft := c.getNodesByState(leaderStateType)[0]
	err = <-raft.Apply([]byte("test"), raft.config.HeartbeatTimeout)
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
	defer func() {
		follower0.Shutdown()
		c.wg.Done()
	}()
	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)
}

func TestRaft_Remove_Rejoin_Follower(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
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
	defer func() {
		follower0.Shutdown()
		c.wg.Done()
	}()

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

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
	success, msg = retry(5, c.isConsistent)
	require.True(t, success, msg)
}

func TestRaft_RemoveFollower_SplitCluster(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(4, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	followers := c.getNodesByState(followerStateType)
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

	leader := c.getNodesByState(leaderStateType)[0]

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

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)
}

func TestRaft_RemoveLeader_AndApply(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = applyAndCheck(leader, 80, 0, nil)
	require.Nil(t, err)

	err = leader.RemovePeer(leader.ID(), 500*time.Millisecond)
	require.Nil(t, err)

	oldLeader := leader
	err = applyAndCheck(oldLeader, 19, 81, ErrNotLeader)
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
	leader = c.getNodesByState(leaderStateType)[0]

	require.Equal(t, 2, len(follower.Voters()))
	require.Equal(t, 2, len(oldLeader.Voters()))
	require.Equal(t, 2, len(leader.Voters()))

	require.True(t, reflect.DeepEqual(leader.Voters(), oldLeader.Voters()))
	require.True(t, reflect.DeepEqual(leader.Voters(), follower.Voters()))

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)
}

func TestRaft_AddKnownPeer(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
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

	leader := c.getNodesByState(leaderStateType)[0]
	err = leader.RemovePeer("8.8.8.8:8888", 0)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find peer")
}

func TestRaft_DemoteVoter(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	follower0 := c.getNodesByState(followerStateType)[0]
	err = leader.DemoteVoter(follower0.ID(), 0)
	require.Nil(t, err)
	voters := leader.Voters()
	for _, v := range voters {
		require.NotEqual(t, follower0.ID(), v)
	}
	voters = follower0.Voters()
	for _, v := range voters {
		require.NotEqual(t, follower0.ID(), v)
	}
}
func TestRaft_VerifyLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	success := leader.VerifyLeader(500 * time.Millisecond)
	require.True(t, success)
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	success := leader.VerifyLeader(500 * time.Millisecond)
	require.True(t, success)
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(2, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
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

	leader := c.getNodesByState(leaderStateType)[0]
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

	err = applyAndCheck(leader, 10, 0, nil)
	require.Nil(t, err)

	sleep()

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

	leader := c.getNodesByState(leaderStateType)[0]

	err = applyAndCheck(leader, 100, 0, nil)
	require.Nil(t, err)

	sleep()
	metas, err := leader.snapstore.List()
	require.Nil(t, err)
	require.NotZero(t, len(metas))
}

func TestRaft_SendLatestSnapshot(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	// conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.NumTrailingLogs = 10
	c, cleanup, err := createTestCluster(3, conf)
	defer cleanup()
	require.Nil(t, err)

	behindFo := c.getNodesByState(followerStateType)[0]
	c.partition(behindFo.ID())

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

	leader := c.getNodesByState(leaderStateType)[0]
	err = applyAndCheck(leader, 100, 0, nil)
	require.Nil(t, err)
	sleep()
	errCh := make(chan error, 3)
	for _, r := range c.rafts {
		go func() {
			_, err := r.Snapshot(0) // truncate logs
			errCh <- err
		}()
	}
	err = drainAndCheckErr(errCh, nil, 3, 5*time.Second)
	require.Nil(t, err)
	// sleep long enough to increase exponential backoff.
	// faulty implementation can cause this scenario:
	// exponential backoff of replicate loop is enormous.
	// if the partioned node transitioned back to follower
	// just about reconnection time, and the leader is frozen
	// by exponential backoff but heartbeat still working,
	// then the behind follower may not receive snapshot
	// and failed consistent check after retry times.
	// with correct implementation, backoffs of heartbeat
	// and replicate will be almost the same in the case of
	// network failure. they succeed or fail together.
	// if heartbeat failed, that makes the node transition to candidate
	// and unblock the frozen situation.
	// furthermore, correct implementation makes backoff time smaller
	// than the sleep time.
	time.Sleep(1 * time.Second)
	c.unPartition(behindFo.ID())
	success, msg := retry(5, c.isConsistent)
	require.True(t, success, msg)
}

func TestRaft_SendLatestSnapshotAndLogs(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	// conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.NumTrailingLogs = 10
	c, cleanup, err := createTestCluster(3, conf)
	defer cleanup()
	require.Nil(t, err)

	behindFo := c.getNodesByState(followerStateType)[0]
	c.partition(behindFo.ID())

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

	leader := c.getNodesByState(leaderStateType)[0]
	err = applyAndCheck(leader, 100, 0, nil)
	require.Nil(t, err)
	sleep()
	errCh := make(chan error, 3)
	for _, r := range c.rafts {
		go func() {
			_, err := r.Snapshot(0) // truncate logs
			errCh <- err
		}()
	}
	err = drainAndCheckErr(errCh, nil, 3, 5*time.Second)
	require.Nil(t, err)

	err = applyAndCheck(leader, 100, 100, nil)
	require.Nil(t, err)
	c.unPartition(behindFo.ID())

	// wait for leadership change to kicks in
	sleep()
	// wait and check until leadership change finished
	ok, msg := retry(5, func() (bool, string) {
		sleep()
		err = checkClusterState(c)
		if err != nil {
			return false, fmt.Sprintf("cluster state check failed: %s", err.Error())
		}
		return true, ""
	})
	require.True(t, ok, msg)

	leader = c.getNodesByState(leaderStateType)[0]

	// if leadership changed, this will send a log with new term
	// and should be added in the state of all nodes.
	// if new leader's logs is shorter than old leader,
	// this should induce log truncation.
	applyAndCheck(leader, 1, 200, nil)

	success, msg := retry(5, c.isConsistent)
	require.True(t, success, msg)
}

func TestRaft_UserRestore(t *testing.T) {
	t.Parallel()
	offsets := []uint64{0, 1, 2, 100, 1000, 10000}
	for _, offset := range offsets {
		t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
			t.Parallel()
			conf := defaultTestConfig()
			conf.HeartbeatTimeout = 500 * time.Millisecond
			conf.ElectionTimeout = 500 * time.Millisecond

			c, cleanup, err := createTestCluster(3, conf)
			defer cleanup()
			require.Nil(t, err)

			leader := c.getNodesByState(leaderStateType)[0]

			err = applyAndCheck(leader, 10, 0, nil)
			require.Nil(t, err)
			sleep()

			openSnapshot, err := leader.Snapshot(0)
			require.Nil(t, err)

			err = applyAndCheck(leader, 10, 10, nil)
			require.Nil(t, err)

			meta, source, err := openSnapshot()
			require.Nil(t, err)
			defer source.Close()
			oldLastIdx, _ := leader.instate.getLastIdxTerm()
			meta.Idx += offset
			err = leader.Restore(meta, source, 5*time.Second)
			require.Nil(t, err)

			consistent, msg := c.isConsistent()
			require.True(t, consistent, msg)

			// 1 idx is to create an index hole
			// 1 idx is from NoOp log
			expectedLastIdx := max(meta.Idx, oldLastIdx) + 2
			newLastIdx, _ := leader.instate.getLastIdxTerm()
			require.Equal(t, expectedLastIdx, newLastIdx)

			firstState := getRecordCommandState(leader)
			// state before snapshot is preserved.
			// state after snapshot is discarded.
			require.Equal(t, 10, len(firstState))
			for i, l := range firstState {
				expected := []byte(fmt.Sprintf("test %d", i))
				require.True(t, bytes.Equal(expected, l.Data), fmt.Sprintf("expect: %s, got: %s", expected, l.Data))
			}

			err = applyAndCheck(leader, 10, 20, nil)
			require.Nil(t, err)

			consistent, msg = c.isConsistent()
			require.True(t, consistent, msg)
		})
	}
}

func TestRaft_LeadershipTransfer(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", time.Second)
	require.Nil(t, err)
	oldLeader := leader
	leader = c.getNodesByState(leaderStateType)[0]
	require.NotEqual(t, oldLeader.ID(), leader.ID())
}

func TestRaft_LeadershipTransferWithOneNode(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(1, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", time.Second)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find most current follower")
}

func TestRaft_LeadershipTransferWithSevenNodes(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(7, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", time.Second)
	require.Nil(t, err)
	oldLeader := leader
	leader = c.getNodesByState(leaderStateType)[0]
	require.NotEqual(t, oldLeader.ID(), leader.ID())
}

func TestRaft_LeadershipTransferToInvalidID(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("awesome_peer", time.Second)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "peer awesome_peer is non-voter")
}

func TestRaft_LeadershipTransferToItself(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership(leader.ID(), time.Second)
	require.NotNil(t, err)
	expectedMsg := fmt.Sprintf("leader %s can not transfer leadership to itself", leader.ID())
	require.Equal(t, expectedMsg, err.Error())
}

func TestRaft_LeadershipTransferIgnoreNonVoters(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(2, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	follower := c.getNodesByState(followerStateType)[0]
	err = leader.DemoteVoter(follower.ID(), time.Second)
	require.Nil(t, err)

	err = leader.TransferLeadership("", time.Second)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find most current follower")
}

func TestRaft_LeadershipTransferIgnoreSpecifiedNonVoter(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	follower0 := c.getNodesByState(followerStateType)[0]
	err = leader.DemoteVoter(follower0.ID(), time.Second)
	require.Nil(t, err)

	err = leader.TransferLeadership(follower0.ID(), time.Second)
	require.NotNil(t, err)
	expectedMsg := fmt.Sprintf("peer %s is non-voter", follower0.ID())
	require.Equal(t, expectedMsg, err.Error())
}

func TestRaft_LeadershipTransferTimeout(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", 20*time.Millisecond)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to wait for force-replication response")
}

func TestRaft_LeadershipTransferInProgress(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster(3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	leader.getLeaderState().inLeadershipTransfer.Store(true)

	errCh := make(chan error, 3)
	go runAndCollectErr(
		func() error { return leader.AddVoter("abc", time.Second) },
		errCh,
	)
	go runAndCollectErr(
		func() error {
			err := <-leader.Apply([]byte("abc"), time.Second)
			return err
		},
		errCh,
	)
	go runAndCollectErr(
		func() error { return leader.DemoteVoter("abc", time.Second) },
		errCh,
	)
	err = drainAndCheckErr(errCh, ErrLeadershipTransferInProgress, 3, time.Second)
	require.Nil(t, err)
}
