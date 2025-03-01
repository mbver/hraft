// Copyright (c) HashiCorp, Inc.
// Copyright (c) 2025 Phuoc Phi
// SPDX-License-Identifier: MPL-2.0
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
	raft, cleanup, err := createTestNode("NoBootstrap_StartStop", nil)
	defer cleanup()
	require.Nil(t, err)
	sleep()
	require.True(t, raft.getStateType() == followerStateType)
}

func TestRaft_AfterShutdown(t *testing.T) {
	c, cleanup, err := createTestCluster("AfterShutdown", 1, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	eventCh := waitForNewLeader(leader, leader.ID(), "")

	c.close()

	require.True(t, waitEventSuccessful(eventCh))

	err = <-leader.Apply(nil, 0)
	require.Equal(t, ErrRaftShutdown, err)
	err = leader.AddVoter("127.0.0.1:7946", 0)
	require.Equal(t, ErrRaftShutdown, err)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("ApplyNonLeader", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	raft := c.getNodesByState(followerStateType)[0]
	err = <-raft.Apply([]byte("test"), raft.getConfig().HeartbeatTimeout)
	require.True(t, isErrNotLeader(err))
}

func TestRaft_Apply_Timeout(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("Apply_Timeout", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	raft := c.getNodesByState(leaderStateType)[0]

	err = <-raft.Apply([]byte("test"), time.Microsecond)
	require.NotNil(t, err, fmt.Sprintf("%v", err))
	require.Contains(t, err.Error(), "timeout")
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("ApplyConcurrent", 3, nil)
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
	_, cleanup, err := createTestCluster("StartStop", 3, nil)
	defer cleanup()
	require.Nil(t, err)
}

func TestCluster_SingleNode(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("SingleNode", 1, nil)
	defer cleanup()
	require.Nil(t, err)

	raft := c.getNodesByState(leaderStateType)[0]
	err = <-raft.Apply([]byte("test"), raft.getConfig().HeartbeatTimeout)
	require.Nil(t, err)
	commands := getRecordCommandState(raft)
	require.Equal(t, 1, len(commands))
	require.True(t, bytes.Equal([]byte("test"), commands[0].Data))
	require.Equal(t, uint64(2), commands[0].Idx)
}

func TestRaft_LeaderFailed(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LeaderFailed", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	errCh := leader.Apply([]byte("test"), leader.getConfig().CommitSyncInterval)
	err = <-errCh
	require.Nil(t, err)

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

	oldLeader := leader
	c.partition(leader.ID())
	hasLeader, msg := retry(20, func() (bool, string) {
		sleep()
		leaders := c.getNodesByState(leaderStateType)
		if len(leaders) != 1 {
			return false, fmt.Sprintf("wrong leader num: %d, expect 1", len(leaders))
		}
		if leader = leaders[0]; leader.ID() == oldLeader.ID() {
			return false, "leader not change"
		}
		return true, ""
	})
	require.True(t, hasLeader, msg)
	require.Greater(t, leader.getTerm(), oldLeader.getTerm())

	errCh = oldLeader.Apply([]byte("old"), oldLeader.getConfig().CommitSyncInterval)
	err = <-errCh
	require.NotNil(t, err)
	require.True(t, isErrNotLeader(err))

	errCh = leader.Apply([]byte("new"), leader.getConfig().CommitSyncInterval)
	err = <-errCh
	require.Nil(t, err)

	resumeHeartbeatCh := waitForHeartbeatResumed(leader, oldLeader.ID())
	c.unPartition(oldLeader.ID())

	require.True(t, waitEventSuccessful(resumeHeartbeatCh))

	consistent, msg = c.isConsistent()
	require.True(t, consistent, msg)
}

func TestRaft_BehindFollowerReconnect(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	c, cleanup, err := createTestCluster("BehindFollowerReconnect", 3, conf)
	defer cleanup()
	require.Nil(t, err)

	behindFo := c.getNodesByState(followerStateType)[0]
	c.partition(behindFo.ID())

	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

	leader := c.getNodesByState(leaderStateType)[0]
	err = applyAndCheck(leader, 100, 0, nil)
	require.Nil(t, err)

	c.unPartition(behindFo.ID())

	consistent, msg = retry(3, c.isConsistent)
	require.True(t, consistent, msg)

	require.Nil(t, checkClusterState(c))
}

func TestRaft_AddNonVoter(t *testing.T) {
	t.Parallel()
	conf := defaultTestConfig()
	c, cleanup, err := createTestCluster("AddNonVoter", 3, conf)
	defer cleanup()
	require.Nil(t, err)

	c1, cleanup1, err := createClusterNoBootStrap("AddNonVoter", 1, conf)
	defer cleanup1()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	outsider := c1.getNodesByState(followerStateType)[0]

	require.Equal(t, 3, len(leader.Voters()))
	require.Equal(t, RoleAbsent, leader.membership.getPeer(outsider.ID()))
	waitReplCh := waitForPeerReplication(leader, outsider.ID())
	waitLeaderCh := waitForNewLeader(outsider, "", leader.ID())

	err = leader.AddNonVoter(outsider.ID(), time.Second)
	require.Nil(t, err)
	require.True(t, waitEventSuccessful(waitReplCh))
	require.True(t, waitEventSuccessful(waitLeaderCh))
	require.Equal(t, 3, len(leader.Voters()))
	require.Equal(t, RoleNonVoter, leader.membership.getPeer(outsider.ID()))

	c.add(outsider)
	err = applyAndCheck(leader, 100, 0, nil)
	require.Nil(t, err)
	consistent, msg := retry(3, c.isConsistent)
	require.True(t, consistent, msg)
}

func TestRaft_RemoveFollower(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("RemoveFollower", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	follower0 := c.getNodesByState(followerStateType)[0]
	follower1 := c.getNodesByState(followerStateType)[1]

	eventCh := waitPeerReplicationStop(leader, follower0.ID())
	err = leader.RemovePeer(follower0.ID(), 500*time.Millisecond)
	require.Nil(t, err)

	require.True(t, waitEventSuccessful(eventCh))

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
	c, cleanup, err := createTestCluster("Remove_Rejoin_Follower", 3, nil)
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
	c, cleanup, err := createTestCluster("RemoveFollower_SplitCluster", 4, nil)
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
	c, cleanup, err := createTestCluster("RemoveLeader", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]

	eventCh := waitForNewLeader(leader, leader.ID(), "")
	err = leader.RemovePeer(leader.ID(), 500*time.Millisecond)
	require.Nil(t, err)

	require.True(t, waitEventSuccessful(eventCh))

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
	c, cleanup, err := createTestCluster("RemoveLeader_AndApply", 3, nil)
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
	c, cleanup, err := createTestCluster("AddKnownPeer", 3, nil)
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
	c, cleanup, err := createTestCluster("TestRaft_RemoveUnknownPeer", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	err = leader.RemovePeer("8.8.8.8:8888", 0)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find peer")
}

func TestRaft_DemoteVoter(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("DemoteVoter", 3, nil)
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
	sleep()
	voters = follower0.Voters()
	for _, v := range voters {
		require.NotEqual(t, follower0.ID(), v)
	}
}

func TestRaft_VerifyLeader(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("VerifyLeader", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	err = leader.VerifyLeader(time.Second)
	require.Nil(t, err)
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("VerifyLeader_Single", 1, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	err = leader.VerifyLeader(time.Second)
	require.Nil(t, err)
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("VerifyLeader_Fail", 2, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	follower := c.getNodesByState(followerStateType)[0]

	follower.setTerm(follower.getTerm() + 1)

	hasLeader, msg := retry(3, func() (bool, string) {
		sleep()
		if len(c.getNodesByState(leaderStateType)) != 1 {
			return false, "no leader"
		}
		return true, ""
	})
	require.True(t, hasLeader, msg)

	oldLeader := leader
	require.Equal(t, followerStateType, oldLeader.getStateType())
	require.Equal(t, 1, len(c.getNodesByState(followerStateType)))
	leader = follower
	require.Equal(t, leaderStateType, leader.getStateType())

	err = oldLeader.VerifyLeader(time.Second)
	require.NotNil(t, err)
	require.True(t, isErrNotLeader(err))
}

func TestRaft_VerifyLeader_PartialDisconnect(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("VerifyLeader_PartialDisconnect", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	follower0 := c.getNodesByState(followerStateType)[0]

	c.partition(follower0.ID())
	sleep()
	err = leader.VerifyLeader(500 * time.Millisecond)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "timeout draining error")
}

func TestRaft_UserSnapshot(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("UserSnapshot", 1, nil)
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
	c, cleanup, err := createTestCluster("AutoSnapshot", 1, conf)
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
	c, cleanup, err := createTestCluster("SendLatestSnapshot", 3, conf)
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
	c, cleanup, err := createTestCluster("SendLatestSnapshotAndLogs", 3, conf)
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
			// starvation and delay can occasionally make heartbeat response
			// arrive much later. if LeaderLeaseTimeout is 100ms and HeartbeatTimeout is 500ms
			// hearbeat interval is 50ms, very close to LeaderLeaseTimeout.
			// LeaderLeaseTimeout can be exceeded by some delay and
			// selfVerify will force leader to stepdown. It is more likely to happen in the middle
			// when leader tries to install snapshot on followers
			// before noOp is committed. noOp timeout is the resuting error.
			conf.LeaderLeaseTimeout = 500 * time.Millisecond
			// if we run a lot of subtests in parallel
			// (like 600), 120 available ips are exhausted.
			// NextAvailAddr will block waiting for used ips to be returned.
			c, cleanup, err := createTestCluster(fmt.Sprintf("UserRestore, offset=%d", offset), 3, conf)
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

func TestRaft_LeadershipTransfer_MostCurrentFollower(t *testing.T) {
	t.Parallel()
	type idNextIdx struct {
		id      string
		nextIdx uint64
	}

	cases := []struct {
		name        string
		peers       []*idNextIdx
		mostCurrent string
	}{
		{"one peer", []*idNextIdx{{"a", 9}}, "a"},
		{"two peers", []*idNextIdx{{"a", 9}, {"b", 8}}, "a"},
		{"three peers", []*idNextIdx{{"c", 9}, {"b", 8}, {"a", 8}}, "c"},
		{"four peers", []*idNextIdx{{"a", 7}, {"b", 11}, {"a", 8}}, "b"},
	}
	cluster, cleanup, err := createClusterNoBootStrap("LTransfer_MostCurrentFollower", len(cases), nil)
	defer cleanup()
	require.Nil(t, err)

	rafts := cluster.getNodesByState(followerStateType)
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			raft := rafts[i]
			l := raft.getLeaderState()
			l.l.Lock()
			raft.membership.l.Lock()
			l.replicationMap = map[string]*peerReplication{}
			for _, p := range c.peers {
				raft.membership.latestPeers = append(raft.membership.latestPeers, &Peer{p.id, RoleVoter})
				l.replicationMap[p.id] = &peerReplication{nextIdx: p.nextIdx}
			}
			l.l.Unlock()
			raft.membership.l.Unlock()
			require.Equal(t, c.mostCurrent, l.mostCurrentFollower())
		})
	}
}

func TestRaft_LeadershipTransferInProgress(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LTransferInProgress", 3, nil)
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

func TestRaft_LeadershipTransfer(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LTransfer", 3, nil)
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
	c, cleanup, err := createTestCluster("LTransferWithOneNode", 1, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", time.Second)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find most current follower")
}

func TestRaft_LeadershipTransferWithSevenNodes(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LTransferWithSevenNodes", 7, nil)
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
	c, cleanup, err := createTestCluster("LTransferToInvalidID", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("awesome_peer", time.Second)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "peer awesome_peer is non-voter")
}

func TestRaft_LeadershipTransferToItself(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LTransferToItself", 3, nil)
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
	c, cleanup, err := createTestCluster("LTransferIgnoreNonVoters", 2, nil)
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
	c, cleanup, err := createTestCluster("LTransferIgnoreSpecifiedNonVoter", 3, nil)
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
	c, cleanup, err := createTestCluster("LTransferTimeout", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]

	err = leader.TransferLeadership("", 20*time.Millisecond)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to wait for force-replication response")
}

func TestRaft_LeadershipTransfer_ReplicationCatchup(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("LTransfer_ReplicationCatchup", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	follower0 := c.getNodesByState(followerStateType)[0]

	for i := 0; i < 1000; i++ {
		// apply a lot, skip error checking because
		// leadership transfer will ocurr right in
		// the middle. applyCh and leadershipTransferCh
		// will be racing to fire in main loop.
		leader.Apply([]byte(fmt.Sprintf("test-%d", i)), time.Second)
	}

	err = leader.TransferLeadership(follower0.ID(), time.Second)
	require.Nil(t, err)

	oldLeader := leader
	leader = c.getNodesByState(leaderStateType)[0]
	require.NotEqual(t, oldLeader.ID(), leader.ID())
	lastIdx0, term0 := oldLeader.instate.getLastIdxTerm()
	lastIdx1, term1 := leader.instate.getLastIdxTerm()
	require.Equal(t, lastIdx0, lastIdx1)
	require.Equal(t, term0, term1)
}

func TestRaft_SelfVerifyFail(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("SelfVerifyFail", 2, nil)
	defer cleanup()
	require.Nil(t, err)
	leader := c.getNodesByState(leaderStateType)[0]
	follower := c.getNodesByState(followerStateType)[0]

	partitionTime := time.Now()
	c.partition(follower.ID())

	stepdown, msg := retry(2, func() (bool, string) {
		time.Sleep(leader.getConfig().LeaderLeaseTimeout)
		err := leader.VerifyLeader(time.Second)
		if !isErrNotLeader(err) {
			return false, "not stepdown"
		}
		return true, ""
	})
	require.True(t, stepdown, msg)

	oldLeader := leader
	require.True(t, oldLeader.LastLeaderContact().After(partitionTime))
	require.Zero(t, len(c.getNodesByState(leaderStateType)))

	lastContact := follower.LastLeaderContact()
	sleep()
	// no further contact
	require.Equal(t, lastContact, follower.LastLeaderContact())
}

func TestRaft_Barrier(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("RemoveFollower", 3, nil)
	defer cleanup()
	require.Nil(t, err)

	leader := c.getNodesByState(leaderStateType)[0]
	for i := 0; i < 100; i++ {
		leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	err = leader.Barrier(time.Second)
	require.Nil(t, err)
	consistent, msg := c.isConsistent()
	require.True(t, consistent, msg)

	commands := getRecordCommandState(leader)
	require.Equal(t, 100, len(commands))
}

func TestRaft_ReloadConfig(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("RemoveFollower", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	r := c.getNodesByState(leaderStateType)[0]

	require.Equal(t, uint64(8192), r.getConfig().SnapshotThreshold)
	require.Equal(t, 120*time.Second, r.getConfig().SnapshotInterval)
	require.Equal(t, uint64(10240), r.getConfig().NumTrailingLogs)

	sub := ReloadableSubConfig{
		SnapshotThreshold: 6789,
		SnapshotInterval:  234 * time.Second,
		NumTrailingLogs:   12345,
	}
	require.Nil(t, r.ReloadConfig(sub))
	require.Equal(t, uint64(6789), r.getConfig().SnapshotThreshold)
	require.Equal(t, 234*time.Second, r.getConfig().SnapshotInterval)
	require.Equal(t, uint64(12345), r.getConfig().NumTrailingLogs)
}

func TestRaft_ReloadConfig_Invalid(t *testing.T) {
	t.Parallel()
	c, cleanup, err := createTestCluster("RemoveFollower", 3, nil)
	defer cleanup()
	require.Nil(t, err)
	r := c.getNodesByState(leaderStateType)[0]

	require.Equal(t, uint64(8192), r.getConfig().SnapshotThreshold)
	require.Equal(t, 120*time.Second, r.getConfig().SnapshotInterval)
	require.Equal(t, uint64(10240), r.getConfig().NumTrailingLogs)

	sub := ReloadableSubConfig{
		SnapshotThreshold: 6789,
		SnapshotInterval:  1 * time.Millisecond,
		NumTrailingLogs:   12345,
	}
	require.NotNil(t, r.ReloadConfig(sub))

	require.Equal(t, uint64(8192), r.getConfig().SnapshotThreshold)
	require.Equal(t, 120*time.Second, r.getConfig().SnapshotInterval)
	require.Equal(t, uint64(10240), r.getConfig().NumTrailingLogs)
}
