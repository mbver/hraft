package hraft

import (
	"fmt"
	"time"
)

type Candidate struct {
	term   uint64
	raft   *Raft
	cancel *ResetableProtectedChan
}

type Vote struct{}

func NewCandidate(r *Raft) *Candidate {
	return &Candidate{
		cancel: newResetableProtectedChan(),
		raft:   r,
	}
}

func (c *Candidate) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		if trans.Term < c.term {
			return
		}
		c.cancel.Close()
		c.raft.setTerm(trans.Term)
		c.raft.setStateType(followerStateType)
	case leaderStateType:
		if trans.Term != c.term { // can this happen?
			return
		}
		// win election
		leader := c.raft.getLeaderState()
		leader.term = c.term
		leader.StepUp()
		c.raft.setStateType(leaderStateType)
	}
}

// heartbeat timeout is blocked in candidate state
func (c *Candidate) HandleHeartbeatTimeout() {}

func (c *Candidate) HandleRPC(rpc *RPC) {}

func (c *Candidate) HandleApply(a *Apply) {}

// will never be called
func (c *Candidate) HandleCommitNotify() {}

// term and vote handling. and how to send request vote
func (c *Candidate) runElection() {
	c.cancel.Reset()

	voteCh := make(chan *Vote, c.raft.NumNodes())

	electionTimeoutCh := time.After(c.raft.config.ElectionTimeout)
	enoughVote := false
	for !enoughVote {
		select {
		case vote := <-voteCh:
			fmt.Println("==== vote", vote)
			// check vote and increase quorum
		case <-c.cancel.Ch():
			return // cancel election
		case <-electionTimeoutCh:
			return // cancel election
		case <-c.raft.shutdownCh():
			return
		}
	}

	// win election
	c.raft.transitionCh <- newTransition(leaderStateType, c.term)
}
