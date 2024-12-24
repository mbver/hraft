package hraft

import (
	"fmt"
	"time"
)

type Candidate struct {
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

func (c *Candidate) HandleAppendEntries(req *AppendEntriesRequest) {
	c.cancel.Close()
	// transition to follower via transitionCh
}

func (c *Candidate) HandleHeartbeatTimeout() {}

// term and vote handling. and how to send request vote
func (c *Candidate) runElection() {
	c.cancel.Reset()

	voteCh := make(chan *Vote, c.raft.NumNodes())

	electionTimeoutCh := time.After(c.raft.config.ElectionTimeout)
	for {
		select {
		case vote := <-voteCh:
			fmt.Println("==== vote", vote)
			// check vote and increase quorum
		case <-c.cancel.Ch():
			return // cancel election
		case <-electionTimeoutCh:
			return // cancel election
		}
	}
	// win election
	// transtion to leader via transtionCh
}
