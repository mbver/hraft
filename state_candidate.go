package hraft

import (
	"sync"
)

type Candidate struct {
	l      sync.Mutex
	term   uint64
	voters []string
	raft   *Raft
	cancel *ResetableProtectedChan
}

func NewCandidate(r *Raft) *Candidate {
	return &Candidate{
		cancel: newResetableProtectedChan(),
		raft:   r,
	}
}

func (c *Candidate) getTerm() uint64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.term
}

func (c *Candidate) getVoters() []string {
	c.l.Lock()
	defer c.l.Unlock()
	res := make([]string, len(c.voters))
	copy(res, c.voters)
	return res
}

func (c *Candidate) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		if trans.Term < c.getTerm() {
			return
		}
		c.cancel.Close()
		c.raft.setTerm(trans.Term)
		c.raft.setStateType(followerStateType)
		c.raft.heartbeatTimeout.reset()
		logFinishTransition(c.raft.logger, trans, candidateStateType, c.getTerm())
	case leaderStateType:
		if trans.Term != c.getTerm() { // can this happen?
			return
		}
		// win election
		leader := c.raft.getLeaderState()
		leader.setTerm(c.getTerm())
		leader.StepUp()
		c.raft.setStateType(leaderStateType)
		logFinishTransition(c.raft.logger, trans, candidateStateType, c.getTerm())
		c.raft.setLeaderId(c.raft.ID())
	}
}

// heartbeat timeout is blocked in candidate state
func (c *Candidate) HandleHeartbeatTimeout() {}

func (c *Candidate) HandleApply(a *ApplyRequest) {
	trySend(a.errCh, errNotLeader(c.raft.ID()))
}

// will never be called
func (c *Candidate) HandleCommitNotify() {}

func (c *Candidate) HandleMembershipChange(change *membershipChangeRequest) {
	trySend(change.errCh, errNotLeader(c.raft.ID()))
}

func (c *Candidate) HandleRestoreRequest(req *userRestoreRequest) {
	trySend(req.errCh, errNotLeader(c.raft.ID()))
}

func (c *Candidate) HandleLeadershipTransfer(req *leadershipTransferRequest) {
	trySend(req.errCh, errNotLeader(c.raft.ID()))
}

func (c *Candidate) HandleVerifyLeader(req *verifyLeaderRequest) {
	trySend(req.errCh, errNotLeader(c.raft.ID()))
}

type voteResult struct {
	VoterId  string
	Response *VoteResponse
}

func (c *Candidate) runElection() {
	// setup election
	c.cancel.Reset()
	voteCh := make(chan *voteResult, len(c.getVoters()))
	// don't persist vote here.
	// if we have to transition back to follower
	// we will grant vote to the first valid vote request
	// and persit vote there.
	voteCh <- &voteResult{
		VoterId: c.raft.ID(),
		Response: &VoteResponse{
			Granted: true,
			Term:    c.getTerm(),
		},
	}

	// dispatch vote requests to peers
	lastIdx, lastTerm := c.raft.getLastLog()
	req := &VoteRequest{
		Term:        c.raft.getTerm(),
		Candidate:   []byte(c.raft.ID()),
		LastLogIdx:  lastIdx,
		LastLogTerm: lastTerm,
	}

	for _, addr := range c.getVoters() {
		if addr == c.raft.ID() {
			continue
		}
		go func() {
			res := &voteResult{
				VoterId:  addr,
				Response: &VoteResponse{},
			}
			err := c.raft.transport.RequestVote(addr, req, res.Response)
			if err != nil {
				c.raft.logger.Error("failed to make requestVote RPC",
					"target", addr,
					"error", err)
				res.Response.Term = req.Term
				res.Response.Granted = false
			} else {
				c.raft.logger.Debug(
					"vote response",
					"from", addr,
					"term", res.Response.Term,
					"current_term", c.getTerm(),
					"granted", res.Response.Granted,
				)
			}

			voteCh <- res
		}()
	}

	// collecting votes
	voteGranted := 0
	voteNeeded := len(c.getVoters())/2 + 1
	electionTimeoutCh := jitterTimeoutCh(c.raft.config.ElectionTimeout)
	for voteGranted < voteNeeded {
		select {
		case vote := <-voteCh:
			if vote.Response.Term > c.getTerm() {
				c.raft.logger.Debug("newer term discovered, fallback to follower")
				<-c.raft.dispatchTransition(followerStateType, vote.Response.Term)
				return
			}
			resp := vote.Response
			if resp.Granted {
				voteGranted++
				c.raft.logger.Debug("vote granted", "from", vote.VoterId, "term", resp.Term, "tally", voteGranted)
			}
		case <-c.cancel.Ch():
			c.raft.logger.Debug("election canceled", "term", c.getTerm())
			return
		case <-electionTimeoutCh: // election timeout, transition back to follower. run election again in next heartbeat timeout.
			c.raft.logger.Debug("election timeout, fallback to follower")
			<-c.raft.dispatchTransition(followerStateType, c.getTerm())
			return
		case <-c.raft.shutdownCh():
			return
		}
	}
	// win election, become leader
	c.raft.logger.Info("election won", "tally", voteGranted)
	<-c.raft.dispatchTransition(leaderStateType, c.getTerm())
}
