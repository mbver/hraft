package hraft

type Candidate struct {
	term   uint64
	peers  []string
	raft   *Raft
	cancel *ResetableProtectedChan
}

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

type voteResult struct {
	VoterId  string
	Response *VoteResponse
}

// setup must be done before running election and collecting votes
func (c *Candidate) setupElection() (chan *voteResult, error) {
	c.cancel.Reset()
	voteCh := make(chan *voteResult, len(c.peers))
	if err := c.raft.persistVote(c.term, []byte(c.raft.ID())); err != nil {
		return nil, err
	}
	voteCh <- &voteResult{
		VoterId: c.raft.ID(),
		Response: &VoteResponse{
			Granted: true,
		},
	}
	return voteCh, nil
}

// term and vote handling. and how to send request vote
func (c *Candidate) runElection(voteCh chan *voteResult) {
	// dispatch vote requests to peers
	lastIdx, lastTerm := c.raft.getLastLog()
	req := &VoteRequest{
		Term:        c.raft.getTerm(),
		Candidate:   []byte(c.raft.ID()),
		LastLogIdx:  lastIdx,
		LastLogTerm: lastTerm,
	}

	for _, addr := range c.peers {
		if addr == c.raft.ID() {
			continue
		}
		go func() {
			// need wg??
			res := &voteResult{VoterId: addr}
			err := c.raft.transport.RequestVote(addr, req, res.Response)
			if err != nil {
				c.raft.logger.Error("failed to make requestVote RPC",
					"target", addr,
					"error", err)
				res.Response.Term = req.Term
				res.Response.Granted = false
			}
			voteCh <- res
		}()
	}

	// collecting votes
	voteGranted := 0
	voteNeeded := len(c.peers)/2 + 1
	electionTimeoutCh := jitterTimeoutCh(c.raft.config.ElectionTimeout) // ===== ELECTION TIME OUT IS FROM 150-300 ms
	for voteGranted < voteNeeded {
		select {
		case vote := <-voteCh:
			if vote.Response.Term > c.term {
				c.raft.logger.Debug("newer term discovered, fallback to follower")
				waitCh := c.raft.dispatchTransition(followerStateType, vote.Response.Term)
				<-waitCh
				return
			}
			resp := vote.Response
			if resp.Granted {
				voteGranted++
				c.raft.logger.Debug("vote granted", "from", vote.VoterId, "term", resp.Term, "tally", voteGranted)
			}
			// check vote and increase quorum
		case <-c.cancel.Ch():
			return
		case <-electionTimeoutCh: // quit election, transition back to follower
			waitCh := c.raft.dispatchTransition(followerStateType, c.term)
			<-waitCh
			return
		case <-c.raft.shutdownCh():
			return
		}
	}
	c.raft.logger.Info("election won", "tally", voteGranted)
	waitCh := c.raft.dispatchTransition(leaderStateType, c.term)
	<-waitCh
}
