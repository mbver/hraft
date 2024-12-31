package hraft

type Follower struct {
	raft *Raft
}

func NewFollower(r *Raft) *Follower {
	return &Follower{
		raft: r,
	}
}

func (f *Follower) HandleTransition(trans *Transition) {
	switch trans.To {
	case candidateStateType:
		if trans.Term <= f.raft.getTerm() {
			return
		}
		f.raft.setTerm(trans.Term)
		candidate := f.raft.getCandidateState()
		candidate.term = trans.Term
		candidate.voters = f.raft.Voters()
		voteCh, err := candidate.setupElection()
		if err != nil {
			f.raft.logger.Error("failed to setup election for candidate. transition failed", "error", err)
			return
		}
		go candidate.runElection(voteCh)
		f.raft.setStateType(candidateStateType)
	case followerStateType:
		if trans.Term > f.raft.getTerm() {
			f.raft.setTerm(trans.Term)
		}
	}
}

func (f *Follower) HandleHeartbeatTimeout() {
	f.raft.heartbeatTimeout.block()
	term := f.raft.getTerm() + 1
	waitCh := f.raft.dispatchTransition(candidateStateType, term)
	<-waitCh
}

func (f *Follower) HandleRPC(rpc *RPC) {}

func (f *Follower) HandleApply(a *Apply) {}

func (f *Follower) HandleCommitNotify() {}

func (f *Follower) HandleMembershipChange(change *membershipChange) {} // return an error??
