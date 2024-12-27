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
		candidate := f.raft.getCandidateState()
		candidate.term = trans.Term
		go candidate.runElection()
		f.raft.setStateType(candidateStateType)
	case followerStateType:
		f.raft.instate.setTerm(trans.Term)
	}
}

func (f *Follower) HandleHeartbeatTimeout() {
	f.raft.heartbeatTimeout.block()
	term := f.raft.instate.incrementTerm()
	waitCh := f.raft.dispatchTransition(candidateStateType, term)
	<-waitCh
}

func (f *Follower) HandleRPC(rpc *RPC) {}

func (f *Follower) HandleApply(a *Apply) {}

func (f *Follower) HandleCommitNotify() {}
