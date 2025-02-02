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
		term := f.raft.getTerm()
		if trans.Term <= term {
			return
		}
		f.raft.logger.Info("transitioning to candidate", "transition", trans.String())
		f.raft.setTerm(trans.Term)
		candidate := f.raft.getCandidateState()
		candidate.l.Lock()
		candidate.term = trans.Term
		candidate.voters = f.raft.Voters()
		candidate.l.Unlock()
		go candidate.runElection()
		f.raft.setStateType(candidateStateType)
		logFinishTransition(f.raft.logger, trans, followerStateType, term)
	case followerStateType:
		term := f.raft.getTerm()
		if trans.Term > term {
			f.raft.setTerm(trans.Term)
			logFinishTransition(f.raft.logger, trans, followerStateType, term)
		}
	}
}

func (f *Follower) HandleHeartbeatTimeout() {
	f.raft.heartbeatTimeout.block()
	term := f.raft.getTerm() + 1
	f.raft.logger.Info("heartbeat timeout, transition to candidate")
	<-f.raft.dispatchTransition(candidateStateType, term)
}

func (f *Follower) HandleApply(a *Apply) {
	trySend(a.errCh, ErrNotLeader)
}

func (f *Follower) HandleCommitNotify() {}

func (f *Follower) HandleMembershipChange(change *membershipChange) {
	trySend(change.errCh, ErrNotLeader)
}

func (f *Follower) HandleRestoreRequest(req *userRestoreRequest) {
	trySend(req.errCh, ErrNotLeader)
}

func (f *Follower) HandleLeadershipTransfer(req *leadershipTransfer) {
	trySend(req.errCh, ErrNotLeader)
}
