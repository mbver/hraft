package hraft

type Follower struct {
	raft *Raft
}

func NewFollower(r *Raft) *Follower {
	return &Follower{
		raft: r,
	}
}

func (f *Follower) HandleTransition(trans *Transition) {}

func (f *Follower) HandleHeartbeatTimeout() {}

func (f *Follower) HandleRPC(rpc *RPC) {}

func (f *Follower) HandleApply(a *Apply) {}

func (f *Follower) HandleCommitNotify()
