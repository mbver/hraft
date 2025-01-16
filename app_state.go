package hraft

import "sync/atomic"

type CommandsApplier interface {
	ApplyCommands([]*Commit)
}

type MembershipApplier interface {
	ApplyMembership(*Commit)
}

type AppState struct {
	mutateCh        chan []*Commit
	batchSize       int
	lastAppliedIdx  uint64
	lastAppliedTerm uint64
	commandState    CommandsApplier
	membershipState MembershipApplier
	stop            *ProtectedChan
	doneCh          chan struct{}
}

func NewAppState(command CommandsApplier, membership MembershipApplier, batchSize int) *AppState {
	return &AppState{
		mutateCh:        make(chan []*Commit, 128),
		batchSize:       batchSize,
		commandState:    command,
		membershipState: membership,
		stop:            newProtectedChan(),
		doneCh:          make(chan struct{}, 1),
	}
}

func (a *AppState) setLastApplied(idx, term uint64) {
	atomic.StoreUint64(&a.lastAppliedIdx, idx)
	atomic.StoreUint64(&a.lastAppliedTerm, term)
}

func (a *AppState) getLastApplied() (idx, term uint64) {
	return atomic.LoadUint64(&a.lastAppliedIdx), atomic.LoadUint64(&a.lastAppliedTerm)
}

func (a *AppState) Stop() {
	a.stop.Close()
	<-a.doneCh
}

func (a *AppState) receiveMutations() {
	for {
		select {
		case commits := <-a.mutateCh:
			batch := make([]*Commit, 0, a.batchSize)
			for _, c := range commits {
				if c.Log.Type == LogCommand {
					batch = append(batch, c)
					if len(batch) == a.batchSize {
						a.commandState.ApplyCommands(batch)
						a.setLastApplied(c.Log.Idx, c.Log.Term)
						batch = make([]*Commit, 0, a.batchSize)
					}
				}
				if c.Log.Type == LogMembership {
					a.membershipState.ApplyMembership(c)
					a.setLastApplied(c.Log.Idx, c.Log.Term)
				}
			}
			if len(batch) > 0 {
				a.commandState.ApplyCommands(batch)
				last := batch[len(batch)-1]
				a.setLastApplied(last.Log.Idx, last.Log.Term)
			}
		case <-a.stop.ch:
			a.doneCh <- struct{}{}
			return
		}
	}
}
