package hraft

type CommandsApplier interface {
	ApplyCommands([]*Commit)
}

type MembershipApplier interface {
	ApplyMembership(*Commit)
}

type AppState struct {
	mutateCh        chan []*Commit
	batchSize       int
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
						batch = make([]*Commit, 0, a.batchSize)
					}
				}
				if c.Log.Type == LogMembership {
					a.membershipState.ApplyMembership(c)
				}
			}
			if len(batch) > 0 {
				a.commandState.ApplyCommands(batch)
			}
		case <-a.stop.ch:
			a.doneCh <- struct{}{}
			return
		}
	}
}
