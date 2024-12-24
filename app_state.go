package hraft

type Applier interface {
	Apply([]*Commit)
}
type AppState struct {
	mutateCh chan []*Commit
	state    Applier
}
