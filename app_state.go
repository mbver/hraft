package hraft

type AppState struct {
	mutateCh chan []*Commit
}
