package hraft

type peerRole int8

type peer struct {
	id   string
	role peerRole
}
type membership struct {
	latest       []*peer
	lastestIdx   uint64
	commited     []*peer
	committedIdx uint64
}
