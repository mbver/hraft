package hraft

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/mbver/mlist/testaddr"
)

// utils for testing
type testAddressesWithSameIP struct {
	ip      net.IP
	port    int
	cleanup func()
}

func newTestAddressesWithSameIP() *testAddressesWithSameIP {
	ip, cleanup := testaddr.BindAddrs.NextAvailAddr()
	return &testAddressesWithSameIP{
		ip:      ip,
		port:    7944,
		cleanup: cleanup,
	}
}

func (a *testAddressesWithSameIP) next() string {
	a.port++
	return net.JoinHostPort(a.ip.String(), strconv.Itoa(a.port))
}

func testTransportConfigFromAddr(addr string) *NetTransportConfig {
	return &NetTransportConfig{
		BindAddr:     addr,
		Timeout:      500 * time.Millisecond,
		TimeoutScale: DefaultTimeoutScale,
		PoolSize:     2,
	}
}

func newTestLogger(name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   name,
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})
}

type PartionableTransport struct {
	net *NetTransport
}

func (t *PartionableTransport) block(addr string) {}

func (t *PartionableTransport) unblock(addr string) {}

func (t *PartionableTransport) AppendEntries(addr string, req *AppendEntriesRequest, res *AppendEntriesResponse) error {
	return t.net.AppendEntries(addr, req, res)
}

func (t *PartionableTransport) RequestVote(addr string, req *VoteRequest, res *VoteResponse) error {
	return t.net.RequestVote(addr, req, res)
}

func (t *PartionableTransport) Close() {
	t.net.Close()
}

func (t *PartionableTransport) HeartbeatCh() chan *RPC {
	return t.net.HeartbeatCh()
}

func (t *PartionableTransport) RpcCh() chan *RPC {
	return t.net.RpcCh()
}

func newPartionableTransport(addr string, logger hclog.Logger) (*PartionableTransport, error) {
	config := testTransportConfigFromAddr(addr)
	netTrans, err := NewNetTransport(config, logger)
	if err != nil {
		return nil, err
	}
	return &PartionableTransport{
		net: netTrans,
	}, err
}

func newTestTransport(addr string) (*NetTransport, error) {
	config := testTransportConfigFromAddr(addr)
	logger := newTestLogger(fmt.Sprintf("transport:%s", addr))
	return NewNetTransport(config, logger)
}

func combineCleanup(cleanups ...func()) func() {
	return func() {
		for _, f := range cleanups {
			f()
		}
	}
}

func newTestTransportWithLogger(logger hclog.Logger) (*NetTransport, func(), error) {
	addresses := newTestAddressesWithSameIP()
	cleanup1 := addresses.cleanup
	addr := addresses.next()
	config := testTransportConfigFromAddr(addr)
	trans, err := NewNetTransport(config, logger)
	if err != nil {
		return nil, cleanup1, err
	}
	return trans, combineCleanup(cleanup1, trans.Close), nil
}

func twoTestTransport() (*NetTransport, *NetTransport, func(), error) {
	addresses := newTestAddressesWithSameIP()
	cleanup1 := addresses.cleanup
	addr1 := addresses.next()
	trans1, err := newTestTransport(addr1)
	if err != nil {
		return nil, nil, cleanup1, err
	}
	cleanup2 := combineCleanup(cleanup1, trans1.Close)
	addr2 := addresses.next()
	trans2, err := newTestTransport(addr2)
	if err != nil {
		return nil, nil, cleanup2, err
	}
	return trans1, trans2, combineCleanup(cleanup2, trans2.Close), nil
}

func getTestAppendEntriesRequestResponse(leader string) (*AppendEntriesRequest, *AppendEntriesResponse) {
	req := &AppendEntriesRequest{
		Term:        10,
		Leader:      []byte(leader),
		PrevLogIdx:  100,
		PrevLogTerm: 4,
		Entries: []*Log{
			{
				Idx:  101,
				Term: 4,
				Type: LogNoOp,
			},
		},
		LeaderCommitIdx: 90,
	}
	resp := &AppendEntriesResponse{
		Term:               4,
		LastLogIdx:         90,
		Success:            true,
		PrevLogCheckFailed: false,
	}
	return req, resp
}

func defaultTestConfig(addr string) *Config {
	return &Config{
		LocalID:            addr,
		ElectionTimeout:    100 * time.Millisecond,
		HeartbeatTimeout:   100 * time.Millisecond,
		CommitSyncInterval: 20 * time.Millisecond,
		MaxAppendEntries:   64,
	}
}

type cluster struct {
	wg     sync.WaitGroup
	rafts  []*Raft
	closed bool
}

func (c *cluster) add(raft *Raft) {
	c.rafts = append(c.rafts, raft)
	c.wg.Add(1)
}

func (c *cluster) remove(addr string) {
	for i, raft := range c.rafts {
		if raft.ID() == addr {
			c.rafts = append(c.rafts[:i], c.rafts[i+1:]...)
			c.wg.Done()
		}
	}
}
func (c *cluster) close() {
	if c.closed {
		return
	}
	c.closed = true
	for _, raft := range c.rafts {
		go func() {
			defer c.wg.Done()
			raft.Shutdown()
		}()
	}
	c.wg.Wait()
}

func (c *cluster) getNodesByState(state RaftStateType) []*Raft {
	res := []*Raft{}
	for _, r := range c.rafts {
		if r.getStateType() == state {
			res = append(res, r)
		}
	}
	return res
}

func logsEqual(log1, log2 *Log) bool {
	return log1.Idx == log2.Idx &&
		log1.Term == log2.Term &&
		log1.Type == log2.Type &&
		bytes.Equal(log1.Data, log2.Data) &&
		log1.DispatchedAt.Equal(log2.DispatchedAt)
}

func compareStates(state1, state2 []*Log) bool {
	if len(state1) != len(state2) {
		return false
	}
	for i := range state1 {
		if !logsEqual(state1[i], state2[i]) {
			return false
		}
	}
	return true
}

func (c *cluster) isConsistent() bool {
	firstCommands := getRecordCommandState(c.rafts[0])
	for _, raft := range c.rafts[1:] {
		state := getRecordCommandState(raft)
		if !compareStates(firstCommands, state) {
			return false
		}
	}
	firstMembership := getRecordMembershipState(c.rafts[0])
	for _, raft := range c.rafts[1:] {
		state := getRecordMembershipState(raft)
		if !compareStates(firstMembership, state) {
			return false
		}
	}
	return true
}

func logsToString(logs []*Log) string {
	buf := strings.Builder{}
	buf.WriteRune('[')
	for _, log := range logs {
		buf.WriteString(fmt.Sprintf("{%d, %d, %s},", log.Term, log.Idx, log.Type.String()))
	}
	buf.WriteRune(']')
	return buf.String()
}

type recordCommandsApplier struct {
	l        sync.Mutex
	commands []*Log
}

func (a *recordCommandsApplier) ApplyCommands(commits []*Commit) {
	a.l.Lock()
	defer a.l.Unlock()
	for _, c := range commits {
		a.commands = append(a.commands, c.Log)
		trySend(c.ErrCh, nil)
	}
}

func getRecordCommandState(r *Raft) []*Log {
	state := r.appstate.commandState.(*recordCommandsApplier)
	state.l.Lock()
	defer state.l.Unlock()
	return copyLogs(state.commands)
}

func copyLogs(in []*Log) []*Log {
	res := make([]*Log, len(in))
	copy(res, in)
	return res
}

type recordMembershipApplier struct {
	l    sync.Mutex
	logs []*Log
}

func (a *recordMembershipApplier) ApplyMembership(c *Commit) {
	a.l.Lock()
	defer a.l.Unlock()
	a.logs = append(a.logs, c.Log)
	trySend(c.ErrCh, nil)
}

func getRecordMembershipState(r *Raft) []*Log {
	state := r.appstate.membershipState.(*recordMembershipApplier)
	state.l.Lock()
	defer state.l.Unlock()
	return copyLogs(state.logs)
}

func createTestNodeFromAddr(addr string) (*Raft, error) {
	b := &RaftBuilder{}

	conf := defaultTestConfig(addr)
	b.WithConfig(conf)

	b.WithLogStore(newInMemLogStore())
	b.WithKVStore(newInMemKVStore())

	b.WithLogger(newTestLogger(addr))

	trans, err := newPartionableTransport(addr, b.logger)
	if err != nil {
		return nil, err
	}
	b.WithTransport(trans)

	b.WithAppState(NewAppState(&recordCommandsApplier{}, &recordMembershipApplier{}, 1))

	raft, err := b.Build()
	if err != nil {
		return nil, err
	}
	return raft, nil
}

func createTestCluster(n int) (*cluster, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		addresses[i] = addrSource.next()
	}
	cluster := &cluster{}
	cleanup := combineCleanup(cluster.close, addrSource.cleanup)
	var first *Raft
	for i, addr := range addresses {
		raft, err := createTestNodeFromAddr(addr)
		if err != nil {
			return nil, cleanup, err
		}
		cluster.add(raft)
		if i == 0 {
			first = raft
			if err := first.Bootstrap(1000 * time.Millisecond); err != nil {
				return nil, cleanup, err
			}
			continue
		}
		if err := first.AddVoter(addr, 200*time.Millisecond); err != nil {
			return nil, cleanup, err
		}
		success, msg := retry(5, func() (bool, string) {
			time.Sleep(100 * time.Millisecond)
			if raft.membership.isLocalVoter() {
				return true, ""
			}
			return false, "unable to become voter: " + raft.ID()
		})
		if !success {
			return nil, cleanup, fmt.Errorf("%s", msg)
		}
	}
	return cluster, cleanup, nil
}

func retry(n int, f func() (bool, string)) (success bool, msg string) {
	for i := 0; i < n; i++ {
		success, msg = f()
		if success {
			return
		}
	}
	return
}

func createTestNode() (*Raft, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addr := addrSource.next()
	raft, err := createTestNodeFromAddr(addr)
	if err != nil {
		return nil, addrSource.cleanup, err
	}
	cleanup := combineCleanup(raft.Shutdown, addrSource.cleanup)
	return raft, cleanup, err
}
