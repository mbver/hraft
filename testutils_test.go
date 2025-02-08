package hraft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/mbver/mlist/testaddr"
)

const (
	testObserveTimeout   = 5 * time.Millisecond
	testWaitEventTimeout = 5 * time.Second
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
		Level:  hclog.Debug,
	})
}

func newTestTransport(addr string) (*NetTransport, error) {
	config := testTransportConfigFromAddr(addr)
	logger := newTestLogger(fmt.Sprintf("transport:%s", addr))
	return NewNetTransport(config, logger, nil)
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
	trans, err := NewNetTransport(config, logger, nil)
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

func defaultTestConfig() *Config {
	return &Config{
		ElectionTimeout:    100 * time.Millisecond,
		HeartbeatTimeout:   100 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitSyncInterval: 20 * time.Millisecond,
		MaxAppendEntries:   64,
		SnapshotThreshold:  8192,
		SnapshotInterval:   120 * time.Second,
		NumTrailingLogs:    10240,
	}
}

type cluster struct {
	wg            sync.WaitGroup
	rafts         map[string]*Raft
	closed        bool
	connGetterMap map[string]*BlockableConnGetter
}

func (c *cluster) add(raft *Raft) {
	c.rafts[raft.ID()] = raft
	c.wg.Add(1)
}

func (c *cluster) remove(addr string) {
	delete(c.rafts, addr)
}

func (c *cluster) getConnGetter(addr string) *BlockableConnGetter {
	return c.connGetterMap[addr]
}

func isInList(addr string, addrs []string) bool {
	for _, a := range addrs {
		if a == addr {
			return true
		}
	}
	return false
}

// partition isolates specified nodes from the cluster.
// this split the cluster into 2 partitions.
// the nodes in a partition are still connected.
func (c *cluster) partition(addrs ...string) {
	for addr0 := range c.connGetterMap {
		if !isInList(addr0, addrs) {
			for _, addr1 := range addrs {
				c.disconnect(addr0, addr1)
			}
		}
	}
	for _, addr := range addrs {
		r := c.rafts[addr]
		r.logger.Debug("[PARTITION], closed all received conns", "id", r.ID())
		// this is to stop the running pipeline and force leader
		// to switch back to replicate mode and use the blocked ConnGetter
		r.transport.ClearReceivedConns()
	}
}

func (c *cluster) disconnect(addr1, addr2 string) {
	getConn1, ok := c.connGetterMap[addr1]
	if ok {
		getConn1.block(addr2)
	}
	getConn2, ok := c.connGetterMap[addr2]
	if ok {
		getConn2.block(addr1)
	}
}

// unPartition takes a list of nodes in a partion
// and reconnect them with other nodes in the cluster
func (c *cluster) unPartition(addrs ...string) {
	for addr0 := range c.connGetterMap {
		if !isInList(addr0, addrs) {
			for _, addr1 := range addrs {
				c.reconnect(addr0, addr1)
			}
		}
	}
}

func (c *cluster) reconnect(addr1, addr2 string) {
	getConn1, ok := c.connGetterMap[addr1]
	if ok {
		getConn1.unblock(addr2)
	}
	getConn2, ok := c.connGetterMap[addr2]
	if ok {
		getConn2.unblock(addr1)
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
			baseDir := filepath.Dir(raft.snapstore.dir)
			os.RemoveAll(baseDir)
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

func peersToString(peers []*Peer) string {
	buf := &strings.Builder{}
	buf.WriteRune('[')
	for i, p := range peers {
		if i == 0 {
			buf.WriteRune('{')
		} else {
			buf.WriteString(",{")
		}
		buf.WriteString(fmt.Sprintf("Id: %s, role: %s", p.ID, p.Role.String()))
		buf.WriteString("}")
	}
	buf.WriteRune(']')
	return buf.String()
}

func stateToString(state []*Log) string {
	buf := &strings.Builder{}
	buf.WriteRune('[')
	for _, l := range state {
		buf.WriteString("\n{")
		data := string(l.Data)
		if l.Type == LogMembership {
			peers := []*Peer{}
			err := decode(l.Data, &peers)
			if err != nil {
				panic(err)
			}
			data = peersToString(peers)
		}
		buf.WriteString(fmt.Sprintf("idx:%d, term: %d, data: %s", l.Idx, l.Term, data))
		buf.WriteString("},")
	}
	buf.WriteString("\n]")
	return buf.String()
}

// this is used for debugging when tests fail by consistency check!
func dumpState(raft *Raft) {
	buf := &strings.Builder{}
	buf.WriteString(fmt.Sprintf("%s:\n", raft.ID()))
	buf.WriteString("command state:")
	commands := getRecordCommandState(raft)
	str := stateToString(commands)
	buf.WriteString(str)
	buf.WriteString("\nmembershitp state:")
	members := getRecordMembershipState(raft)
	str = stateToString(members)
	buf.WriteString(str)
	buf.WriteString("\n")
	fmt.Println(buf.String())
}

func (c *cluster) isConsistent() (bool, string) {
	sleep()
	isFirst := true
	var first *Raft
	var firstCommands []*Log
	for _, raft := range c.rafts {
		if isFirst {
			isFirst = false
			first = raft
			firstCommands = getRecordCommandState(raft)
			continue
		}
		state := getRecordCommandState(raft)
		if !compareStates(firstCommands, state) {
			return false,
				fmt.Sprintf("command_state unmatched. \n%s:%s ---------- \n%s:%s",
					first.ID(), stateToString(firstCommands),
					raft.ID(),
					stateToString(state),
				)
		}
	}
	firstMembership := getRecordMembershipState(first)
	for id, raft := range c.rafts {
		if id == first.ID() {
			continue
		}
		state := getRecordMembershipState(raft)
		if !compareStates(firstMembership, state) {
			return false,
				fmt.Sprintf("membership_state unmatched. \n%s:%s \n---------- \n%s:%s",
					first.ID(), stateToString(firstMembership),
					raft.ID(),
					stateToString(state),
				)
		}
	}
	return true, ""
}

type recordCommandState struct {
	l        sync.Mutex
	commands []*Log
}

func (a *recordCommandState) ApplyCommands(commits []*Commit) {
	a.l.Lock()
	defer a.l.Unlock()
	for _, c := range commits {
		a.commands = append(a.commands, c.Log)
		trySend(c.ErrCh, nil)
	}
}

func (a *recordCommandState) BatchSize() int {
	return 1
}

func (a *recordCommandState) WriteToSnapshot(snap *Snapshot) error {
	a.l.Lock()
	defer a.l.Unlock()
	if len(a.commands) == 0 {
		return nil
	}
	data, err := encode(a.commands)
	if err != nil {
		return err
	}
	n, err := snap.Write(data)
	if n != len(data) {
		return fmt.Errorf("missing state data writing to snapshot: %d/%d, error: %w", n, len(data), err)
	}
	return err
}

func (a *recordCommandState) Restore(source io.ReadCloser) error {
	a.l.Lock()
	defer a.l.Unlock()
	defer source.Close()
	dec := codec.NewDecoder(source, &codec.MsgpackHandle{})
	a.commands = nil
	return dec.Decode(&a.commands)
}

func getRecordCommandState(r *Raft) []*Log {
	state := r.appstate.commandState.(*recordCommandState)
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

type BlockableConnGetter struct {
	l       sync.Mutex
	net     *NetTransport
	blocked map[string]bool
}

func newBlockableConnGetter() *BlockableConnGetter {
	return &BlockableConnGetter{
		blocked: map[string]bool{},
	}
}
func (b *BlockableConnGetter) GetConn(addr string) (*peerConn, error) {
	b.l.Lock()
	blocked := b.blocked[addr]
	b.l.Unlock()
	if !blocked {
		return b.net.getConn(addr)
	}
	return nil, fmt.Errorf("transport to %s is blocked", addr)
}

func (b *BlockableConnGetter) SetTransport(net *NetTransport) {
	b.net = net
}

func (b *BlockableConnGetter) block(addr string) {
	b.l.Lock()
	b.blocked[addr] = true
	b.l.Unlock()
}

func (b *BlockableConnGetter) unblock(addr string) {
	b.l.Lock()
	b.blocked[addr] = false
	b.l.Unlock()
}

func testSnapStoreFromAddr(addr string) (*SnapshotStore, error) {
	// num retains? loggers? baseDir??
	baseDir, err := os.MkdirTemp("", "snapstore_")
	if err != nil {
		return nil, err
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("snapstore: %s:", addr),
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})
	return NewSnapshotStore(baseDir, 3, logger)
}

func createTestNodeFromAddr(testName string, addr string, conf *Config) (*Raft, *BlockableConnGetter, error) {
	b := &RaftBuilder{}

	if conf == nil {
		conf = defaultTestConfig()
	}
	conf.LocalID = addr
	b.WithConfig(conf)

	b.WithLogStore(newInMemLogStore())
	b.WithKVStore(newInMemKVStore())

	snapstore, err := testSnapStoreFromAddr(addr)
	if err != nil {
		return nil, nil, err
	}
	b.WithSnapStore(snapstore)

	logPrefix := fmt.Sprintf("%s: %s", testName, addr)
	b.WithLogger(newTestLogger(logPrefix))
	b.WithTransportConfig(testTransportConfigFromAddr(addr))
	connGetter := newBlockableConnGetter()
	b.WithConnGetter(connGetter)

	b.WithAppState(NewAppState(&recordCommandState{}, &recordMembershipApplier{}))

	raft, err := b.Build()
	if err != nil {
		return nil, nil, err
	}
	return raft, connGetter, nil
}

// used for debug in parallel tests
func errWithAddr(addr string, err error) error {
	return fmt.Errorf("%s:%w", addr, err)
}

func createTestCluster(testname string, n int, conf *Config) (*cluster, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		addresses[i] = addrSource.next()
	}
	addr0 := addresses[0] // used to debug in parallel tests
	cluster := &cluster{
		rafts:         map[string]*Raft{},
		connGetterMap: map[string]*BlockableConnGetter{},
	}
	cleanup := combineCleanup(cluster.close, addrSource.cleanup)
	var first *Raft
	for i, addr := range addresses {
		raft, connGetter, err := createTestNodeFromAddr(testname, addr, conf)
		if err != nil {
			return nil, cleanup, errWithAddr(addr0, err)
		}
		cluster.add(raft)
		cluster.connGetterMap[raft.ID()] = connGetter
		if i == 0 {
			first = raft
			eventCh := waitForNewLeader(raft, "", raft.ID())
			if err := first.Bootstrap(); err != nil {
				return nil, cleanup, errWithAddr(addr0, fmt.Errorf("bootstrap: %w", err))
			}
			if !waitEventSuccessful(eventCh) {
				return nil, cleanup, errWithAddr(addr0, fmt.Errorf("failed to transition to leader in bootstrap"))
			}
			continue
		}
		waitReplCh := waitForPeerReplication(first, raft.ID())
		waitLeaderCh := waitForNewLeader(raft, "", first.ID())
		if err := first.AddVoter(addr, 200*time.Millisecond); err != nil {
			return nil, cleanup, errWithAddr(addr0, fmt.Errorf("addvoter: %w", err))
		}
		if !waitEventSuccessful(waitReplCh) {
			return nil, cleanup, errWithAddr(addr0, fmt.Errorf("failed to start replication to peer %s", raft.ID()))
		}
		if !waitEventSuccessful(waitLeaderCh) {
			return nil, cleanup, errWithAddr(addr0, fmt.Errorf("failed to have correct leader, expect: %s, got: %s", first.ID(), raft.GetLeaderId()))
		}
		success, msg := retry(10, func() (bool, string) {
			time.Sleep(100 * time.Millisecond)
			if raft.membership.isLocalVoter() {
				return true, ""
			}
			return false, "unable to become voter: " + raft.ID()
		})
		if !success {
			return nil, cleanup, errWithAddr(addr0, fmt.Errorf("%s", msg))
		}
	}
	sleep()
	if err := checkClusterState(cluster); err != nil {
		return nil, cleanup, errWithAddr(addr0, err)
	}
	return cluster, cleanup, nil
}

func createClusterNoBootStrap(testName string, n int, conf *Config) (*cluster, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		addresses[i] = addrSource.next()
	}
	cluster := &cluster{
		rafts:         map[string]*Raft{},
		connGetterMap: map[string]*BlockableConnGetter{},
	}
	cleanup := combineCleanup(cluster.close, addrSource.cleanup)
	for _, addr := range addresses {
		raft, _, err := createTestNodeFromAddr(testName, addr, conf)
		if err != nil {
			return nil, cleanup, err
		}
		cluster.add(raft)
	}
	return cluster, cleanup, nil
}

func createTestNode(testName string, conf *Config) (*Raft, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addr := addrSource.next()
	raft, _, err := createTestNodeFromAddr(testName, addr, conf)
	if err != nil {
		return nil, addrSource.cleanup, err
	}
	cleanup := combineCleanup(raft.Shutdown, addrSource.cleanup)
	return raft, cleanup, err
}

func drainAndCheckErr(errCh chan error, wantErr error, n int, timeout time.Duration) error {
	timeoutCh := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case err := <-errCh:
			if !errors.Is(err, wantErr) {
				return err
			}
		case <-timeoutCh:
			return fmt.Errorf("timeout drain and check error %d/%d received", i, n)
		}
	}
	return nil
}

func applyAndCheck(leader *Raft, n int, offset int, wantErr error) error {
	collectErrCh := make(chan error, 10)
	errChs := make([]chan error, n)
	for i := 0; i < n; i++ {
		errCh := leader.Apply([]byte(fmt.Sprintf("test %d", i+offset)), 0)
		errChs[i] = errCh
	}
	go func() {
		for i, errCh := range errChs {
			err := <-errCh
			if err != nil {
				err = fmt.Errorf("errApply, i=%d: %w", i, err)
			}
			collectErrCh <- err
		}
	}()
	return drainAndCheckErr(collectErrCh, wantErr, n, 5*time.Second)
}

func checkClusterState(c *cluster) error {
	// highly concurrent tests can exhaust CPUs and the leader may fail to heartbeat.
	// the delay after cluster created and the check may be enough that the cluster state
	// is changed by the time of checking. sometimes, there is no leader and sometimes no followers!
	nLeader := len(c.getNodesByState(leaderStateType))
	nFollower := len(c.getNodesByState(followerStateType))
	nCandidate := len(c.getNodesByState(candidateStateType))
	if nLeader == 1 && nFollower == len(c.rafts)-1 && nCandidate == 0 {
		return nil
	}
	addr := ""
	for _, r := range c.rafts {
		addr = r.ID()
		break
	}
	return fmt.Errorf(
		"unexpected cluster %s state: leader: %d, follower: %d, candidate: %d",
		addr, nLeader, nFollower, nCandidate,
	)
}

func runAndCollectErr(fn func() error, errCh chan error) {
	errCh <- fn()
}

func waitForEvent(r *Raft, acceptFn func(e RaftEvent) bool) chan RaftEvent {
	eventCh := make(chan RaftEvent, 5)
	observer, _ := NewObserver(eventCh, testObserveTimeout, acceptFn)
	r.RegisterObserver(observer)
	return eventCh
}

func waitForNewLeader(r *Raft, oldLeader, newLeader string) chan RaftEvent {
	return waitForEvent(r, func(e RaftEvent) bool {
		change, ok := e.(LeaderChangeEvent)
		if !ok {
			return false
		}
		return change.RaftID == r.ID() && change.OldLeader == oldLeader && change.NewLeader == newLeader
	})
}

func waitForPeerReplication(r *Raft, peer string) chan RaftEvent {
	return waitForEvent(r, func(e RaftEvent) bool {
		repl, ok := e.(PeerReplicationEvent)
		if !ok {
			return false
		}
		return repl.Peer == peer && repl.Active
	})
}

func waitPeerReplicationStop(r *Raft, peer string) chan RaftEvent {
	return waitForEvent(r, func(e RaftEvent) bool {
		repl, ok := e.(PeerReplicationEvent)
		if !ok {
			return false
		}
		return repl.Peer == peer && !repl.Active
	})
}

func waitEventSuccessful(ch chan RaftEvent) bool {
	select {
	case <-ch:
		return true
	case <-time.After(testWaitEventTimeout):
		return false
	}
}
