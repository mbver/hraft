package hraft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
)

const DefaultTimeoutScale = 256 * 1024

type msgType uint8

const (
	appendEntriesMsgType = iota
	requestVoteMsgType
	installSnapshotMsgType
	timeoutNowMsgType
)

func (t msgType) String() string {
	switch t {
	case appendEntriesMsgType:
		return "append-entries"
	case requestVoteMsgType:
		return "request-vote"
	case installSnapshotMsgType:
		return "install-snapshot"
	case timeoutNowMsgType:
		return "timeout-now"
	}
	return "unknown-message"
}

type AppendEntriesRequest struct {
	Term            uint64
	Leader          []byte
	PrevLogIdx      uint64
	PrevLogTerm     uint64
	Entries         []*Log
	LeaderCommitIdx uint64
}

type AppendEntriesResponse struct {
	Term               uint64
	LastLogIdx         uint64
	Success            bool
	PrevLogCheckFailed bool
}

type VoteRequest struct {
	Term        uint64
	Candidate   []byte
	LastLogIdx  uint64
	LastLogTerm uint64
}

type VoteResponse struct {
	Term    uint64
	Granted bool
}
type InstallSnapshotRequest struct {
	Term        uint64
	LastLogIdx  uint64
	LastLogTerm uint64
	Size        int64
	Peers       []*Peer
	MCommitIdx  uint64
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}

type RpcResponse struct {
	Response interface{}
	Error    error
}
type RPC struct {
	command interface{}
	reader  io.Reader
	respCh  chan interface{}
}

type peerConn struct {
	addr string
	conn net.Conn
	w    *bufio.Writer
	enc  *codec.Encoder
	dec  *codec.Decoder
}

func (p *peerConn) Close() error {
	return p.conn.Close()
}

func (p *peerConn) SetDeadline(d time.Duration) error {
	return p.conn.SetDeadline(time.Now().Add(d))
}

func (p *peerConn) SetReadDeadline(d time.Duration) error {
	return p.conn.SetReadDeadline(time.Now().Add(d))
}

func (p *peerConn) SetWritedDeadline(d time.Duration) error {
	return p.conn.SetWriteDeadline(time.Now().Add(d))
}

func (p *peerConn) sendMsg(mType msgType, msg interface{}) error {
	if err := p.w.WriteByte(byte(mType)); err != nil {
		return err
	}

	if err := p.enc.Encode(msg); err != nil {
		return err
	}

	return p.w.Flush()
}

func (p *peerConn) readResp(res interface{}) error {
	return p.dec.Decode(res)
}

type NetTransportConfig struct {
	BindAddr      string
	AdvertiseAddr string
	Timeout       time.Duration
	TimeoutScale  int
	PoolSize      int
}

type NetTransport struct {
	wg            sync.WaitGroup
	config        *NetTransportConfig
	logger        hclog.Logger
	closed        *ProtectedChan
	connPool      map[string][]*peerConn
	poolL         sync.Mutex
	connGetter    ConnGetter
	receivedConns map[net.Conn]struct{}
	receivedConnL sync.Mutex
	// seems no need for context, just use shutdownCh
	listener    net.Listener
	heartbeatCh chan *RPC
	rpcCh       chan *RPC
}

func isValidAdvertiseAddr(a string) error {
	host, port, err := net.SplitHostPort(a)
	if err != nil {
		return err
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return fmt.Errorf("invalid advertise address %s", host)
	}
	return nil
}

func NewNetTransport(config *NetTransportConfig, logger hclog.Logger, connGetter ConnGetter) (*NetTransport, error) {
	l, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, err
	}
	if config.AdvertiseAddr == "" {
		config.AdvertiseAddr = l.Addr().String()
	}
	if err = isValidAdvertiseAddr(config.AdvertiseAddr); err != nil {
		l.Close()
		return nil, err
	}
	t := &NetTransport{
		config:        config,
		logger:        logger,
		closed:        newProtectedChan(),
		connPool:      map[string][]*peerConn{},
		receivedConns: map[net.Conn]struct{}{},
		listener:      l,
		heartbeatCh:   make(chan *RPC), // ======= do we buffer? do we create in raft and use here? or create here and use in raft?
		rpcCh:         make(chan *RPC), // ======= do we buffer?
	}
	if connGetter == nil {
		connGetter = &TransparentConnGetter{}
	}
	connGetter.SetTransport(t)
	t.connGetter = connGetter
	go t.listen()
	return t, nil
}

func (t *NetTransport) LocalAddr() string {
	return t.listener.Addr().String()
}

func (t *NetTransport) AdvertiseAddr() string {
	return t.config.AdvertiseAddr
}

func (t *NetTransport) AppendEntries(addr string, req *AppendEntriesRequest, res *AppendEntriesResponse) error {
	return t.unaryRPC(addr, appendEntriesMsgType, req, res)
}

func (t *NetTransport) RequestVote(addr string, req *VoteRequest, res *VoteResponse) error {
	return t.unaryRPC(addr, requestVoteMsgType, req, res)
}

func (t *NetTransport) unaryRPC(addr string, mType msgType, req interface{}, res interface{}) error {
	conn, err := t.connGetter.GetConn(addr)
	if err != nil {
		return err
	}
	if t.config.Timeout > 0 {
		conn.SetDeadline(t.config.Timeout)
	}
	if err = conn.sendMsg(mType, req); err != nil {
		conn.Close()
		return err
	}
	if err = conn.readResp(res); err != nil {
		conn.Close()
		return err
	}
	t.returnConn(conn)
	return nil
}

func (t *NetTransport) InstallSnapshot(addr string, req *InstallSnapshotRequest, res *InstallSnapshotResponse, data io.Reader) error {
	return t.streamRPC(addr, installSnapshotMsgType, req, req.Size, res, data)
}

func (t *NetTransport) streamRPC(addr string, mType msgType, req interface{}, size int64, res interface{}, data io.Reader) error {
	conn, err := t.connGetter.GetConn(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if t.config.Timeout > 0 {
		timeout := t.config.Timeout
		scaled := timeout * time.Duration(size/int64(t.config.TimeoutScale))
		if scaled > timeout {
			timeout = scaled
		}
		conn.SetDeadline(timeout)
	}
	if err = conn.sendMsg(mType, req); err != nil {
		conn.Close()
		return err
	}

	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	if err = conn.w.Flush(); err != nil {
		return err
	}

	return conn.readResp(res)
}

func (t *NetTransport) HeartbeatCh() chan *RPC {
	return t.heartbeatCh
}

func (t *NetTransport) RpcCh() chan *RPC {
	return t.rpcCh
}

func (t *NetTransport) Close() {
	if t.closed.IsClosed() {
		return
	}
	t.closed.Close()
	t.listener.Close()
	t.ClearReceivedConns()
	t.ClearPool()
	t.wg.Wait()
}

func (t *NetTransport) ClearReceivedConns() {
	t.receivedConnL.Lock()
	defer t.receivedConnL.Unlock()
	for conn := range t.receivedConns {
		conn.Close()
	}
}

func (t *NetTransport) ClearPool() {
	t.poolL.Lock()
	defer t.poolL.Unlock()

	for k, conns := range t.connPool {
		for _, conn := range conns {
			conn.Close()
		}
		delete(t.connPool, k)
	}
}

func (t *NetTransport) getPooledConn(id string) *peerConn {
	t.poolL.Lock()
	defer t.poolL.Unlock()

	conns, ok := t.connPool[id]
	if !ok || len(conns) == 0 {
		return nil
	}

	n := len(conns)
	conn := conns[n-1]
	conns[n-1] = nil
	t.connPool[id] = conns[:n-1]
	return conn
}

func (t *NetTransport) connectPeer(addr string) (*peerConn, error) {
	conn, err := net.DialTimeout("tcp", addr, t.config.Timeout)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(conn)
	return &peerConn{
		addr: addr,
		conn: conn,
		w:    w,
		enc:  codec.NewEncoder(w, &codec.MsgpackHandle{}),
		dec:  codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
	}, nil
}

// this interface assists testing
// where we need to partion network
type ConnGetter interface {
	GetConn(addr string) (*peerConn, error)
	SetTransport(t *NetTransport)
}

// for real-use, don't interfere
// with network connection management
type TransparentConnGetter struct {
	net *NetTransport
}

func (g *TransparentConnGetter) GetConn(addr string) (*peerConn, error) {
	return g.net.getConn(addr)
}

func (t *TransparentConnGetter) SetTransport(net *NetTransport) {
	t.net = net
}

func (t *NetTransport) getConn(addr string) (*peerConn, error) {
	if conn := t.getPooledConn(addr); conn != nil {
		return conn, nil
	}
	return t.connectPeer(addr)
}

func (t *NetTransport) returnConn(conn *peerConn) {
	t.poolL.Lock()
	defer t.poolL.Unlock()

	addr := conn.addr
	conns := t.connPool[addr]

	if t.closed.IsClosed() || len(conns) == t.config.PoolSize {
		conn.Close()
		return
	}
	t.connPool[addr] = append(conns, conn)
}

func (t *NetTransport) listen() {
	t.wg.Add(1)
	defer t.wg.Done()
	backoff := newBackoff(5*time.Millisecond, time.Second)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			backoff.next()
			select {
			case <-t.closed.Ch():
				return
			case <-time.After(backoff.getValue()):
				t.logger.Error("failed to accept connection", "error", err)
				continue
			}
		}
		t.receivedConnL.Lock()
		t.receivedConns[conn] = struct{}{}
		t.receivedConnL.Unlock()
		backoff.reset()
		t.logger.Debug("accepted connection", "local-address", t.LocalAddr(), "remote-addr", conn.RemoteAddr().String())

		go t.handleConn(conn)
	}
}

func (t *NetTransport) handleConn(conn net.Conn) {
	t.wg.Add(1)
	defer t.wg.Done()
	defer func() {
		conn.Close()
		t.receivedConnL.Lock()
		delete(t.receivedConns, conn)
		t.receivedConnL.Unlock()
	}()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-t.closed.Ch():
			t.logger.Debug("handle conn: transport shutdown")
			return
		default:
		}
		if err := t.handleMessage(r, dec, enc); err != nil {
			if err != io.EOF {
				t.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			t.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

func isHeartbeat(req *AppendEntriesRequest) bool {
	return req.Term != 0 && req.Leader != nil && // SEEMS UNNECESSARY
		req.PrevLogIdx == 0 && req.PrevLogTerm == 0 &&
		len(req.Entries) == 0 && req.LeaderCommitIdx == 0
}

func (t *NetTransport) handleMessage(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	// Get the rpc type
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	mType := msgType(b)
	switch mType {
	case appendEntriesMsgType:
		return t.handleAppendEntries(dec, enc)
	case requestVoteMsgType:
		return t.handleRequestVote(dec, enc)
	case installSnapshotMsgType:
		return t.handleInstallSnapshot(r, dec, enc)
	case timeoutNowMsgType:
		return t.handleRpcTimeoutNow(dec, enc)
	}
	return fmt.Errorf("unknown msg type %s", mType)
}

func newRPC(command interface{}) *RPC {
	return &RPC{
		respCh:  make(chan interface{}, 1),
		command: command,
	}
}

func (t *NetTransport) handleAppendEntries(dec *codec.Decoder, enc *codec.Encoder) error {
	var req AppendEntriesRequest
	if err := dec.Decode(&req); err != nil {
		return err
	}
	rpcCh := t.rpcCh
	if isHeartbeat(&req) {
		rpcCh = t.heartbeatCh
	}
	rpc := newRPC(&req)
	return dispatchWaitRespond(rpc, rpcCh, enc, t.closed.Ch())
}

func (t *NetTransport) handleRequestVote(dec *codec.Decoder, enc *codec.Encoder) error {
	var req VoteRequest
	if err := dec.Decode(&req); err != nil {
		return err
	}
	rpc := newRPC(&req)
	return dispatchWaitRespond(rpc, t.rpcCh, enc, t.closed.Ch())
}

func (t *NetTransport) handleInstallSnapshot(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	var req InstallSnapshotRequest
	if err := dec.Decode(&req); err != nil {
		return err
	}
	rpc := newRPC(&req)
	rpc.reader = io.LimitReader(r, req.Size)
	return dispatchWaitRespond(rpc, t.rpcCh, enc, t.closed.Ch())
}

func (n *NetTransport) handleRpcTimeoutNow(dec *codec.Decoder, enc *codec.Encoder) error { // WHAT THE HECK IS THIS?
	return nil
}

var ErrTransportShutdown = errors.New("transport is shutdown")

func dispatchWaitRespond(rpc *RPC, ch chan *RPC, enc *codec.Encoder, stopCh chan struct{}) error {
	// send RPC
	if !sendUntilStop(rpc, ch, stopCh) {
		return ErrTransportShutdown
	}
	// receive response
	res := receiveUntilStop(rpc.respCh, stopCh)
	if res == nil {
		return ErrTransportShutdown
	}
	return enc.Encode(res)
}

func sendUntilStop(rpc *RPC, ch chan *RPC, stopCh chan struct{}) bool {
	select {
	case ch <- rpc:
		return true
	case <-stopCh:
		return false
	}
}

func receiveUntilStop(ch chan interface{}, stopCh chan struct{}) interface{} {
	select {
	case res := <-ch:
		return res
	case <-stopCh:
		return nil
	}
}
