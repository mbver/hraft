package hraft

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNetTransport_AppendEntries(t *testing.T) {
	trans1, trans2, cleanup, err := twoTestTransport()
	defer cleanup()
	require.Nil(t, err)
	req := &AppendEntriesRequest{
		Term:        10,
		Leader:      []byte(trans1.AdvertiseAddr()),
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
	resp := AppendEntriesResponse{
		Term:               4,
		LastLogIdx:         90,
		Success:            true,
		PrevLogCheckFailed: false,
	}
	trans1.connectPeer(trans2.AdvertiseAddr())
	errCh := make(chan error, 1)
	go func() {
		rpc := <-trans2.RpcCh()
		if !reflect.DeepEqual(rpc.command, req) {
			errCh <- errors.New("request not match")
			rpc.respCh <- nil
			return
		}
		errCh <- nil
		rpc.respCh <- &resp
	}()
	var gotResp AppendEntriesResponse
	trans1.AppendEntries(trans2.AdvertiseAddr(), req, &gotResp)
	require.Nil(t, <-errCh)
	require.True(t, reflect.DeepEqual(resp, gotResp), "response mismatch")
}

func TestTransport_RequestVote(t *testing.T) {
	trans1, trans2, cleanup, err := twoTestTransport()
	defer cleanup()
	require.Nil(t, err)
	req := &VoteRequest{
		Term:        20,
		Candidate:   []byte(trans1.AdvertiseAddr()),
		LastLogIdx:  100,
		LastLogTerm: 19,
	}
	resp := VoteResponse{
		Term:    100,
		Granted: false,
	}
	trans1.connectPeer(trans2.AdvertiseAddr())
	errCh := make(chan error, 1)
	go func() {
		rpc := <-trans2.RpcCh()
		if !reflect.DeepEqual(req, rpc.command) {
			errCh <- errors.New("request not match")
			rpc.respCh <- nil
			return
		}
		errCh <- nil
		rpc.respCh <- &resp
	}()
	var gotResp VoteResponse
	trans1.RequestVote(trans2.AdvertiseAddr(), req, &gotResp)
	require.Nil(t, <-errCh)
	require.True(t, reflect.DeepEqual(resp, gotResp), "response mismatch")
}
