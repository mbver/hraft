package hraft

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetTransport_AppendEntries(t *testing.T) {
	trans1, trans2, cleanup, err := twoTestTransport()
	defer cleanup()
	require.Nil(t, err)
	req, resp := getTestAppendEntriesRequestResponse(trans1.config.AdvertiseAddr)
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
		rpc.respCh <- resp
	}()
	var gotResp AppendEntriesResponse
	err = trans1.AppendEntries(trans2.AdvertiseAddr(), req, &gotResp)
	require.Nil(t, err)
	require.Nil(t, <-errCh)
	require.True(t, reflect.DeepEqual(resp, &gotResp), fmt.Sprintf("want: %+v, got: %+v", resp, gotResp))
}

func TestNetTransport_RequestVote(t *testing.T) {
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
	err = trans1.RequestVote(trans2.AdvertiseAddr(), req, &gotResp)
	require.Nil(t, err)
	require.Nil(t, <-errCh)
	require.True(t, reflect.DeepEqual(resp, gotResp), "response mismatch")
}

func TestNetTransport_ClearPool(t *testing.T) {
	trans1, trans2, cleanup, err := twoTestTransport()
	defer cleanup()
	require.Nil(t, err)
	req, resp := getTestAppendEntriesRequestResponse(trans1.AdvertiseAddr())
	errCh := make(chan error, 15)
	stopReceiveCh := make(chan struct{})
	go func() {
		for {
			select {
			case rpc := <-trans2.RpcCh():
				if !reflect.DeepEqual(rpc.command, req) {
					errCh <- fmt.Errorf("request mismatch: want: %+v, got: %+v", req, rpc.command)
					rpc.respCh <- nil
					return
				}
				errCh <- nil
				rpc.respCh <- resp
			case <-stopReceiveCh:
				errCh <- nil
			case <-time.After(200 * time.Millisecond):
				errCh <- fmt.Errorf("expect no timeout")
			}
		}
	}()
	for i := 0; i < 2; i++ {
		wg := sync.WaitGroup{}
		for k := 0; k < 5; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var gotResp AppendEntriesResponse
				err := trans1.AppendEntries(trans2.AdvertiseAddr(), req, &gotResp)
				errCh <- err
				if !reflect.DeepEqual(resp, &gotResp) {
					errCh <- fmt.Errorf("response mismatch. want:%+v, got:%+v", resp, &gotResp)
					return
				}
				errCh <- nil
			}()
		}
		wg.Wait()
		require.Equal(t, 2, len(trans1.connPool[trans2.AdvertiseAddr()]))
		for n := 0; n < 15; n++ {
			select {
			case err := <-errCh:
				require.Nil(t, err)
			default:
				t.Fatalf("expect no timeout, n = %d", n)
			}
		}
		if i == 0 {
			trans1.ClearPool()
			require.Zero(t, len(trans1.connPool[trans2.AdvertiseAddr()]))
		}
	}
	close(stopReceiveCh)
	err = <-errCh
	require.Nil(t, err)
}
