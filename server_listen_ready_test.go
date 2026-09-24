package sipgo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/emiago/sipgo/sip"
)

const listenReadyRounds = 50

// udpListenReadyObservation is what the listening state looks like at the instant
// the listen-ready signal fires.
type udpListenReadyObservation struct {
	network   string
	addr      string
	ports     []int
	conn      sip.Connection
	connErr   error
	pooled    sip.Connection
	pooledErr error
}

// observeUDPListenReady reads the listening state of srv for the UDP listener on
// addr: the recorded listen ports, the connection a request with addr as its local
// address gets, and the connection the pool holds for addr. It writes no packet.
func observeUDPListenReady(ctx context.Context, srv *Server, network, addr string) udpListenReadyObservation {
	obs := udpListenReadyObservation{network: network, addr: addr}
	tp := srv.TransportLayer()
	obs.ports = tp.ListenPorts("udp")

	host, port, err := sip.ParseAddr(addr)
	if err != nil {
		obs.connErr = err
		return obs
	}
	self := sip.Uri{User: "listener", Host: host, Port: port}
	req := createSimpleRequest(sip.OPTIONS, self, self, "UDP")
	req.Laddr = sip.Addr{IP: net.ParseIP(host), Port: port}

	obs.conn, obs.connErr = tp.ClientRequestConnection(ctx, req)
	obs.pooled, obs.pooledErr = tp.GetConnection("udp", addr)
	return obs
}

// testFreeAddr returns a loopback address with a port that was free a moment ago.
// It lets a test know the listening address even when the ready signal carries none.
func testFreeAddr(t *testing.T, network string) string {
	t.Helper()
	if network == "udp" {
		l, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := l.LocalAddr().String()
		require.NoError(t, l.Close())
		return addr
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// joinServe waits, with a hard bound, for the serving goroutine to return after its
// context was cancelled, so no round leaves a listener behind.
func joinServe(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("serve returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serving goroutine did not return after cancel")
	}
}

// TestListenReadyUDPListenerIsPooled guards the meaning of the listen-ready signal
// for UDP: when it fires, ListenPorts already holds the port and a client request
// whose local address is the listening address reuses the pooled listener instead
// of binding the port a second time. Both ready value kinds are covered.
func TestListenReadyUDPListenerIsPooled(t *testing.T) {
	for _, kind := range []string{"func", "channel"} {
		t.Run(kind, func(t *testing.T) {
			for round := range listenReadyRounds {
				t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
					testListenReadyUDPRound(t, kind)
				})
			}
		})
	}
}

func testListenReadyUDPRound(t *testing.T, kind string) {
	ua, err := NewUA()
	require.NoError(t, err)
	defer ua.Close()
	srv, err := NewServer(ua)
	require.NoError(t, err)
	defer srv.Close()

	addr := testFreeAddr(t, "udp")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obsCh := make(chan udpListenReadyObservation, 1)
	readyCh := make(chan struct{})
	if kind == "func" {
		ctx = context.WithValue(ctx, ListenReadyCtxKey, ListenReadyFuncCtxValue(func(network, readyAddr string) {
			obsCh <- observeUDPListenReady(ctx, srv, network, readyAddr)
		}))
	} else {
		ctx = context.WithValue(ctx, ListenReadyCtxKey, ListenReadyCtxValue(readyCh))
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx, "udp", addr)
	}()

	var obs udpListenReadyObservation
	if kind == "func" {
		select {
		case obs = <-obsCh:
		case err := <-errCh:
			t.Fatalf("serve returned before ready: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("ready never fired")
		}
		assert.Equal(t, "udp", obs.network)
		assert.Equal(t, addr, obs.addr)
	} else {
		select {
		case <-readyCh:
		case err := <-errCh:
			t.Fatalf("serve returned before ready: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("ready never fired")
		}
		obs = observeUDPListenReady(ctx, srv, "udp", addr)
	}

	_, port, err := sip.ParseAddr(addr)
	require.NoError(t, err)
	assert.Contains(t, obs.ports, port, "ListenPorts must hold the port when ready fires")
	if assert.NoError(t, obs.connErr, "a request from the listening address must reuse the listener") {
		assert.Equal(t, addr, obs.conn.LocalAddr().String())
		if assert.NoError(t, obs.pooledErr, "the listener must be pooled when ready fires") {
			assert.Same(t, obs.pooled, obs.conn)
		}
	}

	if obs.conn != nil {
		_, err := obs.conn.TryClose()
		assert.NoError(t, err)
	}
	if obs.pooled != nil {
		_, err := obs.pooled.TryClose()
		assert.NoError(t, err)
	}
	if obs.connErr == nil {
		still, err := srv.TransportLayer().GetConnection("udp", addr)
		if assert.NoError(t, err, "the listener must stay pooled after the references are released") {
			_, err := still.TryClose()
			assert.NoError(t, err)
		}
	}

	cancel()
	joinServe(t, errCh)
}
