package sipgo

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenAndServe runs ListenAndServe(TLS) on a live ctx and returns once the
// listener is serving, with the channel its result arrives on.
func listenAndServe(t *testing.T, srv *Server, network, addr string) (cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(context.Background())
	t.Cleanup(cancelCtx)

	ready := make(chan struct{}, 1)
	ctx = context.WithValue(ctx, ListenReadyCtxKey, ListenReadyCtxValue(ready))
	done := make(chan error, 1)
	go func() {
		switch network {
		case "tls", "wss":
			done <- srv.ListenAndServeTLS(ctx, network, addr, testServerTlsConfig(t))
		default:
			done <- srv.ListenAndServe(ctx, network, addr)
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("serving %s %s failed: %v", network, addr, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("the %s listener on %s was not ready", network, addr)
	}
	return cancelCtx, done
}

func TestUserAgentCloseClosesStreamListeners(t *testing.T) {
	for _, network := range []string{"tcp", "ws", "tls", "wss"} {
		t.Run(network, func(t *testing.T) {
			ua, err := NewUA()
			require.NoError(t, err)
			t.Cleanup(func() { _ = ua.Close() })
			srv, err := NewServer(ua)
			require.NoError(t, err)

			addr := testFreeAddr(t, "tcp")
			_, result := listenAndServe(t, srv, network, addr)

			require.NoError(t, ua.Close())

			select {
			case err := <-result:
				require.ErrorIs(t, err, net.ErrClosed, "ListenAndServe must return with its ctx live")
			case <-time.After(5 * time.Second):
				t.Fatal("ListenAndServe did not return after the user agent closed")
			}
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				_ = conn.Close()
			}
			require.Error(t, err, "the closed listener still accepts")
		})
	}
}

func TestUserAgentCloseClosesUDPListener(t *testing.T) {
	ua, err := NewUA()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ua.Close() })
	srv, err := NewServer(ua)
	require.NoError(t, err)

	_, result := listenAndServe(t, srv, "udp", testFreeAddr(t, "udp"))

	require.NoError(t, ua.Close())

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe on udp did not return after the user agent closed")
	}
}

// listenerWatcherFrames name the functions the goroutines of ListenAndServe
// run in, the watcher of its ctx included.
var listenerWatcherFrames = []string{"(*Server).ListenAndServe", "(*Server).serveWatched"}

func TestListenAndServeJoinsItsWatcher(t *testing.T) {
	for _, network := range []string{"udp", "tcp", "tls"} {
		t.Run(network, func(t *testing.T) {
			ua, err := NewUA()
			require.NoError(t, err)
			srv, err := NewServer(ua)
			require.NoError(t, err)

			_, result := listenAndServe(t, srv, network, testFreeAddr(t, listenNetwork(network)))
			require.NoError(t, ua.Close())
			select {
			case <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("ListenAndServe did not return after the user agent closed")
			}

			// ListenAndServe has returned and its ctx is live: no goroutine
			// of it may remain.
			require.Eventually(t, func() bool {
				buf := make([]byte, 1<<20)
				stacks := string(buf[:runtime.Stack(buf, true)])
				for _, f := range listenerWatcherFrames {
					if strings.Contains(stacks, f) {
						return false
					}
				}
				return true
			}, 2*time.Second, 10*time.Millisecond, "a goroutine of ListenAndServe outlived it")
		})
	}
}

// listenerCloseCapture retains the records of a server's logger.
type listenerCloseCapture struct{ panicLogCapture }

func (c *listenerCloseCapture) logged(msg string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Message == msg {
			return true
		}
	}
	return false
}

// A terminate closes the listener from the ctx watcher; the layer's later
// close of the same listener is the same close, so nothing is reported.
func TestTerminateThenUserAgentCloseClosesListenerOnce(t *testing.T) {
	for _, network := range []string{"udp", "tcp"} {
		t.Run(network, func(t *testing.T) {
			capture := &listenerCloseCapture{}
			ua, err := NewUA()
			require.NoError(t, err)
			srv, err := NewServer(ua, WithServerLogger(slog.New(capture)))
			require.NoError(t, err)

			cancel, result := listenAndServe(t, srv, network, testFreeAddr(t, listenNetwork(network)))
			cancel()
			select {
			case err := <-result:
				if err != nil {
					require.True(t, errors.Is(err, net.ErrClosed), "unexpected result %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ListenAndServe did not return after its ctx ended")
			}

			require.NoError(t, ua.Close())
			assert.False(t, capture.logged("Failed to close listener"))
		})
	}
}

// listenNetwork is the network a free address of the served network is taken on.
func listenNetwork(network string) string {
	if network == "udp" {
		return "udp"
	}
	return "tcp"
}
