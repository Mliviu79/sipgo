package sipgo

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/emiago/sipgo/sip"
)

type serverRunKey struct{}

var errServerRunStop = errors.New("sipgo test: serving stopped")

// servedServer is a Server on a real UDP listener with a client socket.
type servedServer struct {
	ua         *UserAgent
	srv        *Server
	client     net.PacketConn
	serverAddr *net.UDPAddr
}

func newServedServer(t *testing.T) *servedServer {
	t.Helper()
	ua, err := NewUA()
	require.NoError(t, err)
	srv, err := NewServer(ua)
	require.NoError(t, err)

	serverConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_ = srv.ServeUDP(serverConn)
	}()
	t.Cleanup(func() {
		_ = ua.Close()
		_ = serverConn.Close()
		<-readerDone
		_ = client.Close()
	})
	return &servedServer{ua: ua, srv: srv, client: client, serverAddr: serverConn.LocalAddr().(*net.UDPAddr)}
}

func (s *servedServer) options(t *testing.T) {
	t.Helper()
	clientAddr := s.client.LocalAddr().(*net.UDPAddr)
	req := createSimpleRequest(sip.OPTIONS,
		sip.Uri{User: "client", Host: "127.0.0.1", Port: clientAddr.Port},
		sip.Uri{User: "server", Host: "127.0.0.1", Port: s.serverAddr.Port}, "UDP")
	_, err := s.client.WriteTo([]byte(req.String()), s.serverAddr)
	require.NoError(t, err)
}

func (s *servedServer) receive(t *testing.T) *sip.Response {
	t.Helper()
	buf := make([]byte, 65535)
	require.NoError(t, s.client.SetReadDeadline(time.Now().Add(5*time.Second)))
	n, _, err := s.client.ReadFrom(buf)
	require.NoError(t, err)
	msg, err := sip.ParseMessage(buf[:n])
	require.NoError(t, err)
	res, ok := msg.(*sip.Response)
	require.True(t, ok, "expected a response, got %q", buf[:n])
	return res
}

func TestServerHandlerReceivesServeRequestsContext(t *testing.T) {
	s := newServedServer(t)
	seen := make(chan any, 1)
	s.srv.OnOptions(func(ctx context.Context, req *sip.Request, tx sip.ServerTransaction) {
		seen <- ctx.Value(serverRunKey{})
		if err := tx.Respond(sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)); err != nil {
			t.Errorf("respond: %v", err)
		}
	})

	ctx, cancel := context.WithCancelCause(context.WithValue(testCtx(t), serverRunKey{}, "run"))
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- s.srv.ServeRequests(ctx, func() { close(ready) }) }()
	<-ready

	s.options(t)
	assert.Equal(t, sip.StatusOK, s.receive(t).StatusCode)
	assert.Equal(t, "run", <-seen)

	cancel(errServerRunStop)
	require.ErrorIs(t, <-result, errServerRunStop)
}

func TestServerServeRequestsReturnsNilOnUserAgentClose(t *testing.T) {
	s := newServedServer(t)
	s.srv.OnOptions(func(ctx context.Context, req *sip.Request, tx sip.ServerTransaction) {})

	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- s.srv.ServeRequests(testCtx(t), func() { close(ready) }) }()
	<-ready

	require.NoError(t, s.ua.Close())
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeRequests did not return after the user agent closed")
	}
}

func TestServerWithoutRunAnswers480(t *testing.T) {
	s := newServedServer(t)
	ran := make(chan struct{}, 1)
	s.srv.OnOptions(func(ctx context.Context, req *sip.Request, tx sip.ServerTransaction) {
		ran <- struct{}{}
	})

	s.options(t)
	assert.Equal(t, sip.StatusTemporarilyUnavailable, s.receive(t).StatusCode)
	select {
	case <-ran:
		t.Fatal("the handler ran although no run served the server")
	default:
	}
}

// testCtx is the test's root ctx, ended when the test ends: the module's go
// directive predates testing.T.Context.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

// serveRequests serves srv's requests until the test ends and joins the run
// then. Call it once the handlers are registered.
func serveRequests(t testing.TB, srv *Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- srv.ServeRequests(ctx, func() { close(ready) }) }()
	<-ready
	t.Cleanup(func() {
		cancel()
		<-done
	})
}
