package sipgo

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

func TestDialogServerWriteResponseReturnsCauseWhileWaitingForAck(t *testing.T) {
	d, conn := newAckWaitSession(t)

	cause := errors.New("handler ended")
	ctx, cancel := context.WithCancelCause(testCtx(t))
	defer cancel(nil)

	res := sip.NewResponseFromRequest(d.InviteRequest, 486, "Busy Here", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(ctx, res) }()

	select {
	case <-conn.wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("the response was not written")
	}
	cancel(cause)
	select {
	case err := <-answered:
		require.ErrorIs(t, err, cause)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return when its ctx ended")
	}
}

// ackWaitConn tells when a response is written, whatever its class.
type ackWaitConn struct {
	wrote chan struct{}
}

func (c *ackWaitConn) LocalAddr() net.Addr { return nil }

func (c *ackWaitConn) WriteMsg(msg sip.Message) error {
	if _, ok := msg.(*sip.Response); ok {
		select {
		case c.wrote <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *ackWaitConn) Ref(i int) int          { return 0 }
func (c *ackWaitConn) TryClose() (int, error) { return 0, nil }
func (c *ackWaitConn) Close() error           { return nil }

func newAckWaitSession(t *testing.T) (*DialogServerSession, *ackWaitConn) {
	t.Helper()
	ua, _ := NewUA()
	t.Cleanup(func() { _ = ua.Close() })
	cli, _ := NewClient(ua)
	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
	key, err := sip.ServerTxKeyMake(invite)
	require.NoError(t, err)
	conn := &ackWaitConn{wrote: make(chan struct{}, 1)}
	tx := sip.NewServerTx(key, invite, conn, slog.Default())
	require.NoError(t, tx.Init())
	t.Cleanup(tx.Terminate)

	d, err := dialogSrv.ReadInvite(invite, tx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d, conn
}

// A 2xx whose dialog ends while it waits for the ACK reports that, whichever
// of the dialog's own ctx and its state the wait sees first.
func TestDialogServerWriteResponseDialogEndsWhileWaitingForAck(t *testing.T) {
	d, conn := newAckWaitSession(t)

	res := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(d.Context(), res) }()

	select {
	case <-conn.wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("the 2xx was not written")
	}
	d.setState(sip.DialogStateEnded)
	select {
	case err := <-answered:
		require.ErrorIs(t, err, ErrDialogEndedBeforeAck)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return when the dialog ended")
	}
}

// A 2xx waiting for the ACK on a ctx that is not the dialog's returns the
// ctx's cause when it ends, leaving the dialog established for the caller to
// hang up.
func TestDialogServerWriteResponse2xxReturnsCauseWhileWaitingForAck(t *testing.T) {
	d, conn := newAckWaitSession(t)

	cause := errors.New("handler ended")
	ctx, cancel := context.WithCancelCause(testCtx(t))
	defer cancel(nil)

	res := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(ctx, res) }()

	select {
	case <-conn.wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("the 2xx was not written")
	}
	cancel(cause)
	select {
	case err := <-answered:
		require.ErrorIs(t, err, cause)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return when its ctx ended")
	}
	require.Equal(t, sip.DialogStateEstablished, d.LoadState())
}
