package sipgo

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

func TestDialogServerWriteResponseReturnsCauseWhileWaitingForAck(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
	key, err := sip.ServerTxKeyMake(invite)
	require.NoError(t, err)
	conn := &sendTimesConn{wrote: make(chan struct{}, 1)}
	tx := sip.NewServerTx(key, invite, conn, slog.Default())
	require.NoError(t, tx.Init())
	defer tx.Terminate()

	d, err := dialogSrv.ReadInvite(invite, tx)
	require.NoError(t, err)
	defer d.Close()

	cause := errors.New("handler ended")
	ctx, cancel := context.WithCancelCause(testCtx(t))
	defer cancel(nil)

	res := sip.NewResponseFromRequest(d.InviteRequest, 486, "Busy Here", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(ctx, res) }()

	select {
	case err := <-answered:
		t.Fatalf("WriteResponse returned before its ctx ended: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel(cause)
	select {
	case err := <-answered:
		require.ErrorIs(t, err, cause)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return when its ctx ended")
	}
}
