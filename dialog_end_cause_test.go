package sipgo

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/emiago/sipgo/fakes"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// TestDialogServerErrIsTheCauseItEndedWith pins Dialog.Err: nil while the
// dialog is live, the cause its context ended with when the stack ended it,
// and context.Canceled for an end that carries no cause of its own, as a BYE's
// does.
func TestDialogServerErrIsTheCauseItEndedWith(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)
	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)
	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})

	t.Run("Live", func(t *testing.T) {
		tx := sip.NewServerTx("test", invite, nil, slog.Default())
		d, err := dialogSrv.ReadInvite(invite, tx)
		require.NoError(t, err)
		require.NoError(t, d.Err())
	})

	t.Run("EndedByTheTransactionsCancel", func(t *testing.T) {
		conn := &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
		tx := sip.NewServerTx("test", invite, conn, slog.Default())
		tx.Init()
		d, err := dialogSrv.ReadInvite(invite, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Receive(newCancelRequest(invite)))
		<-d.Context().Done()
		require.ErrorIs(t, d.Err(), sip.ErrTransactionCanceled)
	})

	t.Run("EndedWithoutACauseOfItsOwn", func(t *testing.T) {
		tx := sip.NewServerTx("test", invite, nil, slog.Default())
		d, err := dialogSrv.ReadInvite(invite, tx)
		require.NoError(t, err)
		d.setState(sip.DialogStateEnded)
		<-d.Context().Done()
		require.ErrorIs(t, d.Err(), context.Canceled)
	})
}
