package sipgo

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialogServerByeRequest(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@uas.com", "udp", "uas.com:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
	invite.AppendHeader(&sip.RecordRouteHeader{Address: sip.Uri{Host: "P1", Port: 5060}})
	invite.AppendHeader(&sip.RecordRouteHeader{Address: sip.Uri{Host: "P2", Port: 5060}})
	invite.AppendHeader(&sip.RecordRouteHeader{Address: sip.Uri{Host: "P3", Port: 5060}})

	dialog, err := dialogSrv.ReadInvite(invite, sip.NewServerTx("test", invite, nil, slog.Default()))
	require.NoError(t, err)

	res := sip.NewResponseFromRequest(invite, sip.StatusOK, "OK", nil)
	res.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uac", Port: 9876}})

	bye := sip.NewRequest(sip.BYE, invite.Contact().Address)
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	// No execution
	dialog.TransactionRequest(ctxCanceled, bye)
	require.Equal(t, invite.CallID(), bye.CallID())

	routes := bye.GetHeaders("Route")
	assert.Equal(t, "<sip:P1:5060>", routes[0].Value())
	assert.Equal(t, "<sip:P2:5060>", routes[1].Value())
	assert.Equal(t, "<sip:P3:5060>", routes[2].Value())
}

func TestDialogServerTransactionCanceled(t *testing.T) {
	// sip.Timer_H = 0

	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})

	t.Run("TerminatedEarly", func(t *testing.T) {
		tx := sip.NewServerTx("test", invite, nil, slog.Default())
		tx.Terminate()
		_, err := dialogSrv.ReadInvite(invite, tx)
		require.Error(t, err)
		require.ErrorIs(t, err, sip.ErrTransactionTerminated)
	})

	t.Run("TerminatedByCancel", func(t *testing.T) {
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

		err = tx.Receive(newCancelRequest(invite))
		require.NoError(t, err)
		// Context dialog will be terminated and in this case
		// cause of context cancelation could be found
		<-d.Context().Done()
		require.ErrorIs(t, d.err(), sip.ErrTransactionCanceled)
	})

	t.Run("TerminatedByCancelBeforeReadingInvite", func(t *testing.T) {
		conn := &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
		tx := sip.NewServerTx("test", invite, conn, slog.Default())
		tx.Init()
		err := tx.Receive(newCancelRequest(invite))
		require.NoError(t, err)
		_, err = dialogSrv.ReadInvite(invite, tx)
		require.ErrorIs(t, err, sip.ErrTransactionCanceled)
	})

}

func TestDialogServerRequestsWithinDialog(t *testing.T) {
	// https://datatracker.ietf.org/doc/html/rfc3261#section-12.2.2

	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})

	t.Run("InvalidCseq", func(t *testing.T) {
		// This covers issue explained as
		// https://github.com/emiago/sipgo/issues/187
		conn := &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
		tx := sip.NewServerTx("test", invite, conn, slog.Default())
		tx.Init()

		dialog, err := dialogSrv.ReadInvite(invite, tx)
		require.NoError(t, err)
		defer dialog.Close()

		byeWrongCseq := newByeRequestUAC(invite, sip.NewResponseFromRequest(invite, 200, "OK", nil), nil)
		byeWrongCseq.CSeq().SeqNo--
		tx = sip.NewServerTx("test", byeWrongCseq, conn, slog.Default())
		tx.Init()
		err = dialog.ReadBye(byeWrongCseq, tx)
		require.ErrorIs(t, err, ErrDialogInvalidCseq)
	})

	t.Run("TerminateAfterSentRequest", func(t *testing.T) {
		// This covers issue explained as
		// https://github.com/emiago/sipgo/issues/187
		conn := &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
		tx := sip.NewServerTx("test", invite, conn, slog.Default())
		tx.Init()

		dialog, err := dialogSrv.ReadInvite(invite, tx)
		require.NoError(t, err)
		defer dialog.Close()

		reinvite := sip.NewRequest(sip.INVITE, invite.From().Address)
		_, err = dialog.TransactionRequest(context.TODO(), reinvite)
		require.NoError(t, err)

		// The BYE is answered on a connection of its own. The INVITE
		// transaction sends 100 Trying from its timer goroutine once it has
		// gone unanswered for 200 ms, and the writes of the two transactions
		// must not share one buffer.
		byeConn := &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
		bye := newByeRequestUAC(invite, sip.NewResponseFromRequest(invite, 200, "OK", nil), nil)
		tx = sip.NewServerTx("test-bye", bye, byeConn, slog.Default())
		tx.Init()
		err = dialog.ReadBye(bye, tx)
		require.NoError(t, err)
	})
}

// TestDialogServer2xxRetransmission reads the ACK once the 2xx has been
// retransmitted, and checks that the ACK ends the retransmissions.
func TestDialogServer2xxRetransmission(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})

	// Create a server transcation
	key, err := sip.ServerTxKeyMake(invite)
	require.NoError(t, err)
	conn := &sendTimesConn{wrote: make(chan struct{}, 1)}
	tx := sip.NewServerTx(key, invite, conn, slog.Default())
	require.NoError(t, tx.Init())
	defer tx.Terminate()

	// Read Invite
	d, err := dialogSrv.ReadInvite(invite, tx)
	require.NoError(t, err)
	defer d.Close()

	// Respond 200
	// This will block until ACK
	res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(res200) }()

	deadline := time.After(10 * time.Second)
	for len(conn.sendTimes()) < 2 {
		select {
		case <-conn.wrote:
		case err := <-answered:
			t.Fatalf("WriteResponse returned %v before the 2xx was retransmitted", err)
		case <-deadline:
			t.Fatalf("the 2xx was sent %d times and not retransmitted", len(conn.sendTimes()))
		}
	}

	acked := len(conn.sendTimes())
	ackReceive := newAckRequestUAC(d.InviteRequest, res200, nil)
	require.NoError(t, d.ReadAck(ackReceive, tx))
	select {
	case err := <-answered:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return after the ACK")
	}
	// At most the retransmission already due when the ACK was read follows it.
	assert.LessOrEqual(t, len(conn.sendTimes()), acked+1, "the 2xx was retransmitted after its ACK")
}

// TestDialogServerAckAfterBye reads the ACK to our 2xx after the BYE that the
// peer sent behind it. Requests are handled on their own goroutines, so the
// BYE can be read first. The dialog has ended by then, and the late ACK must
// not confirm it again or report a state change.
func TestDialogServerAckAfterBye(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})

	newConn := func() *sip.UDPConnection {
		return &sip.UDPConnection{
			PacketConn: &fakes.UDPConn{
				Writers: map[string]io.Writer{
					"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
				},
			},
		}
	}
	tx := sip.NewServerTx("test", invite, newConn(), slog.Default())
	require.NoError(t, tx.Init())
	defer tx.Terminate()

	d, err := dialogSrv.ReadInvite(invite, tx)
	require.NoError(t, err)
	defer d.Close()

	states := d.StateRead()
	res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(res200) }()

	readState := func() sip.DialogState {
		t.Helper()
		select {
		case s := <-states:
			return s
		case <-time.After(5 * time.Second):
			t.Fatal("no dialog state change")
			return 0
		}
	}
	require.Equal(t, sip.DialogStateEstablished, readState())

	bye := newByeRequestUAC(invite, res200, nil)
	byeTx := sip.NewServerTx("test-bye", bye, newConn(), slog.Default())
	require.NoError(t, byeTx.Init())
	require.NoError(t, d.ReadBye(bye, byeTx))
	require.Equal(t, sip.DialogStateEnded, readState())

	select {
	case err := <-answered:
		require.ErrorIs(t, err, ErrDialogEndedBeforeAck)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return after the dialog ended")
	}

	ack := newAckRequestUAC(d.InviteRequest, res200, nil)
	require.NoError(t, d.ReadAck(ack, tx))
	assert.Equal(t, sip.DialogStateEnded, d.LoadState())
	select {
	case s := <-states:
		t.Fatalf("state %s reported after the dialog ended", s)
	default:
	}
}

// TestDialogServerAnswerAfterCancel answers 200 to an INVITE whose CANCEL was
// read just before. The CANCEL ended the dialog, and the answer that lost the
// race must fail without establishing it again.
func TestDialogServerAnswerAfterCancel(t *testing.T) {
	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
	conn := &sip.UDPConnection{
		PacketConn: &fakes.UDPConn{
			Writers: map[string]io.Writer{
				"127.0.0.1:5090": bytes.NewBuffer(make([]byte, 0)),
			},
		},
	}
	tx := sip.NewServerTx("test", invite, conn, slog.Default())
	require.NoError(t, tx.Init())
	defer tx.Terminate()

	d, err := dialogSrv.ReadInvite(invite, tx)
	require.NoError(t, err)
	defer d.Close()

	require.NoError(t, tx.Receive(newCancelRequest(invite)))
	require.Equal(t, sip.DialogStateEnded, d.LoadState())

	states := d.StateRead()
	err = d.WriteResponse(sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil))
	require.ErrorIs(t, err, sip.ErrTransactionCanceled)
	assert.Equal(t, sip.DialogStateEnded, d.LoadState())
	select {
	case s := <-states:
		t.Fatalf("state %s reported after the dialog ended", s)
	default:
	}
}

// runInChildProcess runs the calling test again, alone, in a child process of
// the test binary, and reports true once it has passed there, so the caller
// returns. In the child it reports false and the caller runs its body. A test
// that shortens the package-wide SIP timers needs a process of its own:
// transactions that earlier tests leave running read those timers from their
// own goroutines, and changing the timers under them is a data race.
func runInChildProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("SIPGO_TEST_CHILD") == t.Name() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^"+t.Name()+"$",
		"-test.count=1",
		"-test.v",
		"-test.timeout=60s",
	)
	cmd.Env = append(os.Environ(), "SIPGO_TEST_CHILD="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child process failed:\n%s", out)
	return true
}

// TestDialogServer2xxAckTimeout answers with a 2xx that is never acknowledged.
// RFC 3261 section 13.3.1.4 bounds the wait: after retransmitting the 2xx for
// 64*T1 the dialog is confirmed, and the session is to be ended with a BYE.
// The bound is WriteResponse's own, whether the transaction outlives it or, as
// RFC 6026 section 8.7 has it, ends with Timer L at the same moment.
func TestDialogServer2xxAckTimeout(t *testing.T) {
	if runInChildProcess(t) {
		return
	}
	restoreSIPTimers(t)
	sip.T1, sip.T2 = 10*time.Millisecond, 40*time.Millisecond

	ua, _ := NewUA()
	defer ua.Close()
	cli, _ := NewClient(ua)

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}
	dialogSrv := NewDialogServerCache(cli, uasContact)

	for _, tc := range []struct {
		name   string
		timerL time.Duration
	}{
		{name: "TransactionOutlivesWait", timerL: time.Minute},
		{name: "TimerLEndsTransaction", timerL: 64 * sip.T1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sip.Timer_L = tc.timerL

			invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
			invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
			tx := siptest.NewServerTxRecorder(invite)
			defer tx.Terminate()

			d, err := dialogSrv.ReadInvite(invite, tx)
			require.NoError(t, err)
			defer d.Close()

			res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
			start := time.Now()
			answered := make(chan error, 1)
			go func() { answered <- d.WriteResponse(res200) }()

			select {
			case err := <-answered:
				require.ErrorIs(t, err, ErrDialogAckTimeout)
			case <-time.After(5 * time.Second):
				// Ending the transaction fails the next retransmission, which
				// returns WriteResponse.
				tx.Terminate()
				t.Fatal("WriteResponse still waits for the ACK long after 64*T1")
			}
			assert.GreaterOrEqual(t, time.Since(start), 64*sip.T1)
			assert.Equal(t, sip.DialogStateConfirmed, d.LoadState())
			assert.Greater(t, len(tx.Result()), 1, "the 2xx is retransmitted while waiting")
		})
	}
}

// sendTimesConn is a connection that records when each 2xx response is
// written. It leaves out the 100 Trying that an INVITE transaction sends when
// nothing is answered within 200 ms.
type sendTimesConn struct {
	mu    sync.Mutex
	times []time.Time
	wrote chan struct{}
}

func (c *sendTimesConn) LocalAddr() net.Addr { return nil }

func (c *sendTimesConn) WriteMsg(msg sip.Message) error {
	if res, ok := msg.(*sip.Response); !ok || !res.IsSuccess() {
		return nil
	}
	c.mu.Lock()
	c.times = append(c.times, time.Now())
	c.mu.Unlock()
	select {
	case c.wrote <- struct{}{}:
	default:
	}
	return nil
}

func (c *sendTimesConn) Ref(i int) int          { return 0 }
func (c *sendTimesConn) TryClose() (int, error) { return 0, nil }
func (c *sendTimesConn) Close() error           { return nil }
func (c *sendTimesConn) sendTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.times...)
}

// TestDialogServer2xxRetransmissionInterval checks the start of the 2xx
// retransmission schedule of RFC 3261 section 13.3.1.4: the interval starts at
// T1 and doubles for each retransmission, until it reaches T2.
func TestDialogServer2xxRetransmissionInterval(t *testing.T) {
	if runInChildProcess(t) {
		return
	}
	restoreSIPTimers(t)
	sip.T1, sip.T2 = 10*time.Millisecond, time.Second

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

	res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(res200) }()

	// The 2xx and its first three retransmissions, all well within the 64*T1
	// that WriteResponse waits for the ACK.
	const sends = 4
	deadline := time.After(10 * time.Second)
	for len(conn.sendTimes()) < sends {
		select {
		case <-conn.wrote:
		case err := <-answered:
			t.Fatalf("WriteResponse returned %v after %d of %d transmissions of the 2xx", err, len(conn.sendTimes()), sends)
		case <-deadline:
			t.Fatalf("only %d of %d transmissions of the 2xx", len(conn.sendTimes()), sends)
		}
	}

	ack := newAckRequestUAC(d.InviteRequest, res200, nil)
	require.NoError(t, d.ReadAck(ack, tx))
	select {
	case err := <-answered:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return after the ACK")
	}

	times := conn.sendTimes()
	for i, want := range []time.Duration{sip.T1, 2 * sip.T1, 4 * sip.T1} {
		interval := times[i+1].Sub(times[i])
		assert.GreaterOrEqual(t, interval, want, "retransmission %d", i+1)
		// Far below T2, which the interval would only reach after the doubling.
		assert.Less(t, interval, sip.T2/2, "retransmission %d", i+1)
	}
}

// TestDialogServerEndedWhileAnswering ends the dialog once WriteResponse has
// established it and before WriteResponse starts waiting for the ACK, as a BYE
// read on another goroutine can before that BYE ends the INVITE transaction.
// WriteResponse must see the end: it sends no 2xx for the ended dialog and
// returns at once, instead of waiting for an ACK until 64*T1.
func TestDialogServerEndedWhileAnswering(t *testing.T) {
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

	// Called inside WriteResponse's move to Established, so the dialog ends
	// before WriteResponse registers for the ACK.
	d.OnState(func(s sip.DialogState) {
		if s == sip.DialogStateEstablished {
			d.setState(sip.DialogStateEnded)
		}
	})

	res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(res200) }()
	select {
	case err := <-answered:
		require.ErrorIs(t, err, ErrDialogEndedBeforeAck)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse waits for the ACK of a dialog that has ended")
	}
	assert.Empty(t, conn.sendTimes(), "a 2xx was sent for a dialog that has ended")
	assert.Equal(t, sip.DialogStateEnded, d.LoadState())
}

// TestDialogServerAnswerAgainAfterAck answers a dialog again once its 2xx has
// been acknowledged. The dialog stays confirmed: WriteResponse neither moves it
// back to Established nor waits for another ACK.
func TestDialogServerAnswerAgainAfterAck(t *testing.T) {
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

	res200 := sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
	answered := make(chan error, 1)
	go func() { answered <- d.WriteResponse(res200) }()
	select {
	case <-conn.wrote:
	case err := <-answered:
		t.Fatalf("WriteResponse returned %v before sending the 2xx", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the 2xx was not sent")
	}
	require.NoError(t, d.ReadAck(newAckRequestUAC(d.InviteRequest, res200, nil), tx))
	select {
	case err := <-answered:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse did not return after the ACK")
	}

	states := d.StateRead()
	go func() { answered <- d.WriteResponse(res200) }()
	select {
	case err := <-answered:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteResponse waits for another ACK to a confirmed dialog")
	}
	assert.Equal(t, sip.DialogStateConfirmed, d.LoadState())
	select {
	case s := <-states:
		t.Fatalf("state %s reported for a confirmed dialog", s)
	default:
	}
}
