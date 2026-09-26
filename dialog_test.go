package sipgo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialogState(t *testing.T) {
	inv, _, _ := createTestInvite(t, "sip:nowhere", "udp", "127.0.0.1")
	d := Dialog{
		InviteRequest: inv,
	}
	d.Init()

	ch := d.StateRead()
	ch2 := d.StateRead()
	go func() {
		d.setState(sip.DialogStateEstablished)
		d.setState(sip.DialogStateConfirmed)
		d.setState(sip.DialogStateEnded)
	}()

	assert.Equal(t, sip.DialogStateEstablished, <-ch)
	assert.Equal(t, sip.DialogStateConfirmed, <-ch)
	assert.Equal(t, sip.DialogStateEnded, <-ch)

	assert.Equal(t, sip.DialogStateEstablished, <-ch2)
	assert.Equal(t, sip.DialogStateConfirmed, <-ch2)
	assert.Equal(t, sip.DialogStateEnded, <-ch2)

}

// TestDialogStateOnlyMovesForward moves a dialog to each state and then back to
// every earlier one. A dialog is established, then confirmed, then ended, and
// never goes back: a move back is refused and reported to no callback.
func TestDialogStateOnlyMovesForward(t *testing.T) {
	states := []sip.DialogState{sip.DialogStateEstablished, sip.DialogStateConfirmed, sip.DialogStateEnded}
	for i, s := range states {
		t.Run(s.String(), func(t *testing.T) {
			inv, _, _ := createTestInvite(t, "sip:nowhere", "udp", "127.0.0.1")
			d := Dialog{InviteRequest: inv}
			d.Init()
			for _, forward := range states[:i+1] {
				d.setState(forward)
			}
			rec := &stateRecorder{}
			d.OnState(rec.record)

			for _, back := range states[:i] {
				d.setState(back)
				assert.Equal(t, s, d.LoadState(), "moved back to %s", back)
			}
			assert.Empty(t, rec.recorded())
		})
	}
}

// stateRecorder records the states a state callback is told of.
type stateRecorder struct {
	mu     sync.Mutex
	states []sip.DialogState
}

func (r *stateRecorder) record(s sip.DialogState) {
	r.mu.Lock()
	r.states = append(r.states, s)
	r.mu.Unlock()
}

func (r *stateRecorder) recorded() []sip.DialogState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sip.DialogState(nil), r.states...)
}

// TestDialogStateCallbacksInOrder makes a transition while the callbacks of the
// one before it still run. Every callback must be told of the states in the
// order the dialog took them, and a callback that is still running must not
// hold up a transition made on another goroutine.
func TestDialogStateCallbacksInOrder(t *testing.T) {
	newDialog := func(t *testing.T) *Dialog {
		inv, _, _ := createTestInvite(t, "sip:nowhere", "udp", "127.0.0.1")
		d := &Dialog{InviteRequest: inv}
		d.Init()
		d.setState(sip.DialogStateEstablished)
		return d
	}

	t.Run("AnotherGoroutine", func(t *testing.T) {
		d := newDialog(t)
		rec := &stateRecorder{}
		d.OnState(rec.record)

		// Holds the notification of Confirmed, before it reaches rec, until
		// the dialog has ended on the test goroutine.
		holding := make(chan struct{})
		release := make(chan struct{})
		var heldTooLong atomic.Bool
		d.OnState(func(s sip.DialogState) {
			if s != sip.DialogStateConfirmed {
				return
			}
			close(holding)
			select {
			case <-release:
			case <-time.After(5 * time.Second):
				heldTooLong.Store(true)
			}
		})

		confirmed := make(chan struct{})
		go func() {
			defer close(confirmed)
			d.setState(sip.DialogStateConfirmed)
		}()
		select {
		case <-holding:
		case <-time.After(5 * time.Second):
			t.Fatal("Confirmed was not notified")
		}

		d.setState(sip.DialogStateEnded)
		close(release)
		select {
		case <-confirmed:
		case <-time.After(10 * time.Second):
			t.Fatal("the transition to Confirmed did not return")
		}
		assert.False(t, heldTooLong.Load(), "the transition to Ended waited for a callback of Confirmed")
		assert.Equal(t, []sip.DialogState{sip.DialogStateConfirmed, sip.DialogStateEnded}, rec.recorded())
		assert.Equal(t, sip.DialogStateEnded, d.LoadState())
	})

	t.Run("FromCallback", func(t *testing.T) {
		d := newDialog(t)
		rec := &stateRecorder{}
		d.OnState(rec.record)
		// Ends the dialog as soon as it is confirmed, before rec is told of
		// the confirmation.
		d.OnState(func(s sip.DialogState) {
			if s == sip.DialogStateConfirmed {
				d.setState(sip.DialogStateEnded)
			}
		})

		confirmed := make(chan struct{})
		go func() {
			defer close(confirmed)
			d.setState(sip.DialogStateConfirmed)
		}()
		select {
		case <-confirmed:
		case <-time.After(5 * time.Second):
			t.Fatal("the transition to Confirmed did not return")
		}
		assert.Equal(t, []sip.DialogState{sip.DialogStateConfirmed, sip.DialogStateEnded}, rec.recorded())
		assert.Equal(t, sip.DialogStateEnded, d.LoadState())
	})

	t.Run("PanickingCallback", func(t *testing.T) {
		d := newDialog(t)
		rec := &stateRecorder{}
		d.OnState(rec.record)
		d.OnState(func(s sip.DialogState) {
			if s == sip.DialogStateConfirmed {
				panic("callback failed")
			}
		})

		require.Panics(t, func() { d.setState(sip.DialogStateConfirmed) })
		// The panic stopped that notification, and must not stop the ones of
		// later transitions.
		d.setState(sip.DialogStateEnded)
		assert.Equal(t, []sip.DialogState{sip.DialogStateEnded}, rec.recorded())
	})
}

// TestDialogStateReplayInOrder registers a callback with OnStateReplay while
// earlier transitions are still being told. The callback is told the state the
// dialog is in, then each later transition, in order: never a replayed state
// ahead of a queued one.
func TestDialogStateReplayInOrder(t *testing.T) {
	newDialog := func(t *testing.T) *Dialog {
		inv, _, _ := createTestInvite(t, "sip:nowhere", "udp", "127.0.0.1")
		d := &Dialog{InviteRequest: inv}
		d.Init()
		return d
	}

	t.Run("WhileTelling", func(t *testing.T) {
		d := newDialog(t)
		// Holds the notification of Established until the dialog has been
		// confirmed and ended, and the replay registered, on the test
		// goroutine.
		holding := make(chan struct{})
		release := make(chan struct{})
		d.OnState(func(s sip.DialogState) {
			if s != sip.DialogStateEstablished {
				return
			}
			close(holding)
			select {
			case <-release:
			case <-time.After(5 * time.Second):
			}
		})

		established := make(chan struct{})
		go func() {
			defer close(established)
			d.setState(sip.DialogStateEstablished)
		}()
		select {
		case <-holding:
		case <-time.After(5 * time.Second):
			t.Fatal("Established was not notified")
		}

		d.setState(sip.DialogStateConfirmed)
		d.setState(sip.DialogStateEnded)
		rec := &stateRecorder{}
		assert.Equal(t, sip.DialogStateEnded, d.OnStateReplay(rec.record))
		close(release)
		select {
		case <-established:
		case <-time.After(10 * time.Second):
			t.Fatal("the transition to Established did not return")
		}
		assert.Equal(t, []sip.DialogState{sip.DialogStateEnded}, rec.recorded())
	})

	t.Run("Idle", func(t *testing.T) {
		d := newDialog(t)
		d.setState(sip.DialogStateEstablished)
		d.setState(sip.DialogStateConfirmed)

		rec := &stateRecorder{}
		assert.Equal(t, sip.DialogStateConfirmed, d.OnStateReplay(rec.record))
		// Told before OnStateReplay returns, as nothing else was being told.
		assert.Equal(t, []sip.DialogState{sip.DialogStateConfirmed}, rec.recorded())

		d.setState(sip.DialogStateEnded)
		assert.Equal(t, []sip.DialogState{sip.DialogStateConfirmed, sip.DialogStateEnded}, rec.recorded())
	})
}

func BenchmarkDialogSettingState(b *testing.B) {
	inv, _, _ := createTestInvite(b, "sip:nowhere", "udp", "127.0.0.1")
	d := Dialog{
		InviteRequest: inv,
	}
	d.Init()
	mustBeCalled := false
	d.OnState(func(s sip.DialogState) {
		mustBeCalled = true
	})
	for i := 0; i < b.N; i++ {
		d.setState(sip.DialogStateConfirmed)
	}

	if !mustBeCalled {
		b.Error("On state not called")
	}

}

// byeInFlight answers each request with a 200, but holds the BYE's until
// release is closed, reporting on inFlight that the BYE reached the
// transaction.
type byeInFlight struct {
	inFlight chan struct{}
	release  chan struct{}
}

func newByeInFlight() *byeInFlight {
	return &byeInFlight{inFlight: make(chan struct{}), release: make(chan struct{})}
}

func (b *byeInFlight) respond(req *sip.Request, w *siptest.ClientTxResponder) {
	res := sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)
	if req.IsInvite() {
		res.To().Params.Add("tag", sip.GenerateTagN(16))
		res.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: 5090}})
	}
	if req.Method == sip.BYE {
		close(b.inFlight)
		select {
		case <-b.release:
		case <-time.After(10 * time.Second):
			return
		}
	}
	w.Receive(res)
}

// TestDialogByeEndsSessionWhenSent sends our BYE, on each side, and holds its
// 200. RFC 3261 section 15.1.1 has the session over as soon as the BYE is
// passed to its client transaction, so a request the peer sends meanwhile,
// such as a re-INVITE, must find the dialog ended: its state is Ended and its
// context done while the BYE is in flight. Bye still waits for the 200, also
// on a context derived from the dialog's, which ends with the dialog.
func TestDialogByeEndsSessionWhenSent(t *testing.T) {
	sides := []struct {
		name string
		// newConfirmed returns a confirmed dialog whose requests b answers,
		// and its Bye.
		newConfirmed func(t *testing.T, b *byeInFlight) (*Dialog, func(context.Context) error)
	}{
		{
			name: "UAS",
			newConfirmed: func(t *testing.T, b *byeInFlight) (*Dialog, func(context.Context) error) {
				cli := testClientResponder(t, b.respond)
				dialogSrv := NewDialogServerCache(cli, sip.ContactHeader{
					Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
				})
				invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "udp", "127.0.0.1:5090")
				invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "uas", Port: 1234}})
				tx := siptest.NewServerTxRecorder(invite)
				t.Cleanup(tx.Terminate)
				d, err := dialogSrv.ReadInvite(invite, tx)
				require.NoError(t, err)
				t.Cleanup(func() { d.Close() })
				d.InviteResponse = sip.NewResponseFromRequest(d.InviteRequest, 200, "OK", nil)
				d.setState(sip.DialogStateConfirmed)
				return &d.Dialog, d.Bye
			},
		},
		{
			name: "UAC",
			newConfirmed: func(t *testing.T, b *byeInFlight) (*Dialog, func(context.Context) error) {
				dua := DialogUA{
					Client:     testClientResponder(t, b.respond),
					ContactHDR: sip.ContactHeader{Address: sip.Uri{User: "uac", Host: "127.0.0.1", Port: 5060}},
				}
				d, err := dua.Invite(context.TODO(), sip.Uri{User: "test", Host: "localhost"}, nil)
				require.NoError(t, err)
				require.NoError(t, d.WaitAnswer(context.TODO(), AnswerOptions{}))
				require.NoError(t, d.Ack(context.TODO()))
				return &d.Dialog, d.Bye
			},
		},
	}

	for _, side := range sides {
		t.Run(side.name, func(t *testing.T) {
			for _, ctxName := range []string{"OwnContext", "DialogContext"} {
				t.Run(ctxName, func(t *testing.T) {
					b := newByeInFlight()
					d, bye := side.newConfirmed(t, b)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if ctxName == "DialogContext" {
						ctx, cancel = context.WithTimeout(d.Context(), 5*time.Second)
					}
					defer cancel()

					sent := make(chan error, 1)
					go func() { sent <- bye(ctx) }()
					select {
					case <-b.inFlight:
					case <-time.After(5 * time.Second):
						t.Fatal("the BYE was not sent")
					}
					// The BYE reaches the peer as the transaction takes it,
					// and the dialog ends once the transaction has.
					select {
					case <-d.Context().Done():
					case <-time.After(5 * time.Second):
						t.Fatal("the session goes on while the BYE is in flight")
					}
					assert.Equal(t, sip.DialogStateEnded, d.LoadState())

					close(b.release)
					select {
					case err := <-sent:
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Fatal("Bye did not return after the 200")
					}
					assert.Equal(t, sip.DialogStateEnded, d.LoadState())
				})
			}
		})
	}
}
