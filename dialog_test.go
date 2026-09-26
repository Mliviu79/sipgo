package sipgo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
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
