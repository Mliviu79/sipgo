package sipgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/emiago/sipgo/sip"
)

var (
	ErrDialogOutsideDialog   = errors.New("Call/Transaction Outside Dialog")
	ErrDialogDoesNotExists   = errors.New("Call/Transaction Does Not Exist")
	ErrDialogInviteNoContact = errors.New("no Contact header")
	ErrDialogInvalidCseq     = errors.New("invalid CSEQ number")
	// ErrDialogAckTimeout is returned when a 2xx to the INVITE was retransmitted
	// for 64*T1 without an ACK. The dialog is then confirmed, and the session
	// should be ended with a BYE (RFC 3261 section 13.3.1.4).
	ErrDialogAckTimeout = errors.New("no ACK received for 2xx within 64*T1")
	// ErrDialogEndedBeforeAck is returned when the dialog ends before the ACK
	// to its 2xx is read, for example on a BYE.
	ErrDialogEndedBeforeAck = errors.New("No ACK received")
)

type ErrDialogResponse struct {
	Res *sip.Response
}

func (e ErrDialogResponse) Error() string {
	return fmt.Sprintf("Invite failed with response: %s", e.Res.StartLine())
}

type DialogStateFn func(s sip.DialogState)

// stateNotice is an entry of the queue of states the state callbacks are yet
// to be told of. With replay set it is a callback that OnStateReplay
// registers, which is added to the callbacks once the entries ahead of it are
// told, and told of state alone.
type stateNotice struct {
	state  sip.DialogState
	replay DialogStateFn
}

type Dialog struct {
	ID string

	// InviteRequest is set when dialog is created. It is not thread safe!
	// Use it only as read only and use methods to change headers
	InviteRequest *sip.Request

	// lastCSeqNo is set for every request within dialog except ACK CANCEL
	lastCSeqNo   atomic.Uint32
	remoteCSeqNo atomic.Uint32

	// InviteResponse is last response received or sent. It is not thread safe!
	// Use it only as read only and do not change values
	InviteResponse *sip.Response

	state atomic.Int32
	// stateMu orders the state transitions and guards the queue of those
	// the state callbacks are yet to be told of, see transition.
	stateMu        sync.Mutex
	stateQueue     []stateNotice
	stateNotifying bool

	ctx    context.Context
	cancel context.CancelCauseFunc

	onStatePointer atomic.Pointer[DialogStateFn]
}

// Init setups dialog state
func (d *Dialog) Init() {
	d.ctx, d.cancel = context.WithCancelCause(context.Background())
	d.state = atomic.Int32{}
	d.lastCSeqNo = atomic.Uint32{}

	// We may have sequence number initialized
	if cseq := d.InviteRequest.CSeq(); cseq != nil {
		d.lastCSeqNo.Store(cseq.SeqNo)
		d.remoteCSeqNo.Store(cseq.SeqNo)
	}
	d.onStatePointer = atomic.Pointer[DialogStateFn]{}
}

// OnState adds f to the callbacks told of each state transition, which are
// called newest first. They are told of one transition at a time, in the order
// the dialog took them, and with no lock held, so a callback may make a
// transition or send a request. A transition made while callbacks run is told
// once they have returned, and so possibly after the call that made it has
// returned.
func (d *Dialog) OnState(f DialogStateFn) {
	for {
		current := d.onStatePointer.Load()
		newCB := f
		if current != nil {
			cb := *current
			newCB = func(s sip.DialogState) {
				f(s)
				cb(s)
			}
		}
		if d.onStatePointer.CompareAndSwap(current, &newCB) {
			return
		}
	}

}

// OnStateReplay adds f to the callbacks told of each state transition, as
// OnState does, and tells f first of the state the dialog is in, which it
// returns. f is told of that state and then of each later transition, in the
// order the dialog took them. When transitions are still being told, f is added
// and told of the state once they have been, and so possibly after
// OnStateReplay has returned.
func (d *Dialog) OnStateReplay(f DialogStateFn) sip.DialogState {
	d.stateMu.Lock()
	state := d.LoadState()
	notify := d.enqueueStateUnsafe(stateNotice{state: state, replay: f})
	d.stateMu.Unlock()

	if notify {
		d.notifyStates()
	}
	return state
}

func (d *Dialog) InitWithState(s sip.DialogState) {
	d.Init()
	d.state.Store(int32(s))
}

// setState moves the dialog to s. A dialog only moves forward, from
// Established to Confirmed to Ended, so a request handled late, such as an ACK
// read after the BYE that followed it, cannot bring an ended dialog back, and a
// 2xx written again after the ACK does not take a confirmed dialog back to
// Established. A transition that changes nothing, repeated or refused, calls
// no state callback. It reports whether the dialog moved to s.
func (d *Dialog) setState(s sip.DialogState) bool {
	return d.transition(s, nil)
}

// endWithCause sets dialog state ended and place context cause error
// Experimental
func (d *Dialog) endWithCause(err error) {
	d.transition(sip.DialogStateEnded, err)
}

// transition moves the dialog to s as setState describes, ending its context
// with cause when s is Ended, and has the state callbacks told of it. The
// transitions are queued in the order they are made, and one goroutine at a
// time tells the callbacks of the queue: the one that finds nobody doing it.
// It reports whether the dialog moved to s.
func (d *Dialog) transition(s sip.DialogState, cause error) bool {
	d.stateMu.Lock()
	if d.state.Load() >= int32(s) {
		d.stateMu.Unlock()
		return false
	}
	d.state.Store(int32(s))
	if s == sip.DialogStateEnded {
		// Before any callback is told of the end.
		d.cancel(cause)
	}
	notify := d.enqueueStateUnsafe(stateNotice{state: s})
	d.stateMu.Unlock()

	if notify {
		d.notifyStates()
	}
	return true
}

// enqueueStateUnsafe queues n for the state callbacks and reports whether the
// caller is to tell them of the queue, as nobody is doing it. It must be called
// with stateMu held.
func (d *Dialog) enqueueStateUnsafe(n stateNotice) bool {
	d.stateQueue = append(d.stateQueue, n)
	notify := !d.stateNotifying
	d.stateNotifying = true
	return notify
}

// notifyStates tells the state callbacks of each queued transition in turn,
// until the queue is empty. No lock is held while a callback runs.
func (d *Dialog) notifyStates() {
	emptied := false
	defer func() {
		if !emptied {
			// A callback panicked. The transitions still queued are told by
			// the goroutine of the next one.
			d.stateMu.Lock()
			d.stateNotifying = false
			d.stateMu.Unlock()
		}
	}()

	for {
		d.stateMu.Lock()
		if len(d.stateQueue) == 0 {
			d.stateNotifying = false
			d.stateMu.Unlock()
			emptied = true
			return
		}
		n := d.stateQueue[0]
		d.stateQueue = d.stateQueue[1:]
		d.stateMu.Unlock()

		if n.replay != nil {
			d.OnState(n.replay)
			n.replay(n.state)
			continue
		}
		if f := d.onStatePointer.Load(); f != nil {
			cb := *f
			cb(n.state)
		}
	}
}

// endOnBye ends the dialog once our BYE is passed to its client transaction:
// RFC 3261 section 15.1.1 has the session over from then on, so a request the
// peer sends meanwhile, such as a re-INVITE, finds the dialog ended. It returns
// the context to wait for the answer to the BYE within, and its cancel. A ctx
// that is, or derives from, the dialog's context ends with the dialog, as the
// dialog's context cancels its children at once; the wait then goes on within
// ctx's deadline, if it has one, and within the BYE transaction's own timeout.
func (d *Dialog) endOnBye(ctx context.Context) (context.Context, context.CancelFunc) {
	live := ctx.Err() == nil
	d.setState(sip.DialogStateEnded)
	if !live || ctx.Err() == nil {
		return ctx, func() {}
	}
	waitCtx := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(waitCtx, deadline)
	}
	return waitCtx, func() {}
}

// Err returns error that caused dialog termination
func (d *Dialog) err() error {
	return context.Cause(d.Context())
}

func (d *Dialog) LoadState() sip.DialogState {
	return sip.DialogState(d.state.Load())
}

func (d *Dialog) StateRead() <-chan sip.DialogState {
	ch := make(chan sip.DialogState, 5)
	d.OnState(func(s sip.DialogState) {
		select {
		case ch <- s:
		default:
		}
	})

	return ch
}

func (d *Dialog) CSEQ() uint32 {
	return d.lastCSeqNo.Load()
}

func (d *Dialog) Context() context.Context {
	return d.ctx
}

// refuseOutOfOrder answers 500 to a request whose CSeq is below the remote
// sequence number and returns ErrDialogInvalidCseq, joined with any error
// sending the answer. RFC 3261 section 12.2.2 has such a request rejected as
// out of order. It returns nil for a request in order, and leaves the remote
// sequence number as it is.
func (d *Dialog) refuseOutOfOrder(req *sip.Request, tx sip.ServerTransaction) error {
	if req.CSeq().SeqNo >= d.remoteCSeqNo.Load() {
		return nil
	}
	res := sip.NewResponseFromRequest(req, sip.StatusInternalServerError, "Internal Server Error", nil)
	return errors.Join(ErrDialogInvalidCseq, tx.Respond(res))
}

func (d *Dialog) ReadRequest(req *sip.Request, tx sip.ServerTransaction) error {
	// UAS role of dialog SHOULD be
	// prepared to receive and process requests with CSeq values more than
	// one higher than the previous received request.
	oldCseq := d.remoteCSeqNo.Load()
	newSeqNo := req.CSeq().SeqNo
	if newSeqNo < oldCseq {
		return ErrDialogInvalidCseq
	}

	// 	A UAS that receives a second INVITE before it sends the final
	//    response to a first INVITE with a lower CSeq sequence number on the
	//    same dialog MUST return a 500 (Server Internal Error) respons
	if !d.remoteCSeqNo.CompareAndSwap(oldCseq, newSeqNo) {
		return ErrDialogInvalidCseq
	}

	return nil
}
