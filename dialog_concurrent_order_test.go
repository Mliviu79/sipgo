//go:build go1.25

package sipgo

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handOffRecorder notes the CSeq number of each request at the moment the
// client hands it to the transaction layer, and passes the request on to the
// requester it stands before. A scriptedPeer reads each request on a goroutine
// of its own, so the order it reads them in is not that of the hand-off.
type handOffRecorder struct {
	next ClientTransactionRequester
	// mu guards numbers, which every sender appends to.
	mu      sync.Mutex
	numbers []uint32
}

// Request notes req's CSeq number and passes req on.
func (r *handOffRecorder) Request(ctx context.Context, req *sip.Request) (sip.ClientTransaction, error) {
	r.mu.Lock()
	r.numbers = append(r.numbers, req.CSeq().SeqNo)
	r.mu.Unlock()
	return r.next.Request(ctx, req)
}

// handedOver returns the numbers noted, in the order of the hand-off.
func (r *handOffRecorder) handedOver() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.numbers)
}

// requestSender is a dialog of either role, with the ways it sends a request
// inside the dialog.
type requestSender interface {
	inDialogSession
	WriteRequest(req *sip.Request) error
}

// TestDialogConcurrentRequestOrder has several goroutines send requests inside
// one dialog at the same time, on either role of the dialog. RFC 3261 section
// 12.2.2 has the peer reject a request whose CSeq number is lower than that of
// a request it has already read, so the requests are handed to the transaction
// layer in the order of their numbers: none overtakes one numbered before it.
// That holds for a request sent with Do, for the copy Do sends again with the
// dialog's credentials when the peer challenges the first, which takes a
// number of its own among those of the other senders, and for a request
// written with WriteRequest.
func TestDialogConcurrentRequestOrder(t *testing.T) {
	const (
		senders   = 8
		perSender = 50
	)
	roles := []struct {
		name string
		// newConfirmed returns a confirmed dialog, the client it sends on and
		// the peer that holds the requests sent inside it.
		newConfirmed func(t *testing.T) (*scriptedPeer, *Client, requestSender)
	}{
		{
			name: "UAC",
			newConfirmed: func(t *testing.T) (*scriptedPeer, *Client, requestSender) {
				p, d := newScriptedDialog(t)
				p.answer(p.next(t, sip.INVITE), sip.StatusOK, "OK")
				require.NoError(t, d.WaitAnswer(t.Context(), AnswerOptions{}))
				require.NoError(t, d.Ack(t.Context()))
				p.next(t, sip.ACK)
				return p, d.UA.Client, d
			},
		},
		{
			name: "UAS",
			newConfirmed: func(t *testing.T) (*scriptedPeer, *Client, requestSender) {
				p, d := newScriptedServerDialog(t)
				return p, d.ua.Client, d
			},
		},
	}
	ways := []struct {
		name string
		// credentials has the dialog hold credentials, which answer a
		// challenge.
		credentials bool
		// requests is how many requests of the dialog one send hands over.
		requests int
		send     func(ctx context.Context, d requestSender) error
		// answer returns what the peer answers r with, nil for no answer.
		answer func(p *scriptedPeer, r sentRequest) *sip.Response
	}{
		{
			name:     "Do",
			requests: 1,
			send: func(ctx context.Context, d requestSender) error {
				_, err := d.Do(ctx, sip.NewRequest(sip.INFO, peerContact))
				return err
			},
			answer: func(p *scriptedPeer, r sentRequest) *sip.Response {
				return p.response(r, sip.StatusOK, "OK")
			},
		},
		{
			// The peer challenges each request that carries no credentials, so
			// every Do sends its request twice.
			name:        "DoChallenged",
			credentials: true,
			requests:    2,
			send: func(ctx context.Context, d requestSender) error {
				_, err := d.Do(ctx, sip.NewRequest(sip.INFO, peerContact))
				return err
			},
			answer: func(p *scriptedPeer, r sentRequest) *sip.Response {
				if r.req.GetHeader("Authorization") == nil {
					return p.challengeTo(r)
				}
				return p.response(r, sip.StatusOK, "OK")
			},
		},
		{
			// A request written to the transport waits for no answer.
			name:     "WriteRequest",
			requests: 1,
			send: func(_ context.Context, d requestSender) error {
				return d.WriteRequest(sip.NewRequest(sip.INFO, peerContact))
			},
			answer: func(*scriptedPeer, sentRequest) *sip.Response { return nil },
		},
	}

	for _, role := range roles {
		t.Run(role.name, func(t *testing.T) {
			for _, way := range ways {
				t.Run(way.name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						p, client, d := role.newConfirmed(t)
						if way.credentials {
							d.SetCredentials(testAuth)
						}
						handOff := &handOffRecorder{next: client.TxRequester}
						client.TxRequester = handOff
						ctx := t.Context()
						first := d.CSEQ() + 1

						start := make(chan struct{})
						results := make(chan error, senders)
						for range senders {
							go func() {
								<-start
								for range perSender {
									if err := way.send(ctx, d); err != nil {
										results <- err
										return
									}
								}
								results <- nil
							}()
						}
						close(start)

						// The peer reads each request on a goroutine of its
						// own, which ends once the request is taken here, so
						// every request is taken, whether or not its sender
						// waits for an answer.
						total := senders * perSender * way.requests
						for read, finished := 0, 0; read < total || finished < senders; {
							select {
							case r := <-p.sent:
								read++
								if res := way.answer(p, r); res != nil {
									go r.w.Receive(res)
								}
							case err := <-results:
								require.NoError(t, err)
								finished++
							}
						}

						want := make([]uint32, total)
						for i := range want {
							want[i] = first + uint32(i)
						}
						assert.Equal(t, want, handOff.handedOver(), "the requests reach the transaction layer in the order of their CSeq numbers")
					})
				})
			}
		})
	}
}
