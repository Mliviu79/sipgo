//go:build go1.25

package sipgo

import (
	"slices"
	"testing"
	"testing/synctest"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDialogConcurrentDoCSeq has several goroutines send requests inside one
// dialog with Do at the same time, on either role of the dialog. RFC 3261
// section 12.2.1.1 has the requests of a dialog numbered increasing by one,
// so no two of them take the same CSeq number and none is skipped: the
// numbers the peer reads are the ones that follow the dialog's last, each
// once, and the dialog's last number is the highest of them.
func TestDialogConcurrentDoCSeq(t *testing.T) {
	const (
		senders   = 8
		perSender = 50
	)
	roles := []struct {
		name string
		// newConfirmed returns a confirmed dialog and the peer that holds
		// the requests sent inside it.
		newConfirmed func(t *testing.T) (*scriptedPeer, inDialogSession)
	}{
		{
			name: "UAC",
			newConfirmed: func(t *testing.T) (*scriptedPeer, inDialogSession) {
				p, d := newScriptedDialog(t)
				p.answer(p.next(t, sip.INVITE), sip.StatusOK, "OK")
				require.NoError(t, d.WaitAnswer(t.Context(), AnswerOptions{}))
				require.NoError(t, d.Ack(t.Context()))
				p.next(t, sip.ACK)
				return p, d
			},
		},
		{
			name: "UAS",
			newConfirmed: func(t *testing.T) (*scriptedPeer, inDialogSession) {
				p, d := newScriptedServerDialog(t)
				return p, d
			},
		},
	}

	for _, role := range roles {
		t.Run(role.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := role.newConfirmed(t)
				ctx := t.Context()
				first := d.CSEQ() + 1

				start := make(chan struct{})
				results := make(chan error, senders)
				for range senders {
					go func() {
						<-start
						for range perSender {
							if _, err := d.Do(ctx, sip.NewRequest(sip.INFO, peerContact)); err != nil {
								results <- err
								return
							}
						}
						results <- nil
					}()
				}
				close(start)

				// A sender returns once the peer has answered each of its
				// requests, so when all have returned the peer has read
				// every request.
				var got []uint32
				for finished := 0; finished < senders; {
					select {
					case r := <-p.sent:
						got = append(got, r.req.CSeq().SeqNo)
						go r.w.Receive(p.response(r, sip.StatusOK, "OK"))
					case err := <-results:
						require.NoError(t, err)
						finished++
					}
				}

				want := make([]uint32, senders*perSender)
				for i := range want {
					want[i] = first + uint32(i)
				}
				slices.Sort(got)
				assert.Equal(t, want, got, "each request takes a CSeq number of its own, the next of the dialog")
				assert.Equal(t, want[len(want)-1], d.CSEQ(), "the dialog's last number is that of the last request numbered")
			})
		})
	}
}
