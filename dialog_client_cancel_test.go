package sipgo

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawUAS is a SIP peer on a loopback UDP socket, driven by hand from the test
// goroutine: it hands over each request the dialog sends, and answers exactly
// what the test tells it to.
type rawUAS struct {
	conn  net.PacketConn
	port  int
	toTag string
	reqs  chan rawRequest
	// seen holds every request handed over, retransmissions left out. Only
	// the test goroutine touches it.
	seen []*sip.Request
	keys map[string]bool
}

// rawRequest is a request the peer read, and where it came from.
type rawRequest struct {
	req *sip.Request
	src net.Addr
}

func newRawUAS(t *testing.T) *rawUAS {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	u := &rawUAS{
		conn:  conn,
		port:  conn.LocalAddr().(*net.UDPAddr).Port,
		toTag: "uas-tag",
		reqs:  make(chan rawRequest, 64),
		keys:  map[string]bool{},
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go u.readLoop(stop, done)
	t.Cleanup(func() {
		close(stop)
		_ = conn.Close()
		<-done
	})
	return u
}

func (u *rawUAS) readLoop(stop, done chan struct{}) {
	defer close(done)
	buf := make([]byte, 65535)
	for {
		n, src, err := u.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		msg, err := sip.ParseMessage(append([]byte(nil), buf[:n]...))
		if err != nil {
			continue
		}
		req, ok := msg.(*sip.Request)
		if !ok {
			continue
		}
		select {
		case u.reqs <- rawRequest{req: req, src: src}:
		case <-stop:
			return
		}
	}
}

// expect returns the next request of the given method. A retransmission of a
// request already read is skipped, as is the ACK a UAC transaction sends for
// a non-2xx final; any other request fails the test.
func (u *rawUAS) expect(t *testing.T, method sip.RequestMethod) rawRequest {
	t.Helper()
	for {
		select {
		case r := <-u.reqs:
			if !r.req.IsAck() {
				branch, _ := r.req.Via().Params.Get("branch")
				key := string(r.req.Method) + " " + branch
				if u.keys[key] {
					continue
				}
				u.keys[key] = true
			}
			u.seen = append(u.seen, r.req)
			if r.req.Method == method {
				return r
			}
			t.Fatalf("the peer read %s while it expected %s", r.req.Method, method)
		case <-time.After(5 * time.Second):
			t.Fatalf("the peer did not read a %s", method)
		}
	}
}

// sawMethod reports whether the peer read a request of the given method.
func (u *rawUAS) sawMethod(method sip.RequestMethod) bool {
	for _, req := range u.seen {
		if req.Method == method {
			return true
		}
	}
	return false
}

// reasons are the reason phrases of the statuses the peer answers with.
var reasons = map[int]string{
	sip.StatusRinging:                      "Ringing",
	sip.StatusOK:                           "OK",
	sip.StatusCallTransactionDoesNotExists: "Call/Transaction Does Not Exist",
	sip.StatusRequestTerminated:            "Request Terminated",
}

// respond answers r with code and returns the response. A response to the
// INVITE carries the peer's To tag and Contact, so its 2xx sets up the dialog.
func (u *rawUAS) respond(t *testing.T, r rawRequest, code int) *sip.Response {
	t.Helper()
	res := sip.NewResponseFromRequest(r.req, code, reasons[code], nil)
	if r.req.IsInvite() {
		res.To().Params.Add("tag", u.toTag)
		res.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: u.port}})
	}
	u.send(t, res, r.src)
	return res
}

func (u *rawUAS) send(t *testing.T, res *sip.Response, dst net.Addr) {
	t.Helper()
	_, err := u.conn.WriteTo([]byte(res.String()), dst)
	require.NoError(t, err)
}

// newRawClientDialog sends an INVITE to u over UDP and returns its dialog.
func newRawClientDialog(t *testing.T, u *rawUAS) *DialogClientSession {
	t.Helper()
	ua, err := NewUA()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ua.Close() })
	cli, err := NewClient(ua, WithClientHostname("127.0.0.1"))
	require.NoError(t, err)
	dua := &DialogUA{
		Client:     cli,
		ContactHDR: sip.ContactHeader{Address: sip.Uri{User: "uac", Host: "127.0.0.1", Port: 5060}},
	}
	d, err := dua.Invite(context.Background(), sip.Uri{User: "uas", Host: "127.0.0.1", Port: u.port}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// doneProbe is a context that reports when its Done channel is first asked
// for. WaitAnswer asks for it as it selects on the context, so a response the
// peer sends only after that finds the context already chosen.
type doneProbe struct {
	context.Context
	once   sync.Once
	called chan struct{}
}

func newDoneProbe(ctx context.Context) *doneProbe {
	return &doneProbe{Context: ctx, called: make(chan struct{})}
}

func (p *doneProbe) Done() <-chan struct{} {
	p.once.Do(func() { close(p.called) })
	return p.Context.Done()
}

// TestDialogClientAnsweredAfterCancel has the INVITE answered 2xx after the
// context WaitAnswer waits on has ended: the answer crosses our CANCEL, or
// arrives before any provisional let a CANCEL be sent (RFC 3261 section 9.1).
// The UAC core must acknowledge every 2xx it receives (RFC 3261 section
// 13.2.2.4), so WaitAnswer returns ErrAnsweredAfterCancel with the dialog
// established and its INVITE transaction kept: the caller's Ack reaches the
// peer, a retransmitted 2xx is acknowledged again, and the caller's Bye ends
// the call. The INVITE's final is read whatever the CANCEL's own answer; a
// non-2xx final returns the context's error, as before.
func TestDialogClientAnsweredAfterCancel(t *testing.T) {
	tests := []struct {
		name string
		// deadline has the context end by its deadline rather than be
		// cancelled on the 180.
		deadline bool
		// ringing has the peer send 180 before anything else. Without it the
		// context ends before any response.
		ringing bool
		// cancelStatus is the peer's answer to our CANCEL; 0 when no CANCEL
		// may be sent.
		cancelStatus int
		// crossing has the peer send its 2xx before answering the CANCEL,
		// which no longer matches the answered INVITE (RFC 3261 section 9.2).
		crossing    bool
		finalStatus int
		wantErr     error
	}{
		{name: "the CANCEL crosses the 200 and is answered 481", ringing: true, cancelStatus: sip.StatusCallTransactionDoesNotExists, crossing: true, finalStatus: sip.StatusOK, wantErr: context.Canceled},
		{name: "the CANCEL is answered 200 and the INVITE 200", ringing: true, cancelStatus: sip.StatusOK, finalStatus: sip.StatusOK, wantErr: context.Canceled},
		{name: "a 200 with no provisional after the context ended", finalStatus: sip.StatusOK, wantErr: context.Canceled},
		{name: "the CANCEL is answered 200 and the INVITE 487", ringing: true, cancelStatus: sip.StatusOK, finalStatus: sip.StatusRequestTerminated, wantErr: context.Canceled},
		{name: "the context ends by its deadline", deadline: true, ringing: true, cancelStatus: sip.StatusOK, finalStatus: sip.StatusOK, wantErr: context.DeadlineExceeded},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u := newRawUAS(t)
			d := newRawClientDialog(t, u)

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)
			if tc.deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			probe := newDoneProbe(ctx)

			invite := u.expect(t, sip.INVITE)
			if tc.ringing {
				u.respond(t, invite, sip.StatusRinging)
			} else {
				cancel()
			}

			opts := AnswerOptions{OnResponse: func(res *sip.Response) error {
				if res.StatusCode == sip.StatusRinging && !tc.deadline {
					cancel()
				}
				return nil
			}}
			answered := make(chan error, 1)
			go func() { answered <- d.WaitAnswer(probe, opts) }()

			var final *sip.Response
			if tc.cancelStatus == 0 {
				select {
				case <-probe.called:
				case <-time.After(5 * time.Second):
					t.Fatal("WaitAnswer did not select on its context")
				}
				final = u.respond(t, invite, tc.finalStatus)
			} else {
				cancelReq := u.expect(t, sip.CANCEL)
				if tc.crossing {
					final = u.respond(t, invite, tc.finalStatus)
					u.respond(t, cancelReq, tc.cancelStatus)
				} else {
					u.respond(t, cancelReq, tc.cancelStatus)
					final = u.respond(t, invite, tc.finalStatus)
				}
			}

			var err error
			select {
			case err = <-answered:
			case <-time.After(10 * time.Second):
				t.Fatal("WaitAnswer did not return")
			}
			require.ErrorIs(t, err, tc.wantErr)
			assert.False(t, tc.cancelStatus == 0 && u.sawMethod(sip.CANCEL), "a CANCEL was sent before any provisional")

			var answer *ErrAnsweredAfterCancel
			if !final.IsSuccess() {
				assert.False(t, errors.As(err, &answer), "a non-2xx final is not an answer")
				require.NotNil(t, d.InviteResponse)
				assert.Equal(t, tc.finalStatus, d.InviteResponse.StatusCode)
				return
			}

			require.ErrorAs(t, err, &answer)
			require.NotNil(t, answer.Res)
			assert.Equal(t, sip.StatusOK, answer.Res.StatusCode)
			assert.Contains(t, answer.Error(), strconv.Itoa(sip.StatusOK))
			assert.Equal(t, sip.DialogStateEstablished, d.LoadState())
			wantID, idErr := sip.DialogIDFromResponse(final)
			require.NoError(t, idErr)
			assert.Equal(t, wantID, d.ID)

			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			require.NoError(t, d.Ack(callCtx))
			u.expect(t, sip.ACK)

			// A retransmission of the 2xx is acknowledged again.
			u.send(t, final, invite.src)
			u.expect(t, sip.ACK)

			bye := make(chan error, 1)
			go func() { bye <- d.Bye(callCtx) }()
			u.respond(t, u.expect(t, sip.BYE), sip.StatusOK)
			select {
			case err := <-bye:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Bye did not return")
			}
		})
	}
}
