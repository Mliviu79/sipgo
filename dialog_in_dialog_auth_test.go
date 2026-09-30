package sipgo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/icholy/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAuth is what the dialog answers the peer's challenge with.
var testAuth = DigestAuth{Username: "alice", Password: "alice-test-password"}

const testRealm = "peer.test"

// inDialogSession is what the tests drive on either role of a dialog.
type inDialogSession interface {
	Bye(ctx context.Context) error
	Do(ctx context.Context, req *sip.Request) (*sip.Response, error)
	SetCredentials(auth DigestAuth)
	LoadState() sip.DialogState
	Context() context.Context
}

// inDialogRole sets up a confirmed dialog whose peer is a raw UAS, and returns
// it with the peer's remote target.
type inDialogRole struct {
	name         string
	newConfirmed func(t *testing.T, u *rawUAS) (inDialogSession, sip.Uri)
}

var inDialogRoles = []inDialogRole{
	{
		name: "UAS",
		newConfirmed: func(t *testing.T, u *rawUAS) (inDialogSession, sip.Uri) {
			ua, err := NewUA()
			require.NoError(t, err)
			t.Cleanup(func() { _ = ua.Close() })
			cli, err := NewClient(ua, WithClientHostname("127.0.0.1"))
			require.NoError(t, err)
			dua := &DialogUA{
				Client:     cli,
				ContactHDR: sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: 5060}},
			}
			peer := sip.Uri{User: "peer", Host: "127.0.0.1", Port: u.port}
			invite, _, _ := createTestInvite(t, "sip:uas@127.0.0.1", "UDP", fmt.Sprintf("127.0.0.1:%d", u.port))
			invite.AppendHeader(&sip.ContactHeader{Address: peer})
			tx := siptest.NewServerTxRecorder(invite)
			t.Cleanup(tx.Terminate)
			d, err := dua.ReadInvite(invite, tx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			d.InviteResponse = sip.NewResponseFromRequest(invite, sip.StatusOK, "OK", nil)
			d.setState(sip.DialogStateConfirmed)
			return d, peer
		},
	},
	{
		name: "UAC",
		newConfirmed: func(t *testing.T, u *rawUAS) (inDialogSession, sip.Uri) {
			d := newRawClientDialog(t, u)
			answered := make(chan error, 1)
			go func() { answered <- d.WaitAnswer(context.Background(), AnswerOptions{}) }()
			u.respond(t, u.expect(t, sip.INVITE), sip.StatusOK)
			select {
			case err := <-answered:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("WaitAnswer did not return after the 200")
			}
			require.NoError(t, d.Ack(context.Background()))
			u.expect(t, sip.ACK)
			return d, d.InviteResponse.Contact().Address
		},
	},
}

// inDialogRow is a request sent inside the dialog and what the peer answers
// each copy of it with.
type inDialogRow struct {
	name        string
	credentials bool
	// reinvite sends a re-INVITE through Do; otherwise the dialog's Bye is
	// sent.
	reinvite bool
	// answers holds, for each copy of the request the peer reads, the
	// statuses it answers that copy with, in turn.
	answers [][]int
	// wantStatus is the status of the final returned: the response Do
	// returns, or the ErrDialogResponse Bye returns, 200 standing for nil.
	wantStatus int
}

// TestDialogInDialogChallengeAnsweredOnce has the peer challenge a request we
// send inside a dialog. RFC 3261 section 8.1.3.5 has the UAC retry a request
// answered 401 or 407 with credentials (sections 22.2 and 22.3), so with
// credentials set the request is sent once more, on a new transaction of the
// dialog with its next CSeq (section 12.2.1.1), carrying Authorization or
// Proxy-Authorization for them, and that copy's final is the answer. A second
// challenge is the credentials refused and is returned as the final; with no
// credentials the first challenge is. A BYE ends the dialog at its hand-off
// (section 15.1.1), before any answer.
func TestDialogInDialogChallengeAnsweredOnce(t *testing.T) {
	rows := []inDialogRow{
		{
			name:        "a BYE answered 401 is sent again with Authorization",
			credentials: true,
			answers:     [][]int{{sip.StatusUnauthorized}, {sip.StatusOK}},
			wantStatus:  sip.StatusOK,
		},
		{
			name:        "a BYE answered 407 is sent again with Proxy-Authorization",
			credentials: true,
			answers:     [][]int{{sip.StatusProxyAuthRequired}, {sip.StatusOK}},
			wantStatus:  sip.StatusOK,
		},
		{
			name:        "a second challenge is the final",
			credentials: true,
			answers:     [][]int{{sip.StatusUnauthorized}, {sip.StatusUnauthorized}},
			wantStatus:  sip.StatusUnauthorized,
		},
		{
			name:       "with no credentials the first challenge is the final",
			answers:    [][]int{{sip.StatusUnauthorized}},
			wantStatus: sip.StatusUnauthorized,
		},
		{
			name:        "a re-INVITE through Do answered 401 is sent again",
			credentials: true,
			reinvite:    true,
			answers:     [][]int{{sip.StatusUnauthorized}, {sip.StatusOK}},
			wantStatus:  sip.StatusOK,
		},
	}
	runInDialogRows(t, rows)
}

// TestDialogWriteByeReadsPastProvisional has the peer answer our BYE with a
// provisional before its final. The non-INVITE client transaction passes a
// provisional to the TU and completes on the final (RFC 3261 section
// 17.1.2.2), so a provisional is not the BYE's answer: the final is, a
// challenge after it included.
func TestDialogWriteByeReadsPastProvisional(t *testing.T) {
	rows := []inDialogRow{
		{
			name:       "100 then 200",
			answers:    [][]int{{sip.StatusTrying, sip.StatusOK}},
			wantStatus: sip.StatusOK,
		},
		{
			name:        "100 then 401 is sent again, then 200",
			credentials: true,
			answers:     [][]int{{sip.StatusTrying, sip.StatusUnauthorized}, {sip.StatusOK}},
			wantStatus:  sip.StatusOK,
		},
	}
	runInDialogRows(t, rows)
}

// runInDialogRows runs each row on each role of a dialog.
func runInDialogRows(t *testing.T, rows []inDialogRow) {
	t.Helper()
	for _, role := range inDialogRoles {
		t.Run(role.name, func(t *testing.T) {
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					runInDialogRow(t, role, row)
				})
			}
		})
	}
}

// inDialogResult is what the request sent inside the dialog returned.
type inDialogResult struct {
	res *sip.Response
	err error
}

func runInDialogRow(t *testing.T, role inDialogRole, row inDialogRow) {
	u := newRawUAS(t)
	d, target := role.newConfirmed(t, u)
	if row.credentials {
		d.SetCredentials(testAuth)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	method := sip.BYE
	if row.reinvite {
		method = sip.INVITE
	}
	done := make(chan inDialogResult, 1)
	go func() {
		if row.reinvite {
			res, err := d.Do(ctx, sip.NewRequest(sip.INVITE, target))
			done <- inDialogResult{res: res, err: err}
			return
		}
		done <- inDialogResult{err: d.Bye(ctx)}
	}()

	var read []*sip.Request
	for i, statuses := range row.answers {
		r := u.expectPastAck(t, method)
		if i == 0 && method == sip.BYE {
			select {
			case <-d.Context().Done():
			case <-time.After(5 * time.Second):
				t.Fatal("the dialog goes on after our BYE was handed over")
			}
		}
		read = append(read, r.req)
		for _, status := range statuses {
			u.reply(t, r, status, nonceFor(i))
		}
	}

	var got inDialogResult
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the %s sent inside the dialog did not return", method)
	}
	u.assertNoOther(t, method)

	switch {
	case row.reinvite:
		require.NoError(t, got.err)
		require.NotNil(t, got.res)
		assert.Equal(t, row.wantStatus, got.res.StatusCode)
	case row.wantStatus == sip.StatusOK:
		assert.NoError(t, got.err)
	default:
		var answer ErrDialogResponse
		require.ErrorAs(t, got.err, &answer)
		assert.Equal(t, row.wantStatus, answer.Res.StatusCode)
	}
	if method == sip.BYE {
		assert.Equal(t, sip.DialogStateEnded, d.LoadState())
	}

	require.Len(t, read, len(row.answers))
	assert.Nil(t, read[0].GetHeader("Authorization"))
	assert.Nil(t, read[0].GetHeader("Proxy-Authorization"))
	for i := 1; i < len(read); i++ {
		prev, retry := read[i-1], read[i]
		assert.Equal(t, prev.CSeq().SeqNo+1, retry.CSeq().SeqNo, "the copy sent again takes the dialog's next CSeq")
		assert.Equal(t, method, retry.CSeq().MethodName)
		assert.Equal(t, prev.CallID().Value(), retry.CallID().Value())
		prevBranch, _ := prev.Via().Params.Get("branch")
		retryBranch, _ := retry.Via().Params.Get("branch")
		assert.NotEqual(t, prevBranch, retryBranch, "the copy sent again is a new transaction")
		challenge := row.answers[i-1][len(row.answers[i-1])-1]
		assertAuthorized(t, retry, challenge, nonceFor(i-1))
	}
}

func nonceFor(i int) string {
	return fmt.Sprintf("nonce-%d", i)
}

// reply answers r with status. A 401 carries WWW-Authenticate and a 407
// Proxy-Authenticate, each a challenge for nonce.
func (u *rawUAS) reply(t *testing.T, r rawRequest, status int, nonce string) {
	t.Helper()
	phrases := map[int]string{
		sip.StatusTrying:            "Trying",
		sip.StatusOK:                "OK",
		sip.StatusUnauthorized:      "Unauthorized",
		sip.StatusProxyAuthRequired: "Proxy Authentication Required",
	}
	res := sip.NewResponseFromRequest(r.req, status, phrases[status], nil)
	challenge := fmt.Sprintf(`Digest realm="%s", nonce="%s", algorithm=MD5`, testRealm, nonce)
	switch status {
	case sip.StatusUnauthorized:
		res.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge))
	case sip.StatusProxyAuthRequired:
		res.AppendHeader(sip.NewHeader("Proxy-Authenticate", challenge))
	}
	u.send(t, res, r.src)
}

// expectPastAck returns the next request of method the peer reads, passing
// over the ACKs a UAC transaction sends for a non-2xx final to an INVITE and
// the retransmissions of a request already read.
func (u *rawUAS) expectPastAck(t *testing.T, method sip.RequestMethod) rawRequest {
	t.Helper()
	for {
		select {
		case r := <-u.reqs:
			if r.req.IsAck() {
				continue
			}
			if u.seenBefore(r.req) {
				continue
			}
			u.seen = append(u.seen, r.req)
			if r.req.Method != method {
				t.Fatalf("the peer read %s while it expected %s", r.req.Method, method)
			}
			return r
		case <-time.After(5 * time.Second):
			t.Fatalf("the peer did not read a %s", method)
		}
	}
}

// assertNoOther fails when the peer reads another request of method, other
// than a retransmission of one already read. A request is on the loopback
// socket once its sender has returned, so the wait is short.
func (u *rawUAS) assertNoOther(t *testing.T, method sip.RequestMethod) {
	t.Helper()
	bound := time.After(300 * time.Millisecond)
	for {
		select {
		case r := <-u.reqs:
			if r.req.IsAck() || u.seenBefore(r.req) {
				continue
			}
			if r.req.Method == method {
				t.Fatalf("the peer read another %s", method)
			}
		case <-bound:
			return
		}
	}
}

// seenBefore reports whether req is a retransmission of a request already
// read, and records it as read otherwise.
func (u *rawUAS) seenBefore(req *sip.Request) bool {
	branch, _ := req.Via().Params.Get("branch")
	key := string(req.Method) + " " + branch
	if u.keys[key] {
		return true
	}
	u.keys[key] = true
	return false
}

// assertAuthorized checks that retry answers the challenge status, a 401 or
// a 407, for nonce with testAuth: its digest is for its own method and
// Request-URI.
func assertAuthorized(t *testing.T, retry *sip.Request, status int, nonce string) {
	t.Helper()
	header, other := "Authorization", "Proxy-Authorization"
	if status == sip.StatusProxyAuthRequired {
		header, other = other, header
	}
	assert.Nil(t, retry.GetHeader(other))
	h := retry.GetHeader(header)
	require.NotNil(t, h, "the copy sent again carries %s", header)
	cred, err := digest.ParseCredentials(h.Value())
	require.NoError(t, err)
	uri := retry.Recipient.Addr()
	assert.Equal(t, testAuth.Username, cred.Username)
	assert.Equal(t, testRealm, cred.Realm)
	assert.Equal(t, nonce, cred.Nonce)
	assert.Equal(t, uri, cred.URI)
	want, err := digest.Digest(&digest.Challenge{Realm: testRealm, Nonce: nonce, Algorithm: "MD5"}, digest.Options{
		Method:   retry.Method.String(),
		URI:      uri,
		Username: testAuth.Username,
		Password: testAuth.Password,
	})
	require.NoError(t, err)
	assert.Equal(t, want.Response, cred.Response)
}
