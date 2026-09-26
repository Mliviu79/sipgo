package sipgo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationDialog(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}

	ua, _ := NewUA()
	defer ua.Close()
	srv, _ := NewServer(ua)
	cli, _ := NewClient(ua)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}

	dialogSrv := NewDialogServerCache(cli, uasContact)
	// digestChal := digest.Challenge{
	// 	Username: "alice",
	// 	Password: "alice123",
	// }
	digestChal := digest.Challenge{
		Realm:     "sipgo-server",
		Nonce:     fmt.Sprintf("%d", time.Now().UnixMicro()),
		Opaque:    "sipgo",
		Algorithm: "MD5",
	}
	auth := digest.Options{
		Method:   "INVITE",
		URI:      uasContact.Address.Addr(),
		Username: "alice",
		Password: "1234",
	}

	// Handlers run off the test goroutine, so they report their failures
	// here for the test goroutine, and callDone has each call the UAS answered
	// once its handler is done with it.
	errs := newHandlerErrors()
	callDone := make(chan struct{}, 2)
	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		dlg, err := dialogSrv.ReadInvite(req, tx)
		if err != nil {
			errs.report("UAS: read INVITE: %w", err)
			return
		}
		// defer dlg.Close()

		if err := dlg.authDigest(&digestChal, auth); err != nil {
			// The UAC sends the INVITE again with its credentials.
			if !errors.Is(err, errDialogUnauthorized) {
				errs.report("UAS: challenge INVITE: %w", err)
			}
			return
		}
		defer func() { callDone <- struct{}{} }()

		for _, code := range []int{sip.StatusTrying, sip.StatusRinging} {
			if err := dlg.Respond(code, "", nil); err != nil {
				errs.report("UAS: respond %d: %w", code, err)
				return
			}
		}

		err = dlg.Respond(sip.StatusOK, "OK", nil)
		if errors.Is(err, ErrDialogEndedBeforeAck) {
			// Requests are handled on goroutines of their own, so the BYE that
			// the UAC sends right behind its ACK can be read first and end the
			// dialog before the ACK is read.
			return
		}
		if err != nil {
			errs.report("UAS: respond 200: %w", err)
			return
		}

		// The UAC says which side hangs up.
		if h := req.GetHeader("X-Hangup"); h != nil && h.Value() == "uas" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := dlg.Bye(ctx); err != nil {
				errs.report("UAS: BYE: %w", err)
			}
			return
		}
		select {
		case <-dlg.Context().Done():
		case <-time.After(5 * time.Second):
			errs.report("UAS: the UAC's BYE did not end the dialog")
		}
	})

	srv.OnAck(func(req *sip.Request, tx sip.ServerTransaction) {
		if req.Recipient.Addr() != uasContact.Address.Addr() {
			tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Not valid SIP uri", nil))
			return
		}
		if err := dialogSrv.ReadAck(req, tx); err != nil {
			tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, err.Error(), nil))
		}
	})

	srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
		if req.Recipient.Addr() != uasContact.Address.Addr() {
			tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Not valid SIP uri", nil))
			return
		}

		if err := dialogSrv.ReadBye(req, tx); err != nil {
			tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, err.Error(), nil))
		}
	})

	srv.serveRequest(func(r *sip.Request) {
		t.Log("UAS server: ", r.StartLine())
	})

	startTestServer(t, ctx, srv, uasContact.Address.HostPort())

	// Client
	{
		ua, _ := NewUA()
		defer ua.Close()

		srv, _ := NewServer(ua)
		cli, _ := NewClient(ua, WithClientConnectionAddr("127.0.0.200:0"))

		// Use for now empheral contact based on client connection
		contactHDR := sip.ContactHeader{}
		dialogCli := NewDialogClientCache(cli, contactHDR)

		// Setup server side
		srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
			if err := dialogCli.ReadBye(req, tx); err != nil {
				errs.report("UAC: read BYE: %w", err)
			}
		})
		srv.serveRequest(func(r *sip.Request) {
			t.Log("UAC server: ", r.StartLine())
		})

		// waitCall waits for the UAS to be done with the call.
		waitCall := func(t *testing.T) {
			t.Helper()
			select {
			case <-callDone:
			case <-time.After(10 * time.Second):
				t.Fatal("the UAS handler did not return")
			}
			errs.check(t)
		}

		t.Run("UAShangup", func(t *testing.T) {
			// INVITE
			t.Log("UAC: INVITE")
			sess, err := dialogCli.Invite(context.TODO(), uasContact.Address, nil, sip.NewHeader("X-Hangup", "uas"))
			require.NoError(t, err)
			defer sess.Close()

			err = sess.WaitAnswer(ctx, AnswerOptions{
				Username: auth.Username,
				Password: auth.Password,
			})
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, sess.InviteResponse.StatusCode)

			// ACK
			t.Log("UAC: ACK")
			err = sess.Ack(context.TODO())
			require.NoError(t, err)

			select {
			case <-sess.inviteTx.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("the UAS's BYE did not end the call")
			}
			waitCall(t)
		})

		t.Run("UAC hangup", func(t *testing.T) {
			// INVITE
			t.Log("UAC: INVITE")
			sess, err := dialogCli.Invite(context.TODO(), uasContact.Address, nil, sip.NewHeader("X-Hangup", "uac"))
			require.NoError(t, err)
			defer sess.Close()

			err = sess.WaitAnswer(ctx, AnswerOptions{
				Username: auth.Username,
				Password: auth.Password,
			})
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, sess.InviteResponse.StatusCode)

			// ACK
			t.Log("UAC: ACK")
			err = sess.Ack(context.TODO())
			require.NoError(t, err)
			// BYE
			t.Log("UAC: BYE")
			err = sess.Bye(context.TODO())
			require.NoError(t, err)

			select {
			case <-sess.inviteTx.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("the BYE did not end the call")
			}
			waitCall(t)
		})

		require.Empty(t, dialogCli.dialogsLen())
	}

}

func TestIntegrationDialogBrokenUAC(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}

	ua, _ := NewUA()
	defer ua.Close()
	srv, _ := NewServer(ua)
	cli, _ := NewClient(ua)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.201", Port: 5099},
	}

	dialogSrv := NewDialogServerCache(cli, uasContact)

	// Handlers run off the test goroutine, so they report their failures
	// here for the test goroutine.
	errs := newHandlerErrors()
	defer errs.check(t)
	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		dlg, err := dialogSrv.ReadInvite(req, tx)
		if err != nil {
			errs.report("UAS: read INVITE: %w", err)
			return
		}
		// defer dlg.Close()

		err = dlg.Respond(sip.StatusTrying, "Trying", nil)
		if err != nil {
			fmt.Println("Error OnInvite", err)
			return
		}
		err = dlg.Respond(sip.StatusRinging, "Ringing", nil)
		if err != nil {
			fmt.Println("Error OnInvite", err)
			return
		}
		err = dlg.Respond(sip.StatusOK, "OK", nil)
		if err != nil {
			fmt.Println("Error OnInvite", err)
			return
		}
		select {
		case <-dlg.Context().Done():
		case <-ctx.Done():
		}
	})

	srv.OnAck(func(req *sip.Request, tx sip.ServerTransaction) {
		dialogSrv.ReadAck(req, tx)
	})

	srv.serveRequest(func(r *sip.Request) {
		t.Log("UAS server: ", r.StartLine())
	})

	startTestServer(t, ctx, srv, uasContact.Address.HostPort())

	// Client
	{
		ua, _ := NewUA()
		defer ua.Close()

		srv, _ := NewServer(ua)
		cli, _ := NewClient(ua)

		contactHDR := sip.ContactHeader{
			Address: sip.Uri{User: "test", Host: "127.0.0.201", Port: 5088},
		}
		dialogCli := NewDialogClientCache(cli, contactHDR)

		// Setup server side
		srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
			if err := dialogCli.ReadBye(req, tx); err != nil {
				errs.report("UAC: read BYE: %w", err)
			}
		})
		srv.serveRequest(func(r *sip.Request) {
			t.Log("UAC server: ", r.StartLine())
		})

		startTestServer(t, ctx, srv, contactHDR.Address.HostPort())

		t.Run("UAS BYE Error", func(t *testing.T) {
			srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
				tx.Respond(sip.NewResponseFromRequest(req, sip.StatusInternalServerError, "", nil))
			})
			// INVITE
			t.Log("UAC: INVITE ", uasContact.Address.String())
			sess, err := dialogCli.Invite(context.TODO(), uasContact.Address, nil)
			require.NoError(t, err)
			defer sess.Close()

			err = sess.WaitAnswer(ctx, AnswerOptions{})
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, sess.InviteResponse.StatusCode)

			// ACK
			t.Log("UAC: ACK")
			err = sess.Ack(context.TODO())
			require.NoError(t, err)
			// BYE
			t.Log("UAC: BYE")
			err = sess.Bye(context.TODO())
			require.Error(t, err)
			require.Empty(t, dialogCli.dialogsLen())
		})

		t.Run("UAS ACK Error", func(t *testing.T) {
			// INVITE
			t.Log("UAC: INVITE ", uasContact.Address.String())
			sess, err := dialogCli.Invite(context.TODO(), uasContact.Address, nil)
			require.NoError(t, err)
			defer sess.Close()

			err = sess.WaitAnswer(ctx, AnswerOptions{})
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, sess.InviteResponse.StatusCode)

			// ACK
			t.Log("UAC: ACK")
			sess.InviteResponse.Contact().Address.Host = "nodestination.dst"
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
			defer cancel()
			err = sess.Ack(ctx)
			require.Error(t, err)

			sess.Close()
			require.Empty(t, dialogCli.dialogsLen())
		})

	}

}

func TestIntegrationDialogCancel(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}

	ua, _ := NewUA()
	defer ua.Close()
	srv, _ := NewServer(ua)
	cli, _ := NewClient(ua)
	// sip.SetTimers(10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uasContact := sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5099},
	}

	dialogSrv := NewDialogServerCache(cli, uasContact)
	// The handler runs off the test goroutine, so it reports its failures
	// here for the test goroutine, and closes handled when it returns.
	errs := newHandlerErrors()
	handled := make(chan struct{})
	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		defer close(handled)
		dlg, err := dialogSrv.ReadInvite(req, tx)
		if err != nil {
			errs.report("UAS: read INVITE: %w", err)
			return
		}

		for _, code := range []int{sip.StatusTrying, sip.StatusRinging} {
			if err := dlg.Respond(code, "", nil); err != nil {
				errs.report("UAS: respond %d: %w", code, err)
				return
			}
		}

		select {
		case <-dlg.Context().Done():
		case <-time.After(10 * time.Second):
			errs.report("UAS: the CANCEL did not end the dialog")
		}
	})

	srv.OnCancel(func(req *sip.Request, tx sip.ServerTransaction) {
		fmt.Println("Cancel received")
	})

	srv.serveRequest(func(r *sip.Request) {
		fmt.Println("UAS server: ", r.StartLine())
	})

	startTestServer(t, ctx, srv, uasContact.Address.HostPort())

	// Client
	{
		ua, _ := NewUA()
		defer ua.Close()

		srv, _ := NewServer(ua)
		cli, _ := NewClient(ua)

		contactHDR := sip.ContactHeader{
			Address: sip.Uri{User: "test", Host: "127.0.0.200", Port: 5088},
		}
		dialogCli := NewDialogClientCache(cli, contactHDR)

		srv.serveRequest(func(r *sip.Request) {
			t.Log("UAC server: ", r.StartLine())
		})

		startTestServer(t, ctx, srv, contactHDR.Address.HostPort())

		// INVITE
		t.Log("UAC: INVITE")
		sess, err := dialogCli.Invite(context.TODO(), uasContact.Address, nil)
		require.NoError(t, err)
		defer sess.Close()

		// Cancel a call
		ctx, cancel := context.WithCancel(sess.Context())
		err = sess.WaitAnswer(ctx, AnswerOptions{OnResponse: func(res *sip.Response) error {
			if res.StatusCode == sip.StatusRinging {
				cancel()
			}
			return nil
		}})
		require.ErrorIs(t, err, context.Canceled)
		assert.EqualValues(t, 487, sess.InviteResponse.StatusCode)
	}

	select {
	case <-handled:
	case <-time.After(15 * time.Second):
		t.Fatal("the UAS handler did not return")
	}
	errs.check(t)
}

// startTestServer serves srv over UDP on hostPort until ctx is done, and
// returns once its listener is ready, which it signals only once the listener
// is in the connection pool.
func startTestServer(t testing.TB, ctx context.Context, srv *Server, hostPort string) {
	t.Helper()
	srvReady := make(chan struct{})
	served := make(chan error, 1)
	go func() {
		served <- srv.ListenAndServe(
			context.WithValue(ctx, ListenReadyCtxKey, ListenReadyCtxValue(srvReady)),
			"udp",
			hostPort,
		)
	}()
	select {
	case <-srvReady:
	case err := <-served:
		t.Fatalf("serving %s failed: %v", hostPort, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("the listener on %s was not ready", hostPort)
	}
}

// handlerErrors collects the failures of server handlers. Handlers run off
// the test goroutine, where a test may not be failed, so the test goroutine
// reports them.
type handlerErrors chan error

func newHandlerErrors() handlerErrors {
	return make(handlerErrors, 32)
}

// report records a failure, dropping it once 32 are waiting.
func (h handlerErrors) report(format string, args ...any) {
	select {
	case h <- fmt.Errorf(format, args...):
	default:
	}
}

// check fails t with each failure recorded so far.
func (h handlerErrors) check(t testing.TB) {
	t.Helper()
	for {
		select {
		case err := <-h:
			t.Error(err)
		default:
			return
		}
	}
}
