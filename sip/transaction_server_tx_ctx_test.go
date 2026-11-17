package sip

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerTxRespondEndedContextHandsNothing(t *testing.T) {
	tx, req, _ := newTestInviteServerTx(t, 0)

	cause := errors.New("handler ended")
	ctx, cancel := context.WithCancelCause(testCtx(t))
	cancel(cause)

	res := NewResponseFromRequest(req, StatusBusyHere, "Busy Here", nil)
	err := tx.Respond(ctx, res)
	require.ErrorIs(t, err, cause)

	tx.fsmMu.Lock()
	defer tx.fsmMu.Unlock()
	assert.Nil(t, tx.fsmResp, "a response was handed to the state machine")
	assert.False(t, tx.fsmRespPassed)
}

func TestServerTxRespondLiveContextSends(t *testing.T) {
	tx, req, _ := newTestInviteServerTx(t, 0)

	res := NewResponseFromRequest(req, StatusRinging, "Ringing", nil)
	require.NoError(t, tx.Respond(testCtx(t), res))

	tx.fsmMu.Lock()
	defer tx.fsmMu.Unlock()
	assert.Same(t, res, tx.fsmResp)
}
