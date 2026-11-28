package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectorAuthorizationAttemptValidate(t *testing.T) {
	valid := func() ConnectorAuthorizationAttempt {
		return ConnectorAuthorizationAttempt{ID: "attempt", CustomerID: "acme-app", ConnectionID: "connection",
			Kind: AttemptConsent, StateHash: AuthorizationStateHash("state"), AttemptSealed: []byte("sealed"),
			KEKVersion: 1, ExpiresAt: time.Now().Add(time.Minute)}
	}
	for _, kind := range []string{AttemptConsent, AttemptReconnect, AttemptStepUp, AttemptAdminConsent} {
		attempt := valid()
		attempt.Kind = kind
		require.NoError(t, attempt.Validate(), kind)
	}

	for field, broken := range map[string]func(*ConnectorAuthorizationAttempt){
		"id":             func(a *ConnectorAuthorizationAttempt) { a.ID = "" },
		"customer_id":    func(a *ConnectorAuthorizationAttempt) { a.CustomerID = "" },
		"connection_id":  func(a *ConnectorAuthorizationAttempt) { a.ConnectionID = "" },
		"kind":           func(a *ConnectorAuthorizationAttempt) { a.Kind = "install" },
		"state_hash":     func(a *ConnectorAuthorizationAttempt) { a.StateHash = "" },
		"attempt_sealed": func(a *ConnectorAuthorizationAttempt) { a.AttemptSealed = nil },
		"kek_version":    func(a *ConnectorAuthorizationAttempt) { a.KEKVersion = 0 },
		"expires_at":     func(a *ConnectorAuthorizationAttempt) { a.ExpiresAt = time.Time{} },
	} {
		attempt := valid()
		broken(&attempt)
		var invalid *ValidationError
		require.True(t, errors.As(attempt.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}

func TestAnAttemptThatHasAlreadyExpiredIsRefused(t *testing.T) {
	attempt := &ConnectorAuthorizationAttempt{
		ID: "attempt", CustomerID: "acme-app", ConnectionID: "connection", StateHash: AuthorizationStateHash("state"),
		Kind: AttemptConsent, AttemptSealed: []byte("sealed"), KEKVersion: 1, ExpiresAt: time.Now().Add(-time.Second),
	}

	err := (&Store{}).ConnectorAuthorizationAttempts.Insert(context.Background(), attempt)

	require.ErrorContains(t, err, "expires in the future")
}

func TestTheStateHashIsNotTheState(t *testing.T) {
	hash := AuthorizationStateHash("bearer-state")

	require.Len(t, hash, 64, "hex of a SHA-256")
	require.NotContains(t, hash, "bearer-state")
	require.Equal(t, hash, AuthorizationStateHash("bearer-state"), "the same state finds the same row")
}

func TestAHandoffWithoutTheBlobItReadIsRefused(t *testing.T) {
	err := (&Store{}).ConnectorAuthorizationAttempts.HandOff(context.Background(), "attempt", nil, []byte("sealed"), 1, nil)

	require.ErrorContains(t, err, "the blob it read")
}
