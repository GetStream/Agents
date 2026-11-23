package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The error goes to the logs, past their redaction, so it must not carry the DSN's password.
func TestADSNThatDoesNotParseIsRefusedWithoutItsPassword(t *testing.T) {
	_, err := Open("postgres://router:s3cret@db host:5432/router")

	require.Error(t, err)
	require.NotContains(t, err.Error(), "s3cret")
}
