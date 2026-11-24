package store

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/pgdriver"
)

// The error goes to the logs, past their redaction, so it must not carry the DSN's password.
func TestADSNThatDoesNotParseIsRefusedWithoutItsPassword(t *testing.T) {
	_, err := Open("postgres://router:s3cret@db host:5432/router")

	require.Error(t, err)
	require.NotContains(t, err.Error(), "s3cret")
}

func TestADSNsParameterWinsWhateverTheCaseItIsWrittenIn(t *testing.T) {
	conf := pgdriver.Config{ConnParams: map[string]any{"TimeZone": "Europe/Lisbon", "Statement_Timeout": "1s"}}

	sessionDefaults(&conf)

	require.Equal(t, map[string]any{
		"statement_timeout":                   "1s",
		"lock_timeout":                        "3000ms",
		"idle_in_transaction_session_timeout": "30000ms",
		"timezone":                            "Europe/Lisbon",
	}, conf.ConnParams)
}
