package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTelephonySuspensionValidate(t *testing.T) {
	valid := TelephonySuspension{CustomerID: "app-1", Reason: "fraud", SuspendedBy: "staff@example.com"}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*TelephonySuspension){
		"customer_id":  func(s *TelephonySuspension) { s.CustomerID = "" },
		"reason":       func(s *TelephonySuspension) { s.Reason = "  " },
		"suspended_by": func(s *TelephonySuspension) { s.SuspendedBy = "" },
	} {
		suspension := valid
		broken(&suspension)
		var invalid *ValidationError
		require.True(t, errors.As(suspension.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
