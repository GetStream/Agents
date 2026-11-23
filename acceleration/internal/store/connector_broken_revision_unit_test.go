package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorBrokenRevisionValidate(t *testing.T) {
	valid := func() ConnectorBrokenRevision {
		return ConnectorBrokenRevision{ConnectorID: "acme", Revision: 2, Reason: "the API moved", MarkedBy: 3}
	}
	mark := valid()
	require.NoError(t, mark.Validate())

	for field, broken := range map[string]func(*ConnectorBrokenRevision){
		"connector_id": func(m *ConnectorBrokenRevision) { m.ConnectorID = "custom_acme" },
		"revision":     func(m *ConnectorBrokenRevision) { m.Revision = 0 },
		"reason":       func(m *ConnectorBrokenRevision) { m.Reason = "" },
		"marked_by":    func(m *ConnectorBrokenRevision) { m.MarkedBy = 2 },
	} {
		mark := valid()
		broken(&mark)
		var invalid *ValidationError
		require.True(t, errors.As(mark.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
