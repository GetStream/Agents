package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorToolPinValidate(t *testing.T) {
	valid := func() ConnectorToolPin {
		return ConnectorToolPin{ConnectionID: "connection", ToolName: "create_issue", SchemaDigest: "d1"}
	}
	pin := valid()
	require.NoError(t, pin.Validate())

	for field, broken := range map[string]func(*ConnectorToolPin){
		"connection_id": func(p *ConnectorToolPin) { p.ConnectionID = "" },
		"tool_name":     func(p *ConnectorToolPin) { p.ToolName = "" },
		"schema_digest": func(p *ConnectorToolPin) { p.SchemaDigest = "" },
	} {
		pin := valid()
		broken(&pin)
		var invalid *ValidationError
		require.True(t, errors.As(pin.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
