package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorInvocationValidate(t *testing.T) {
	valid := func() ConnectorInvocation {
		return ConnectorInvocation{CustomerID: "acme-app", ConnectionID: "connection", Tool: "create_issue", LatencyMs: 12}
	}
	for _, errorType := range []string{"", InvocationCustomerAuth, InvocationExternalServer, InvocationClientTimeout, InvocationOutcomeUnknown, InvocationDenied} {
		invocation := valid()
		invocation.ErrorType = errorType
		require.NoError(t, invocation.Validate(), errorType)
	}

	for field, broken := range map[string]func(*ConnectorInvocation){
		"customer_id":   func(i *ConnectorInvocation) { i.CustomerID = "" },
		"connection_id": func(i *ConnectorInvocation) { i.ConnectionID = "" },
		"tool":          func(i *ConnectorInvocation) { i.Tool = "" },
		"error_type":    func(i *ConnectorInvocation) { i.ErrorType = "timeout" },
		"latency_ms":    func(i *ConnectorInvocation) { i.LatencyMs = -1 },
	} {
		invocation := valid()
		broken(&invocation)
		var invalid *ValidationError
		require.True(t, errors.As(invocation.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
