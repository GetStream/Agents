package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAgentLogValidate(t *testing.T) {
	valid := func() AgentLog {
		return AgentLog{CustomerID: "acme", Source: "agent", Severity: "warn", EventType: "turn"}
	}
	log := valid()
	require.NoError(t, log.Validate())

	for field, broken := range map[string]func(*AgentLog){
		"customer_id": func(l *AgentLog) { l.CustomerID = "" },
		"source":      func(l *AgentLog) { l.Source = "" },
		"severity":    func(l *AgentLog) { l.Severity = "debug" },
		"event_type":  func(l *AgentLog) { l.EventType = "" },
	} {
		log := valid()
		broken(&log)
		var invalid *ValidationError
		require.True(t, errors.As(log.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
