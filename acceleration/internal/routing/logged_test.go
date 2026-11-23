package routing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The fields and their order are those the "model call timing" line wrote before the speech
// lines shared them, so the line a log view already parses is the same.
func TestALoggedCallNamesWhoseItWasThenWhatServedIt(t *testing.T) {
	owner := Owner{CustomerID: "acme", AgentID: "concierge", CallID: "ignored:owner-call"}
	config := ProviderConfig{Provider: "gemma", Model: "gemma-4", Region: "eu", BYOK: true}

	require.Equal(t, []any{
		"call", "default:lobby", "app_id", "acme", "agent_id", "concierge",
		"provider", "gemma", "model", "gemma-4", "vendor_host", "gemma.example.test",
		"region", "eu", "byok", true,
	}, LoggedCall("default:lobby", owner, config, "vendor_host", "gemma.example.test"))
}

func TestALoggedCallLeavesOutWhatItDoesNotKnow(t *testing.T) {
	require.Equal(t, []any{"call", "", "provider", "gemma", "model", "gemma-4"},
		LoggedCall("", Owner{}, ProviderConfig{Provider: "gemma", Model: "gemma-4"}))
}
