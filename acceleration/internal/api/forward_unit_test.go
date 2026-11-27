package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A list is about no session, so it is not looked up where one lives: from a region away from
// the shared database that lookup is a round trip on every page.
func TestTheSessionListIsAboutNoSession(t *testing.T) {
	_, ok := sessionInPath("/v1/agents/sessions/query")
	require.False(t, ok)

	id, ok := sessionInPath("/v1/agents/sessions/s-1/responses")
	require.True(t, ok)
	require.Equal(t, "s-1", id)
}

func TestTheReviewAnalysisIsAboutNoCall(t *testing.T) {
	_, ok := callInPath("/v1/agents/calls/reviews/analysis")
	require.False(t, ok)

	id, ok := callInPath("/v1/agents/calls/c-1/transcript")
	require.True(t, ok)
	require.Equal(t, "c-1", id)
}
