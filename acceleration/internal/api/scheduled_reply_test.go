package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
)

func TestATurnThatSaidNothingIsNoReply(t *testing.T) {
	_, _, err := captured(nil, nil)

	var failure *scheduling.Failure
	require.True(t, errors.As(err, &failure), "%v", err)
	require.Equal(t, scheduling.ReasonNoReply, failure.Reason)
	require.False(t, failure.Final, "a later attempt may read the reply back")
}

func TestATurnThatOnlyMadeAFileIsCarried(t *testing.T) {
	chart := sandbox.Attachment{Name: "chart.png", MIME: "image/png", URL: "https://cdn.example/chart.png"}

	text, files, err := captured([]string{""}, []sandbox.Attachment{chart})

	require.NoError(t, err)
	require.Empty(t, text)
	require.Equal(t, []sandbox.Attachment{chart}, files)
}

func TestAReplyOfTwoTurnsIsJoinedAsTheConversationJoinsThem(t *testing.T) {
	text, _, err := captured([]string{"First.", "Second."}, nil)

	require.NoError(t, err)
	require.Equal(t, "First.\n\nSecond.", text)
}

// A reply ends however its turn did, interrupted included: a regenerate or an edit during the
// turn interrupts it (Conversation.switchTo) while the session stays open, and a wait that took
// it for running would hold its worker to the end of the reply window (AI-3594 R1.3).
func TestAReplyEndsHoweverItsTurnEnded(t *testing.T) {
	for _, state := range []string{"completed", "cancelled", "interrupted", "failed"} {
		require.True(t, replyEnded(state), state)
	}
	for _, state := range []string{"", "thinking", "writing", "tools", "queued"} {
		require.False(t, replyEnded(state), state)
	}
}
