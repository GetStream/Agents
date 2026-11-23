package api

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

func TestAPeerIsGivenLongEnoughToRingAndHoldATasksCall(t *testing.T) {
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}}})
	require.NoError(t, err)
	// The ring, the task sent while a reply is waited out, and the call held
	// to its limit: 90 s + 10 min + 10 min.
	require.Equal(t, 21*time.Minute+30*time.Second, server.scheduledPeerWindow())
	require.Greater(t, server.scheduledPeerWindow(), server.phoneWindow+server.callLimit+time.Minute,
		"a call answered at the last ring and held to its limit is not cut by the asker")
	require.Equal(t, 3*time.Minute, server.replyWindow, "a router reads a channel's answer for 3 min")
}

// A peer handed an occurrence rebuilds its delivery with taskDelivery from the task's row, so
// whatever the row holds reaches the call it may place from there, never from the payload.
func TestADeliveryCarriesWhatItsTasksRowHolds(t *testing.T) {
	due := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	task := store.ScheduledTask{
		ID: "task", CustomerID: "customer", SessionID: "session", ConversationID: "conversation",
		ConfigID: "config", UserID: "user", CallerKind: "authenticated", APIKey: "key",
		UsageCategories: []string{"project:reminders"}, Timezone: "Europe/Amsterdam",
		Channels: []string{"sms", "phone"},
	}

	delivery := taskDelivery(task, "run", "the prompt", due)

	require.Equal(t, scheduledDelivery{
		CustomerID: "customer", TaskID: "task", RunID: "run", SessionID: "session",
		ConversationID: "conversation", ConfigID: "config", UserID: "user", CallerKind: "authenticated",
		Timezone: "Europe/Amsterdam", CommandID: "st_run", Text: "the prompt",
		Channels: []string{"sms", "phone"}, Due: due,
		APIKey: "key", UsageCategories: []string{"project:reminders"},
	}, delivery)
}

// A peer reads only the ids it checks of what it is handed, and the reason the asker's last
// attempt failed under: any other field would be the asker's word on the conversation, zone,
// command, prompt or due time it delivers under. An older router hands over the whole delivery,
// and its ids still read.
func TestAHandOffReadsOnlyTheIdsAPeerChecks(t *testing.T) {
	sent, err := json.Marshal(scheduledDelivery{
		CustomerID: "customer", TaskID: "task", RunID: "run", SessionID: "session",
		ConversationID: "conversation", ConfigID: "config", UserID: "user", CallerKind: "authenticated",
		Timezone: "Europe/Amsterdam", CommandID: "st_run", Text: "the prompt", Channels: []string{"phone"},
		Due: time.Now(), APIKey: "key", UsageCategories: []string{"project:reminders"},
		Earlier: codeSpendCheckUnavailable,
	})
	require.NoError(t, err)

	var handOff scheduledHandOff
	require.NoError(t, json.Unmarshal(sent, &handOff))

	require.Equal(t, scheduledHandOff{
		CustomerID: "customer", TaskID: "task", RunID: "run", SessionID: "session",
		UserID: "user", CallerKind: "authenticated", Earlier: codeSpendCheckUnavailable,
	}, handOff)
	read, err := json.Marshal(handOff)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(read, &fields))
	require.ElementsMatch(t, []string{"customer_id", "task_id", "run_id", "session_id", "user_id", "caller_kind", "earlier"},
		slices.Collect(maps.Keys(fields)))
}

// A retry reruns a failed turn after every refusal for money, not only after the check that
// could not run: a refusal the check could not settle can carry insufficient_credits or
// spend_limit_reached too (policy's uncheckedWith), and passes again on its own as well. A reason
// that is not about money reruns nothing.
func TestEveryMoneyCodeRerunsAFailedTurn(t *testing.T) {
	for _, code := range []string{codeAccountSuspended, codeInsufficientCredits, codeTopUpRequired,
		codeSystemLimitReached, codeSpendLimitReached, codeUserLimitReached, codeSpendCheckUnavailable} {
		require.True(t, moneyCode(code), code)
	}
	for _, code := range []string{"", scheduling.ReasonReplyFailed, scheduling.ReasonTierLimit, scheduling.ReasonNoReply} {
		require.False(t, moneyCode(code), code)
	}
}
