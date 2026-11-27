package session

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
)

type ToolCallStateSuite struct {
	suite.Suite
	calls *toolCalls
	began time.Time
}

func TestToolCallState(t *testing.T) {
	suite.Run(t, new(ToolCallStateSuite))
}

func (s *ToolCallStateSuite) SetupTest() {
	s.calls = &toolCalls{approves: func(tool string) bool { return tool == "book_flight" }}
	s.began = time.Now().UTC().Add(-time.Second)
}

// apply feeds the tracker event and returns the change it made.
func (s *ToolCallStateSuite) apply(event agent.Event) (ToolCallChanged, bool) {
	return s.calls.observe(event)
}

func (s *ToolCallStateSuite) start(id, tool string) ToolCallChanged {
	changed, ok := s.apply(agent.ToolStarted{ID: id, TurnID: "turn-1", Tool: tool, StartedAt: s.began})
	s.Require().True(ok)
	return changed
}

func (s *ToolCallStateSuite) TestACallThatNeedsApprovalWaitsThenRunsOnceAllowedAndSucceeds() {
	started := s.start("call-1", "book_flight")
	s.Equal(ToolCallStarted, started.Phase)
	s.Equal(ToolCallWaiting, started.State.Status)

	allowed, ok := s.apply(agent.ToolApprovalDecided{ID: "call-1", TurnID: "turn-1", Allowed: true})
	s.Require().True(ok)
	s.Equal(ToolCallProgress, allowed.Phase)
	s.Equal(ToolCallInProgress, allowed.State.Status)
	s.False(allowed.State.ApprovedAt.IsZero())

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", TurnID: "turn-1", Tool: "book_flight", Result: "booked"})
	s.Require().True(ok)
	s.Equal(ToolCallFinished, finished.Phase)
	s.Equal(ToolCallSuccess, finished.State.Status)
	s.Equal(allowed.State.ApprovedAt, finished.State.ApprovedAt)
	s.GreaterOrEqual(finished.State.Duration, time.Second)
}

func (s *ToolCallStateSuite) TestADeclinedCallFinishesCancelledWithWhenItWasRejected() {
	s.start("call-1", "book_flight")

	declined, ok := s.apply(agent.ToolApprovalDecided{ID: "call-1", TurnID: "turn-1", Summary: "Not booked"})

	s.Require().True(ok)
	s.Equal(ToolCallFinished, declined.Phase)
	s.Equal(ToolCallCancelled, declined.State.Status)
	s.False(declined.State.RejectedAt.IsZero())
	s.True(declined.State.ApprovedAt.IsZero())
	s.Equal("Not booked", declined.State.Error)
	_, late := s.apply(agent.ToolRan{ID: "call-1", Tool: "book_flight", Err: errors.New("declined")})
	s.False(late, "a call is finished once")
}

func (s *ToolCallStateSuite) TestAnUpdateWhileTheCallRunsIsItsProgress() {
	s.start("call-1", "search_flights")

	progress, ok := s.apply(agent.ToolUpdated{ID: "call-1", Tool: "search_flights", Text: "Searching for flights"})

	s.Require().True(ok)
	s.Equal(ToolCallProgress, progress.Phase)
	s.Equal("Searching for flights", progress.State.Progress)
	s.Equal(ToolCallInProgress, progress.State.Status)
	_, again := s.apply(agent.ToolUpdated{ID: "call-1", Tool: "search_flights", Text: "Searching for flights"})
	s.False(again, "the same update changes nothing")
}

func (s *ToolCallStateSuite) TestAnUpdatesDataReplacesTheCallsAndOutlivesIt() {
	s.start("call-1", "search_flights")
	s.apply(agent.ToolUpdated{ID: "call-1", Tool: "search_flights", Text: "Comparing 40 flights",
		Custom: map[string]any{"cheapest": 129, "airline": "TAP"}})

	dataOnly, ok := s.apply(agent.ToolUpdated{ID: "call-1", Tool: "search_flights", Custom: map[string]any{"cheapest": 99}})
	s.Require().True(ok)
	s.Equal("Comparing 40 flights", dataOnly.State.Progress, "an update with only data keeps the title")
	s.Equal(map[string]any{"cheapest": 99}, dataOnly.State.Custom, "the data is replaced whole, not merged")

	textOnly, ok := s.apply(agent.ToolUpdated{ID: "call-1", Tool: "search_flights", Text: "Booking the cheapest"})
	s.Require().True(ok)
	s.Equal(map[string]any{"cheapest": 99}, textOnly.State.Custom, "an update without data keeps the data")

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", Tool: "search_flights", Result: "booked"})
	s.Require().True(ok)
	s.Equal(map[string]any{"cheapest": 99}, finished.State.Custom)
}

func (s *ToolCallStateSuite) TestACallStoppedByItsContextFinishesCancelled() {
	s.start("call-1", "search_flights")

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", Tool: "search_flights",
		Err: fmt.Errorf("session: search_flights stopped: %w", context.Canceled)})

	s.Require().True(ok)
	s.Equal(ToolCallCancelled, finished.State.Status)
	s.True(finished.State.RejectedAt.IsZero(), "nobody declined it")
}

func (s *ToolCallStateSuite) TestACallThatErrsFinishesFailedSayingWhy() {
	s.start("call-1", "search_flights")

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", Tool: "search_flights", Err: errors.New("the airline is down")})

	s.Require().True(ok)
	s.Equal(ToolCallFailed, finished.State.Status)
	s.Equal("the airline is down", finished.State.Error)
}

// The tool_call.finished frame says what the tool_ran frame beside it says, redacted the same.
func (s *ToolCallStateSuite) TestAFailedCallsErrorHoldsNoKeyTheToolEchoed() {
	key := "sk-test-" + rand.Text()
	s.start("call-1", "search_flights")

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", Tool: "search_flights",
		Err: errors.New("search_flights: 401 Unauthorized\nAuthorization: Bearer " + key)})

	s.Require().True(ok)
	s.Equal(ToolCallFailed, finished.State.Status)
	s.Equal("search_flights: 401 Unauthorized\n[redacted]", finished.State.Error)
}

func (s *ToolCallStateSuite) TestAVoiceCallWaitsOnItsQuestionAndFinishesWithTheConfirmationThatRanIt() {
	s.start("call-1", "transfer_money")

	waiting, ok := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "transfer_money", Question: "Send 20 euros to Ana?"})
	s.Require().True(ok)
	s.Equal(ToolCallWaiting, waiting.State.Status)
	s.Equal("Send 20 euros to Ana?", waiting.State.Progress)
	_, asked := s.apply(agent.ToolRan{ID: "call-1", Tool: "transfer_money", Result: "Ask the caller", Asked: true})
	s.False(asked, "the question is not the call's outcome")
	_, confirmation := s.apply(agent.ToolStarted{ID: "call-2", Tool: agent.ConfirmTool, StartedAt: s.began})
	s.False(confirmation, "the confirmation is not a call of its own")
	_, ok = s.apply(agent.ToolApprovalDecided{ID: "call-1", Allowed: true})
	s.Require().True(ok)

	finished, ok := s.apply(agent.ToolRan{ID: "call-2", Tool: agent.ConfirmTool,
		Arguments: `{"tool_call_id":"call-1","allowed":true}`, Result: "sent"})

	s.Require().True(ok)
	s.Equal("call-1", finished.State.ID)
	s.Equal(ToolCallSuccess, finished.State.Status)
	s.False(finished.State.ApprovedAt.IsZero())
}

func (s *ToolCallStateSuite) TestAVoiceQuestionNobodyAnsweredInTimeIsRejected() {
	s.start("call-1", "transfer_money")
	s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "transfer_money", Question: "Send it?"})

	lapsed, ok := s.apply(agent.ToolApprovalDecided{ID: "call-1", Summary: "Not confirmed in time"})

	s.Require().True(ok)
	s.Equal(ToolCallCancelled, lapsed.State.Status)
	s.False(lapsed.State.RejectedAt.IsZero())
}

func (s *ToolCallStateSuite) TestAConnectorCallHeldBeforeItsStartCameThroughStartsWaiting() {
	_, early := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(early)

	started := s.start("call-1", "github__merge")

	s.Equal(ToolCallWaiting, started.State.Status)
	s.Equal("Allow github to run merge?", started.State.Progress)
}

func (s *ToolCallStateSuite) TestACallIsShownDoingWhatItsToolWasDeclaredWith() {
	session := &Session{
		spec:       Spec{Tools: []harness.Tool{{Name: "get_location", DisplayTitle: "Checking where you are"}}},
		otherTools: []harness.Tool{{Name: "weather_lookup", DisplayTitle: "Checking the weather"}, {Name: "lookup_order"}},
	}
	s.calls.titles = session.toolTitle

	s.Equal("Checking where you are", s.start("call-1", "get_location").State.DisplayTitle)
	s.Equal("Checking the weather", s.start("call-2", "weather_lookup").State.DisplayTitle, "a hosted tool's title")
	s.Equal("Searching the web", s.start("call-3", "web_search").State.DisplayTitle)
	s.Empty(s.start("call-4", "lookup_order").State.DisplayTitle, "a tool that declared no title")
	finished, ok := s.apply(agent.ToolRan{ID: "call-2", Tool: "weather_lookup", Result: "Sunny"})
	s.Require().True(ok)
	s.Equal("Checking the weather", finished.State.DisplayTitle, "the title stays on the finished call")
}

func (s *ToolCallStateSuite) TestADeclinedCallBeforeItsStartClearsTheHoldAndIgnoresTheLateStart() {
	_, held := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(held)

	_, declined := s.apply(agent.ToolApprovalDecided{ID: "call-1", Allowed: false, Summary: "Not allowed in time"})
	s.False(declined)
	s.Empty(s.calls.early, "declining before start clears the early hold")

	_, started := s.apply(agent.ToolStarted{ID: "call-1", TurnID: "turn-1", Tool: "github__merge", StartedAt: s.began})
	s.False(started, "a late start after decline is ignored")
	s.Empty(s.calls.calls, "no active call is left behind")

	_, lateHold := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(lateHold)
	s.Empty(s.calls.early, "a late hold after decline is not stored")
}

func (s *ToolCallStateSuite) TestACallThatFinishesBeforeItsStartClearsTheHoldAndIgnoresTheLateStart() {
	_, held := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(held)

	_, ran := s.apply(agent.ToolRan{ID: "call-1", Tool: "github__merge", Err: errors.New("cancelled")})
	s.False(ran)
	s.Empty(s.calls.early, "finishing before start clears the early hold")

	_, started := s.apply(agent.ToolStarted{ID: "call-1", TurnID: "turn-1", Tool: "github__merge", StartedAt: s.began})
	s.False(started, "a late start after ToolRan is ignored")
	s.Empty(s.calls.calls, "no active call is left behind")
}

func (s *ToolCallStateSuite) TestACallThatRunsBeforeItsStartIgnoresTheLateStart() {
	_, ran := s.apply(agent.ToolRan{ID: "call-1", Tool: "search_flights", Result: "done"})
	s.False(ran)

	_, started := s.apply(agent.ToolStarted{ID: "call-1", TurnID: "turn-1", Tool: "search_flights", StartedAt: s.began})
	s.False(started, "a late start after ToolRan is ignored")
	s.Empty(s.calls.calls)

	_, lateAllowed := s.apply(agent.ToolApprovalDecided{ID: "call-1", Allowed: true})
	s.False(lateAllowed)
	s.Empty(s.calls.early, "a late approval after ToolRan is not stored")
}

func (s *ToolCallStateSuite) TestAnApprovedHoldBeforeItsStartStartsInProgress() {
	_, held := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(held)
	_, allowed := s.apply(agent.ToolApprovalDecided{ID: "call-1", Allowed: true})
	s.False(allowed)

	started := s.start("call-1", "github__merge")
	s.Equal(ToolCallInProgress, started.State.Status)
	s.False(started.State.ApprovedAt.IsZero())
	s.Empty(s.calls.early)

	finished, ok := s.apply(agent.ToolRan{ID: "call-1", Tool: "github__merge", Result: "merged"})
	s.Require().True(ok)
	s.Equal(ToolCallSuccess, finished.State.Status)
	s.Equal(started.State.ApprovedAt, finished.State.ApprovedAt)

	_, heldCaller := s.apply(agent.ToolAwaitingApproval{ID: "call-2", Tool: "book_flight", Question: "Book it?"})
	s.False(heldCaller)
	_, allowedCaller := s.apply(agent.ToolApprovalDecided{ID: "call-2", Allowed: true})
	s.False(allowedCaller)

	startedCaller := s.start("call-2", "book_flight")
	s.Equal(ToolCallInProgress, startedCaller.State.Status, "approval before start overrides the tool's approval gate")
	s.False(startedCaller.State.ApprovedAt.IsZero())
}

func (s *ToolCallStateSuite) TestAnEarlyApprovalIsNotOverwrittenByALateHoldBeforeStart() {
	_, allowed := s.apply(agent.ToolApprovalDecided{ID: "call-1", Allowed: true})
	s.False(allowed)
	_, held := s.apply(agent.ToolAwaitingApproval{ID: "call-1", Tool: "github__merge", Question: "Allow github to run merge?"})
	s.False(held)

	started := s.start("call-1", "github__merge")
	s.Equal(ToolCallInProgress, started.State.Status)
	s.False(started.State.ApprovedAt.IsZero())
}

func (s *ToolCallStateSuite) TestFinishedCallMemoryEvictsOldestBeyondTheCap() {
	for i := range 64 {
		_, ok := s.apply(agent.ToolRan{ID: fmt.Sprintf("call-%d", i), Tool: "search_flights", Result: "done"})
		s.False(ok)
	}
	// A duplicate terminal event for an already-remembered id must not evict another entry.
	s.apply(agent.ToolRan{ID: "call-0", Tool: "search_flights", Result: "done"})

	_, blockedAtCap := s.apply(agent.ToolStarted{ID: "call-0", TurnID: "turn-1", Tool: "search_flights", StartedAt: s.began})
	s.False(blockedAtCap, "the oldest of 64 finished ids is still remembered")

	s.apply(agent.ToolRan{ID: "call-64", Tool: "search_flights", Result: "done"})

	_, stillBlocked := s.apply(agent.ToolStarted{ID: "call-1", TurnID: "turn-1", Tool: "search_flights", StartedAt: s.began})
	s.False(stillBlocked, "the second-oldest finished id is still remembered after the 65th finishes")

	_, evicted := s.apply(agent.ToolStarted{ID: "call-0", TurnID: "turn-1", Tool: "search_flights", StartedAt: s.began})
	s.True(evicted, "the 65th finished id evicts the oldest")
}

func (s *ToolCallStateSuite) TestEarlyHoldMemoryEvictsOldestBeyondTheCap() {
	for i := range 64 {
		s.apply(agent.ToolAwaitingApproval{ID: fmt.Sprintf("early-%d", i), Tool: "github__merge", Question: "Allow?"})
	}
	// Updating an existing early hold must not consume another slot.
	s.apply(agent.ToolAwaitingApproval{ID: "early-0", Tool: "github__merge", Question: "Allow?"})
	s.Len(s.calls.early, 64)

	s.apply(agent.ToolAwaitingApproval{ID: "early-64", Tool: "github__merge", Question: "Allow?"})
	s.Len(s.calls.early, 64, "the 65th early hold evicts the oldest")

	evicted := s.start("early-0", "github__merge")
	s.Equal(ToolCallInProgress, evicted.State.Status, "the evicted hold no longer marks the call waiting")

	kept := s.start("early-2", "github__merge")
	s.Equal(ToolCallWaiting, kept.State.Status, "an un-evicted hold is still kept")
	s.Len(s.calls.early, 63)

	// Starting early-2 freed its order slot, so adding one more hold brings the map back to 64
	// without evicting early-1.
	s.apply(agent.ToolAwaitingApproval{ID: "early-65", Tool: "github__merge", Question: "Allow?"})
	s.Len(s.calls.early, 64)
	stillKept := s.start("early-1", "github__merge")
	s.Equal(ToolCallWaiting, stillKept.State.Status, "early-1 was not evicted when early-2 was started")
}

func (s *ToolCallStateSuite) TestEmptyIDEventsDoNotPolluteEarlyOrFinishedState() {
	s.apply(agent.ToolAwaitingApproval{ID: "", Tool: "github__merge", Question: "Allow?"})
	s.Empty(s.calls.early)
	s.apply(agent.ToolApprovalDecided{ID: "", Allowed: true})
	s.Empty(s.calls.early)
	s.apply(agent.ToolRan{ID: "", Tool: "search_flights", Result: "done"})
	s.apply(agent.ToolRan{ID: "confirm-1", Tool: agent.ConfirmTool, Arguments: `{}`, Result: "done"})
	s.Empty(s.calls.finished)
}

func (s *ToolCallStateSuite) TestConcurrentOutOfOrderEventsNeverLeaveStuckCalls() {
	var wg sync.WaitGroup
	for i := range 32 {
		id := fmt.Sprintf("race-%d", i)
		events := []agent.Event{
			agent.ToolStarted{ID: id, TurnID: "turn-1", Tool: "github__merge", StartedAt: s.began},
			agent.ToolAwaitingApproval{ID: id, Tool: "github__merge", Question: "Allow?"},
			agent.ToolApprovalDecided{ID: id, Allowed: false, Summary: "Denied"},
			agent.ToolRan{ID: id, Tool: "github__merge", Err: errors.New("denied")},
		}
		for _, ev := range events {
			wg.Add(1)
			go func(e agent.Event) {
				defer wg.Done()
				s.calls.observe(e)
			}(ev)
		}
	}
	wg.Wait()
	s.Empty(s.calls.calls, "every concurrently terminated call is cleared from active calls")
	s.Empty(s.calls.early, "every concurrently terminated call is cleared from early holds")
}
