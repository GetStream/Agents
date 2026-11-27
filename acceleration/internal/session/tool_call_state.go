package session

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ToolCallStatus is where one tool call stands.
type ToolCallStatus string

const (
	ToolCallInProgress ToolCallStatus = "in_progress"
	// ToolCallWaiting is a call held for a person's approval.
	ToolCallWaiting   ToolCallStatus = "waiting"
	ToolCallSuccess   ToolCallStatus = "success"
	ToolCallFailed    ToolCallStatus = "failed"
	ToolCallCancelled ToolCallStatus = "cancelled"
)

// ToolCallPhase says which of a call's changes a ToolCallChanged is: its first, one while it
// runs, or its last.
type ToolCallPhase string

const (
	ToolCallStarted  ToolCallPhase = "started"
	ToolCallProgress ToolCallPhase = "progress"
	ToolCallFinished ToolCallPhase = "finished"
)

// ToolCallState is everything a client shows of one tool call, as it is now.
type ToolCallState struct {
	ID     string
	Tool   string
	TurnID string
	Status ToolCallStatus
	// DisplayTitle is what the call is doing, in words for the people in the conversation,
	// as its tool declared it. Empty when the tool declared none.
	DisplayTitle string
	// Progress is the last thing said about the call while it runs, such as "Searching for
	// flights", or the question it waits on.
	Progress  string
	StartedAt time.Time
	// Duration is from StartedAt to this change, or to the end for a finished call.
	Duration   time.Duration
	ApprovedAt time.Time
	RejectedAt time.Time
	// Error is why a call failed, or what a declined one shows, such as "Not allowed in time".
	Error string
	// Custom is the data whoever runs the call last sent with an update, kept once it
	// finishes. Each update with data replaces it whole, so it is never changed in place.
	Custom map[string]any
}

// ToolCallChanged is one change to a tool call's state, sent to every watcher.
type ToolCallChanged struct {
	Phase ToolCallPhase
	State ToolCallState
}

// finishedKept is how many finished call ids toolCalls remembers so a late start or
// approval event cannot revive a call that already ended.
const finishedKept = 64

// earlyKept caps pre-start approval states in case a call's start event never arrives.
const earlyKept = 64

type earlyToolCall struct {
	waiting    bool
	question   string
	approvedAt time.Time
}

// toolCalls follows the session's tool calls from their agent's events to the state a client
// shows. A call is forgotten once it finishes, while a bounded ring of finished ids ignores
// late start or approval events that arrive out of order.
type toolCalls struct {
	// approves reports whether a call to a tool waits for a person before it runs.
	approves func(tool string) bool
	// titles is what a call to a tool is shown doing, or empty for nothing.
	titles func(tool string) string

	mu    sync.Mutex
	calls map[string]*ToolCallState
	// early holds approval events that arrived before their agent's ToolStarted came
	// through, which the two report on separate goroutines. earlyOrder bounds it, oldest first.
	early      map[string]earlyToolCall
	earlyOrder []string
	// finished remembers ids of calls that have already ended, bounded by finishedOrder
	// oldest first, so out-of-order start or approval events cannot recreate them.
	finished      map[string]struct{}
	finishedOrder []string
}

// observe applies event and returns the change it made, reporting false for none.
func (t *toolCalls) observe(event agent.Event) (ToolCallChanged, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	switch typed := event.(type) {
	case agent.ToolStarted:
		if typed.Tool == agent.ConfirmTool || t.calls[typed.ID] != nil || t.isFinishedLocked(typed.ID) {
			return ToolCallChanged{}, false
		}
		state := &ToolCallState{ID: typed.ID, Tool: typed.Tool, TurnID: typed.TurnID, Status: ToolCallInProgress,
			Progress: typed.PreSpeech, StartedAt: typed.StartedAt}
		if state.StartedAt.IsZero() {
			state.StartedAt = now
		}
		if t.approves != nil && t.approves(typed.Tool) {
			state.Status = ToolCallWaiting
		}
		if t.titles != nil {
			state.DisplayTitle = t.titles(typed.Tool)
		}
		if early, ok := t.early[typed.ID]; ok {
			t.deleteEarlyLocked(typed.ID)
			if early.waiting {
				state.Status, state.Progress = ToolCallWaiting, early.question
			} else {
				state.Status, state.ApprovedAt = ToolCallInProgress, early.approvedAt
			}
		}
		if t.calls == nil {
			t.calls = map[string]*ToolCallState{}
		}
		t.calls[typed.ID] = state
		return ToolCallChanged{Phase: ToolCallStarted, State: *state}, true
	case agent.ToolAwaitingApproval:
		if t.isFinishedLocked(typed.ID) {
			return ToolCallChanged{}, false
		}
		if t.calls[typed.ID] == nil {
			if existing, ok := t.early[typed.ID]; ok && !existing.waiting {
				return ToolCallChanged{}, false
			}
			t.setEarlyLocked(typed.ID, earlyToolCall{waiting: true, question: typed.Question})
			return ToolCallChanged{}, false
		}
		return t.changeLocked(typed.ID, now, func(state *ToolCallState) {
			state.Status = ToolCallWaiting
			if typed.Question != "" {
				state.Progress = typed.Question
			}
		})
	case agent.ToolUpdated:
		return t.changeLocked(typed.ID, now, func(state *ToolCallState) {
			if typed.Text != "" {
				state.Progress = typed.Text
			}
			if typed.Custom != nil {
				state.Custom = typed.Custom
			}
		})
	case agent.ToolApprovalDecided:
		if !typed.Allowed {
			return t.finishLocked(typed.ID, now, ToolCallCancelled, typed.Summary)
		}
		if t.isFinishedLocked(typed.ID) {
			return ToolCallChanged{}, false
		}
		if t.calls[typed.ID] == nil {
			t.setEarlyLocked(typed.ID, earlyToolCall{approvedAt: now})
			return ToolCallChanged{}, false
		}
		return t.changeLocked(typed.ID, now, func(state *ToolCallState) {
			state.Status, state.ApprovedAt = ToolCallInProgress, now
		})
	case agent.ToolRan:
		if typed.Asked {
			return ToolCallChanged{}, false
		}
		id := typed.ID
		// An approved call runs inside the confirmation that answered its question.
		if typed.Tool == agent.ConfirmTool {
			var answer struct {
				ToolCallID string `json:"tool_call_id"`
			}
			if json.Unmarshal([]byte(typed.Arguments), &answer) != nil {
				return ToolCallChanged{}, false
			}
			id = answer.ToolCallID
		}
		switch {
		case errors.Is(typed.Err, context.Canceled):
			return t.finishLocked(id, now, ToolCallCancelled, "")
		case typed.Err != nil:
			return t.finishLocked(id, now, ToolCallFailed, store.SafeLogText(typed.Err.Error()))
		}
		if state := t.calls[id]; state != nil && state.Status == ToolCallWaiting {
			return t.finishLocked(id, now, ToolCallCancelled, "")
		}
		return t.finishLocked(id, now, ToolCallSuccess, "")
	}
	return ToolCallChanged{}, false
}

// changeLocked applies change to a call still running. The caller holds t.mu.
func (t *toolCalls) changeLocked(id string, now time.Time, change func(*ToolCallState)) (ToolCallChanged, bool) {
	state := t.calls[id]
	if state == nil {
		return ToolCallChanged{}, false
	}
	before := *state
	change(state)
	if reflect.DeepEqual(*state, before) {
		return ToolCallChanged{}, false
	}
	state.Duration = now.Sub(state.StartedAt)
	return ToolCallChanged{Phase: ToolCallProgress, State: *state}, true
}

// finishLocked ends a call as status, clears any pre-start state, and remembers its id so
// late events cannot revive it. A call declined while it waited is rejected now.
// The caller holds t.mu.
func (t *toolCalls) finishLocked(id string, now time.Time, status ToolCallStatus, reason string) (ToolCallChanged, bool) {
	t.deleteEarlyLocked(id)
	t.markFinishedLocked(id)
	state := t.calls[id]
	if state == nil {
		return ToolCallChanged{}, false
	}
	delete(t.calls, id)
	if status == ToolCallCancelled && state.Status == ToolCallWaiting {
		state.RejectedAt = now
	}
	state.Status, state.Error, state.Duration = status, reason, now.Sub(state.StartedAt)
	return ToolCallChanged{Phase: ToolCallFinished, State: *state}, true
}

func (t *toolCalls) setEarlyLocked(id string, state earlyToolCall) {
	if id == "" {
		return
	}
	if t.early == nil {
		t.early = map[string]earlyToolCall{}
	}
	if _, exists := t.early[id]; !exists {
		t.earlyOrder = append(t.earlyOrder, id)
	}
	t.early[id] = state
	for len(t.earlyOrder) > earlyKept {
		delete(t.early, t.earlyOrder[0])
		t.earlyOrder = slices.Delete(t.earlyOrder, 0, 1)
	}
}

func (t *toolCalls) deleteEarlyLocked(id string) {
	if _, exists := t.early[id]; !exists {
		return
	}
	delete(t.early, id)
	if idx := slices.Index(t.earlyOrder, id); idx >= 0 {
		t.earlyOrder = slices.Delete(t.earlyOrder, idx, idx+1)
	}
}

func (t *toolCalls) markFinishedLocked(id string) {
	if id == "" {
		return
	}
	if t.finished == nil {
		t.finished = map[string]struct{}{}
	}
	if _, exists := t.finished[id]; exists {
		return
	}
	t.finished[id] = struct{}{}
	t.finishedOrder = append(t.finishedOrder, id)
	for len(t.finishedOrder) > finishedKept {
		delete(t.finished, t.finishedOrder[0])
		t.finishedOrder = slices.Delete(t.finishedOrder, 0, 1)
	}
}

func (t *toolCalls) isFinishedLocked(id string) bool {
	_, done := t.finished[id]
	return done
}

// trackToolCall sends every watcher the change event made to a tool call, if it made one.
func (s *Session) trackToolCall(event agent.Event) {
	if changed, ok := s.toolCalls.observe(event); ok {
		s.broadcast(changed)
	}
}

// approves reports whether a call to the caller's tool waits for a person before it runs.
func (s *Session) approves(tool string) bool {
	for _, declared := range s.declaredTools() {
		if declared.Name == tool {
			return declared.Approval != nil
		}
	}
	return false
}

// toolTitle is what a call to the tool is shown doing, as whoever declared it said: the
// caller, or the worker hosting it.
func (s *Session) toolTitle(tool string) string {
	all := slices.Concat(s.declaredTools(), s.otherTools)
	title := ""
	if at := slices.IndexFunc(all, named(tool)); at >= 0 {
		title = all[at].DisplayTitle
	}
	return persistent.ToolTitle(tool, title)
}
