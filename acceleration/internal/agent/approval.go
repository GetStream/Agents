package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// confirmTool is what a voice model calls with the caller's answer to a tool's approval
// question. It is offered only on a voice call with a tool that asks one.
const confirmTool = "confirm_tool_call"

// ConfirmTool is the tool the model answers a call's approval question with. The call it
// confirms runs inside it, so its ToolRan is that call's outcome too.
const ConfirmTool = confirmTool

// approvalWait is how long a voice caller has to answer an approval question, from when it
// finished playing.
const approvalWait = 15 * time.Second

// approvalAnswer is what a call that needs a yes is answered with at once, on a cascade that
// has said its question as written: the call is closed, so every model can go on.
const approvalAnswer = "Not run yet: %s runs only once the caller says yes, and they have just been asked: %q. " +
	"When they answer, call " + confirmTool + " with tool_call_id %q and allowed true for a yes, or false for " +
	"anything else. Do not call %s again for this and do not say it was done."

// nativeApprovalAnswer is approvalAnswer for a speech-to-speech model, which says the
// question itself.
const nativeApprovalAnswer = "Not run yet: %s runs only once the caller says yes. Ask them now, in these words: %q. " +
	"When they answer, call " + confirmTool + " with tool_call_id %q and allowed true for a yes, or false for " +
	"anything else. Do not call %s again for this and do not say it was done."

// Outcomes of confirm_tool_call that ran nothing.
const (
	approvalDeclined = "The caller declined, so %s was not run. Tell them so in a few words."
	approvalLapsed   = "The caller did not say yes in time, so %s was not run and that question is closed. " +
		"If they want it, call %s again to ask them again."
	approvalUnknown = "There is no question waiting for the caller's answer under that id, so nothing was run. " +
		"If they want something that needs their yes, call its tool to ask them."
	approvalUnheard = "Nothing was run: the caller has not heard the question for %s to its end yet, so they cannot " +
		"have answered it. Call " + confirmTool + " only once it has been said and they have answered."
	approvalUnanswered = "Nothing was run: the caller has not spoken since they were asked about %s. Wait for " +
		"their answer, then call " + confirmTool + " with it."
	approvalCut = "The caller did not hear the question to its end, so %s was not run and that question is " +
		"closed. If they want it, call %s again to ask them again."
)

// approvalSilence is the note a cascade's history is given when the caller let the wait pass.
const approvalSilence = "(The caller did not say yes to %q within %d seconds, so %s was not run. " +
	"If they want it, call %s again to ask them again.)"

// maxTerminalApprovals is the number of finished approvals kept so late confirmations naming
// a lapsed or already-decided call id never fall back to another waiting call.
const maxTerminalApprovals = 64

// approvalState is the lifecycle state of one tool call approval.
type approvalState uint8

const (
	// approvalStateQueued is waiting for its question to be handed to whatever says it.
	approvalStateQueued approvalState = iota
	// approvalStateAsked has been handed to the voice or native model and waits for quiet before
	// its silence countdown begins.
	approvalStateAsked
	// approvalStateExpiring has a silence deadline set and lapses once it passes.
	approvalStateExpiring
	// approvalStateApprovedRunning is running under the confirmation that allowed it.
	approvalStateApprovedRunning
	// approvalStateCompleted finished running after the caller said yes.
	approvalStateCompleted
	// approvalStateDenied was declined by the caller or closed because its question was cut off.
	approvalStateDenied
	// approvalStateLapsed was closed by silence or a tool change before a yes arrived.
	approvalStateLapsed
)

func (s approvalState) pending() bool {
	return s == approvalStateQueued || s == approvalStateAsked || s == approvalStateExpiring
}

// approvalQuestion is one question a cascade owes the caller, paired with its approval entry.
type approvalQuestion struct {
	entry *approval
	text  string
}

// approval is one call's approval entry. Its fields are guarded by Agent.mu.
type approval struct {
	call     llm.ToolCall
	question string
	state    approvalState
	// turn is the turn a cascade says the question in, as written.
	turn string
	// said is set once the question was said: given to a cascade's voice, or a native model's
	// reply after it was told to ask spoke. cut is set once the caller talked over it, or the
	// voice failed, before it finished playing (Agent.interrupted).
	said, cut bool
	// played is set once the question finished playing, with heard the caller's turns then: a
	// yes counts only in reply to a turn they spoke after it.
	played bool
	heard  int
	// expires is when silence declines the call, set once the question finished playing.
	expires time.Time
}

// approvedRun is a call the caller said yes to, running under the confirmation that ran it.
type approvedRun struct {
	confirmation string
	tool         string
}

type approvedKey struct{}

// WithApproval is ctx for a call the caller said yes to.
func WithApproval(ctx context.Context) context.Context {
	return context.WithValue(ctx, approvedKey{}, true)
}

// Approved reports whether a call's context is that of a call the caller said yes to, so
// whoever runs it does not ask them again.
func Approved(ctx context.Context) bool {
	approved, _ := ctx.Value(approvedKey{}).(bool)
	return approved
}

// placeholder is one {name} in an approval question.
var placeholder = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// fillQuestion is template with each {name} replaced by that argument of the call. A name
// the arguments do not have is left as written.
func fillQuestion(template, arguments string) string {
	var values map[string]any
	if json.Unmarshal([]byte(arguments), &values) != nil {
		return template
	}
	return placeholder.ReplaceAllStringFunc(template, func(found string) string {
		value, ok := values[found[1:len(found)-1]]
		if !ok || value == nil {
			return found
		}
		if text, isText := value.(string); isText {
			return text
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return found
		}
		return string(encoded)
	})
}

// approvalQuestion is the question a call to tool asks before it runs, when it asks one: only
// on a call, since in writing the app shows the call and asks.
func (a *Agent) approvalQuestion(tool harness.Tool) string {
	if a.options.Text || tool.Approval == nil {
		return ""
	}
	return strings.TrimSpace(tool.Approval.Title)
}

// asksApproval reports whether any tool on offer asks before it runs.
func (a *Agent) asksApproval() bool {
	for _, tool := range a.tools().Tools {
		if a.approvalQuestion(tool) != "" {
			return true
		}
	}
	return false
}

// offersConfirm reports whether the model is offered confirm_tool_call: a tool on offer
// asks, and no tool of the caller's has the name.
func (a *Agent) offersConfirm() bool {
	if a.options.ToolRunner == nil || !a.asksApproval() {
		return false
	}
	_, taken := a.tools().Lookup(confirmTool)
	return !taken
}

// confirmToolDefinition is confirm_tool_call as the model is offered it.
func confirmToolDefinition() harness.Tool {
	return harness.Tool{
		Name: confirmTool,
		Description: "Give the caller's answer to a question a tool asked before running. Call it only once " +
			"they have answered, with the tool_call_id the question came with.",
		Parameters: map[string]any{
			"type":     "object",
			"required": []string{"tool_call_id", "allowed"},
			"properties": map[string]any{
				"tool_call_id": map[string]any{"type": "string", "description": "The id the question came with."},
				"allowed":      map[string]any{"type": "boolean", "description": "True only for a yes."},
			},
		},
	}
}

// askApproval holds call for the caller's yes and returns what the model is answered with
// at once. A cascade owes the caller the question as written (respondAfterTool); a native
// model is told to ask it.
func (a *Agent) askApproval(call llm.ToolCall, question string) []llm.ContentPart {
	question = fillQuestion(question, call.Arguments)
	answer := approvalAnswer
	native := a.native()
	if native {
		answer = nativeApprovalAnswer
	}
	a.mu.Lock()
	if a.approvals == nil {
		a.approvals = map[string]*approval{}
	}
	entry := &approval{call: call, question: question, state: approvalStateQueued}
	a.approvals[call.ID] = entry
	if !native {
		a.approvalQuestions = append(a.approvalQuestions, approvalQuestion{entry: entry, text: question})
	}
	a.mu.Unlock()
	a.logger.Info("a call waits for the caller's yes", "tool", call.Name, "tool_call", call.ID)
	a.emitter.Send(ToolAwaitingApproval{ID: call.ID, TurnID: call.TurnID, Tool: call.Name, Question: question})
	return llm.TextParts(fmt.Sprintf(answer, call.Name, question, call.ID, call.Name))
}

// heardLocked counts a turn the caller spoke, first marking as played each question said before
// it when drained, the edge has no speech left to play, and the agent is quiet: the edge is
// also empty between a question handed to the voice and its first audio. The caller holds the lock.
func (a *Agent) heardLocked(drained bool) {
	a.playedLocked(drained && a.quietLocked())
	a.callerTurns++
}

// callerTyped says whether a typed turn is the caller's own, written on their device: not a
// scheduled task's, nor one a customer's server or the agent itself wrote.
func callerTyped(sender routing.Caller, scheduled bool) bool {
	return !scheduled && !sender.IsZero() && !sender.ServerSide
}

// quietLocked reports whether the agent is idle: not interrupting, recovering its voice,
// generating, speaking, holding a reply or waiting for a native reply. The caller holds
// the lock.
func (a *Agent) quietLocked() bool {
	return !a.turnPhaseLocked().talking()
}

// playedLocked marks each pending question said as played, when quiet. The caller holds the lock.
func (a *Agent) playedLocked(quiet bool) {
	if !quiet {
		return
	}
	for _, waiting := range a.approvals {
		if waiting.state.pending() && waiting.said && !waiting.played {
			waiting.played, waiting.heard = true, a.callerTurns
		}
	}
}

// cutLocked marks the pending questions a cascade said in a turn that was cut off: a native
// model's are settled by repliedLocked. The caller holds the lock.
func (a *Agent) cutLocked(turnID string) {
	for _, waiting := range a.approvals {
		if waiting.state.pending() && waiting.turn == turnID {
			waiting.cut = true
		}
	}
}

// repliedLocked settles the pending questions a native model holds and has not finished playing,
// as a reply other than the one that made the call ends: cut when the caller talked over it, said
// when it spoke. Any later reply is one the model began once told to ask, which can come back
// before the router has marked the question asked. The caller holds the lock.
func (a *Agent) repliedLocked(responseID string, interrupted, spoke bool) {
	for _, waiting := range a.approvals {
		if !waiting.state.pending() {
			continue
		}
		switch {
		case waiting.played || waiting.call.TurnID == responseID:
		case interrupted:
			waiting.cut = true
		case spoke:
			waiting.said = true
		}
	}
}

// askedApproval marks the question a native model was answered with as asked.
func (a *Agent) askedApproval(callID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if waiting := a.approvals[callID]; waiting != nil && waiting.state == approvalStateQueued {
		waiting.state = approvalStateAsked
	}
}

// takeApprovalQuestionsLocked returns the questions a cascade still owes the caller along with
// their approval entries, skipping any whose approval is no longer queued. The caller holds the lock.
func (a *Agent) takeApprovalQuestionsLocked() (string, []*approval) {
	if len(a.approvalQuestions) == 0 {
		return "", nil
	}
	texts := make([]string, 0, len(a.approvalQuestions))
	entries := make([]*approval, 0, len(a.approvalQuestions))
	for _, q := range a.approvalQuestions {
		if q.entry.state != approvalStateQueued {
			continue
		}
		texts = append(texts, q.text)
		entries = append(entries, q.entry)
	}
	a.approvalQuestions = nil
	if len(entries) == 0 {
		return "", nil
	}
	return strings.Join(texts, " "), entries
}

// askLocked says an approval question as written, in place of the reply to entries,
// and unlocks. It goes into the history as the agent's turn, so the yes that comes back answers
// a question the model can see.
func (a *Agent) askLocked(turnID, question string, entries []*approval) error {
	a.toolReply = false
	a.speakingTurn, a.keptReply = turnID, turnID
	a.saying = question
	for _, waiting := range entries {
		if waiting.state.pending() && waiting.turn == "" {
			waiting.turn, waiting.said = turnID, true
		}
	}
	said := llm.Message{Role: llm.Assistant, Content: question}
	if callsOpen(a.history) {
		a.lateResults = append(a.lateResults, said)
	} else {
		a.history = append(a.history, said)
	}
	a.mu.Unlock()

	a.turns.started(turnID, kindTool)
	err := a.speakWhole(turnID, question)
	// Marked only for the exact approval entries whose questions were spoken, once the voice has
	// them, so an approval queued concurrently (even reusing a call id) cannot start expiring
	// before its own question is spoken.
	a.mu.Lock()
	for _, waiting := range entries {
		if waiting.state == approvalStateQueued {
			waiting.state = approvalStateAsked
		}
	}
	a.mu.Unlock()
	if err != nil {
		return err
	}
	a.emitter.Send(Responded{TurnID: turnID, Text: question})
	return nil
}

// finishApprovalLocked transitions entry to a terminal state and evicts the oldest terminal
// entries once their count exceeds maxTerminalApprovals. The caller holds the lock.
func (a *Agent) finishApprovalLocked(entry *approval, state approvalState) {
	entry.state = state
	a.terminalApprovals = append(a.terminalApprovals, entry)
	for len(a.terminalApprovals) > maxTerminalApprovals {
		oldest := a.terminalApprovals[0]
		copy(a.terminalApprovals, a.terminalApprovals[1:])
		a.terminalApprovals[len(a.terminalApprovals)-1] = nil
		a.terminalApprovals = a.terminalApprovals[:len(a.terminalApprovals)-1]
		if a.approvals[oldest.call.ID] == oldest {
			delete(a.approvals, oldest.call.ID)
		}
	}
}

// onlyPendingApprovalLocked returns the lone pending approval, or nil when zero or more than
// one are pending. The caller holds the lock.
func (a *Agent) onlyPendingApprovalLocked() *approval {
	var only *approval
	for _, entry := range a.approvals {
		if !entry.state.pending() {
			continue
		}
		if only != nil {
			return nil
		}
		only = entry
	}
	return only
}

// confirm runs the call the caller answered, when they said yes in time to its question played
// out, and returns what the model is told. A yes before they could answer runs nothing and
// leaves the call waiting, or closes it when its question was cut off. The confirmation's own
// context is the call's: its timeout starts now. The run gets its own "a tool call came back"
// line, under the call's name and id.
func (a *Agent) confirm(ctx context.Context, confirmation llm.ToolCall) ([]llm.ContentPart, bool, error) {
	var answer struct {
		ToolCallID string `json:"tool_call_id"`
		Allowed    bool   `json:"allowed"`
	}
	if err := json.Unmarshal([]byte(confirmation.Arguments), &answer); err != nil {
		return nil, false, harness.Correctable(fmt.Errorf("agent: %s's arguments: %w", confirmTool, err))
	}
	a.mu.Lock()
	waiting, lapsed, known := a.approvalLocked(answer.ToolCallID)
	if !known && !answer.Allowed {
		// Declining is always safe, so a no that names an unknown id declines the only one waiting.
		waiting = a.onlyPendingApprovalLocked()
	}
	var unanswered string
	if waiting != nil && answer.Allowed {
		unanswered = a.unansweredLocked(waiting)
	}
	closed := waiting != nil && (unanswered == "" || waiting.cut)
	if closed {
		if answer.Allowed && !waiting.cut && a.options.ToolRunner != nil {
			waiting.state = approvalStateApprovedRunning
			if a.approvedRuns == nil {
				a.approvedRuns = map[string]approvedRun{}
			}
			a.approvedRuns[waiting.call.ID] = approvedRun{confirmation: confirmation.ID, tool: waiting.call.Name}
		} else {
			a.finishApprovalLocked(waiting, approvalStateDenied)
		}
	}
	a.mu.Unlock()
	switch {
	case lapsed != "":
		return llm.TextParts(fmt.Sprintf(approvalLapsed, lapsed, lapsed)), false, nil
	case waiting == nil:
		return llm.TextParts(approvalUnknown), false, nil
	case unanswered != "":
		a.logger.Info("a yes came before the caller answered a call's question", "tool", waiting.call.Name,
			"tool_call", waiting.call.ID, "closed", closed)
		if closed {
			a.emitter.Send(ToolApprovalDecided{ID: waiting.call.ID, TurnID: waiting.call.TurnID, Summary: "Question not heard"})
		}
		return llm.TextParts(unanswered), false, nil
	}
	call := waiting.call
	a.emitter.Send(ToolApprovalDecided{ID: call.ID, TurnID: call.TurnID, Allowed: answer.Allowed})
	a.logger.Info("the caller answered a call's question", "tool", call.Name, "tool_call", call.ID, "allowed", answer.Allowed)
	if !answer.Allowed {
		return llm.TextParts(fmt.Sprintf(approvalDeclined, call.Name)), false, nil
	}
	if a.options.ToolRunner == nil {
		return nil, false, harness.Correctable(fmt.Errorf("agent: %s is not a tool this agent can run", call.Name))
	}
	defer func() {
		a.mu.Lock()
		delete(a.approvedRuns, call.ID)
		a.finishApprovalLocked(waiting, approvalStateCompleted)
		a.mu.Unlock()
	}()
	began := time.Now()
	runCtx, report := WithToolReport(ctx)
	parts, err := a.options.ToolRunner.Run(WithApproval(runCtx), call)
	a.mu.Lock()
	hold := a.toolHolds[confirmation.ID]
	answered := hold != nil && hold.answered
	a.mu.Unlock()
	a.logToolBack(call, a.loggedTool(*report, err), time.Since(began), toolOutcome(ctx.Err() != nil, err != nil, false, answered),
		"approval", "allowed", "confirmation", confirmation.ID)
	return parts, true, err
}

// unansweredLocked is what the model is told when a yes to waiting cannot run it yet, or empty
// when it can: its question played to its end and the caller spoke after. The caller holds the
// lock.
func (a *Agent) unansweredLocked(waiting *approval) string {
	name := waiting.call.Name
	switch {
	case waiting.cut:
		return fmt.Sprintf(approvalCut, name, name)
	case !waiting.played:
		return fmt.Sprintf(approvalUnheard, name)
	case a.callerTurns <= waiting.heard:
		return fmt.Sprintf(approvalUnanswered, name)
	}
	return ""
}

// approvalLocked looks up id across pending, running and terminal approvals. Exact matches
// are returned before any single-pending fallback, so a lapsed or already-decided id never
// resolves to another waiting call. The caller holds the lock.
func (a *Agent) approvalLocked(id string) (waiting *approval, lapsed string, known bool) {
	entry, ok := a.approvals[id]
	if !ok {
		return nil, "", false
	}
	switch {
	case entry.state.pending():
		return entry, "", true
	case entry.state == approvalStateLapsed:
		return nil, entry.call.Name, true
	default:
		return nil, "", true
	}
}

// approvedName is the tool a confirmation would run, for the policy it runs under, or the
// confirmation's own name when it names nothing waiting.
func (a *Agent) approvedName(call llm.ToolCall) string {
	if call.Name != confirmTool {
		return call.Name
	}
	var answer struct {
		ToolCallID string `json:"tool_call_id"`
	}
	_ = json.Unmarshal([]byte(call.Arguments), &answer)
	a.mu.Lock()
	defer a.mu.Unlock()
	if waiting, _, _ := a.approvalLocked(answer.ToolCallID); waiting != nil {
		return waiting.call.Name
	}
	return call.Name
}

// runningAsLocked is the call a running call's id is held under: an approved call runs under the
// confirmation that ran it, which is the call the model has open. The caller holds the lock.
func (a *Agent) runningAsLocked(id string) string {
	if run, ok := a.approvedRuns[id]; ok {
		return run.confirmation
	}
	return id
}

// tickApprovals starts each asked question's wait once nothing is being said, and declines
// a call whose wait has passed.
func (a *Agent) tickApprovals() {
	quiet := !a.waitingForPlayout()
	now := time.Now()
	wait := a.options.ApprovalWait
	if wait <= 0 {
		wait = approvalWait
	}
	a.mu.Lock()
	quiet = quiet && a.quietLocked()
	a.playedLocked(quiet)
	var lapsed []*approval
	for _, waiting := range a.approvals {
		switch waiting.state {
		case approvalStateAsked:
			if quiet {
				waiting.expires = now.Add(wait)
				waiting.state = approvalStateExpiring
			}
		case approvalStateExpiring:
			if now.After(waiting.expires) {
				a.finishApprovalLocked(waiting, approvalStateLapsed)
				lapsed = append(lapsed, waiting)
				if a.native() {
					continue
				}
				note := llm.Message{Role: llm.User, Content: fmt.Sprintf(approvalSilence,
					waiting.question, int(wait.Round(time.Second)/time.Second), waiting.call.Name, waiting.call.Name),
					Typed: true, Router: true}
				if callsOpen(a.history) {
					a.lateResults = append(a.lateResults, note)
				} else {
					a.history = append(a.history, note)
				}
			}
		}
	}
	a.mu.Unlock()
	for _, waiting := range lapsed {
		a.logger.Info("nobody said yes to a call in time", "tool", waiting.call.Name, "tool_call", waiting.call.ID)
		a.emitter.Send(ToolApprovalDecided{ID: waiting.call.ID, TurnID: waiting.call.TurnID, Summary: "Not confirmed in time"})
	}
}
