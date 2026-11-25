package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Why one conversation stopped.
const (
	// EndedComplete is the caller deciding it had asked everything it came to ask.
	EndedComplete = "complete"
	// EndedTurns is the conversation running out of turns.
	EndedTurns = "turns"
	// EndedTimeout is it running out of time.
	EndedTimeout = "timeout"
	// EndedFailed is the agent stopping answering.
	EndedFailed = "failed"
)

// SimulationCase is one conversation. With variations off a run has one of these; with
// them expanded, ten, each asking the same thing a different way.
type SimulationCase struct {
	bun.BaseModel `bun:"table:simulation_cases,alias:smc"`

	ID    string `bun:"id,pk"`
	RunID string `bun:"run_id,notnull"`
	// Variation is which way of asking this was, and the order they are listed in.
	Variation int `bun:"variation,notnull"`
	// Scenario is the wording this case used.
	Scenario string `bun:"scenario,notnull"`
	State    string `bun:"state,notnull"`
	// CallID is the session that held the conversation, written as soon as it exists so a
	// run in progress can be watched.
	CallID string `bun:"call_id,nullzero"`
	// Transcript is what was said, oldest first.
	Transcript []SimulationLine `bun:"transcript,type:jsonb"`
	Turns      int              `bun:"turns,notnull"`
	// Passed, Verdict and Score are the judge's ruling. A nil Passed means it never got to
	// rule, which is not the same as having ruled against.
	Passed  *bool  `bun:"passed"`
	Verdict string `bun:"verdict,nullzero"`
	Score   *int   `bun:"score"`
	// Ended is why the conversation stopped.
	Ended      string     `bun:"ended,notnull"`
	Error      string     `bun:"error,nullzero"`
	StartedAt  time.Time  `bun:"started_at,notnull"`
	FinishedAt *time.Time `bun:"finished_at"`
}

// SimulationLine is one thing said, in the order it was said.
type SimulationLine struct {
	// Caller is true when the simulated caller said it rather than the agent.
	Caller bool   `json:"caller"`
	Text   string `json:"text"`
	// Intended is what the agent meant to say, where that differs from what the caller
	// heard. Only an audio simulation has both, and the difference is the whole point of
	// running one.
	Intended string `json:"intended,omitempty"`
	// Tools are what the agent did on this turn rather than said, which is what a question
	// about whether something was actually done is settled by.
	Tools []SimulationTool `json:"tools,omitempty"`
	At    time.Time        `json:"at"`
}

// SimulationTool is one tool the agent ran during a simulated conversation.
type SimulationTool struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Result    string `json:"result"`
	Error     string `json:"error,omitempty"`
}

// Validate refuses a case the table would refuse.
func (c *SimulationCase) Validate() error {
	return validate("simulation case").
		required("run_id", c.RunID).
		oneOf("state", c.State, SimulationPending, SimulationRunning, SimulationPassed, SimulationFailed, SimulationErrored, SimulationCancelled).
		oneOf("ended", c.Ended, "", EndedComplete, EndedTurns, EndedTimeout, EndedFailed).
		check(c.Turns >= 0, "turns", "is negative").
		done()
}

// SimulationCases are the conversations of simulation runs, in the shared database. A case
// belongs to its run, whose owner the caller has checked already.
type SimulationCases struct {
	store *Store
}

// Start records which session is holding a conversation, which is what makes a run in
// progress watchable.
func (r *SimulationCases) Start(ctx context.Context, id, callID string) error {
	if id == "" {
		return stack.Wrap(errors.New("store: a case id is required"))
	}

	_, err := r.store.global.NewUpdate().Model((*SimulationCase)(nil)).
		Set("state = ?", SimulationRunning).
		Set("call_id = ?", callID).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start simulation case: %w", err))
	}
	return nil
}

// Finish records how one conversation went and what the judge made of it.
func (r *SimulationCases) Finish(ctx context.Context, kase SimulationCase) error {
	if kase.ID == "" {
		return stack.Wrap(errors.New("store: a case id is required"))
	}
	kase.Transcript = orEmpty(kase.Transcript)
	// The error is the agent's when it stopped answering, which can be a provider's text.
	kase.Error = SafeLogText(kase.Error)
	kase.Transcript = redactedTranscript(kase.Transcript)

	now := time.Now().UTC()
	kase.FinishedAt = &now

	_, err := r.store.global.NewUpdate().Model(&kase).
		Column("state", "call_id", "transcript", "turns", "passed", "verdict", "score",
			"ended", "error", "finished_at").
		Where("id = ?", kase.ID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish simulation case: %w", err))
	}
	return nil
}

// Select returns a run's conversations, in the order they were asked.
func (r *SimulationCases) Select(ctx context.Context, runID string) ([]SimulationCase, error) {
	if runID == "" {
		return nil, stack.Wrap(errors.New("store: a run id is required"))
	}

	var cases []SimulationCase
	err := r.store.global.NewSelect().Model(&cases).
		Where("run_id = ?", runID).
		Order("variation ASC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: simulation cases: %w", err))
	}
	return cases, nil
}

// redactedTranscript is a copy of transcript with each tool's result put through SafeLogText,
// as a session's items are: a result can be a provider's text echoing the key it was sent. It
// is a copy, because the caller's transcript is what the judge read.
func redactedTranscript(transcript []SimulationLine) []SimulationLine {
	out := make([]SimulationLine, len(transcript))
	for i, line := range transcript {
		out[i] = line
		if len(line.Tools) == 0 {
			continue
		}
		out[i].Tools = make([]SimulationTool, len(line.Tools))
		for j, tool := range line.Tools {
			tool.Result = SafeLogText(tool.Result)
			out[i].Tools[j] = tool
		}
	}
	return out
}
