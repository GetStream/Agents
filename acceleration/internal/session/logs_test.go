package session

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

func (s *SessionSuite) joinedRowFor(sfu agent.SFU) store.AgentLog {
	queue := make(chan store.AgentLog, 1)
	sess := &Session{
		logs: &logRecorder{queue: queue, store: &store.Store{}},
		spec: Spec{CustomerID: "customer-1"},
	}
	sess.recordLog(agent.Joined{At: time.Now(), SFU: sfu})
	return <-queue
}

func (s *SessionSuite) TestTheJoinedRowNamesTheAgentsSFU() {
	row := s.joinedRowFor(agent.SFU{Edge: "sfu-a1", Host: "sfu-a1.example.com"})

	s.Equal("joined", row.EventType)
	s.Equal(map[string]any{"sfu_edge": "sfu-a1", "sfu_host": "sfu-a1.example.com"}, row.Details)
}

func (s *SessionSuite) TestAJoinedRowWithoutAnSFUHasNoSFUKeys() {
	row := s.joinedRowFor(agent.SFU{})

	s.Equal("joined", row.EventType)
	s.Empty(row.Details)
}

// logged is a session that writes its agent log rows to the returned queue.
func (s *SessionSuite) logged() (*Session, chan store.AgentLog) {
	queue := make(chan store.AgentLog, 1)
	return &Session{
		logs:   &logRecorder{queue: queue, store: &store.Store{}},
		spec:   Spec{CustomerID: "customer-1"},
		logger: slog.New(slog.DiscardHandler),
	}, queue
}

func (s *SessionSuite) TestAFailedTaskRowSaysOnlyThatTheToolFailed() {
	sess, queue := s.logged()

	sess.recordLog(agent.TaskSettled{TaskID: "task-1", Skill: "think",
		Err: errors.New(`http 400: {"error":"no model at https://vendor.example/v1"}`)})

	row := <-queue
	s.Equal("task_settled", row.EventType)
	s.Equal("the tool failed", row.Details["error_message"])
}

func (s *SessionSuite) TestARuntimeErrorRowSaysNothingOfAVendorsFailure() {
	sess, queue := s.logged()

	sess.Report(errors.New(`dial tcp 10.0.3.7:5432: connect: connection refused`), "llm")

	row := <-queue
	s.Equal("runtime_error", row.EventType)
	s.Equal("something went wrong", row.Details["error_message"])
}

func (s *SessionSuite) TestARuntimeErrorRowKeepsARefusalTheCallerCanCorrectInItsWords() {
	sess, queue := s.logged()

	sess.Report(fmt.Errorf("%w: temperature must be at most 2", routing.ErrRequest), "llm")

	row := <-queue
	s.Equal("runtime_error", row.EventType)
	s.Contains(row.Details["error_message"], "temperature must be at most 2")
}

func (s *SessionSuite) TestARuntimeErrorRowForATurnTheScreenRefusedNamesNoHarm() {
	sess, queue := s.logged()

	sess.Report(fmt.Errorf("%w: jailbreak", policy.ErrPromptInjection), "llm")

	row := <-queue
	s.Equal(policy.ErrPromptInjection.Error(), row.Details["error_message"])
}

func (s *SessionSuite) TestARuntimeErrorRowForATurnTheScreenCouldNotCheckDoesNotCallItAnInjection() {
	for name, cause := range map[string]error{
		"no cause":            policy.ErrScreenUnavailable,
		"a routing refusal":   fmt.Errorf("%w: it allows none of internalvendor/judge-model", routing.ErrModelNotAllowed),
		"a request refusal":   fmt.Errorf("%w: internalvendor/judge-model does not take realtime", routing.ErrRequest),
		"a refusal with code": statusRefusal{"internalvendor/judge-model is refused"},
	} {
		sess, queue := s.logged()

		sess.Report(fmt.Errorf("%w: %w: %w", policy.ErrPromptInjection, policy.ErrScreenUnchecked, cause), "llm")

		row := <-queue
		s.Equal(policy.ErrScreenUnchecked.Error(), row.Details["error_message"], name)
	}
}

// statusRefusal is a refusal with the HTTP status it is answered with, as the API's are.
type statusRefusal struct{ message string }

func (r statusRefusal) Error() string  { return r.message }
func (r statusRefusal) GetStatus() int { return 400 }

func (s *SessionSuite) TestARuntimeErrorRowKeepsTheMessageOfARefusalTheCallerIsAnsweredWith() {
	sess, queue := s.logged()

	sess.Report(fmt.Errorf("dispatch to 10.0.3.7: %w", statusRefusal{"tool_approval needs allowed"}), "tool")

	row := <-queue
	s.Equal("tool_approval needs allowed", row.Details["error_message"])
}

func (s *SessionSuite) TestARuntimeErrorRowForARefusalAboutMoneyNamesItsCode() {
	sess, queue := s.logged()

	sess.Report(fmt.Errorf("llm: reserve at 10.0.3.7: %w", policy.ErrNoCredits), "llm")

	row := <-queue
	s.Equal("insufficient_credits", row.Details["error_message"])
}
