package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/decisionmodel"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

type FlowSuite struct {
	suite.Suite
	ctx context.Context

	model   *stubLLM
	flow    *flow
	emitter *Emitter
	drained chan struct{}

	mu      sync.Mutex
	decided []Decided
	// logs is what the controller logged, as JSON, one record a line.
	logs *logLines
}

// logLines keeps what is logged, as JSON, one record a line.
type logLines struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// records returns every record logged with message.
func (l *logLines) records(message string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []map[string]any
	lines := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	for lines.Scan() {
		var record map[string]any
		if json.Unmarshal(lines.Bytes(), &record) == nil && record["msg"] == message {
			found = append(found, record)
		}
	}
	return found
}

func TestFlowSuite(t *testing.T) {
	suite.Run(t, new(FlowSuite))
}

func (s *FlowSuite) SetupTest() {
	s.ctx = context.Background()
	s.model = newStubLLM()
	s.decided = nil

	s.emitter = NewEmitter(16)
	s.drained = make(chan struct{})
	go func() {
		defer close(s.drained)
		for event := range s.emitter.Events() {
			decided, ok := event.(Decided)
			if !ok {
				continue
			}
			s.mu.Lock()
			s.decided = append(s.decided, decided)
			s.mu.Unlock()
		}
	}()

	s.logs = &logLines{}
	s.flow = newFlow(stubSession(&s.Suite, s.ctx, s.model), s.emitter, slog.New(slog.NewJSONHandler(s.logs, nil)))
	s.T().Cleanup(func() {
		_ = s.flow.Close()
		s.emitter.Close()
		<-s.drained
	})
}

// decisions is what the controller has decided so far.
func (s *FlowSuite) decisions() []Decided {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Decided(nil), s.decided...)
}

// waitAsked waits until the model has been asked n times. Create runs on a goroutine, so
// asserting on requests immediately after Decide would race it.
func (s *FlowSuite) waitAsked(n int) []llm.ResponseParams {
	s.T().Helper()
	s.Require().Eventually(func() bool { return len(s.model.requests()) >= n }, settleFor,
		5*time.Millisecond, "the controller was never asked")
	return s.model.requests()
}

func (s *FlowSuite) TestWordsThatCallSomebodyByAFamilyNameAreForThem() {
	// A child at the caller's end said "Mom, can we go to the park after?", the controller ruled
	// it respond, and the agent answered the child.
	s.Contains(flowInstructions, "Words that open by calling somebody else in the room by a kinship term or a nickname")
	s.Contains(flowInstructions, "addressed to them, not the agent: choose ignore")
}

func (s *FlowSuite) TestACallerWhoSaidTheyWouldGiveEverythingIsWaitedOn() {
	// "I'll give you everything up front so we don't go back and forth" was ruled respond,
	// and the agent started talking ten times inside the caller's monologue.
	s.Contains(flowInstructions, "Once the caller has said they will give everything at once, choose wait at each pause")
}

func (s *FlowSuite) TestACallerGivingAThoughtInPartsIsSaidToBe() {
	// Big Bench Audio reads a question out a clause at a time. Each clause was ruled a
	// complete thought, answered, and talked over: 42 of 84 answers in one run.
	s.Contains(flowInstructions, "when you are told the caller is giving one thought in parts: choose wait")
	s.Contains(flowInstructions, "respond once they ask something, correct or change something, or plainly finish")

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "candidate-1", Participant: "Alex", Text: "Delphina says Andre lies.", InParts: true,
	}))
	s.Contains(s.waitAsked(1)[0].Input[0].Content, "so they are giving one thought in parts")

	s.NotContains(flowQuestion(FlowTurn{Participant: "Alex", Text: "Delphina says Andre lies."}), "in parts",
		"nothing says so of a caller who has not")
}

func (s *FlowSuite) TestTheConversationIsQuotedRatherThanReplayed() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID:           "candidate-1",
		Instructions: "be brief",
		Participant:  "Alex",
		History: []llm.Message{
			{Role: llm.User, Content: "what is the capital of France"},
			{Role: llm.Assistant, Content: "Paris."},
		},
		Text: "and of Spain",
	}))

	asked := s.waitAsked(1)
	s.Require().Lenf(asked[0].Input, 1,
		"a conversation replayed as turns is one the controller answers instead of judging: %v",
		asked[0].Input)
	question := asked[0].Input[0].Content
	s.Contains(question, "Caller: what is the capital of France")
	s.Contains(question, "Agent: Paris.")
	s.Contains(question, `"and of Spain"`, "the words being judged have to be in there")
	s.Equal(llm.FormatJSONSchema, asked[0].Text.Format,
		"the answer is parsed, so prose is not an option")
	s.Equal(512, asked[0].MaxOutputTokens,
		"thinking models spend the budget before the JSON, and 32 was only enough for a fragment")
}

func (s *FlowSuite) TestAnOverlappingReplyIsQuotedSoTheControllerCanHold() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID:          "candidate-1",
		Participant: "Alex",
		Text:        "cough",
		Speaking:    true,
		Reply:       "party of three at 6:15",
	}))

	question := s.waitAsked(1)[0].Input[0].Content
	s.Contains(question, `is speaking right now and has so far said "party of three at 6:15"`)
}

func (s *FlowSuite) TestOnlyTheRecentConversationIsShown() {
	var history []llm.Message
	for turn := range flowHistory {
		history = append(history,
			llm.Message{Role: llm.User, Content: fmt.Sprintf("question %d", turn)},
			llm.Message{Role: llm.Assistant, Content: fmt.Sprintf("answer %d", turn)},
		)
	}

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", History: history, Text: "and now"}))

	question := s.waitAsked(1)[0].Input[0].Content
	s.NotContains(question, "question 0", "an old turn does not decide whose the floor is")
	s.Contains(question, fmt.Sprintf("answer %d", flowHistory-1))
}

func (s *FlowSuite) TestAnUnreadableAnswerStillEarnsTheCallerAReply() {
	// The controller answering the caller instead of classifying them is exactly how a
	// question used to go unanswered: nobody was left to notice it had been asked.
	s.model.answers["candidate-1"] = "With three days, I would start in the Alfama district."

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "candidate-1", Participant: "Alex", Text: "what should I not miss",
	}))

	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the caller's turn was dropped")
	decided := s.decisions()[0]
	s.Equal(Respond, decided.Disposition)
	s.Equal(Continue, decided.Floor, "an unreadable answer is no reason to cut the agent off")
	s.NoError(decided.Error())
}

func (s *FlowSuite) TestAnUnreadableOverlapStopsTheAgent() {
	s.model.answers["over-1"] = `{"dis`

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "over-1", Participant: "Alex", Text: "oh my god stop", Unfinished: true, Speaking: true,
	}))

	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the overlap was never ruled")
	decided := s.decisions()[0]
	s.Equal(Wait, decided.Disposition)
	s.Equal(Stop, decided.Floor, "truncated JSON while the caller talks over the agent is a stop")
}

func (s *FlowSuite) TestEveryConversationalDecisionParses() {
	for _, disposition := range []Disposition{Wait, Ignore, Respond, Clarify} {
		for _, floor := range []Floor{Stop, Shorten, Continue} {
			answer, _, err := parseFlow(`{"disposition":"` + string(disposition) +
				`","floor":"` + string(floor) + `"}`)

			s.Require().NoError(err)
			s.Equal(disposition, answer.Disposition)
			s.Equal(floor, answer.Floor)
		}
	}
}

func (s *FlowSuite) TestMarkdownFencesDoNotHideAValidDecision() {
	answer, _, err := parseFlow("```json\n" +
		`{"disposition":"respond","floor":"continue"}` +
		"\n```")

	s.Require().NoError(err)
	s.Equal(Respond, answer.Disposition)
	s.Equal(Continue, answer.Floor)
}

func (s *FlowSuite) TestInventedDecisionsAreRejected() {
	_, invalid, err := parseFlow(`{"disposition":"guess","floor":"continue"}`)
	s.ErrorContains(err, "invalid flow disposition")
	s.Equal("disposition", invalid)

	_, invalid, err = parseFlow(`{"disposition":"respond","floor":"talk-over"}`)
	s.ErrorContains(err, "invalid floor decision")
	s.Equal("floor", invalid)
}

func (s *FlowSuite) TestExtraSpeechIsRejected() {
	_, invalid, err := parseFlow(`Sure: {"disposition":"respond","floor":"continue"}`)
	s.ErrorContains(err, "decode flow decision")
	s.Equal("decode", invalid)
}

func (s *FlowSuite) TestTheControllerIsHeldToTheValuesItIsTrustedWith() {
	// Gemma, asked only for a JSON object, ruled {"disposition":"continue","floor":"continue"}.
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", Participant: "Alex", Text: "go on"}))

	text := s.waitAsked(1)[0].Text
	s.Require().NotNil(text.Schema, "a schema is what keeps a floor out of the disposition")
	sent, err := json.Marshal(text.Schema.Schema)
	s.Require().NoError(err)
	s.JSONEq(`{
		"type": "object",
		"properties": {
			"disposition": {"type": "string", "enum": ["wait", "ignore", "respond", "clarify"]},
			"floor": {"type": "string", "enum": ["stop", "shorten", "continue"]}
		},
		"required": ["disposition", "floor"],
		"additionalProperties": false
	}`, string(sent))
}

func (s *FlowSuite) TestARefusedRulingIsCounted() {
	before := testutil.ToFloat64(invalidRulings.WithLabelValues("disposition"))
	s.model.answers["candidate-1"] = `{"disposition":"continue","floor":"continue"}`

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", Participant: "Alex", Text: "go on"}))

	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the caller's turn was dropped")
	s.Equal(before+1, testutil.ToFloat64(invalidRulings.WithLabelValues("disposition")),
		"the fallback stands in for a refused ruling, which is invisible unless it is counted")
}

func (s *FlowSuite) TestCancelAbandonsACreateThatHasNotReturned() {
	hold := make(chan struct{})
	s.model.holdCreate = hold

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "old", Participant: "Alex", Text: "okay",
	}))
	s.waitAsked(1)

	s.Require().NoError(s.flow.Cancel("old"))
	s.Require().Eventually(func() bool {
		return len(s.model.requests()) == 1 && len(s.decisions()) == 0
	}, settleFor, 5*time.Millisecond, "the cancelled create still produced a ruling")
	s.Empty(s.decisions(), "a ruling about words that have moved on must not be acted on")
}

func (s *FlowSuite) TestCancelStartsTheWaitingTurnBeforeCreateReturns() {
	hold := make(chan struct{})
	s.model.holdCreate = hold

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "old", Participant: "Alex", Text: "okay"}))
	s.waitAsked(1)

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "mid", Participant: "Alex", Text: "okay wait"}))
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "new", Participant: "Alex", Text: "okay wait stop"}))
	time.Sleep(20 * time.Millisecond)
	s.Len(s.model.requests(), 1, "Cerebras must not see a pile of superseded asks")

	s.Require().NoError(s.flow.Cancel("old"))
	asked := s.waitAsked(2)
	s.Equal("new", asked[1].ID, "the follow-up must not wait for the cancelled create to return")
	for _, d := range s.decisions() {
		s.NotContains([]string{"old", "mid"}, d.CandidateID,
			"superseded words must not be ruled on")
	}
}

func (s *FlowSuite) TestCancelOfAHungCreateLetsTheNextDecideStart() {
	hold := make(chan struct{})
	s.model.holdCreate = hold

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "old", Participant: "Alex", Text: "okay"}))
	s.waitAsked(1)
	s.Require().NoError(s.flow.Cancel("old"))

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "new", Participant: "Alex", Text: "tell me something else",
	}))
	asked := s.waitAsked(2)
	s.Equal("new", asked[1].ID, "a hung create that was cancelled must not keep the mailbox busy")
}

func (s *FlowSuite) TestCancelCanOverlapTheReturningStream() {
	// Release each blocked Create concurrently with Cancel and repeat to exercise the stream
	// handoff under the race detector.
	for round := range 100 {
		id := fmt.Sprintf("round-%d", round)
		hold := make(chan struct{})
		s.model.mu.Lock()
		s.model.holdCreate = hold
		s.model.mu.Unlock()
		s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: id, Participant: "Alex", Text: "okay"}))
		s.waitAsked(round + 1)

		cancelled := make(chan error, 1)
		go func() { cancelled <- s.flow.Cancel(id) }()
		close(hold)
		s.Require().NoError(<-cancelled)
	}
}

func (s *FlowSuite) TestNothingIsAskedOnceTheFlowIsClosed() {
	s.Require().NoError(s.flow.Close())

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "late", Participant: "Alex", Text: "okay"}))
	s.flow.running.Wait()
	s.emitter.Close()
	<-s.drained

	s.Empty(s.model.requests(), "a decision started after Close would outlive the sessions it was given")
	s.Empty(s.decisions())
	s.flow.mu.Lock()
	s.Nil(s.flow.active)
	s.Nil(s.flow.queued)
	s.flow.mu.Unlock()
}

func (s *FlowSuite) TestSlowOverlapYieldsBeforeTheOriginalReplyFinishes() {
	s.model.holdCreate = make(chan struct{})
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "over-slow", Participant: "Alex", Text: "stop, give me one sentence", Unfinished: true, Speaking: true}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor, 5*time.Millisecond)
	decision := s.decisions()[0]
	s.Equal("over-slow", decision.CandidateID)
	s.Equal(Wait, decision.Disposition)
	s.Equal(Stop, decision.Floor)
	s.Equal(Deadline, decision.Ruler, "no model ruled on it")
	s.NoError(decision.Error())
}

func (s *FlowSuite) TestSupersededSlowOverlapCannotInterruptLater() {
	s.model.holdCreate = make(chan struct{})
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "over-old", Participant: "Alex", Text: "stop", Unfinished: true, Speaking: true}))
	s.Require().NoError(s.flow.Cancel("over-old"))
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "over-new", Participant: "Alex", Text: "wait, make it shorter", Unfinished: true, Speaking: true}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor, 5*time.Millisecond)
	s.Equal("over-new", s.decisions()[0].CandidateID)
	s.Equal(Stop, s.decisions()[0].Floor)
}

func (s *FlowSuite) TestSlowOverlapStreamAlsoYieldsTheFloor() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "over-stream", Participant: "Alex", Text: "stop, give me one sentence", Unfinished: true, Speaking: true}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor, 5*time.Millisecond)
	s.Equal(Stop, s.decisions()[0].Floor)
	s.Equal(Wait, s.decisions()[0].Disposition)
	s.Equal(Deadline, s.decisions()[0].Ruler, "no model ruled on it")
	s.NoError(s.decisions()[0].Error())
}

func (s *FlowSuite) TestADecisionThatOutlivesTheDeadlineStillRulesOnTheTurn() {
	// Staging, 2026-10-19 22:01Z: the decision stream opened, no token came within 3 s, the
	// deadline closed it, and the caller's question was never ruled on, so never answered.
	s.flow.deadline = 50 * time.Millisecond

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "slow", Participant: "Alex", Text: "what time do you open"}))

	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "a decision the deadline closed left the caller's turn unruled")
	decided := s.decisions()[0]
	s.Equal("slow", decided.CandidateID)
	s.ErrorIs(decided.Error(), context.DeadlineExceeded,
		"the agent answers a finished turn whose ruling failed, so the timeout has to reach it")
	s.model.mu.Lock()
	script := s.model.scripts["slow"]
	s.model.mu.Unlock()
	s.Require().NotNil(script, "the stream had started, so the deadline is what closed it")
	s.True(script.Abandoned())
}

func (s *FlowSuite) TestAFastDecisionIsRuledAsAnswered() {
	s.model.answers["fast"] = `{"disposition":"respond","floor":"continue"}`

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "fast", Participant: "Alex", Text: "what time do you open"}))

	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the decision was never ruled")
	decided := s.decisions()[0]
	s.Equal("fast", decided.CandidateID)
	s.Equal(Respond, decided.Disposition)
	s.Equal(Continue, decided.Floor)
	s.NoError(decided.Error())
}

func (s *FlowSuite) TestAStreamClosedBeforeTheDeadlineIsNotRuledOn() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "closed", Participant: "Alex", Text: "what time do you open"}))
	var stream *llm.Stream
	s.Require().Eventually(func() bool {
		s.flow.mu.Lock()
		defer s.flow.mu.Unlock()
		if asked := s.flow.active; asked != nil && asked.turn.ID == "closed" {
			stream = asked.stream
		}
		return stream != nil
	}, settleFor, 5*time.Millisecond, "the stream never started")

	s.Require().NoError(stream.Close())
	s.flow.running.Wait()
	s.emitter.Close()
	<-s.drained

	s.Empty(s.decisions(), "only the deadline turns a closed stream into a ruling")
}

func (s *FlowSuite) TestARulingSaysWhoseAnswerItIs() {
	ruledOn := func(turn FlowTurn, answer string, writtenBy [2]string) Ruler {
		id := turn.ID
		s.model.mu.Lock()
		s.model.answers[id], s.model.writtenBy = answer, writtenBy
		s.model.mu.Unlock()
		s.Require().NoError(s.flow.Decide(s.T().Context(), turn))
		s.Require().Eventually(func() bool { return len(s.decisions()) > 0 && s.decisions()[len(s.decisions())-1].CandidateID == id },
			settleFor, 5*time.Millisecond, "no ruling came back")
		return s.decisions()[len(s.decisions())-1].Ruler
	}
	ruled := func(id, answer string, writtenBy [2]string) Ruler {
		return ruledOn(FlowTurn{ID: id, Participant: "Alex", Text: "two"}, answer, writtenBy)
	}
	respond := `{"disposition":"respond","floor":"continue"}`

	s.Equal(Primary, ruled("own", respond, [2]string{}))
	s.Equal(Unusable, ruled("unusable", "not json", [2]string{}), "a default the controller fell back on is nobody's answer")
	s.Equal(Unusable, ruledOn(FlowTurn{ID: "unusable-over", Participant: "Alex", Text: "two", Unfinished: true}, "not json", [2]string{}),
		"the stop a caller still talking gets for an unreadable answer is nobody's answer either")
	s.Equal(Fallback, ruled("hedged", respond, [2]string{"", "another-model"}))
	s.Equal(Fallback, ruled("elsewhere", respond, [2]string{"another-provider", ""}))
}

func (s *FlowSuite) TestARequestTheModelRefusedIsLoggedWithItsProviderAndModel() {
	s.model.refuses = errors.New("502 Bad Gateway: failed to connect to local service")
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", Participant: "Alex", Text: "go on"}))

	s.requireLoggedFailure("failed to connect to local service")
}

func (s *FlowSuite) TestADecisionThatFailedMidStreamIsLoggedWithItsProviderAndModel() {
	s.model.failing = true
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", Participant: "Alex", Text: "go on"}))

	s.requireLoggedFailure(errModelDown.Error())
}

func (s *FlowSuite) TestADecisionAnotherProviderFailedMidStreamIsLoggedWithThatProviderAndModel() {
	s.model.failing, s.model.writtenBy = true, [2]string{"another-provider", "another-model"}
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "candidate-1", Participant: "Alex", Text: "go on"}))

	s.requireLoggedFailureBy("another-provider", "another-model", errModelDown.Error())
}

// requireLoggedFailure waits for the controller's one failure line and checks it names the
// provider and model the session routes to, so a search for either finds it.
func (s *FlowSuite) requireLoggedFailure(said string) {
	s.requireLoggedFailureBy("stub", "stub-model", said)
}

func (s *FlowSuite) requireLoggedFailureBy(provider, model, said string) {
	s.Require().Eventually(func() bool { return len(s.logs.records("flow controller failed")) == 1 }, settleFor,
		5*time.Millisecond, "the failure was never logged")
	failure := s.logs.records("flow controller failed")[0]
	s.Equal(provider, failure["provider"])
	s.Equal(model, failure["model"])
	s.Contains(failure["error"], said)
}

type recordingJudge struct {
	mu     sync.Mutex
	ctxs   []context.Context
	closes int
	base   respondingJudge
}

func (j *recordingJudge) Classify(ctx context.Context, req decisionmodel.Request) (decisionmodel.Result, error) {
	j.mu.Lock()
	j.ctxs = append(j.ctxs, ctx)
	j.mu.Unlock()
	return j.base.Classify(ctx, req)
}

func (j *recordingJudge) Close() error {
	j.mu.Lock()
	j.closes++
	j.mu.Unlock()
	return nil
}

func (j *recordingJudge) closeCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.closes
}

func (s *FlowSuite) TestCandidateContextIsCancelledOnNormalCompletion() {
	s.model.answers["done"] = `{"disposition":"respond","floor":"continue"}`

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "done", Participant: "Alex", Text: "hello"}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the decision was never ruled")
	s.flow.running.Wait()

	ctxs := s.model.contexts()
	s.Require().Len(ctxs, 1)
	s.ErrorIs(ctxs[0].Err(), context.Canceled, "a finished candidate must release its timeout timer")

	s.model.mu.Lock()
	s.model.refuses = errors.New("502 Bad Gateway")
	s.model.mu.Unlock()
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{ID: "err", Participant: "Alex", Text: "hello"}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 2 }, settleFor,
		5*time.Millisecond, "the failed decision was never ruled")
	s.flow.running.Wait()

	ctxs = s.model.contexts()
	s.Require().Len(ctxs, 2)
	s.ErrorIs(ctxs[1].Err(), context.Canceled, "a failed candidate must also release its timeout timer")
}

func (s *FlowSuite) TestCandidateContextIsCancelledAfterJudgeCompletes() {
	judge := &recordingJudge{}
	s.flow.judge = judge

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "judged", Participant: "Alex", Text: "hello", Ended: true,
	}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "the judge decision was never ruled")
	s.flow.running.Wait()

	judge.mu.Lock()
	ctxs := append([]context.Context(nil), judge.ctxs...)
	judge.mu.Unlock()
	s.Require().Len(ctxs, 1)
	s.ErrorIs(ctxs[0].Err(), context.Canceled, "a judged candidate must release its timeout timer")
}

func (s *FlowSuite) TestDuplicateTurnIDWhileInFlightDoesNotAbandonQueuedCandidate() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "first words",
	}))
	s.waitAsked(1)

	// Queue another candidate with the same turn.ID while the first is still streaming.
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "second words",
	}))

	s.model.mu.Lock()
	s.model.answers["dup"] = `{"disposition":"respond","floor":"continue"}`
	s.model.mu.Unlock()
	s.model.settle("dup", `{"disposition":"wait","floor":"continue"}`)

	s.Require().Eventually(func() bool { return len(s.decisions()) == 2 }, settleFor,
		5*time.Millisecond, "the queued candidate with a duplicate turn id was abandoned")
	s.flow.running.Wait()

	asked := s.model.requests()
	s.Require().Len(asked, 2)
	s.Contains(asked[0].Input[0].Content, `"first words"`)
	s.Contains(asked[1].Input[0].Content, `"second words"`)

	decisions := s.decisions()
	s.Equal("dup", decisions[0].CandidateID)
	s.Equal(Wait, decisions[0].Disposition)
	s.NoError(decisions[0].Error())
	s.Equal("dup", decisions[1].CandidateID)
	s.Equal(Respond, decisions[1].Disposition)
	s.NoError(decisions[1].Error())

	for _, cctx := range s.model.contexts() {
		s.ErrorIs(cctx.Err(), context.Canceled)
	}
}

func (s *FlowSuite) TestDuplicateTurnIDReplacingQueuedCandidateStillRunsLatest() {
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "first words",
	}))
	s.waitAsked(1)

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "second words",
	}))
	s.flow.mu.Lock()
	replacedCtx := s.flow.queued.ctx
	s.flow.mu.Unlock()
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "third words",
	}))
	s.ErrorIs(replacedCtx.Err(), context.Canceled, "replacing a queued candidate must cancel its timeout timer")

	s.model.mu.Lock()
	s.model.answers["dup"] = `{"disposition":"respond","floor":"continue"}`
	s.model.mu.Unlock()
	s.model.settle("dup", `{"disposition":"wait","floor":"continue"}`)

	s.Require().Eventually(func() bool { return len(s.decisions()) == 2 }, settleFor,
		5*time.Millisecond, "the latest queued candidate with a duplicate turn id did not run")
	s.flow.running.Wait()

	asked := s.model.requests()
	s.Require().Len(asked, 2)
	s.Contains(asked[0].Input[0].Content, `"first words"`)
	s.Contains(asked[1].Input[0].Content, `"third words"`)
}

func (s *FlowSuite) TestCancelWithDuplicateTurnIDInFlightAndQueuedCancelsBoth() {
	hold := make(chan struct{})
	s.model.holdCreate = hold

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "first words",
	}))
	s.waitAsked(1)

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "second words",
	}))
	s.flow.mu.Lock()
	queuedCtx := s.flow.queued.ctx
	s.flow.mu.Unlock()
	s.Require().NoError(s.flow.Cancel("dup"))
	s.ErrorIs(queuedCtx.Err(), context.Canceled, "cancelling a queued duplicate must cancel its timeout timer")
	s.ErrorIs(s.model.contexts()[0].Err(), context.Canceled, "cancelling a duplicate id must also cancel the in-flight candidate")
	close(hold)
	s.flow.running.Wait()

	s.Len(s.model.requests(), 1, "a cancelled queued duplicate must not start")
	s.Empty(s.decisions(), "neither cancelled duplicate may emit a ruling")

	s.model.mu.Lock()
	s.model.holdCreate = nil
	s.model.answers["after"] = `{"disposition":"respond","floor":"continue"}`
	s.model.mu.Unlock()
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "after", Participant: "Alex", Text: "still working",
	}))
	s.Require().Eventually(func() bool { return len(s.decisions()) == 1 }, settleFor,
		5*time.Millisecond, "flow did not return to idle after cancelling duplicates")
	s.Equal("after", s.decisions()[0].CandidateID)
	s.Equal(Respond, s.decisions()[0].Disposition)
}

type gatedJudge struct {
	mu      sync.Mutex
	calls   int
	entered []chan struct{}
	release []chan struct{}
	base    respondingJudge
}

func (j *gatedJudge) Classify(ctx context.Context, req decisionmodel.Request) (decisionmodel.Result, error) {
	j.mu.Lock()
	idx := j.calls
	j.calls++
	j.mu.Unlock()

	if idx < len(j.entered) {
		close(j.entered[idx])
	}
	if idx < len(j.release) {
		// Deliberately ignore ctx.Done so the test controls when a cancelled call returns,
		// modeling a provider that finishes its round trip after cancellation.
		<-j.release[idx]
	}
	return j.base.Classify(ctx, req)
}

func (j *gatedJudge) count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.calls
}

func (j *gatedJudge) Close() error { return nil }

func (s *FlowSuite) TestCancelledActiveWithDuplicateTurnIDDoesNotAdvanceNewerActive() {
	judge := &gatedJudge{
		entered: []chan struct{}{make(chan struct{}), make(chan struct{})},
		release: []chan struct{}{make(chan struct{}), make(chan struct{})},
	}
	s.flow.judge = judge

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "first words", Ended: true,
	}))
	<-judge.entered[0]

	s.Require().NoError(s.flow.Cancel("dup"))

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "dup", Participant: "Alex", Text: "second words", Ended: true,
	}))
	<-judge.entered[1]

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "queued", Participant: "Alex", Text: "third words", Ended: true,
	}))

	// Release the cancelled first call while the newer "dup" is still active. Its deferred
	// advance must match by pointer and leave the newer active candidate untouched.
	close(judge.release[0])
	s.Never(func() bool { return judge.count() >= 3 }, 30*time.Millisecond, 2*time.Millisecond,
		"the cancelled candidate's deferred advance must not clear or advance past a newer active candidate with the same turn id")

	close(judge.release[1])
	s.Require().Eventually(func() bool { return len(s.decisions()) == 2 }, settleFor,
		5*time.Millisecond, "the newer active duplicate and queued follow-up must both produce rulings")
	s.flow.running.Wait()

	decisions := s.decisions()
	s.Equal("dup", decisions[0].CandidateID)
	s.Equal(Respond, decisions[0].Disposition)
	s.NoError(decisions[0].Error())
	s.Equal("queued", decisions[1].CandidateID)
	s.Equal(Respond, decisions[1].Disposition)
	s.NoError(decisions[1].Error())
}

func (s *FlowSuite) TestCancelOfClaimedCandidateDoesNotStartQueuedOutOfOrder() {
	emitter := NewEmitter(0)
	f := newFlow(nil, emitter, slog.New(slog.NewJSONHandler(s.logs, nil)))
	judge := &gatedJudge{
		entered: []chan struct{}{make(chan struct{}), make(chan struct{})},
		release: []chan struct{}{make(chan struct{})},
	}
	f.judge = judge

	var releaseFirst sync.Once
	unblockFirst := func() { releaseFirst.Do(func() { close(judge.release[0]) }) }
	s.T().Cleanup(func() {
		unblockFirst()
		emitter.Close()
		_ = f.Close()
	})

	s.Require().NoError(f.Decide(s.T().Context(), FlowTurn{
		ID: "first", Participant: "Alex", Text: "first words", Ended: true,
	}))
	<-judge.entered[0]

	s.Require().NoError(f.Decide(s.T().Context(), FlowTurn{
		ID: "second", Participant: "Alex", Text: "second words", Ended: true,
	}))

	// Let the first candidate finish Classify and claim its ruling, then hold it in emitter.Send
	// before its deferred advance runs.
	unblockFirst()
	s.Require().Eventually(func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.active != nil && f.active.done
	}, settleFor, 5*time.Millisecond, "the first candidate never claimed its ruling")

	s.Require().NoError(f.Cancel("first"))
	s.Never(func() bool { return judge.count() >= 2 }, 30*time.Millisecond, 2*time.Millisecond,
		"cancelling a candidate that has already claimed its ruling must not start the queued candidate out of order")

	firstDecided, ok := (<-emitter.Events()).(Decided)
	s.Require().True(ok)
	s.Equal("first", firstDecided.CandidateID)
	s.Equal(Respond, firstDecided.Disposition)
	s.NoError(firstDecided.Error())

	select {
	case <-judge.entered[1]:
	case <-time.After(settleFor):
		s.FailNow("the queued candidate never started after the first ruling was delivered")
	}

	secondDecided, ok := (<-emitter.Events()).(Decided)
	s.Require().True(ok)
	s.Equal("second", secondDecided.CandidateID)
	s.Equal(Respond, secondDecided.Disposition)
	s.NoError(secondDecided.Error())

	f.running.Wait()
}

func (s *FlowSuite) TestClosingFlowTwiceClosesJudgeOnce() {
	judge := &recordingJudge{}
	s.flow.judge = judge

	s.Require().NoError(s.flow.Close())
	s.Equal(1, judge.closeCount(), "Close must close the judge")

	s.Require().NoError(s.flow.Close())
	s.Equal(1, judge.closeCount(), "a second Close must not close the judge again")
}

func (s *FlowSuite) TestCloseCancelsActiveAndQueuedCandidates() {
	hold := make(chan struct{})
	s.model.holdCreate = hold

	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "active", Participant: "Alex", Text: "first words",
	}))
	s.waitAsked(1)
	s.Require().NoError(s.flow.Decide(s.T().Context(), FlowTurn{
		ID: "queued", Participant: "Alex", Text: "second words",
	}))
	s.flow.mu.Lock()
	queuedCtx := s.flow.queued.ctx
	s.flow.mu.Unlock()

	s.Require().NoError(s.flow.Close())
	s.ErrorIs(s.model.contexts()[0].Err(), context.Canceled, "Close must cancel the active candidate")
	s.ErrorIs(queuedCtx.Err(), context.Canceled, "Close must cancel the queued candidate")
	s.Len(s.model.requests(), 1, "Close must not start the queued candidate")
}

func (s *FlowSuite) TestSetModelAfterCloseClosesTheNewSession() {
	lateModel := newStubLLM()
	lateSession := stubSession(&s.Suite, s.ctx, lateModel)

	s.Require().NoError(s.flow.Close())
	s.flow.setModel(lateSession)
	s.Equal(1, lateModel.closeCount(), "a session handed to a closed flow must be closed immediately")

	s.flow.setModel(nil)
	s.Require().NoError(s.flow.Close())
	s.Equal(1, lateModel.closeCount(), "a second Close must not re-close the rejected session")
}

func (s *FlowSuite) TestConcurrentDecideCancelSetModelAndClose() {
	s.model.mu.Lock()
	s.model.automatic = `{"disposition":"respond","floor":"continue"}`
	s.model.mu.Unlock()

	var wg sync.WaitGroup
	var lateModels []*stubLLM
	var lateMu sync.Mutex
	for i := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := range 20 {
				id := fmt.Sprintf("t-%d", j%3)
				_ = s.flow.Decide(s.T().Context(), FlowTurn{
					ID: id, Participant: "Alex", Text: fmt.Sprintf("words %d %d", worker, j),
				})
				if j%4 == 0 {
					_ = s.flow.Cancel(id)
				}
				if j%10 == 0 {
					m := newStubLLM()
					m.automatic = `{"disposition":"respond","floor":"continue"}`
					sess := stubSession(&s.Suite, s.ctx, m)
					lateMu.Lock()
					lateModels = append(lateModels, m)
					lateMu.Unlock()
					s.flow.setModel(sess)
				}
			}
		}(i)
	}
	time.Sleep(5 * time.Millisecond)
	s.Require().NoError(s.flow.Close())
	wg.Wait()

	lateMu.Lock()
	models := append([]*stubLLM(nil), lateModels...)
	lateMu.Unlock()
	for _, m := range models {
		s.Equal(1, m.closeCount(), "every swapped session must be closed once")
	}
}
