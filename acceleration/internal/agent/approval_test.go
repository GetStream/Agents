package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

func (s *AgentSuite) TestACallThatNeedsApprovalAsksTheQuestionAsWrittenAndDoesNotRun() {
	s.onFakeClock(func() {
		s.askedToApprove()

		s.never(func() bool { return len(s.runner.asked()) > 0 }, "the call ran before the caller said yes")
		asked := s.historyWith(llm.ToolResult)
		s.Contains(asked, "Book order 12?")
		s.Contains(asked, confirmTool)
		s.Len(s.model.requests(), 1, "the question is said as written, without a turn of the model's")
	})
}

func (s *AgentSuite) TestAYesRunsTheCallOnceWithItsOwnID() {
	s.onFakeClock(func() {
		s.askedToApprove()
		var approved atomic.Bool
		s.runner.run = func(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
			approved.Store(Approved(ctx))
			return llm.TextParts("order 12 booked"), nil
		}
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes please")

		s.eventually(func() bool { return len(s.runner.asked()) == 1 }, "the call never ran")
		ran := s.runner.asked()[0]
		s.Equal("call-1", ran.ID, "the call runs under the id the model gave it")
		s.Equal("lookup_order", ran.Name)
		s.Equal(`{"order":"12"}`, ran.Arguments)
		// Read once the result is back: the stub records a call before it runs it.
		s.eventually(func() bool { return strings.Contains(s.historyWith(llm.ToolResult), "order 12 booked") },
			"the call's result never answered the confirmation")
		s.True(approved.Load(), "the call is marked approved for whoever runs it")
		s.eventually(func() bool { return countOf[ToolApprovalDecided](s.reported()) == 1 }, "the answer was never reported")
		decided, _ := firstOf[ToolApprovalDecided](s.reported())
		s.Equal(ToolApprovalDecided{ID: "call-1", TurnID: decided.TurnID, Allowed: true}, decided)
	})
}

// Keys of a line for a call that was answered with its question, as main logs them: the asked
// line has only approval added to them.
var askedLineKeys = []string{"agent_id", "app_id", "call", "customer", "duration_ms", "error_code", "level", "msg",
	"outcome", "session", "success", "time", "tool", "tool_call", "turn"}

// connectorLineKeys are the keys main logs a connector's call with: one that asks nothing, and
// the confirmation that ran one that asked.
var connectorLineKeys = []string{"agent_id", "app_id", "call", "connector", "customer", "duration_ms", "error_code",
	"level", "msg", "outcome", "session", "success", "time", "tool", "tool_call", "turn"}

// confirmedLines has the caller asked whether to book order 12 and answer with arguments, run
// carrying out the call they allow, and returns the "a tool call came back" lines once the
// confirmation's has been logged.
func (s *AgentSuite) confirmedLines(arguments string, run func(context.Context, llm.ToolCall) ([]llm.ContentPart, error)) []map[string]any {
	s.logs = &logLines{}
	s.askedToApprove()
	s.runner.run = run
	s.confirms(arguments)

	s.says(stt.Participant{ID: "alice"}, "yes please")

	s.eventually(func() bool {
		return slices.ContainsFunc(s.logs.records(toolBack), func(line map[string]any) bool { return line["tool"] == confirmTool })
	}, "the confirmation never came back")
	return s.logs.records(toolBack)
}

// The call a caller said yes to is logged under its own name, connector and run, so an
// operator sees the tool that ran and not only the confirmation it ran inside.
func (s *AgentSuite) TestAnApprovedCallIsLoggedUnderItsOwnName() {
	s.onFakeClock(func() {
		okBefore, _ := histogramOf("router_agent_tool_call_seconds", "outcome", "ok")
		refusedBefore, _ := histogramOf("router_agent_tool_call_seconds", "outcome", "refused")
		lines := s.confirmedLines(`{"tool_call_id":"call-1","allowed":true}`,
			func(ctx context.Context, _ llm.ToolCall) ([]llm.ContentPart, error) {
				ReportTool(ctx, ToolReport{Connector: "github"})
				time.Sleep(2 * time.Second)
				return llm.TextParts("order 12 booked"), nil
			})

		s.Require().Len(lines, 3)
		asked, ran, confirmed := lines[0], lines[1], lines[2]
		s.Equal("lookup_order", asked["tool"])
		s.Equal("call-1", asked["tool_call"])
		s.Equal("refused", asked["outcome"], "the outcome the metric counts an asked call under is kept")
		s.Equal("asked", asked["approval"], "the call was answered with its question")
		s.Equal(askedLineKeys, slices.Sorted(maps.Keys(withoutKey(asked, "approval"))))

		s.Equal("lookup_order", ran["tool"])
		s.Equal("call-1", ran["tool_call"])
		s.Equal(asked["turn"], ran["turn"], "the run is in the turn that asked for it")
		s.Equal("allowed", ran["approval"])
		s.Equal("call-2", ran["confirmation"])
		s.Equal("github", ran["connector"])
		s.Equal("ok", ran["outcome"])
		s.Equal(true, ran["success"])
		s.Equal("", ran["error_code"])
		s.Equal(float64(2000), ran["duration_ms"], "the run took the two seconds the tool did")
		s.Equal("acme", ran["app_id"])
		s.Equal(s.agentID, ran["agent_id"])

		s.Equal(confirmTool, confirmed["tool"])
		s.Equal("call-2", confirmed["tool_call"])
		s.Equal("github", confirmed["connector"], "the confirmation is logged as on main")
		s.Equal(connectorLineKeys, slices.Sorted(maps.Keys(confirmed)))

		okAfter, _ := histogramOf("router_agent_tool_call_seconds", "outcome", "ok")
		refusedAfter, _ := histogramOf("router_agent_tool_call_seconds", "outcome", "refused")
		s.Equal(okBefore+1, okAfter, "the run is timed once, under its confirmation, as on main")
		s.Equal(refusedBefore+1, refusedAfter)
	})
}

// An approved run that failed says so on its own line, with what its runner reported in
// place of the error it returned.
func (s *AgentSuite) TestAnApprovedCallThatFailedIsLoggedAsFailed() {
	s.onFakeClock(func() {
		lines := s.confirmedLines(`{"tool_call_id":"call-1","allowed":true}`,
			func(ctx context.Context, _ llm.ToolCall) ([]llm.ContentPart, error) {
				ReportTool(ctx, ToolReport{Connector: "github", ErrorCode: "customer_auth", Error: "the token was refused"})
				return nil, errors.New("stub: POST /orders/12/refund was refused")
			})

		s.Require().Len(lines, 3)
		ran, confirmed := lines[1], lines[2]
		s.Equal("lookup_order", ran["tool"])
		s.Equal("error", ran["outcome"])
		s.Equal(false, ran["success"])
		s.Equal("customer_auth", ran["error_code"])
		s.Equal("the token was refused", ran["error"])
		s.NotContains(fmt.Sprint(ran), "/orders/12/refund", "the returned error is not logged")
		s.Equal("customer_auth", confirmed["error_code"], "the confirmation is logged as on main")
		s.NotContains(fmt.Sprint(confirmed), "/orders/12/refund")
	})
}

// interruptedRun has the caller say yes to booking order 12, under a tool policy, and talk
// over the run, and returns the line the run was logged with.
func (s *AgentSuite) interruptedRun(policy ToolPolicy) map[string]any {
	s.logs = &logLines{}
	s.toolPolicy = func(string) ToolPolicy { return policy }
	s.askedToApprove()
	began, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s.runner.run = func(ctx context.Context, _ llm.ToolCall) ([]llm.ContentPart, error) {
		close(began)
		select {
		case <-release:
			return llm.TextParts("order 12 booked"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.confirms(`{"tool_call_id":"call-1","allowed":true}`)
	participant := stt.Participant{ID: "alice"}
	s.says(participant, "yes please")
	select {
	case <-began:
	case <-time.After(settleFor):
		s.FailNow("the approved call never ran")
	}
	s.flow.then = []string{`{"disposition":"wait","floor":"stop"}`}
	s.replyWentOut()

	s.mutters(participant, "stop, hold on")

	if !policy.Cancels {
		s.eventually(func() bool { return strings.Contains(fmt.Sprint(s.history()), stillRunning) },
			"the interrupted confirmation was not answered")
		release <- struct{}{}
	}
	s.eventually(func() bool { return len(s.logs.records(toolBack)) == 3 }, "the run never came back")
	return s.logs.records(toolBack)[1]
}

// A run the caller talked over is answered with stillRunning and let finish, as the
// confirmation it ran inside is.
func (s *AgentSuite) TestAnApprovedCallTheCallerTalkedOverIsLoggedAsStillRunning() {
	s.onFakeClock(func() {
		ran := s.interruptedRun(ToolPolicy{Waits: true})
		s.Equal("lookup_order", ran["tool"])
		s.Equal("still-running", ran["outcome"])
	})
}

// A run of a tool that asks to be cancelled when talked over is logged as cancelled.
func (s *AgentSuite) TestAnApprovedCallCancelledByTheCallerIsLoggedAsCancelled() {
	s.onFakeClock(func() {
		ran := s.interruptedRun(ToolPolicy{Cancels: true})
		s.Equal("lookup_order", ran["tool"])
		s.Equal("cancelled", ran["outcome"])
		s.Equal("cancelled", ran["error_code"])
	})
}

// A no runs nothing, so nothing is logged as run: only the question and the confirmation.
func (s *AgentSuite) TestADeclinedCallLogsNoRun() {
	s.onFakeClock(func() {
		lines := s.confirmedLines(`{"tool_call_id":"call-1","allowed":false}`, nil)

		s.Require().Len(lines, 2)
		s.Equal("asked", lines[0]["approval"])
		s.Equal(confirmTool, lines[1]["tool"])
		s.NotContains(lines[1], "approval")
	})
}

func (s *AgentSuite) TestANoNeverRunsTheCall() {
	s.onFakeClock(func() {
		s.askedToApprove()
		s.confirms(`{"tool_call_id":"call-1","allowed":false}`)

		s.says(stt.Participant{ID: "alice"}, "no, leave it")

		s.eventually(func() bool { return strings.Contains(s.historyWith(llm.ToolResult), "declined") },
			"the model was never told the caller declined")
		s.never(func() bool { return len(s.runner.asked()) > 0 }, "a declined call ran")
		s.eventually(func() bool { return countOf[ToolApprovalDecided](s.reported()) == 1 }, "the answer was never reported")
		decided, _ := firstOf[ToolApprovalDecided](s.reported())
		s.False(decided.Allowed)
	})
}

func (s *AgentSuite) TestSilenceDeclinesOnceTheWaitHasPassedSinceTheQuestionWasSaid() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(300 * time.Millisecond)
		s.voice.mu.Lock()
		s.voice.silent = true
		s.voice.mu.Unlock()
		s.askFor()

		s.eventually(func() bool { return s.spokenText("Book order 12?") }, "the question was never said")
		time.Sleep(600 * time.Millisecond)
		s.False(s.declinedBySilence(), "the wait counted from when the question started, not when it finished")
		finished := time.Now()
		s.voiceFinishes()

		s.eventually(s.declinedBySilence, "silence never declined the call")
		s.GreaterOrEqual(time.Since(finished), 300*time.Millisecond, "declined before the wait had passed")
		s.never(func() bool { return len(s.runner.asked()) > 0 }, "a call nobody allowed ran")
	})
}

// The note that silence declined a call is the router's, on the caller's side of the
// conversation, and no caller's speech: a model of the call's speech is not told it was said.
func (s *AgentSuite) TestTheNoteThatSilenceDeclinedIsNotSaid() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(100 * time.Millisecond)
		s.askFor()
		s.eventually(s.declinedBySilence, "silence never declined the call")

		at := slices.IndexFunc(s.history(), func(m llm.Message) bool { return strings.Contains(m.Content, "did not say yes") })
		s.Require().GreaterOrEqual(at, 0)
		s.Equal(llm.User, s.history()[at].Role)
		s.True(s.history()[at].Typed, "the router's note was kept as said on the call")
	})
}

func (s *AgentSuite) TestAYesAfterTheWaitAsksTheModelToCallTheToolAgain() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(100 * time.Millisecond)
		s.askFor()
		s.eventually(s.declinedBySilence, "silence never declined the call")
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "oh, yes")

		s.eventually(func() bool { return strings.Contains(s.historyWith(llm.ToolResult), "call lookup_order again") },
			"a late yes was not sent back to ask again")
		s.never(func() bool { return len(s.runner.asked()) > 0 }, "a late yes ran the call")
	})
}

func (s *AgentSuite) TestAYesToACallWhoseToolChangedSinceDoesNotRunIt() {
	s.onFakeClock(func() {
		s.askedToApprove()
		changed := s.agent.tools()
		changed.Tools = slices.Clone(changed.Tools)
		changed.Tools[0].Approval = &harness.ToolApproval{Title: "Book order {order} for tomorrow?"}
		ctx, cancel := context.WithTimeout(s.ctx, settleFor)
		defer cancel()
		s.Require().NoError(s.agent.SetTools(ctx, changed, func() {}))
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes please")

		s.eventually(func() bool { return strings.Contains(s.historyWith(llm.ToolResult), "call lookup_order again") },
			"the yes was not sent back to ask again")
		s.never(func() bool { return len(s.runner.asked()) > 0 },
			"a yes to a call asked about as the tool was ran the tool as it is now")
	})
}

func (s *AgentSuite) TestConfirmToolCallIsOfferedOnlyWhenAToolNeedsApproval() {
	s.ownsTools("")
	s.join(true)
	_, offered := s.agent.availableTools().Lookup(confirmTool)
	s.False(offered, "confirm_tool_call was offered with nothing to confirm")

	s.ownsTools("")
	s.approves()
	s.join(true)
	_, offered = s.agent.availableTools().Lookup(confirmTool)
	s.True(offered)
}

func (s *AgentSuite) TestAToolThatNeedsApprovalIsNotAskedAboutInWriting() {
	s.ownsTools("order 12 ships tomorrow")
	s.tools.Tools[0].Approval = &harness.ToolApproval{Title: "Book order {order}?"}
	s.joinText()
	s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: llm.ToolCall{ID: "call-1", Name: "lookup_order", Arguments: `{"order":"12"}`}})

	s.Len(s.runner.asked(), 1, "in writing the app asks, as it always has")
	_, offered := s.agent.availableTools().Lookup(confirmTool)
	s.False(offered)
}

func (s *AgentSuite) TestTheQuestionFillsInTheCallsArguments() {
	for _, c := range []struct{ template, arguments, want string }{
		{"Book flight {flight_id}?", `{"flight_id":"BA 117"}`, "Book flight BA 117?"},
		{"Send {count} seats to {city}?", `{"count":2,"city":"Rome"}`, "Send 2 seats to Rome?"},
		{"Cancel {order}?", `{}`, "Cancel {order}?"},
		{"Cancel {order}?", `not json`, "Cancel {order}?"},
	} {
		s.Equal(c.want, fillQuestion(c.template, c.arguments), c.template+" "+c.arguments)
	}
}

// approves makes lookup_order need the caller's yes, for an agent joined after it.
func (s *AgentSuite) approves() {
	s.tools.Tools[0].Approval = &harness.ToolApproval{Title: "Book order {order}?"}
}

// agentAsksAndWaits is a voice call on which lookup_order needs a yes, waiting wait for one.
func (s *AgentSuite) agentAsksAndWaits(wait time.Duration) {
	s.ownsTools("order 12 booked")
	s.approves()
	s.join(true)
	s.agent.options.ApprovalWait = wait
	s.agent.options.ToolRunner = s.runner
}

// askFor has the caller ask for order 12, which the model calls lookup_order for.
func (s *AgentSuite) askFor() {
	s.model.reply = []string{"Sure."}
	s.asksFor("lookup_order", `{"order":"12"}`)
	participant := stt.Participant{ID: "alice"}
	s.speak(participant)
	s.says(participant, "book order 12")
}

// askedToApprove is a voice call on which the caller was asked whether to book order 12.
func (s *AgentSuite) askedToApprove() {
	s.agentAsksAndWaits(time.Minute)
	s.askFor()
	s.eventually(func() bool { return s.spokenText("Book order 12?") }, "the question was never said")
	s.replyWentOut()
}

// confirms makes the model's next reply call confirm_tool_call with arguments.
func (s *AgentSuite) confirms(arguments string) {
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	s.model.calls = []llm.ToolCall{{ID: "call-2", Name: confirmTool, Arguments: arguments}}
	s.model.callsOn = len(s.model.asked) + 1
	s.model.then = []string{"Okay."}
}

// voiceFinishes has a silent voice finish everything it was asked to say.
func (s *AgentSuite) voiceFinishes() {
	s.voice.mu.Lock()
	s.voice.silent = false
	said := slices.Clone(s.voice.said)
	s.voice.mu.Unlock()
	finished := map[string]bool{}
	for _, request := range said {
		if !request.Final || finished[request.ID] {
			continue
		}
		finished[request.ID] = true
		s.voice.emitter.Send(tts.AudioChunk{
			SynthesisID: request.ID,
			Audio:       audio.PcmData{Samples: make([]int16, 160), SampleRate: 16_000, Channels: 1},
		})
		s.voice.emitter.Send(tts.SynthesisComplete{SynthesisID: request.ID, AudioDurationMs: 10, TimeToFirstByteMs: 5})
	}
}

func (s *AgentSuite) declinedBySilence() bool {
	return strings.Contains(s.historyWith(llm.User), "did not say yes")
}

// historyWith is the text of every message in the history with role.
func (s *AgentSuite) historyWith(role llm.Role) string {
	var text []string
	for _, message := range s.history() {
		if message.Role == role {
			text = append(text, message.Content)
		}
	}
	return strings.Join(text, "\n")
}

// withoutKey is line without key.
func withoutKey(line map[string]any, key string) map[string]any {
	kept := maps.Clone(line)
	delete(kept, key)
	return kept
}
