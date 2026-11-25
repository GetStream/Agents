package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
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

// confirmationLineKeys are the keys the confirmation's line has: the run it approved has its
// own line, with the connector and the error.
var confirmationLineKeys = []string{"agent_id", "app_id", "call", "customer", "duration_ms", "level", "msg",
	"outcome", "session", "success", "time", "tool", "tool_call", "turn"}

// connectorLineKeys are the keys main logs a connector's call with that asks nothing.
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
		s.Equal(true, confirmed["success"])
		s.Equal("ok", confirmed["outcome"])
		s.Equal(confirmationLineKeys, slices.Sorted(maps.Keys(confirmed)), "the connector is on the run's line only")

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
		s.Equal(false, confirmed["success"])
		s.Equal("error", confirmed["outcome"])
		s.Equal(confirmationLineKeys, slices.Sorted(maps.Keys(confirmed)), "the error is on the run's line only")
		s.NotContains(fmt.Sprint(confirmed), "/orders/12/refund")
		s.NotContains(fmt.Sprint(confirmed), "the token was refused")
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

// A yes the model gives in the turn that made the call runs nothing: the caller has not heard
// the question yet, let alone answered it.
func (s *AgentSuite) TestAYesInTheTurnThatMadeTheCallRunsNothing() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(time.Minute)
		s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: llm.ToolCall{ID: "call-1", Name: "lookup_order", Arguments: `{"order":"12"}`}})
		s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: llm.ToolCall{ID: "call-2", Name: confirmTool, Arguments: `{"tool_call_id":"call-1","allowed":true}`}})

		s.confirmationSays("call-2", "has not heard the question for lookup_order")
		s.never(func() bool { return len(s.runner.asked()) > 0 }, "a yes the caller never gave ran the call")
		s.Zero(countOf[ToolApprovalDecided](s.reported()), "the call stopped waiting for the caller")
	})
}

// A no needs nothing of the caller: given at once, it still closes the call unrun.
func (s *AgentSuite) TestANoInTheTurnThatMadeTheCallDeclinesIt() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(time.Minute)
		s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: llm.ToolCall{ID: "call-1", Name: "lookup_order", Arguments: `{"order":"12"}`}})
		s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: llm.ToolCall{ID: "call-2", Name: confirmTool, Arguments: `{"tool_call_id":"call-1","allowed":false}`}})

		s.confirmationSays("call-2", "The caller declined")
		decided, _ := firstOf[ToolApprovalDecided](s.reported())
		s.Equal(ToolApprovalDecided{ID: "call-1", TurnID: "turn-1"}, decided)
		s.Empty(s.runner.asked())
	})
}

// Once the question has played, a yes still waits for the caller to speak: one in a turn they
// did not speak, as a typed one is, runs nothing and leaves the call waiting for theirs.
func (s *AgentSuite) TestAYesBeforeTheCallerSpokeRunsNothingAndTheCallWaits() {
	s.onFakeClock(func() {
		s.askedToApprove()
		time.Sleep(time.Second) // A tick of the fake clock: the question has finished playing.
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)
		_, err := s.agent.RespondTo(s.ctx, "yes", nil, options.LLM{})
		s.Require().NoError(err)
		s.confirmationSays("call-2", "has not spoken since they were asked about lookup_order")
		s.Empty(s.runner.asked(), "a yes the caller did not speak ran the call")

		// The caller's yes counts for the model's next turn, after the question's wait began.
		s.model.mu.Lock()
		s.model.then = []string{"Let me check."}
		s.model.mu.Unlock()
		s.says(stt.Participant{ID: "alice"}, "yes please")
		s.eventually(func() bool { return s.spokenText("Let me check.") }, "the caller's yes was never answered")
		time.Sleep(time.Second)
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)
		_, err = s.agent.RespondTo(s.ctx, "go ahead", nil, options.LLM{})
		s.Require().NoError(err)

		s.eventually(func() bool { return len(s.runner.asked()) == 1 }, "the caller's own yes never ran the call")
	})
}

// A question asked later takes nothing from one asked before: the caller cutting off the second
// leaves their yes to the first standing.
func (s *AgentSuite) TestCuttingOffASecondQuestionLeavesTheFirstStanding() {
	s.onFakeClock(func() {
		s.askedToApprove()
		time.Sleep(time.Second)
		s.model.mu.Lock()
		s.model.calls = []llm.ToolCall{{ID: "call-3", Name: "lookup_order", Arguments: `{"order":"13"}`}}
		s.model.callsOn = len(s.model.asked) + 1
		s.model.mu.Unlock()
		s.voice.mu.Lock()
		s.voice.silent = true
		s.voice.mu.Unlock()
		s.says(stt.Participant{ID: "alice"}, "and order 13 too")
		s.eventually(func() bool { return s.spokenText("Book order 13?") }, "the second question was never said")
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes to the first")

		s.eventually(func() bool { return len(s.runner.asked()) == 1 }, "the yes to the first question never ran it")
		s.Equal("call-1", s.runner.asked()[0].ID)
	})
}

// A turn answered with the reply speculated for it is one the caller spoke.
func (s *AgentSuite) TestAYesInAReplySpeculatedForTheCallersTurnRunsTheCall() {
	s.onFakeClock(func() {
		s.speculates = true
		s.askedToApprove()
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes please")

		s.eventually(func() bool { return len(s.runner.asked()) == 1 }, "the caller's yes never ran the call")
	})
}

// A yes runs only the call whose id it names, not the only one waiting.
func (s *AgentSuite) TestAYesUnderAnotherIDRunsNothing() {
	for _, id := range []string{"", "call-9"} {
		s.Run(strconv.Quote(id), func() {
			s.onFakeClock(func() {
				s.askedToApprove()
				s.confirms(`{"tool_call_id":"` + id + `","allowed":true}`)

				s.says(stt.Participant{ID: "alice"}, "yes please")

				s.confirmationSays("call-2", "no question waiting")
				s.Empty(s.runner.asked(), "a yes under another id ran the waiting call")
			})
		})
	}
}

// A question the caller talked over was never heard out, so a yes to it runs nothing and closes it.
func (s *AgentSuite) TestAYesToAQuestionTheCallerTalkedOverRunsNothing() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(time.Minute)
		s.voice.mu.Lock()
		s.voice.silent = true
		s.voice.mu.Unlock()
		s.askFor()
		s.eventually(func() bool { return s.spokenText("Book order 12?") }, "the question was never said")
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes do it")

		s.confirmationSays("call-2", "did not hear the question to its end")
		s.Empty(s.runner.asked(), "a yes to a question talked over ran the call")
		decided, _ := firstOf[ToolApprovalDecided](s.reported())
		s.Equal(ToolApprovalDecided{ID: "call-1", TurnID: decided.TurnID, Summary: "Question not heard"}, decided)
	})
}

// A question the voice refused to say was never heard, so a yes to it runs nothing and closes it.
func (s *AgentSuite) TestAYesToAQuestionTheVoiceRefusedRunsNothing() {
	s.onFakeClock(func() {
		s.agentAsksAndWaits(time.Minute)
		s.voice.mu.Lock()
		s.voice.refuses = errors.New("voice unavailable")
		s.voice.mu.Unlock()
		s.askFor()
		s.eventually(func() bool { return countOf[ToolRan](s.reported()) == 1 }, "the call was never asked about")
		time.Sleep(time.Second) // A tick of the fake clock: nothing is left playing.
		s.confirms(`{"tool_call_id":"call-1","allowed":true}`)

		s.says(stt.Participant{ID: "alice"}, "yes please")

		s.confirmationSays("call-2", "did not hear the question to its end")
		s.Empty(s.runner.asked())
	})
}

// confirmationSays waits for the model to be answered for the confirm_tool_call with id, and
// checks the answer has text.
func (s *AgentSuite) confirmationSays(id, text string) {
	var result string
	s.eventually(func() bool {
		for _, event := range s.reported() {
			if ran, ok := event.(ToolRan); ok && ran.ID == id {
				result = ran.Result
				return true
			}
		}
		return false
	}, "the confirmation was never answered")
	s.Contains(result, text)
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

// A confirmation that fails on its own runs nothing, so its line keeps its own error: no run
// line carries it. Three ways to fail on its own: arguments that are not an answer, a native
// model's call refused before it is run, and a caller's own tool that has the name.
func (s *AgentSuite) TestAConfirmationThatFailedOnItsOwnKeepsItsError() {
	s.onFakeClock(func() {
		lines := s.confirmedLines(`{"tool_call_id":"call-1","allowed":{"yes":1}}`, nil)

		s.Require().Len(lines, 2)
		s.Equal(confirmTool, lines[1]["tool"])
		s.Equal(false, lines[1]["success"])
		s.Equal("error", lines[1]["error_code"])
		s.Contains(lines[1]["error"], "confirm_tool_call's arguments")
	})
}

func (s *AgentSuite) TestANativeConfirmationTheRouterRefusedKeepsItsError() {
	s.logs = &logLines{}
	s.ownsTools("")
	peer := s.joinNativeWith(func(options *Options) {
		options.Logger = slog.New(slog.NewJSONHandler(s.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		options.ToolRunner = s.runner
		options.Tools = s.tools
	})
	s.nativeFrame(peer, "session.update")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r1"}}`)
	s.nativeSend(peer, `{"type":"response.function_call_arguments.done","response_id":"r1","call_id":"call2","name":"confirm_tool_call","arguments":"{\"tool_call_id\":\"call1\",\"allowed\":true}"}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r1","status":"completed"}}`)

	s.eventually(func() bool {
		return slices.ContainsFunc(s.logs.records(toolBack), func(line map[string]any) bool { return line["tool"] == confirmTool })
	}, "the refused confirmation never came back")
	lines := s.logs.records(toolBack)
	s.Require().Len(lines, 1)
	s.Equal(false, lines[0]["success"])
	s.Equal("error", lines[0]["error_code"])
	s.Contains(lines[0]["error"], "is not one of the model's tools")
}

func (s *AgentSuite) TestACallersToolNamedConfirmToolCallKeepsItsError() {
	s.onFakeClock(func() {
		s.logs = &logLines{}
		s.ownsTools("")
		s.approves()
		s.tools.Tools = append(s.tools.Tools, harness.Tool{Name: confirmTool, Description: "the caller's own", Parameters: map[string]any{"type": "object"}})
		s.join(true)
		s.agent.options.ToolRunner = s.runner
		s.runner.run = func(context.Context, llm.ToolCall) ([]llm.ContentPart, error) {
			return nil, errors.New("the caller's tool failed")
		}
		s.model.reply = []string{"Sure."}
		s.asksFor(confirmTool, `{}`)
		participant := stt.Participant{ID: "alice"}
		s.speak(participant)
		s.says(participant, "do it")

		s.eventually(func() bool { return len(s.logs.records(toolBack)) > 0 }, "the call never came back")
		line := s.logs.records(toolBack)[0]
		s.Equal(confirmTool, line["tool"])
		s.Equal(false, line["success"])
		s.Equal("error", line["error_code"])
		s.Contains(line["error"], "the caller's tool failed")
	})
}
