//go:build integration

package api

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// DispatchSuite covers the worker side of dispatch: what a worker has to send to be given
// calls, and what it is given.
type DispatchSuite struct {
	RouterSuite
}

func TestDispatchSuite(t *testing.T) {
	runSuite(t, new(DispatchSuite))
}

func (s *DispatchSuite) SetupTest() {
	s.useFixture("standard")

	// A worker the last test opened is dropped a moment after its socket closes, and work
	// handed to that one is work this test never sees.
	s.Require().Eventually(func() bool {
		return len(s.dispatch.Workers(s.customerID())) == 0
	}, settleFor, 10*time.Millisecond, "the last test's worker is still in the pool")
}

func (s *DispatchSuite) TestAWorkerWithoutACredentialCannotWaitForCalls() {
	_, status := s.unauthenticatedClient.watch("/v1/dispatch")

	s.Equal(http.StatusUnauthorized, status)
}

func (s *DispatchSuite) TestACapacityThatIsNotANumberOfCallsIsRefused() {
	_, status := s.serverClient.watch("/v1/dispatch?capacity=plenty")

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestAWorkerWithNoRoomForACallIsRefused() {
	_, status := s.serverClient.watch("/v1/dispatch?capacity=0")

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestAWorkerTakingOneCallAtATimeIsTakenIn() {
	s.opensWith("/v1/dispatch?capacity=1")

	s.Len(s.dispatch.Workers(s.customerID()), 1)
}

func (s *DispatchSuite) TestAWorkerTakingTheMostCallsAWorkerCanIsTakenIn() {
	s.opensWith(fmt.Sprintf("/v1/dispatch?capacity=%d", maxCapacity))

	s.Len(s.dispatch.Workers(s.customerID()), 1)
}

func (s *DispatchSuite) TestACapacityAboveTheMostAWorkerCanHoldIsRefusedNamingTheLimit() {
	status, message := s.serverClient.refusal(fmt.Sprintf("/v1/dispatch?capacity=%d", maxCapacity+1))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(message, strconv.Itoa(maxCapacity), "a worker refused should learn what it may ask for")
	s.Empty(s.dispatch.Workers(s.customerID()))
}

func (s *DispatchSuite) TestTheLargestCapacityANumberCanSayIsRefused() {
	_, status := s.serverClient.watch(fmt.Sprintf("/v1/dispatch?capacity=%d", math.MaxInt64))

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestACapacityPastWhatANumberCanSayIsRefused() {
	_, status := s.serverClient.watch("/v1/dispatch?capacity=9223372036854775808")

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestAWorkerAfterARefusedCapacityIsStillTakenIn() {
	_, _ = s.serverClient.watch(fmt.Sprintf("/v1/dispatch?capacity=%d", math.MaxInt64))

	worker := s.worker()
	s.assign(dispatch.Call{CallID: "call-1"})

	s.Equal("call-1", s.next(worker)["call_id"])
}

func (s *DispatchSuite) TestWorkAWorkerCannotAlreadyBeHoldingIsRefused() {
	_, status := s.serverClient.watch("/v1/dispatch?active=-1")

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestAKindOfWorkThisServiceDoesNotHandOutIsRefused() {
	// Taking it would leave the worker believing it had opted out of calls when it had not.
	_, status := s.serverClient.watch("/v1/dispatch?handles=telegrams")

	s.Equal(http.StatusBadRequest, status)
}

func (s *DispatchSuite) TestWorkCarriesTheIdItIsReportedFinishedUnder() {
	worker := s.worker()

	s.assign(dispatch.Call{CallID: "phone-+15125551234"})

	s.NotEmpty(s.next(worker)["work_id"], "without it the worker cannot say which call it finished")
}

func (s *DispatchSuite) TestAWorkerIsHeldToTheCapacityItDeclaredUntilItReportsWorkDone() {
	// The queue empties as the frame is written, which is not the worker finishing the
	// call. Without the report, a worker that promised one call would be handed the next.
	worker := s.reportingWorker(1)

	s.assign(dispatch.Call{CallID: "call-1"})
	handed := s.next(worker)

	_, err := s.dispatch.Assign(s.customerID(), dispatch.Call{CallID: "call-2"})
	s.Require().ErrorContains(err, "at capacity")

	s.Require().NoError(worker.WriteJSON(frame{"type": "done", "work_id": handed["work_id"]}))
	s.Require().Eventually(func() bool {
		_, err := s.dispatch.Assign(s.customerID(), dispatch.Call{CallID: "call-2"})
		return err == nil
	}, settleFor, 10*time.Millisecond, "reporting the call finished should give the worker its room back")

	s.Equal("call-2", s.next(worker)["call_id"])
}

func (s *DispatchSuite) TestAWorkerThatAnswersOnlyMessagesIsNotOfferedCalls() {
	connection := s.serverClient.opens("/v1/dispatch?handles=message")
	s.Equal("ready", s.next(connection)["type"])
	s.Require().Eventually(func() bool {
		return len(s.dispatch.Workers(s.customerID())) == 1
	}, settleFor, 10*time.Millisecond)

	_, err := s.dispatch.Assign(s.customerID(), dispatch.Call{CallID: "call-1"})

	s.Require().ErrorContains(err, "handles call")
	_, err = s.dispatch.AssignMessage(s.customerID(),
		dispatch.Message{ChannelID: "chat-1", Text: "hello"})
	s.Require().NoError(err)
	s.Equal("message", s.next(connection)["type"])
}

func (s *DispatchSuite) TestAConnectedWorkerIsHandedAnArrivingCall() {
	worker := s.worker()

	s.assign(dispatch.Call{
		CallID:       "phone-+15125551234",
		CallType:     "default",
		CalledNumber: "+15125551234",
		CallerNumber: "+15550001111",
		Custom:       map[string]string{"line": "support"},
		At:           time.Now().UTC(),
	})

	call := s.next(worker)
	s.Equal("call", call["type"])
	s.Equal("phone-+15125551234", call["call_id"])
	s.Equal("default", call["call_type"])
	s.Equal("+15125551234", call["called_number"])
	s.Equal("+15550001111", call["caller_number"])
	s.Equal(map[string]any{"line": "support"}, call["custom"])
	s.NotEmpty(call["at"])
}

func (s *DispatchSuite) TestACallWithNoCustomDataStillCarriesAnObject() {
	worker := s.worker()

	s.assign(dispatch.Call{CallID: "phone-+15125551234"})

	s.Equal(map[string]any{}, s.next(worker)["custom"],
		"a client should not have to tell absent from empty")
}

func (s *DispatchSuite) TestAWorkerOnlyGetsItsOwnCustomersCalls() {
	worker := s.worker()

	_, err := s.dispatch.Assign("somebody-else", dispatch.Call{CallID: "phone-+15125559999"})
	s.Require().ErrorIs(err, dispatch.ErrNoWorkers)

	s.Require().NoError(worker.SetReadDeadline(time.Now().Add(dropped)))
	var call frame
	s.Error(worker.ReadJSON(&call), "nothing should arrive on somebody else's call")
}

func (s *DispatchSuite) TestWhatAWorkerReportsAboutItselfIsReadable() {
	s.worker().WriteJSON(frame{
		"type": "load", "active_agents": 2, "cpu_percent": 37.5,
		"memory_percent": 61.25, "latency_ms": 12.5,
	})

	// The report crosses a socket, so it is waited for rather than assumed to have landed.
	s.Require().Eventually(func() bool {
		waiting := s.dispatch.Workers(s.customerID())
		return len(waiting) == 1 && waiting[0].Load().ActiveAgents == 2
	}, settleFor, 10*time.Millisecond)

	load := s.dispatch.Workers(s.customerID())[0].Load()
	s.InDelta(37.5, load.CPUPercent, 0.001)
	s.InDelta(61.25, load.MemoryPercent, 0.001)
	s.InDelta(12.5, load.LatencyMs, 0.001)
}

func (s *DispatchSuite) TestAWorkerCanMeasureItsOwnRoundTrip() {
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{"type": "ping", "at": 1234.5}))

	pong := s.next(worker)
	s.Equal("pong", pong["type"])
	s.Equal(1234.5, pong["at"], "the timestamp comes back so the worker can subtract it")
}

func (s *DispatchSuite) TestPingRepliesAndCallDeliveryShareTheSocket() {
	worker := s.worker()
	s.Require().NoError(worker.SetWriteDeadline(time.Now().Add(settleFor)))
	for i := range 32 {
		s.Require().NoError(worker.WriteJSON(frame{"type": "ping", "at": i}))
	}
	s.assign(dispatch.Call{CallID: "concurrent-call"})

	seen := make(map[float64]bool)
	calls := 0
	for range 33 {
		event := s.next(worker)
		switch event["type"] {
		case "pong":
			at, ok := event["at"].(float64)
			s.Require().True(ok)
			s.False(seen[at], "each ping has exactly one reply")
			seen[at] = true
		case "call":
			s.Equal("concurrent-call", event["call_id"])
			calls++
		default:
			s.FailNow("unexpected dispatch event")
		}
	}
	s.Equal(1, calls)
	for i := range 32 {
		s.True(seen[float64(i)], "all ping timestamps survive call delivery")
	}
}

func (s *DispatchSuite) TestAMessageTheServerCannotReadDoesNotEndTheConnection() {
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{"type": "who-knows"}))

	// Still in the rotation, and still handed calls.
	s.assign(dispatch.Call{CallID: "phone-+15125551234"})
	s.Equal("call", s.next(worker)["type"])
}

func (s *DispatchSuite) TestAFrameThatIsNotJSONEndsTheWorkerWithPolicyViolation() {
	worker := s.worker()

	s.Require().NoError(worker.WriteMessage(websocket.TextMessage, []byte("not json")))
	read, ended := s.socketEnd(worker)

	s.Require().Len(read, 1)
	s.Equal(frame{
		"type": "error", "error": errUnreadableFrame.Message,
		"code": errUnreadableFrame.Code, "error_type": string(errUnreadableFrame.Type),
	}, read[0])
	s.Equal(errUnreadableFrame.Message, s.closedWith(ended, websocket.ClosePolicyViolation).Text)
	s.Require().Eventually(func() bool {
		return len(s.dispatch.Workers(s.customerID())) == 0
	}, settleFor, 10*time.Millisecond, "the worker the socket ended is still in the rotation")
}

// A tool's answer of 5 MiB is taken whole.
func (s *DispatchSuite) TestAToolsAnswerOfTheLargestFrameIsTaken() {
	worker := s.hostingWorker()
	answered := s.runsHosted("call-1")
	s.Require().Equal("call-1", s.next(worker)["id"])
	opening, closing := `{"id":"call-1","tool_call_id":"call-1","type":"tool_result","output":"`, `"}`
	output := strings.Repeat("a", 5<<20-len(opening)-len(closing))

	s.Require().NoError(worker.WriteMessage(websocket.TextMessage, []byte(opening+output+closing)))

	got := s.hostedAnswer(answered)
	s.Require().NoError(got.err)
	s.Require().Len(got.output, 1)
	s.Equal(len(output), len(got.output[0].Text))
}

// The call a longer answer was for fails at once, wherever in the frame the call is named, and the
// worker goes on with its other calls.
func (s *DispatchSuite) TestAToolsAnswerOverTheLimitFailsThatCall() {
	s.failsItsCall(`{"type":"tool_result","tool_call_id":"call-1","id":"call-1","output":"`, `"}`)
}

func (s *DispatchSuite) TestAToolsAnswerOverTheLimitFailsACallNamedAfterIt() {
	s.failsItsCall(`{"output":"`, `","tool_call_id":"call-1","type":"tool_result"}`)
}

func (s *DispatchSuite) TestAToolsErrorOverTheLimitFailsACallNamedAfterIt() {
	s.failsItsCall(`{"error":"`, `","id":"call-1","tool_call_id":"call-1","type":"tool_result"}`)
}

func (s *DispatchSuite) TestAToolsAnswerOverTheLimitByItsOldNameFailsThatCall() {
	s.failsItsCall(`{"id":"call-1","output":"`, `","type":"tool_result"}`)
}

func (s *DispatchSuite) TestAToolsAnswerOverTheLimitFailsTheCallOfItsCurrentName() {
	s.failsItsCall(`{"id":"call-2","output":"`, `","tool_call_id":"call-1","type":"tool_result"}`)
}

func (s *DispatchSuite) TestAToolsPartsOverTheLimitFailTheCallNamedAfterThem() {
	s.failsItsCall(`{"output":[{"type":"text","id":"call-2","text":"} ] \" \\`, `"},[0]],"custom":{"tool_call_id":"call-2"}, "id" : "call-1" ,`+"\n\t\r"+`"type":"tool_result"}`)
}

// The end of a string that is dropped is found past a quote that is escaped.
func (s *DispatchSuite) TestAToolsAnswerOfEscapesOverTheLimitFailsThatCall() {
	s.failsItsCall(`{"output":"`, `\",\"id\":\"call-2\\\\\"","id":"call-1","type":"tool_result"}`)
}

// An escape may fall anywhere in what is kept of a string.
func (s *DispatchSuite) TestAToolsAnswerOfUnicodeEscapesOverTheLimitFailsThatCall() {
	s.failsItsCall(`{"output":"x`+strings.Repeat(`\u00e9`, 1000), `","id":"call-1","type":"tool_result"}`)
}

func (s *DispatchSuite) TestALargeUpdateFailsNoCall() {
	s.failsNoCall(`{"tool_call_id":"call-1","text":"`, `","type":"tool_update"}`)
}

func (s *DispatchSuite) TestALargeLoadFailsNoCall() {
	s.failsNoCall(`{"type":"load","padding":"`, `"}`)
}

func (s *DispatchSuite) TestALargeFrameOfNoTypeFailsNoCall() {
	s.failsNoCall(`{"tool_call_id":"call-1","id":"call-1","output":"`, `"}`)
}

func (s *DispatchSuite) TestALargeFrameThatIsNoObjectFailsNoCall() {
	s.failsNoCall(`["tool_result","call-1","`, `"]`)
}

func (s *DispatchSuite) TestALargeFrameThatIsNotJSONFailsNoCall() {
	s.failsNoCall(`{"type":"tool_result","id":"call-1","output":"`, `"`)
}

func (s *DispatchSuite) TestALargeFrameOfManyMembersFailsNoCall() {
	s.failsNoCall(`{`+strings.Repeat(`"member":1,`, 1<<19), `"type":"tool_result","id":"call-1"}`)
}

// The piece of work a large done reports gives its room back.
func (s *DispatchSuite) TestALargeDoneNamedAfterItsErrorGivesItsRoomBack() {
	s.givesItsRoomBack(`{"error":"`, `","type":"done","work_id":"%s"}`)
}

func (s *DispatchSuite) TestALargeDoneNamedBeforeItsErrorGivesItsRoomBack() {
	s.givesItsRoomBack(`{"type":"done","work_id":"%s","error":"`, `"}`)
}

func (s *DispatchSuite) givesItsRoomBack(opening, closing string) {
	worker := s.reportingWorker(1)
	s.assign(dispatch.Call{CallID: "call-1"})
	work := s.next(worker)["work_id"].(string)
	opening, closing = strings.ReplaceAll(opening, "%s", work), strings.ReplaceAll(closing, "%s", work)
	_, err := s.dispatch.Assign(s.customerID(), dispatch.Call{CallID: "call-2"})
	s.Require().Error(err, "a worker of one call took two")

	s.Require().NoError(worker.WriteMessage(websocket.TextMessage, s.overTheLimit(opening, closing)))
	s.Equal("error", s.next(worker)["type"])

	s.assign(dispatch.Call{CallID: "call-3"})
	s.Equal("call-3", s.next(worker)["call_id"])
}

// failsItsCall checks a frame over the limit, of opening and closing with a string's inside
// between, fails call-1 and leaves the worker its other call.
func (s *DispatchSuite) failsItsCall(opening, closing string) {
	worker := s.hostingWorker()
	large, other := s.runsHosted("call-1"), s.runsHosted("call-2")
	s.next(worker)
	s.next(worker)

	s.Require().NoError(worker.WriteMessage(websocket.TextMessage, s.overTheLimit(opening, closing)))

	s.Require().ErrorContains(s.hostedAnswer(large).err, "the tool's answer is too large")
	s.Equal(frame{
		"type": "error", "error": "a frame on this socket is at most 5 MiB",
		"code": "payload_too_large", "error_type": "payload_too_large",
	}, s.next(worker))
	s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "id": "call-2", "output": "done"}))
	got := s.hostedAnswer(other)
	s.Require().NoError(got.err)
	s.Equal("done", got.output[0].Text)
}

// failsNoCall checks a frame over the limit, as failsItsCall sends one, leaves the worker call-1.
func (s *DispatchSuite) failsNoCall(opening, closing string) {
	worker := s.hostingWorker()
	waiting := s.runsHosted("call-1")
	s.next(worker)

	s.Require().NoError(worker.WriteMessage(websocket.TextMessage, s.overTheLimit(opening, closing)))

	s.Equal("error", s.next(worker)["type"])
	s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "id": "call-1", "output": "done"}))
	s.NoError(s.hostedAnswer(waiting).err)
}

// A frame over the limit the worker stopped sending ends its socket, as a shorter one does.
func (s *DispatchSuite) TestAFrameOverTheLimitCutOffEndsTheWorker() {
	worker := s.hostingWorker()
	waiting := s.runsHosted("call-1")
	s.next(worker)
	writer, err := worker.NextWriter(websocket.TextMessage)
	s.Require().NoError(err)
	_, err = writer.Write(append(s.overTheLimit(`{"type":"tool_result","id":"call-1","output":"`, ""), make([]byte, 64<<10)...))
	s.Require().NoError(err)

	s.Require().NoError(worker.UnderlyingConn().Close())

	s.ErrorContains(s.hostedAnswer(waiting).err, "the worker running this tool disconnected")
}

// overTheLimit is a frame of opening, a string's inside and closing: one byte over 5 MiB, or as long
// as opening and closing are.
func (s *DispatchSuite) overTheLimit(opening, closing string) []byte {
	return []byte(opening + strings.Repeat("a", max(0, 5<<20+1-len(opening)-len(closing))) + closing)
}

// hostingWorker is a worker hosting investigate_sdk for stream-support.
func (s *DispatchSuite) hostingWorker() *websocket.Conn {
	s.serverClient.agentNamed("stream-support")
	worker := s.worker()
	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support",
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))
	s.Require().Equal("hosting", s.next(worker)["type"])
	return worker
}

// hostedCallEnd is how a hosted call ended.
type hostedCallEnd struct {
	output []llm.ContentPart
	err    error
}

// runsHosted calls investigate_sdk, and tells how the call ended.
func (s *DispatchSuite) runsHosted(id string) <-chan hostedCallEnd {
	ended := make(chan hostedCallEnd, 1)
	ctx, customer := s.T().Context(), s.customerID()
	go func() {
		output, err := s.dispatch.RunHosted(ctx, customer, "stream-support", dispatch.ToolCall{ID: id, Name: "investigate_sdk"})
		ended <- hostedCallEnd{output, err}
	}()
	return ended
}

// hostedAnswer waits for a hosted call to end, for less than the 30 s it is given to answer.
func (s *DispatchSuite) hostedAnswer(ended <-chan hostedCallEnd) hostedCallEnd {
	select {
	case end := <-ended:
		return end
	case <-time.After(settleFor):
		s.FailNow("the hosted call was not answered")
		return hostedCallEnd{}
	}
}

func (s *DispatchSuite) TestAWorkerThatDisconnectsLeavesTheRotation() {
	worker := s.worker()

	s.Require().NoError(worker.Close())

	s.Require().Eventually(func() bool {
		return len(s.dispatch.Workers(s.customerID())) == 0
	}, settleFor, 10*time.Millisecond, "a closed socket must not hold a slot in the rotation")
}

func (s *DispatchSuite) TestTwoWorkersOnOneCustomerShareTheCalls() {
	first, second := s.worker(), s.worker()

	s.assign(dispatch.Call{CallID: "call-1"})
	s.assign(dispatch.Call{CallID: "call-2"})

	s.Equal("call-1", s.next(first)["call_id"])
	s.Equal("call-2", s.next(second)["call_id"])
}

func (s *DispatchSuite) TestHostedToolsAreKeptForTheAgentTheWorkerNames() {
	s.serverClient.agentNamed("stream-support")
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support",
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))

	hosting := s.next(worker)
	s.Equal("hosting", hosting["type"])
	s.Equal("stream-support", hosting["agent_id"])

	tools, timeout := s.dispatch.HostedTools(s.customerID(), "stream-support")
	s.Require().Len(tools, 1)
	s.Equal("investigate_sdk", tools[0].Name)
	s.Equal(30*time.Second, timeout, "a worker that names no timeout takes the session's default")
}

func (s *DispatchSuite) TestAHostedToolKeepsWhatItsCallsAreShownDoing() {
	s.serverClient.agentNamed("stream-support")
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support",
		"tools": []frame{{"name": "weather_lookup", "description": "Look up the weather", "display_title": " Checking the weather "}},
	}))

	s.Require().Equal("hosting", s.next(worker)["type"])
	tools, _ := s.dispatch.HostedTools(s.customerID(), "stream-support")
	s.Require().Len(tools, 1)
	s.Equal("Checking the weather", tools[0].DisplayTitle)
}

func (s *DispatchSuite) TestAHostedToolsOwnTimeoutReplacesTheWorkers() {
	s.serverClient.agentNamed("stream-support")
	worker := s.worker()
	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support", "timeout_ms": 60_000,
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source", "timeout_ms": 50, "interruptible": true}},
	}))
	s.Require().Equal("hosting", s.next(worker)["type"])
	tools, _ := s.dispatch.HostedTools(s.customerID(), "stream-support")
	s.Require().Len(tools, 1)
	s.True(tools[0].Interruptible)
	answered := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := s.dispatch.RunHosted(s.T().Context(), s.customerID(), "stream-support",
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
		answered <- err
	}()
	s.Equal("tool_call", s.next(worker)["type"])

	s.Equal("tool_cancel", s.next(worker)["type"])
	s.Error(<-answered)
	s.Less(time.Since(started), 5*time.Second, "the call waited out the worker's timeout")
}

func (s *DispatchSuite) TestANegativeHostedToolTimeoutIsRefused() {
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support",
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source", "timeout_ms": -1}},
	}))

	s.Equal("hosting_refused", s.next(worker)["type"])
}

func (s *DispatchSuite) TestHostedToolsForAnAgentTheAppDoesNotHaveAreRefused() {
	s.serverClient.agentNamed("stream-support")
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-suport",
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))

	refused := s.next(worker)
	s.Equal("hosting_refused", refused["type"])
	s.Equal("stream-suport", refused["agent_id"])
	s.Contains(refused["reason"], "there is no agent called stream-suport")
	tools, _ := s.dispatch.HostedTools(s.customerID(), "stream-suport")
	s.Empty(tools, "a misspelled agent hosts nothing")
}

func (s *DispatchSuite) TestHostedToolsNeedAnAgentToBeFor() {
	worker := s.worker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type":  "host_tools",
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))

	s.Equal("hosting_refused", s.next(worker)["type"])
	tools, _ := s.dispatch.HostedTools(s.customerID(), dispatch.EveryAgent)
	s.Empty(tools, "an agent id left out offered the tools to every agent")

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": "stream-support", "every_agent": true,
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))
	s.Equal("hosting_refused", s.next(worker)["type"])
}

func (s *DispatchSuite) TestToolsHostedForEveryAgentAreTheApps() {
	worker := s.worker()
	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "every_agent": true,
		"tools": []frame{{"name": "weather_lookup", "description": "Look up the weather"}},
	}))
	reply := s.next(worker)
	s.Equal("hosting", reply["type"], reply)
	s.Equal(true, reply["every_agent"])

	tools, _ := s.dispatch.HostedTools(s.customerID(), dispatch.EveryAgent)
	s.Require().Len(tools, 1)
	s.Equal("weather_lookup", tools[0].Name)
	tools, _ = s.dispatch.HostedTools(s.customerID(), "stream-support")
	s.Empty(tools, "tools hosted for every agent are not one agent's own")
}

func (s *DispatchSuite) TestAHostedToolCallIsAnsweredOverTheSocket() {
	worker := s.worker()
	s.Require().NoError(s.dispatch.Host(s.dispatch.Workers(s.customerID())[0], "stream-support",
		[]dispatch.Tool{{Name: "investigate_sdk", Description: "Read SDK source"}}, time.Minute))

	type answer struct {
		output []llm.ContentPart
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		output, err := s.dispatch.RunHosted(s.T().Context(), s.customerID(), "stream-support",
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1",
				Name: "investigate_sdk", Arguments: `{"sdk":"android"}`})
		answered <- answer{output, err}
	}()

	call := s.next(worker)
	s.Equal("tool_call", call["type"])
	s.Equal("call-1", call["id"])
	s.Equal("session-1", call["session_id"])
	s.Equal(`{"sdk":"android"}`, call["arguments"])

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "tool_result", "id": "call-1", "output": "targetSdkVersion 35"}))

	got := <-answered
	s.Require().NoError(got.err)
	s.Equal("targetSdkVersion 35", llm.TextOf(got.output))
}

// hostedAnswer is what came of one hosted call.
type hostedAnswer struct {
	parts []llm.ContentPart
	err   error
}

// asksTheWorker hosts investigate_sdk on the suite's worker and makes one call to it.
func (s *DispatchSuite) asksTheWorker() (*websocket.Conn, <-chan hostedAnswer) {
	worker := s.worker()
	s.Require().NoError(s.dispatch.Host(s.dispatch.Workers(s.customerID())[0], "stream-support",
		[]dispatch.Tool{{Name: "investigate_sdk", Description: "Read SDK source"}}, time.Minute))
	answered := make(chan hostedAnswer, 1)
	go func() {
		parts, err := s.dispatch.RunHosted(s.T().Context(), s.customerID(), "stream-support",
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
		answered <- hostedAnswer{parts, err}
	}()
	s.Equal("tool_call", s.next(worker)["type"])
	return worker, answered
}

func (s *DispatchSuite) TestAHostedCallSaysWhoAskedAndInWhichConversation() {
	worker := s.worker()
	s.Require().NoError(s.dispatch.Host(s.dispatch.Workers(s.customerID())[0], "stream-support",
		[]dispatch.Tool{{Name: "investigate_sdk", Description: "Read SDK source"}}, time.Minute))
	go func() {
		_, _ = s.dispatch.RunHosted(s.T().Context(), s.customerID(), "stream-support", dispatch.ToolCall{
			ID: "call-1", SessionID: "session-1", TurnID: "turn-1", Name: "investigate_sdk",
			User:         &dispatch.User{ID: "jean", Name: "Jean"},
			Conversation: &dispatch.Conversation{CID: "messaging:support-1", Custom: map[string]any{"plan": "gold"}},
		})
	}()

	call := s.next(worker)

	s.Equal("turn-1", call["turn_id"])
	s.Equal(frame{"id": "jean", "name": "Jean"}, frame(call["user"].(map[string]any)))
	s.Equal(frame{"cid": "messaging:support-1", "custom": map[string]any{"plan": "gold"}},
		frame(call["conversation"].(map[string]any)))
}

func (s *DispatchSuite) TestAHostedResultNamesItsCallAsASessionsDoes() {
	worker, answered := s.asksTheWorker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "tool_result", "tool_call_id": "call-1", "output": "targetSdkVersion 35"}))

	got := <-answered
	s.Require().NoError(got.err)
	s.Equal(llm.TextParts("targetSdkVersion 35"), got.parts)
}

func (s *DispatchSuite) TestAHostedToolCanAnswerWithAPicture() {
	worker, answered := s.asksTheWorker()

	s.Require().NoError(worker.WriteJSON(frame{
		"type": "tool_result", "tool_call_id": "call-1", "output": []frame{
			{"type": "text", "text": "the settings screen"},
			{"type": "image_url", "image_url": frame{"url": roses}},
		}}))

	got := <-answered
	s.Require().NoError(got.err)
	s.Require().Len(got.parts, 2)
	s.Equal("the settings screen", got.parts[0].Text)
	s.Require().NotNil(got.parts[1].Image, "the picture was dropped")
	s.Equal("image/png", got.parts[1].Image.MIME)
}

func (s *DispatchSuite) TestAHostedResultThatIsNotContentFailsTheCall() {
	worker, answered := s.asksTheWorker()

	s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "tool_call_id": "call-1", "output": 42}))

	s.Error((<-answered).err)
}

func (s *DispatchSuite) TestAHostedCallTheSessionGivesUpOnIsCancelledOnTheWorker() {
	worker := s.worker()
	s.Require().NoError(s.dispatch.Host(s.dispatch.Workers(s.customerID())[0], "stream-support",
		[]dispatch.Tool{{Name: "investigate_sdk", Description: "Read SDK source"}}, time.Minute))
	ctx, giveUp := context.WithCancel(s.T().Context())
	go func() {
		_, _ = s.dispatch.RunHosted(ctx, s.customerID(), "stream-support",
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	}()
	s.Equal("tool_call", s.next(worker)["type"])

	giveUp()

	cancel := s.next(worker)
	s.Equal("tool_cancel", cancel["type"])
	s.Equal("call-1", cancel["id"])
	s.Equal("session-1", cancel["session_id"])
}

func (s *DispatchSuite) TestAHostedCallThatTimesOutIsCancelledOnTheWorker() {
	worker := s.worker()
	s.Require().NoError(s.dispatch.Host(s.dispatch.Workers(s.customerID())[0], "stream-support",
		[]dispatch.Tool{{Name: "investigate_sdk", Description: "Read SDK source"}}, 50*time.Millisecond))
	go func() {
		_, _ = s.dispatch.RunHosted(s.T().Context(), s.customerID(), "stream-support",
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	}()
	s.Equal("tool_call", s.next(worker)["type"])

	cancel := s.next(worker)

	s.Equal("tool_cancel", cancel["type"])
	s.Equal("call-1", cancel["id"])
}

func (s *DispatchSuite) TestADrainingRouterSendsItsWorkersToReconnectElsewhere() {
	other, router := s.otherRouter(dispatch.NewPool())
	worker := s.serverClient.on(other).opens("/v1/dispatch")
	s.Equal("ready", s.next(worker)["type"])

	router.Drain()

	s.Require().NoError(worker.SetReadDeadline(time.Now().Add(settleFor)))
	_, _, err := worker.ReadMessage()
	s.True(websocket.IsCloseError(err, websocket.CloseGoingAway),
		"a worker told anything but going away stops waiting rather than reconnecting: %v", err)
}

func (s *DispatchSuite) TestADrainingRouterWaitsForTheWorkerToAnswerItsClose() {
	other, router := s.otherRouter(dispatch.NewPool())
	worker := s.serverClient.on(other).opens("/v1/dispatch")
	s.Equal("ready", s.next(worker)["type"])

	router.Drain()

	closed, held := s.unanswered(worker)
	s.Equal(websocket.CloseGoingAway, closed.Code)
	s.Equal("router restarting", closed.Text)
	s.heldUntilAnswered(held)
}

// A ping that crosses the router's close is not answered, and does not stop the router reading
// the worker's answer to the close.
func (s *DispatchSuite) TestAWorkerThatPingsAsTheRouterClosesHasItsAnswerRead() {
	other, router := s.otherRouter(dispatch.NewPool())
	worker := s.serverClient.on(other).opens("/v1/dispatch")
	s.Equal("ready", s.next(worker)["type"])
	worker.SetCloseHandler(func(int, string) error { return nil })
	router.Drain()
	s.Require().NoError(worker.SetReadDeadline(time.Now().Add(settleFor)))
	_, _, err := worker.ReadMessage()
	s.Require().True(websocket.IsCloseError(err, websocket.CloseGoingAway), "%v", err)

	s.Require().NoError(worker.WriteJSON(frame{"type": "ping", "at": 1}))
	answered := time.Now()
	s.Require().NoError(worker.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")))

	_, err = worker.UnderlyingConn().Read(make([]byte, 64))
	s.Require().ErrorIs(err, io.EOF)
	s.Less(time.Since(answered), closeWait/2, "the router did not read the answer behind the ping")
}

// worker opens the dispatch socket as a backend and waits until it is in the rotation, so a
// test that assigns a call straight afterwards does not race the registration.
// agentNamed is an agent config the client's app holds under name, which a worker needs
// before it can host tools for the agent. One the app holds already is kept.
func (c *testClient) agentNamed(name string) {
	status := c.do(http.MethodPost, "/v1/agents/configs",
		AgentConfigRequest{Name: name, Instructions: pointerTo("be brief")}, nil)
	c.suite.Require().Contains([]int{http.StatusCreated, http.StatusConflict}, status, "creating agent %s", name)
}

func (s *DispatchSuite) worker() *websocket.Conn {
	return s.opensWith("/v1/dispatch")
}

// reportingWorker is a worker that reports each piece of work finished, which is what holds
// it to the capacity it declared.
func (s *DispatchSuite) reportingWorker(capacity int) *websocket.Conn {
	return s.opensWith(fmt.Sprintf("/v1/dispatch?capacity=%d&active=0", capacity))
}

func (s *DispatchSuite) opensWith(address string) *websocket.Conn {
	before := len(s.dispatch.Workers(s.customerID()))
	connection := s.serverClient.opens(address)

	ready := s.next(connection)
	s.Equal("ready", ready["type"])
	s.NotEmpty(ready["worker_id"], "a worker is told what it is called")
	s.Require().Eventually(func() bool {
		return len(s.dispatch.Workers(s.customerID())) == before+1
	}, settleFor, 10*time.Millisecond)
	return connection
}

// assign hands a call to the customer's workers, one of which must take it.
func (s *DispatchSuite) assign(call dispatch.Call) {
	_, err := s.dispatch.Assign(s.customerID(), call)
	s.Require().NoError(err)
}

// next reads the next frame a worker is sent.
func (s *DispatchSuite) next(connection *websocket.Conn) frame {
	s.Require().NoError(connection.SetReadDeadline(time.Now().Add(settleFor)))
	var event frame
	s.Require().NoError(connection.ReadJSON(&event))
	return event
}
