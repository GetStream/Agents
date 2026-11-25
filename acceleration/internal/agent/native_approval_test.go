package agent

import (
	"context"
	"slices"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

func (s *AgentSuite) TestANativeModelIsToldToAskBeforeACallThatNeedsApprovalRuns() {
	peer := s.nativeAsked(time.Minute)

	asked := s.nativeFrame(peer, "conversation.item.create")
	s.Contains(string(asked["item"]), "Ask them now, in these words")
	s.Contains(string(asked["item"]), "Book order 12?")
	s.nativeFrame(peer, "response.create")
	s.never(func() bool { return len(s.runner.asked()) > 0 }, "the call ran before the caller said yes")
}

func (s *AgentSuite) TestANativeModelsConfirmationRunsTheCallWithItsOwnID() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeQuestionSaid(peer)
	s.nativeCallerSpoke(peer)
	var approved bool
	s.runner.run = func(ctx context.Context, _ llm.ToolCall) ([]llm.ContentPart, error) {
		approved = Approved(ctx)
		return llm.TextParts("order 12 booked"), nil
	}

	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r3"}}`)
	s.nativeSend(peer, `{"type":"response.function_call_arguments.done","response_id":"r3","call_id":"call2","name":"confirm_tool_call","arguments":"{\"tool_call_id\":\"call1\",\"allowed\":true}"}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r3","status":"completed"}}`)

	result := s.nativeFrame(peer, "conversation.item.create")
	s.Contains(string(result["item"]), "order 12 booked")
	s.Contains(string(result["item"]), "call2", "the result answers the confirmation the model has open")
	s.Require().Len(s.runner.asked(), 1)
	s.Equal("call1", s.runner.asked()[0].ID)
	s.True(approved)
}

// A yes the model gives right after it was told to ask runs nothing: the caller has not heard
// the question.
func (s *AgentSuite) TestANativeYesBeforeTheQuestionWasSaidRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeFrame(peer, "response.create")

	s.nativeConfirms(peer, "r2")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not heard the question for lookup_order")
	s.never(func() bool { return len(s.runner.asked()) > 0 }, "a yes the caller never gave ran the call")
}

// A reply that said nothing did not ask the question, nor does the caller's silence answer it.
func (s *AgentSuite) TestANativeYesAfterAReplyThatSaidNothingRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeFrame(peer, "response.create")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r2"}}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r2","status":"completed"}}`)
	s.nativeCallerSpoke(peer)

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not heard the question for lookup_order")
	s.Empty(s.runner.asked())
}

// Once the model said the question, a yes still waits for the caller to speak after it.
func (s *AgentSuite) TestANativeYesBeforeTheCallerSpokeRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeQuestionSaid(peer)
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.approvals["call1"].played
	}, "the question never finished playing")

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not spoken since they were asked about lookup_order")
	s.Empty(s.runner.asked())

	// The call still waits: a reply cut into once the question played does not close it.
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r4"}}`)
	s.nativeSend(peer, `{"type":"response.output_audio.delta","response_id":"r4","delta":"AQACAA=="}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r4","status":"cancelled"}}`)
	s.nativeReplied("r4")
	s.nativeCallerSpoke(peer)
	s.nativeConfirms(peer, "r5")
	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "order 12 booked")
	s.Len(s.runner.asked(), 1)
}

// What the caller types after the question played is their answer to it.
func (s *AgentSuite) TestANativeTypedYesAfterTheQuestionPlayedRunsTheCallOnce() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeQuestionSaid(peer)
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.approvals["call1"].played
	}, "the question never finished playing")

	_, err := s.agent.RespondTo(routing.WithCaller(s.ctx, routing.Caller{UserID: "alice"}), "yes, book it", nil, options.LLM{})
	s.Require().NoError(err)
	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "yes, book it")
	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "order 12 booked")
	s.Len(s.runner.asked(), 1)
}

// Text a customer's server wrote into the call is not the caller's answer.
func (s *AgentSuite) TestANativeYesToTextAServerWroteRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeQuestionSaid(peer)
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.approvals["call1"].played
	}, "the question never finished playing")

	_, err := s.agent.RespondTo(routing.WithCaller(s.ctx, routing.Caller{UserID: "alice", ServerSide: true}), "yes", nil, options.LLM{})
	s.Require().NoError(err)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not spoken since they were asked about lookup_order")
	s.Empty(s.runner.asked())
}

// Words the caller says while the question is still playing out are no answer to it.
func (s *AgentSuite) TestANativeYesToWordsSaidOverTheQuestionRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.edge.holdSpeech(true)
	s.nativeQuestionSaid(peer)
	s.nativeCallerSpoke(peer)
	s.edge.holdSpeech(false)
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.approvals["call1"].played
	}, "the question never finished playing")

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not spoken since they were asked about lookup_order")
	s.Empty(s.runner.asked())
}

// The reply that made the call is not the one that asks its question, even when it spoke and
// ended after the model was told to ask.
func (s *AgentSuite) TestANativeYesAfterOnlyTheReplyThatMadeTheCallSpokeRunsNothing() {
	s.ownsTools("order 12 booked")
	s.approves()
	peer := s.joinNativeWith(func(options *Options) {
		options.Tools, options.ToolRunner, options.Skills = s.tools, s.runner, harness.Skills{}
	})
	s.nativeFrame(peer, "session.update")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r1"}}`)
	s.nativeSend(peer, `{"type":"response.output_audio.delta","response_id":"r1","delta":"AQACAA=="}`)
	s.nativeSend(peer, `{"type":"response.function_call_arguments.done","response_id":"r1","call_id":"call1","name":"lookup_order","arguments":"{\"order\":\"12\"}"}`)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r1","status":"completed"}}`)
	s.nativeReplied("r1")
	s.nativeCallerSpoke(peer)

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not heard the question for lookup_order")
	s.Empty(s.runner.asked())
}

// A question the caller cut into was never heard out: a yes to it runs nothing and closes it.
func (s *AgentSuite) TestANativeYesToAQuestionTheCallerCutIntoRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeFrame(peer, "response.create")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r2"}}`)
	s.nativeSend(peer, `{"type":"response.output_audio.delta","response_id":"r2","delta":"AQACAA=="}`)
	s.nativeSend(peer, `{"type":"input_audio_buffer.speech_started"}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r2","status":"cancelled"}}`)
	s.nativeReplied("r2")
	s.stoppedSpeaking(peer)

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "did not hear the question to its end")
	s.Empty(s.runner.asked())
	s.eventually(func() bool { return countOf[ToolApprovalDecided](s.reported()) == 1 }, "the closed call was never reported")
	decided, _ := firstOf[ToolApprovalDecided](s.reported())
	s.Equal("Question not heard", decided.Summary)
}

func (s *AgentSuite) TestANativeCallerWhoSaysNothingDeclinesOnceTheQuestionWasSaid() {
	peer := s.nativeAsked(200 * time.Millisecond)
	s.nativeFrame(peer, "conversation.item.create")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r2"}}`)
	time.Sleep(500 * time.Millisecond)
	s.Zero(countOf[ToolApprovalDecided](s.reported()), "the wait started while the question was still being said")

	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r2","status":"completed"}}`)

	s.eventually(func() bool { return countOf[ToolApprovalDecided](s.reported()) == 1 }, "silence never declined the call")
	decided, _ := firstOf[ToolApprovalDecided](s.reported())
	s.Equal(ToolApprovalDecided{ID: "call1", TurnID: decided.TurnID, Summary: "Not confirmed in time"}, decided)
	s.never(func() bool { return len(s.runner.asked()) > 0 }, "a call nobody allowed ran")
}

// nativeAsked is a native call on which the model asked for lookup_order, which needs the
// caller's yes and waits wait for it.
func (s *AgentSuite) nativeAsked(wait time.Duration) *nativePeer {
	s.ownsTools("order 12 booked")
	s.approves()
	peer := s.joinNativeWith(func(options *Options) {
		options.Tools = s.tools
		options.ToolRunner = s.runner
		options.Skills = harness.Skills{}
		options.ApprovalWait = wait
	})
	setup := s.nativeFrame(peer, "session.update")
	s.Contains(string(setup["session"]), confirmTool)
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r1"}}`)
	s.nativeSend(peer, `{"type":"response.function_call_arguments.done","response_id":"r1","call_id":"call1","name":"lookup_order","arguments":"{\"order\":\"12\"}"}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r1","status":"completed"}}`)
	return peer
}

// nativeQuestionSaid is the model saying the question it was told to ask, to its end.
func (s *AgentSuite) nativeQuestionSaid(peer *nativePeer) {
	s.nativeFrame(peer, "response.create")
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"r2"}}`)
	s.nativeSend(peer, `{"type":"response.output_audio.delta","response_id":"r2","delta":"AQACAA=="}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"r2","status":"completed"}}`)
	s.nativeReplied("r2")
}

// nativeReplied waits for the agent to settle the model's reply id.
func (s *AgentSuite) nativeReplied(id string) {
	s.eventually(func() bool {
		return slices.ContainsFunc(s.reported(), func(event Event) bool {
			said, ok := event.(Responded)
			return ok && said.TurnID == id
		})
	}, "the reply never settled")
}

// nativeCallerSpoke is the caller saying something, once the model has stopped.
func (s *AgentSuite) nativeCallerSpoke(peer *nativePeer) {
	s.nativeSend(peer, `{"type":"input_audio_buffer.speech_started"}`)
	s.heardSpeechStart()
	s.stoppedSpeaking(peer)
}

// nativeConfirms is the model's reply id answering call1's question with a yes.
func (s *AgentSuite) nativeConfirms(peer *nativePeer, id string) {
	s.nativeSend(peer, `{"type":"response.created","response":{"id":"`+id+`"}}`)
	s.nativeSend(peer, `{"type":"response.function_call_arguments.done","response_id":"`+id+`","call_id":"yes-`+id+`","name":"confirm_tool_call","arguments":"{\"tool_call_id\":\"call1\",\"allowed\":true}"}`)
	s.nativeSend(peer, `{"type":"response.done","response":{"id":"`+id+`","status":"completed"}}`)
}

// native, the question's reply completed but its audio is still on the edge; the caller types yes.
func (s *AgentSuite) TestNativeTypedYesWhileTheQuestionIsOnTheEdgeRunsNothing() {
	peer := s.nativeAsked(time.Minute)
	s.nativeFrame(peer, "conversation.item.create")
	s.edge.holdSpeech(true)
	s.nativeQuestionSaid(peer)
	_, err := s.agent.RespondTo(routing.WithCaller(s.ctx, routing.Caller{UserID: "alice"}), "yes, book it", nil, options.LLM{})
	s.Require().NoError(err)
	s.nativeFrame(peer, "conversation.item.create")
	s.edge.holdSpeech(false)
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.approvals["call1"].played
	}, "the question never finished playing")

	s.nativeConfirms(peer, "r3")

	s.Contains(string(s.nativeFrame(peer, "conversation.item.create")["item"]), "has not spoken since they were asked about lookup_order")
	s.Empty(s.runner.asked())
}
