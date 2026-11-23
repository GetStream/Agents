//go:build integration

package api

import (
	"strings"

	"github.com/gorilla/websocket"
)

func (s *SessionEventsSuite) TestAToolResultMayNameItsCallByTheIDTheToolCallGaveIt() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	s.respond(opened.Id)

	asked := s.await(watching, "tool_call")
	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_result", "id": asked["id"], "turn_id": asked["turn_id"], "output": "it ships tomorrow",
	}))

	ran := s.await(watching, "tool_ran")
	s.Equal("it ships tomorrow", ran["result"])
	s.Empty(ran["error"])
}

func (s *SessionEventsSuite) TestAToolResultsToolCallIDWinsOverItsID() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	s.respond(opened.Id)

	asked := s.await(watching, "tool_call")
	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": asked["id"], "id": s.utils.uuid(), "output": "it ships tomorrow",
	}))

	s.Equal("it ships tomorrow", s.await(watching, "tool_ran")["result"])
}

func (s *SessionEventsSuite) TestAToolResultNamingNoCallIsAnsweredWithAnError() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	s.respond(opened.Id)
	s.await(watching, "tool_call")

	s.Require().NoError(watching.WriteJSON(map[string]any{"type": "tool_result", "output": "it ships tomorrow"}))

	failed := s.await(watching, "error")
	s.Equal("tool", failed["context"])
	s.Contains(failed["error"], "tool_call_id")
}

func (s *SessionEventsSuite) TestAToolResultNoCallWaitsForIsAnsweredWithWhyAndTheSocketStaysOpen() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	s.respond(opened.Id)
	asked := s.await(watching, "tool_call")
	unknown := s.utils.uuid()

	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": unknown, "output": "it ships tomorrow",
	}))

	ignored := s.await(watching, "tool_result_ignored")
	s.Equal(unknown, ignored["tool_call_id"])
	s.NotContains(ignored, "request_id", "an id it was not sent is left out")
	s.NotEmpty(ignored["reason"])

	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": asked["id"], "output": "it ships tomorrow",
	}))
	s.Equal("it ships tomorrow", s.await(watching, "tool_ran")["result"], "the call still takes its own result")
}

// Every command naming a call takes the name the tool_call gave it, as tool_result does.
func (s *SessionEventsSuite) TestEveryCommandNamingACallMayNameItByID() {
	for _, command := range []struct {
		name string
		// durable asks the call from a durable command, whose approval is recorded.
		durable bool
		send    map[string]any
		// then is sent after send, before wanted is awaited.
		then   map[string]any
		wanted string
	}{
		{name: "tool_hold", send: map[string]any{"type": "tool_hold"}, wanted: "tool_held"},
		{name: "tool_update", send: map[string]any{"type": "tool_update", "text": "searching 3 airlines"}, wanted: "tool_updated"},
		{
			name: "tool_approval", durable: true, send: map[string]any{"type": "tool_approval", "allowed": true},
			then: map[string]any{"type": "tool_result", "output": "it ships tomorrow"}, wanted: "tool_ran",
		},
	} {
		s.Run(command.name, func() {
			var watching *websocket.Conn
			var ids map[string]any
			if command.durable {
				watching, ids = s.commandsCallAsked()
			} else {
				opened := s.withATool()
				watching = s.watch(opened.Id)
				s.respond(opened.Id)
				asked := s.await(watching, "tool_call")
				ids = map[string]any{"tool_call_id": asked["id"]}
			}
			named := map[string]any{"id": ids["tool_call_id"], "request_id": ids["request_id"], "turn_id": ids["turn_id"]}

			s.Require().NoError(watching.WriteJSON(merged(command.send, named)))
			if command.then != nil {
				s.Require().NoError(watching.WriteJSON(merged(command.then, ids)))
			}

			answered := s.awaitWithoutError(watching, command.wanted)
			if command.wanted != "tool_ran" {
				s.Equal(ids["tool_call_id"], answered["id"])
			}
		})
	}
}

// A durable command's result naming another call is answered with every id it was sent with.
func (s *SessionEventsSuite) TestAToolResultOfACommandNamingNoCallIsAnsweredWithItsIDs() {
	watching, ids := s.commandsCallAsked()

	s.Require().NoError(watching.WriteJSON(merged(ids, map[string]any{
		"type": "tool_result", "tool_call_id": "call-nobody-made", "output": "it ships tomorrow",
	})))

	s.Equal(frame(merged(ids, map[string]any{"type": "tool_result_ignored", "tool_call_id": "call-nobody-made",
		"reason": errNoResultWaits.Error()})), frame(s.await(watching, "tool_result_ignored")))
}

// A replayed call answered twice, as replay_pending_tools allows, has its second result
// answered: the first is the one the model took.
func (s *SessionEventsSuite) TestAToolResultSentTwiceIsAnsweredTheSecondTime() {
	watching, ids := s.commandsCallAsked()
	result := merged(ids, map[string]any{"type": "tool_result", "output": "it ships tomorrow"})
	s.Require().NoError(watching.WriteJSON(result))
	s.Equal("it ships tomorrow", s.await(watching, "tool_ran")["result"])

	s.Require().NoError(watching.WriteJSON(result))

	s.Equal(frame(merged(ids, map[string]any{"type": "tool_result_ignored", "reason": errNoResultWaits.Error()})),
		frame(s.await(watching, "tool_result_ignored")))
}

// An id is repeated no longer than a request_id may be, so a watcher cannot fill the
// replay with what it sends.
func (s *SessionEventsSuite) TestAToolResultIgnoredRepeatsNoMoreThanTheStartOfALongID() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	long := strings.Repeat("x", 4*maxEchoedID)

	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": long, "turn_id": long, "output": "it ships tomorrow",
	}))

	ignored := s.await(watching, "tool_result_ignored")
	s.Equal(long[:maxEchoedID], ignored["tool_call_id"])
	s.Equal(long[:maxEchoedID], ignored["turn_id"])
}
