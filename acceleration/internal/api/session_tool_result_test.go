//go:build integration

package api

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
