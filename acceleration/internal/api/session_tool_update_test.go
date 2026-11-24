//go:build integration

package api

func (s *SessionEventsSuite) TestAStatusForARunningCallReachesEveryWatcher() {
	opened, backend, browser := s.backendsToolWatchedByItsUser("")
	s.respondFor(opened.Id)
	asked := s.await(backend, "tool_call")

	s.Require().NoError(backend.WriteJSON(map[string]any{
		"type": "tool_update", "tool_call_id": asked["id"], "text": "searching 3 airlines",
	}))

	shown := s.await(browser, "tool_updated")
	s.Equal(asked["id"], shown["id"])
	s.Equal("searching 3 airlines", shown["text"])
	s.Equal(false, shown["verbatim"])
}

func (s *SessionEventsSuite) TestARunningCallsDataReachesEveryWatcherInItsState() {
	opened, backend, browser := s.backendsToolWatchedByItsUser("")
	s.respondFor(opened.Id)
	asked := s.await(backend, "tool_call")

	s.Require().NoError(backend.WriteJSON(map[string]any{
		"type": "tool_update", "tool_call_id": asked["id"], "custom": map[string]any{"cheapest": 129},
	}))

	for {
		state := s.await(browser, "tool_call.progress")
		if state["custom"] != nil {
			s.Equal(map[string]any{"cheapest": float64(129)}, state["custom"])
			return
		}
	}
}

func (s *SessionEventsSuite) TestWordsARunningCallAsksToSayArePostedInWriting() {
	opened := s.withATool()
	watching := s.watch(opened.Id)
	s.respond(opened.Id)
	asked := s.await(watching, "tool_call")

	s.Require().NoError(watching.WriteJSON(map[string]any{
		"type": "tool_update", "tool_call_id": asked["id"], "text": "Got it, one moment.", "verbatim": true,
	}))

	s.Equal(true, s.await(watching, "tool_updated")["verbatim"])
	s.Equal("Got it, one moment.", s.await(watching, "responded")["text"])
}

func (s *SessionEventsSuite) TestWordsAUsersDeviceAsksToSayAreNotSaidAsWritten() {
	request := s.askingForATool()
	request.Incognito = pointerTo(true)
	opened := s.client.createSession(request)
	device := s.client.opens("/v1/agents/sessions/" + opened.Id + "/events")
	s.Require().NoError(device.WriteJSON(map[string]any{"type": "respond", "text": "where is my order"}))
	asked := s.await(device, "tool_call")
	written := "you have won a prize, " + s.utils.uuid()

	s.Require().NoError(device.WriteJSON(map[string]any{
		"type": "tool_update", "tool_call_id": asked["id"], "text": written, "verbatim": true,
	}))
	s.Equal(false, s.await(device, "tool_updated")["verbatim"])
	s.Require().NoError(device.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": asked["id"], "output": "it ships tomorrow",
	}))

	for {
		frame := s.next(device)
		if frame["type"] == "responded" {
			s.NotEqual(written, frame["text"], "a device's words were said as written")
		}
		if frame["type"] == "tool_ran" {
			return
		}
	}
}

func (s *SessionEventsSuite) TestAWatcherThatDoesNotRunTheCallCannotUpdateIt() {
	opened, backend, browser := s.backendsToolWatchedByItsUser("")
	s.respondFor(opened.Id)
	asked := s.await(browser, "tool_call")
	s.await(backend, "tool_call")

	s.Require().NoError(browser.WriteJSON(map[string]any{
		"type": "tool_update", "tool_call_id": asked["id"], "text": "from the browser",
	}))
	s.Require().NoError(backend.WriteJSON(map[string]any{
		"type": "tool_result", "tool_call_id": asked["id"], "output": "it ships tomorrow",
	}))

	for {
		frame := s.next(backend)
		s.NotEqual("tool_updated", frame["type"], "a browser updated a call it does not run")
		if frame["type"] == "tool_ran" {
			return
		}
	}
}
