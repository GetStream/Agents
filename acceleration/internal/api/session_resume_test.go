//go:build integration

package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// SessionResumeSuite covers a resumable events socket: the cursor its frames carry, what a
// socket that comes back is sent again, and a conversation followed through the router
// holding it stopping.
type SessionResumeSuite struct {
	RouterSuite
}

func TestSessionResumeSuite(t *testing.T) {
	runSuite(t, new(SessionResumeSuite))
}

func (s *SessionResumeSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *SessionResumeSuite) TestAResumableSocketStartsWithWhereItsStreamIsAndNumbersItsFrames() {
	opened := s.inWriting()
	watching := s.serverClient.opens(s.events(opened.Id, "resumable=true"))

	resumed := s.await(watching, "resumed")
	s.Equal(true, resumed["complete"])
	s.respond(opened.Id)

	s.NotEmpty(s.await(watching, "responded")["cursor"])
}

func (s *SessionResumeSuite) TestASocketThatDidNotAskToResumeIsSentTheFramesItAlwaysWas() {
	opened := s.inWriting()
	watching := s.serverClient.opens(s.events(opened.Id, ""))
	s.respond(opened.Id)

	answered := s.await(watching, "responded")

	s.NotContains(answered, "cursor")
}

func (s *SessionResumeSuite) TestASocketThatComesBackIsSentWhatItMissed() {
	opened := s.inWriting()
	watching := s.serverClient.opens(s.events(opened.Id, "resumable=true"))
	last := s.await(watching, "resumed")["cursor"].(string)
	s.Require().NoError(watching.Close())
	// Somebody else watching sees the answer arrive, so it was given before the socket is back.
	bystander := s.serverClient.opens(s.events(opened.Id, ""))
	s.respond(opened.Id)
	s.await(bystander, "responded")

	again := s.serverClient.opens(s.events(opened.Id, "cursor="+url.QueryEscape(last)))

	s.Equal(true, s.await(again, "resumed")["complete"])
	s.NotEmpty(s.await(again, "responded")["cursor"], "the answer given while nobody watched is sent again")
}

func (s *SessionResumeSuite) TestACursorNoRouterHandedOutIsRefused() {
	opened := s.inWriting()

	_, status := s.serverClient.watch(s.events(opened.Id, "cursor=nonsense"))

	s.Equal(http.StatusBadRequest, status)
}

func (s *SessionResumeSuite) TestAnotherAppCannotResumeASession() {
	opened := s.inWriting()
	watching := s.serverClient.opens(s.events(opened.Id, "resumable=true"))
	last := s.await(watching, "resumed")["cursor"].(string)

	_, status := s.data.backendOfAnotherApp().watch(s.events(opened.Id, "cursor="+url.QueryEscape(last)))

	s.Equal(http.StatusNotFound, status, "a cursor is no credential")
}

func (s *SessionResumeSuite) TestAConversationIsFollowedThroughTheRouterHoldingItStopping() {
	stopping, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	there := s.serverClient.on(stopping)
	request := textSession(nil)
	request.Llm = pointerTo("stuck/stuck-model")
	opened := there.createSession(request)
	watching := there.opens(s.events(opened.Id, "resumable=true"))
	last := s.await(watching, "resumed")["cursor"].(string)
	command := s.utils.uuid()
	s.Require().Equal(http.StatusOK, there.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/respond",
		RespondRequest{RequestId: &command, Text: "a long question"}, nil))

	s.Require().NoError(sessions.Shutdown())

	s.Require().NoError(watching.SetReadDeadline(time.Now().Add(2 * settleFor)))
	for {
		var received map[string]any
		err := watching.ReadJSON(&received)
		if err != nil {
			s.True(websocket.IsCloseError(err, websocket.CloseServiceRestart),
				"a resumable socket is told the router is restarting: %v", err)
			break
		}
		if cursor, ok := received["cursor"].(string); ok {
			last = cursor
		}
	}
	again := s.serverClient.opens(s.events(opened.Id, "cursor="+url.QueryEscape(last)+"&request_id="+command))

	resumed := s.await(again, "resumed")
	s.Equal(false, resumed["complete"], "what the stopped router said after the cursor is gone")
	s.Require().NotNil(resumed["command"], "the command the socket waited on is reported")
	s.Equal("interrupted", resumed["command"].(map[string]any)["state"],
		"a reply the router stopped is told apart from one somebody cancelled")
}

// cmd/router closes the server before it ends the sessions still held, so the end of a
// session is told to a socket on another node after the server's background work is gone.
func (s *SessionResumeSuite) TestASocketElsewhereIsToldTheRouterIsRestartingAfterItsServerIsClosed() {
	stopping, server, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	request := textSession(nil)
	request.Llm = pointerTo("stuck/stuck-model")
	opened := s.serverClient.on(stopping).createSession(request)
	watching := s.serverClient.opens(s.events(opened.Id, "resumable=true"))
	s.await(watching, "resumed")

	ctx, cancel := context.WithTimeout(context.Background(), settleFor)
	defer cancel()
	s.Require().NoError(server.Close(ctx))
	s.Require().NoError(sessions.Shutdown())

	s.Require().NoError(watching.SetReadDeadline(time.Now().Add(2 * settleFor)))
	for {
		var received map[string]any
		if err := watching.ReadJSON(&received); err != nil {
			s.True(websocket.IsCloseError(err, websocket.CloseServiceRestart),
				"a resumable socket on another node is told the router is restarting: %v", err)
			return
		}
	}
}

// inWriting opens a conversation that keeps no transcript, the cheapest session with an
// events socket.
func (s *SessionResumeSuite) inWriting() Session {
	return s.serverClient.createSession(CreateSessionRequest{Incognito: pointerTo(true), Llm: pointerTo("llm-flow")})
}

func (s *SessionResumeSuite) events(id, query string) string {
	path := "/v1/agents/sessions/" + id + "/events"
	if query != "" {
		path += "?" + query
	}
	return path
}

func (s *SessionResumeSuite) respond(id string) {
	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+id+"/respond", SayRequest{Text: "hello"}, nil))
}
