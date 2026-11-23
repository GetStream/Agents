//go:build integration

package api

import (
	"crypto/rand"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
)

// SocketCloseLogSuite covers why a socket closed, as its access line says it: the events
// socket on the node running the session and on another one, /v1/agents/socket and
// /v1/dispatch. StreamsSuite covers /v1/{modality}/stream.
type SocketCloseLogSuite struct {
	RouterSuite
	logged *lockedLog
	// other is a node running nothing, logging to logged too.
	other *httptest.Server
}

func TestSocketCloseLogSuite(t *testing.T) {
	runSuite(t, new(SocketCloseLogSuite))
}

func (s *SocketCloseLogSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
	s.other, _ = s.otherRouterSealedBy(nil, s.sealer,
		slog.New(NewLogHandler(slog.NewTextHandler(s.logged, nil))))
}

func (s *SocketCloseLogSuite) SetupTest() {
	s.useFixture("standard")
}

// watched opens a session that keeps no transcript and its events socket, on node (nil for
// the suite's own), named by a request id of its own.
func (s *SocketCloseLogSuite) watched(node *httptest.Server) (Session, *websocket.Conn, string) {
	opened := s.serverClient.createSession(CreateSessionRequest{Incognito: pointerTo(true), Llm: pointerTo("llm-flow")})
	id := rand.Text()
	client := s.serverClient.withRequestID(id)
	if node != nil {
		client = client.on(node)
	}
	return opened, client.opens("/v1/agents/sessions/" + opened.Id + "/events"), id
}

// socketSession opens /v1/agents/socket and starts a session on it, named by a request id
// of its own.
func (s *SocketCloseLogSuite) socketSession() (*websocket.Conn, string, string) {
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{
		"type": "start", "session": map[string]any{"greeting": map[string]any{"text": "hello"}},
	}))
	answered := s.await(connection, "session")
	return connection, answered["session"].(map[string]any)["id"].(string), id
}

// closeLine is the access line of the socket opened with request id once it has closed.
func (s *SocketCloseLogSuite) closeLine(id string) string {
	return s.closeLineIn(s.logged, id)
}

// closeLineIn is the access line in logged of the socket opened with request id once it has
// closed.
func (s *RouterSuite) closeLineIn(logged *lockedLog, id string) string {
	var found string
	s.Require().Eventually(func() bool {
		found = lineWith(logged, `msg="served a request"`, " request_id="+id)
		return found != ""
	}, settleFor, 20*time.Millisecond, "the socket left no access line")
	return found
}

// closesWith sends a close frame of code and waits for the router's answer.
func (s *SocketCloseLogSuite) closesWith(connection *websocket.Conn, code int) {
	s.Require().NoError(connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, "")))
	_, _ = s.socketEnd(connection)
}

// awaitRouterClose reads until the router's close frame arrives and leaves it unanswered, so
// the caller can still send what it would have sent as the close crossed it.
func (s *SocketCloseLogSuite) awaitRouterClose(connection *websocket.Conn) {
	connection.SetCloseHandler(func(int, string) error { return nil })
	s.Require().NoError(connection.SetReadDeadline(time.Now().Add(settleFor)))
	for {
		if _, _, err := connection.ReadMessage(); err != nil {
			s.Require().True(websocket.IsCloseError(err, websocket.CloseNormalClosure), "the router sent no close: %v", err)
			return
		}
	}
}

// commandAfterTheEnd stops the session, then sends command once the router's close for the
// session's end has arrived, then answers that close.
func (s *SocketCloseLogSuite) commandAfterTheEnd(connection *websocket.Conn, session, command string) {
	s.serverClient.stopSession(session)
	s.awaitRouterClose(connection)
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": command}))
	s.Require().NoError(connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")))
}

func (s *SocketCloseLogSuite) TestACloseCommandAfterTheSessionEndedIsTheSessionsEnd() {
	// Catches the first cause lost: an SDK hanging up sends close as the router's close for
	// the session's end crosses it.
	opened, connection, id := s.watched(nil)

	s.commandAfterTheEnd(connection, opened.Id, "close")

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

func (s *SocketCloseLogSuite) TestARelayedCloseCommandAfterTheSessionEndedIsTheSessionsEnd() {
	opened, connection, id := s.watched(s.other)

	s.commandAfterTheEnd(connection, opened.Id, "close")

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestARelayedCloseCommandAheadOfTheEndItFollowedIsTheSessionsEnd() {
	// Catches the end judged on the socket's node: the holder's word of the end may reach it
	// after a close command the caller sent once the session had ended.
	opened, connection, id := s.watched(s.other)

	s.serverClient.stopSession(opened.Id)
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "close"}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestAStopOnTheVoiceSocketAfterTheSessionEndedIsTheSessionsEnd() {
	connection, session, id := s.socketSession()

	s.commandAfterTheEnd(connection, session, "stop")

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

// commandThenTheEnd sends a command other than close, waits for the session's answer to it,
// then stops the session.
func (s *SocketCloseLogSuite) commandThenTheEnd(connection *websocket.Conn, session string) {
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "not-a-command"}))
	s.await(connection, "error")
	s.serverClient.stopSession(session)
	_, _ = s.socketEnd(connection)
}

func (s *SocketCloseLogSuite) TestAnotherCommandBeforeTheSessionEndsIsNotTheCallersClose() {
	// Catches any command taken for a close command.
	opened, connection, id := s.watched(nil)

	s.commandThenTheEnd(connection, opened.Id)

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

func (s *SocketCloseLogSuite) TestAnotherRelayedCommandBeforeTheSessionEndsIsNotTheCallersClose() {
	opened, connection, id := s.watched(s.other)

	s.commandThenTheEnd(connection, opened.Id)

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestAudioOnTheVoiceSocketAfterTheSessionEndedIsTheSessionsEnd() {
	// Catches audio the agent can no longer hear taken for a connection that broke.
	connection, session, id := s.socketSession()

	s.serverClient.stopSession(session)
	s.awaitRouterClose(connection)
	s.Require().NoError(connection.WriteMessage(websocket.BinaryMessage, make([]byte, 640)))
	s.Require().NoError(connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")))

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

func (s *SocketCloseLogSuite) TestACloseCommandIsTheCallersClose() {
	// Catches the session's end, which the command causes before the reader stops, logged in
	// place of the command.
	_, connection, id := s.watched(nil)

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "close"}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestACloseFrameIsLoggedWithTheCallersCode() {
	// Catches the caller's code dropped, or replaced by the one the router answers with.
	_, connection, id := s.watched(nil)

	s.closesWith(connection, 4000)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4000 ")
}

func (s *SocketCloseLogSuite) TestADroppedConnectionIsClientGoneWithNoCode() {
	// Catches gorilla's 1006 for a dropped connection logged as a close frame the caller sent.
	_, connection, id := s.watched(nil)

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}

func (s *SocketCloseLogSuite) TestAnUnreadableFrameIsARefusal() {
	_, connection, id := s.watched(nil)

	s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte("not json")))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1008 ")
}

func (s *SocketCloseLogSuite) TestASessionThatEndsNamesItsEndReason() {
	// Catches a session's end logged without why the session ended, or as the caller's close.
	opened, connection, id := s.watched(nil)

	s.serverClient.stopSession(opened.Id)
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

func (s *SocketCloseLogSuite) TestARelayedSocketLogsWhyItClosedOnTheNodeItLandedOn() {
	// Catches the relayed socket's own reasons left out: its end reason is the holder's to log.
	opened, connection, id := s.watched(s.other)

	s.serverClient.stopSession(opened.Id)
	_, _ = s.socketEnd(connection)

	line := s.closeLine(id)
	s.Contains(line, " close_reason=session_ended close_code=1000 ")
	s.NotContains(line, "end_reason=")
}

func (s *SocketCloseLogSuite) TestARelayedCloseCommandIsTheCallersClose() {
	// Catches the session's end, which the holder reports back, logged in place of the command.
	_, connection, id := s.watched(s.other)

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "close"}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestARelayedCloseCommandSentTwiceIsTheCallersClose() {
	// Catches the second close, which finds the session already gone, clearing the first.
	_, connection, id := s.watched(s.other)

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "close"}))
	_ = connection.WriteJSON(map[string]any{"type": "close"})
	_ = connection.WriteJSON(map[string]any{"type": "close"})
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestARelayedSocketLogsTheCallersClose() {
	_, connection, id := s.watched(s.other)

	s.closesWith(connection, 4001)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4001 ")
}

func (s *SocketCloseLogSuite) TestARelayedSocketLogsAnUnreadableFrameAsARefusal() {
	_, connection, id := s.watched(s.other)

	s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte("[1]")))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1008 ")
}

func (s *SocketCloseLogSuite) TestAStopOnTheVoiceSocketIsTheCallersClose() {
	connection, _, id := s.socketSession()

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "stop"}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=1000 ")
}

func (s *SocketCloseLogSuite) TestACloseFrameOnTheVoiceSocketIsLoggedWithTheCallersCode() {
	connection, _, id := s.socketSession()

	s.closesWith(connection, 4002)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4002 ")
}

func (s *SocketCloseLogSuite) TestADroppedVoiceSocketIsClientGone() {
	connection, _, id := s.socketSession()

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}

func (s *SocketCloseLogSuite) TestAVoiceSessionThatEndsIsNotTheCallersClose() {
	// Catches the caller's answer to the router's close logged as a close of the caller's own.
	connection, session, id := s.socketSession()

	s.serverClient.stopSession(session)
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=session_ended close_code=1000 end_reason=closed ")
}

func (s *SocketCloseLogSuite) TestAVoiceSocketWithNoStartFrameIsARefusal() {
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/agents/socket")

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "hello"}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1008 ")
}

func (s *SocketCloseLogSuite) TestAVoiceSocketClosedBeforeItsStartFrameIsTheCallersClose() {
	// Catches a caller that left before saying what it wanted logged as refused.
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/agents/socket")

	s.closesWith(connection, 4003)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4003 ")
}

func (s *SocketCloseLogSuite) TestAVoiceSocketDroppedBeforeItsStartFrameIsClientGone() {
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/agents/socket")

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}

func (s *SocketCloseLogSuite) TestAVoiceSocketNamingACallIsARefusal() {
	// Catches a refusal that names its failure's code (refuseWith) left unlogged.
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/agents/socket")

	s.Require().NoError(connection.WriteJSON(map[string]any{
		"type": "start", "session": map[string]any{"call": map[string]any{"id": "a-call", "type": "default"}},
	}))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1008 ")
}

// worker opens /v1/dispatch on node (nil for the suite's own) and waits for its ready frame,
// named by a request id of its own.
func (s *SocketCloseLogSuite) worker(node *httptest.Server) (*websocket.Conn, string) {
	id := rand.Text()
	client := s.serverClient.withRequestID(id)
	if node != nil {
		client = client.on(node)
	}
	connection := client.opens("/v1/dispatch")
	s.await(connection, "ready")
	return connection, id
}

func (s *SocketCloseLogSuite) TestADispatchWorkersCloseFrameIsLoggedWithItsCode() {
	// Catches the reader's error dropped (it returned nil on base), which logs every close as
	// client_gone.
	connection, id := s.worker(nil)

	s.closesWith(connection, 4004)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4004 ")
}

func (s *SocketCloseLogSuite) TestADispatchWorkersCloseFrameAfterAPingIsLoggedWithItsCode() {
	// Catches the pong write the reader's answer to the close refuses logged as client_gone:
	// the writer is handed the ping as the close arrives, so most rounds race it.
	for range 5 {
		connection, id := s.worker(nil)

		s.Require().NoError(connection.WriteJSON(frame{"type": "ping", "at": 1}))
		s.closesWith(connection, 4005)

		s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4005 ")
	}
}

func (s *SocketCloseLogSuite) TestADroppedDispatchWorkerIsClientGone() {
	connection, id := s.worker(nil)

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}

func (s *SocketCloseLogSuite) TestAnUnreadableFrameFromADispatchWorkerIsARefusal() {
	connection, id := s.worker(nil)

	s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte("not json")))
	_, _ = s.socketEnd(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1008 ")
}

func (s *SocketCloseLogSuite) TestADispatchWorkerSentElsewhereByADrainIsAShutdown() {
	// Catches the drain's close logged as the worker's own answer to it, or with no code.
	other, router := s.otherRouterSealedBy(dispatch.NewPool(), s.sealer,
		slog.New(NewLogHandler(slog.NewTextHandler(s.logged, nil))))
	connection, id := s.worker(other)

	router.Drain()
	_, _ = s.socketEnd(connection)

	line := s.closeLine(id)
	s.Contains(line, " close_reason=shutdown close_code=1001 ")
	s.NotContains(line, "end_reason=")
}
