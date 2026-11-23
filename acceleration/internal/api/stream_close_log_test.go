//go:build integration

package api

import (
	"crypto/rand"
	"fmt"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// The access line of a /v1/{modality}/stream socket says why it closed, with the values a
// session socket uses (SocketCloseLogSuite). These sockets hold no session, so none names an
// end_reason.

// closeLine is the access line of the socket opened with request id once it has closed.
func (s *StreamsSuite) closeLine(id string) string {
	line := s.closeLineIn(s.logged, id)
	s.NotContains(line, "end_reason=", "a stream socket holds no session")
	return line
}

// started opens a socket on path that the router started a model for.
func (s *StreamsSuite) started(path string, opening frame) (*websocket.Conn, string) {
	connection, id := s.startNamed(path, opening)
	s.Require().Equal("started", s.nextConversationFrame(connection)["type"])
	return connection, id
}

func (s *StreamsSuite) TestEachStreamSocketLogsTheCallersCloseWithItsCode() {
	// Catches a read loop that left no reason when the caller closed: each modality reads in
	// a loop of its own.
	for code, opened := range map[int]struct {
		path    string
		opening frame
	}{
		4011: {"/v1/stt/stream", frame{"target": "en-low-latency"}},
		4012: {"/v1/tts/stream", frame{"target": "en-low-latency"}},
		4013: {"/v1/llm/stream", frame{"target": "echo/echo-model"}},
		4014: {"/v1/sts/stream", frame{"target": "stub/stub-sts"}},
	} {
		s.Run(opened.path, func() {
			connection, id := s.started(opened.path, opened.opening)

			s.Require().NoError(connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, "")))
			_, _ = s.ending(connection)

			s.Contains(s.closeLine(id), fmt.Sprintf(" close_reason=client_closed close_code=%d ", code))
		})
	}
}

func (s *StreamsSuite) TestADroppedStreamSocketIsClientGone() {
	connection, id := s.started("/v1/llm/stream", frame{"target": "echo/echo-model"})

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}

func (s *StreamsSuite) TestAFrameAStartedStreamSocketCannotReadIsARefusal() {
	// Catches it logged as a dropped connection. The router closes it with 1000, as on base
	// (TestASocketTheRouterEndsWithoutAFailureClosesNormally).
	connection, id := s.started("/v1/llm/stream", frame{"target": "echo/echo-model"})

	s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte("not json")))
	_, _ = s.ending(connection)

	s.Contains(s.closeLine(id), " close_reason=refused close_code=1000 ")
}

func (s *StreamsSuite) TestAStartFrameWithBytesAfterItsJSONStarts() {
	// Catches a read stricter than base's ReadJSON, which decodes the frame's first JSON value
	// and leaves what follows it.
	connection := s.serverClient.opens("/v1/llm/stream")

	s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte(`{"type":"start","target":"echo/echo-model"} trailing`)))

	s.Equal("started", s.nextConversationFrame(connection)["type"])
}

func (s *StreamsSuite) TestAStartTheCallerMustChangeIsARefusal() {
	// Catches each refusal of the caller's start logged as the router's failure. The close
	// codes are the ones the caller is sent on base: 1011 for a plain failure, 1008 for an
	// unknown target or a setting the model cannot take
	// (TestAStartPinnedToABatchModelEndsAsAPolicyViolation).
	keyterms := make([]string, stt.MaxKeyterms+1)
	for i := range keyterms {
		keyterms[i] = fmt.Sprintf("term %d", i)
	}
	for name, refused := range map[string]struct {
		path    string
		opening frame
		code    int
	}{
		"no target":         {"/v1/stt/stream", frame{"tts": frame{"target": "en-low-latency"}}, 1011},
		"an unknown config": {"/v1/stt/stream", frame{"config_id": "no such config " + s.utils.uuid()}, 1011},
		"a bad tag":         {"/v1/stt/stream", frame{"target": "en-low-latency", "tags": frame{"Not A Key!": "x"}}, 1011},
		"an stt option":     {"/v1/stt/stream", frame{"target": "en-low-latency", "stt": frame{"mode": "whichever"}}, 1011},
		"too many keyterms": {"/v1/stt/stream", frame{"target": "stub/stub-model", "stt": frame{"keyterms": keyterms}}, 1011},
		"a tts option":      {"/v1/tts/stream", frame{"target": "en-low-latency", "tts": frame{"overwrites": frame{"": frame{}}}}, 1011},
		"an sts option":     {"/v1/sts/stream", frame{"target": "stub/stub-sts", "sts": frame{"turn_detection": "whichever"}}, 1011},
		"a batch model":     {"/v1/stt/stream", frame{"target": "batch/" + batchModel}, 1008},
		"an unknown target": {"/v1/stt/stream", frame{"target": "nobody/nothing"}, 1008},
	} {
		s.Run(name, func() {
			connection, id := s.startNamed(refused.path, refused.opening)
			_, _ = s.ending(connection)

			s.Contains(s.closeLine(id), fmt.Sprintf(" close_reason=refused close_code=%d ", refused.code))
		})
	}
}

func (s *StreamsSuite) TestAFirstFrameThatIsNoStartFrameIsARefusal() {
	// Catches a frame the router could not read logged as a dropped connection.
	for name, first := range map[string]string{
		"not JSON":         "not json",
		"another type":     `{"type": "hello", "target": "en-low-latency"}`,
		"JSON of no shape": "[1]",
	} {
		s.Run(name, func() {
			id := rand.Text()
			connection := s.serverClient.withRequestID(id).opens("/v1/stt/stream")

			s.Require().NoError(connection.WriteMessage(websocket.TextMessage, []byte(first)))
			_, _ = s.ending(connection)

			s.Contains(s.closeLine(id), " close_reason=refused close_code=1011 ")
		})
	}
}

func (s *StreamsSuite) TestAProviderThatFailsToStartIsAServerError() {
	// Catches a provider's failure logged as the caller's refusal.
	connection, id := s.startNamed("/v1/stt/stream", frame{"target": "down/" + downModel})
	_, _ = s.ending(connection)

	s.Contains(s.closeLine(id), " close_reason=server_error close_code=1011 ")
}

func (s *StreamsSuite) TestAConfigThatCannotBeReadIsAServerError() {
	// Catches the internal error the router answers a failed read with taken for a refusal:
	// both are an APIError.
	config := s.utils.uuid()
	s.failQueries(config)
	connection, id := s.startNamed("/v1/stt/stream", frame{"config_id": config})
	_, _ = s.ending(connection)

	s.Contains(s.closeLine(id), " close_reason=server_error close_code=1011 ")
}

func (s *StreamsSuite) TestAStreamSocketClosedBeforeItsStartFrameIsTheCallersClose() {
	// Catches a caller that left before saying what it wanted logged as refused.
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/stt/stream")

	s.Require().NoError(connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4015, "")))
	_, _ = s.ending(connection)

	s.Contains(s.closeLine(id), " close_reason=client_closed close_code=4015 ")
}

func (s *StreamsSuite) TestAStreamSocketDroppedBeforeItsStartFrameIsClientGone() {
	id := rand.Text()
	connection := s.serverClient.withRequestID(id).opens("/v1/stt/stream")

	s.Require().NoError(connection.UnderlyingConn().Close())

	line := s.closeLine(id)
	s.Contains(line, " close_reason=client_gone ")
	s.NotContains(line, "close_code=")
}
