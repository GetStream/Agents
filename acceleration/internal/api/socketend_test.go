package api

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// SocketEndSuite is why a socket closed, as its access line names it.
type SocketEndSuite struct{ suite.Suite }

func TestSocketEndSuite(t *testing.T) { suite.Run(t, new(SocketEndSuite)) }

// served is the access line of a request whose handler recorded ended, none recorded when
// ended is nil.
func (s *SocketEndSuite) served(ended *socketEnd) string {
	logged := &bytes.Buffer{}
	routes := chi.NewRouter()
	routes.Get("/v1/agents/socket", func(w http.ResponseWriter, r *http.Request) {
		if ended != nil {
			recordClose(r.Context(), *ended)
		}
	})
	server := &Server{logger: slog.New(slog.NewTextHandler(logged, nil))}
	server.withRequestLog(routes, routes).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/agents/socket", nil))
	return logged.String()
}

func (s *SocketEndSuite) TestTheAccessLineNamesWhyTheSocketClosed() {
	line := s.served(&socketEnd{cause: sessionEnded, code: websocket.CloseNormalClosure, endReason: session.EndAgentHangup})

	s.Contains(line, " close_reason=session_ended close_code=1000 end_reason=agent_hangup")
}

func (s *SocketEndSuite) TestACloseWithNoFrameOrSessionEndLogsNeither() {
	line := s.served(&socketEnd{cause: clientGone})

	s.Contains(line, " close_reason=client_gone")
	s.NotContains(line, "close_code=")
	s.NotContains(line, "end_reason=")
}

func (s *SocketEndSuite) TestARequestThatOpenedNoSocketLogsNoCloseReason() {
	// The control: the line of a request that recorded no close is the line it was on base
	// 4bee273a1, which had no close fields at all.
	line := s.served(nil)

	s.Contains(line, `msg="served a request"`)
	s.False(strings.Contains(line, "close_"), line)
}

func (s *SocketEndSuite) TestACloseFrameIsTheCallersCloseWithItsCode() {
	// Catches a wrapped close (readCommands wraps its read error) left unrecognised.
	s.Equal(socketEnd{cause: closedByClient, code: 4001}, readEnd(stack.Wrap(&websocket.CloseError{Code: 4001})))
	s.Equal(socketEnd{cause: closedByClient, code: websocket.CloseGoingAway},
		readEnd(&websocket.CloseError{Code: websocket.CloseGoingAway}))
}

func (s *SocketEndSuite) TestADroppedConnectionIsClientGoneWithNoCode() {
	// gorilla reports a dropped connection as 1006; no peer sends that code.
	s.Equal(socketEnd{cause: clientGone}, readEnd(&websocket.CloseError{Code: websocket.CloseAbnormalClosure}))
	s.Equal(socketEnd{cause: clientGone}, readEnd(stack.Wrap(io.ErrUnexpectedEOF)))
	s.Equal(socketEnd{cause: clientGone}, readEnd(&net.OpError{Op: "read", Err: errors.New("connection reset by peer")}))
}

func (s *SocketEndSuite) TestAReadPastItsDeadlineIsATimeout() {
	s.Equal(socketEnd{cause: timedOut}, readEnd(stack.Wrap(&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded})))
}

func (s *SocketEndSuite) TestAFramePastTheReadLimitIsRefusedWith1009() {
	s.Equal(socketEnd{cause: refusedByRouter, code: websocket.CloseMessageTooBig}, readEnd(stack.Wrap(websocket.ErrReadLimit)))
}

func (s *SocketEndSuite) TestARouterRestartingIsAShutdownAndAnyOtherEndTheSessions() {
	s.Equal(socketEnd{cause: routerShutdown, code: websocket.CloseServiceRestart}, sessionEnd(websocket.CloseServiceRestart))
	s.Equal(socketEnd{cause: sessionEnded, code: websocket.CloseNormalClosure}, sessionEnd(websocket.CloseNormalClosure))
}
