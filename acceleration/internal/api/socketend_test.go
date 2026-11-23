package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
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

// brokenSocket is the router's side of a real socket whose connection is closed under it in
// both directions, so the next write to it fails.
func (s *SocketEndSuite) brokenSocket() *websocket.Conn {
	routerSide, client := s.connected()
	s.breaks(routerSide, client)
	return routerSide
}

// connected is the router's side of a real socket and the caller's.
func (s *SocketEndSuite) connected() (*websocket.Conn, *websocket.Conn) {
	upgraded := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		s.Require().NoError(err)
		upgraded <- connection
		<-r.Context().Done()
	}))
	s.T().Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	s.Require().NoError(err)
	return <-upgraded, client
}

// breaks closes a socket's connection under both its sides.
func (s *SocketEndSuite) breaks(routerSide, client *websocket.Conn) {
	s.Require().NoError(client.UnderlyingConn().Close())
	s.Require().NoError(routerSide.UnderlyingConn().Close())
}

func (s *SocketEndSuite) TestAnEventTheCallerCannotBeSentIsClientGone() {
	// Catches a failed write left with no reason.
	events := make(chan session.Event, 1)
	events <- agent.Said{TurnID: "a-turn", Text: "hello"}
	server := &Server{logger: slog.New(slog.DiscardHandler)}

	ended := server.writeEvents(s.brokenSocket(), events, wanted{}, make(chan error), endingOf(resuming{}, func() bool { return false }))

	s.Equal(socketEnd{cause: clientGone}, ended)
}

func (s *SocketEndSuite) TestARelayedFrameTheCallerCannotBeSentIsClientGone() {
	socket := &remoteSocket{frames: make(chan json.RawMessage, 1), closed: make(chan struct{})}
	socket.frames <- json.RawMessage(`{"type":"said"}`)
	server := &Server{logger: slog.New(slog.DiscardHandler)}

	ended := server.writeRelayedEvents(s.brokenSocket(), nil, socket, "a-watcher", "a-session", wanted{}, make(chan error),
		endingOf(resuming{}, func() bool { return false }))

	s.Equal(socketEnd{cause: clientGone}, ended)
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

func (s *SocketEndSuite) TestADispatchWorkerThatCannotBeSentItsReadyFrameIsClientGone() {
	pool := dispatch.NewPool()
	worker, release := pool.Register("a-customer", dispatch.Registration{Capacity: 1})
	defer release()
	server := &Server{logger: slog.New(slog.DiscardHandler), draining: make(chan struct{})}

	ended := server.writeCalls(s.brokenSocket(), worker, make(chan error), nil, nil, func() {})

	s.Equal(socketEnd{cause: clientGone}, ended)
}

func (s *SocketEndSuite) TestWorkADispatchWorkerCannotBeSentIsClientGone() {
	// Catches a failed write to a worker left with no reason, for each kind of frame it is sent.
	hosted := []dispatch.Tool{{Name: "a-tool", Description: "does a thing"}}
	for name, send := range map[string]func(*dispatch.Pool, *dispatch.Worker, chan<- float64, chan<- frame){
		"pong": func(_ *dispatch.Pool, _ *dispatch.Worker, pongs chan<- float64, _ chan<- frame) { pongs <- 1 },
		"reply": func(_ *dispatch.Pool, _ *dispatch.Worker, _ chan<- float64, replies chan<- frame) {
			replies <- frame{"type": "hosting"}
		},
		"call": func(pool *dispatch.Pool, _ *dispatch.Worker, _ chan<- float64, _ chan<- frame) {
			_, err := pool.Assign("a-customer", dispatch.Call{CallID: "a-call"})
			s.Require().NoError(err)
		},
		"message": func(pool *dispatch.Pool, _ *dispatch.Worker, _ chan<- float64, _ chan<- frame) {
			_, err := pool.AssignMessage("a-customer", dispatch.Message{ChannelID: "a-channel"})
			s.Require().NoError(err)
		},
		"tool call": func(pool *dispatch.Pool, worker *dispatch.Worker, _ chan<- float64, _ chan<- frame) {
			s.Require().NoError(pool.Host(worker, "an-agent", hosted, time.Second))
			go func() {
				_, _ = pool.RunHosted(s.T().Context(), "a-customer", "an-agent", dispatch.ToolCall{ID: "a-call", Name: "a-tool"})
			}()
		},
	} {
		s.Run(name, func() {
			pool := dispatch.NewPool()
			worker, release := pool.Register("a-customer", dispatch.Registration{Capacity: 1})
			defer release()
			server := &Server{logger: slog.New(slog.DiscardHandler), draining: make(chan struct{})}
			routerSide, client := s.connected()
			pongs, replies := make(chan float64), make(chan frame)
			ended := make(chan socketEnd, 1)
			go func() { ended <- server.writeCalls(routerSide, worker, make(chan error), pongs, replies, func() {}) }()
			var ready frame
			s.Require().NoError(client.ReadJSON(&ready))
			s.Require().Equal("ready", ready["type"])
			s.breaks(routerSide, client)

			send(pool, worker, pongs, replies)

			select {
			case got := <-ended:
				s.Equal(socketEnd{cause: clientGone}, got)
			case <-time.After(5 * time.Second):
				s.Fail("the writer did not stop on a failed write")
			}
		})
	}
}
