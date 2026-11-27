package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/sdks/go/acceleration"
	"github.com/GetStream/Vision-Agents/sdks/go/tools"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/suite"
)

// PipelineLifecycleSuite proves the pipeline's watch, leave and tool-change transitions stay
// consistent when called concurrently.
type PipelineLifecycleSuite struct {
	suite.Suite
}

func TestPipelineLifecycleSuite(t *testing.T) {
	suite.Run(t, new(PipelineLifecycleSuite))
}

func (s *PipelineLifecycleSuite) toolRegistry(name string) *tools.Registry {
	s.T().Helper()
	reg := tools.NewRegistry()
	err := tools.Register(reg, name, "Test tool "+name, func(context.Context, struct{}) (any, error) {
		return "ok", nil
	})
	s.Require().NoError(err)
	return reg
}

// TestWatchDuringLeaveDoesNotKillTheNewSession proves that calling Watch while Leave is waiting
// for the previous session's hangup does not let Leave tear down the new session's socket.
func (s *PipelineLifecycleSuite) TestWatchDuringLeaveDoesNotKillTheNewSession() {
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	secondSaid := make(chan Frame, 1)
	var sockets atomic.Int32

	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		n := sockets.Add(1)
		if n == 1 {
			for {
				var frame Frame
				if err := connection.ReadJSON(&frame); err != nil {
					return
				}
				if frame.Type() == "close" {
					close(firstClosed)
					<-releaseHangup
					return
				}
			}
		}
		_ = connection.WriteJSON(Frame{"type": "joined"})
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if frame.Type() == "close" {
				return
			}
			secondSaid <- frame
		}
	})

	pipeline := Accelerated(Config{Backend: Backend{URL: backend.URL, CustomerID: "acme"}})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()

	select {
	case <-firstClosed:
	case <-time.After(3 * time.Second):
		s.FailNow("Leave never sent close on the first socket")
	}

	watchDone := make(chan error, 1)
	go func() {
		watchDone <- pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-2", CallId: "call-2"})
	}()

	// Give Watch a moment to race against Leave while Leave is blocked in awaitHangup.
	time.Sleep(50 * time.Millisecond)
	close(releaseHangup)

	s.Require().NoError(<-leaveDone)
	s.Require().NoError(<-watchDone)

	got := pipeline.Session()
	s.Require().NotNil(got, "new session was cleared by the previous Leave")
	s.Equal("session-2", got.Id)

	event := next(s.T(), pipeline.Events())
	s.Equal("joined", event.Kind)

	s.Require().NoError(pipeline.Say("still alive", false), "new session socket was closed by the previous Leave")
	select {
	case frame := <-secondSaid:
		s.Equal("say", frame.Type())
		s.Equal("still alive", frame.String("text"))
	case <-time.After(3 * time.Second):
		s.FailNow("new session socket never received Say")
	}
}

// TestJoinDuringHTTPStopDoesNotKillTheNewSession proves that calling Join while Leave is
// stopping the previous session over HTTP does not let Leave close the new session's socket.
func (s *PipelineLifecycleSuite) TestJoinDuringHTTPStopDoesNotKillTheNewSession() {
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	secondSaid := make(chan Frame, 1)
	var sessionSeq atomic.Int32
	var socketSeq atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions", func(w http.ResponseWriter, _ *http.Request) {
		n := sessionSeq.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(acceleration.Session{
			Id: fmt.Sprintf("session-%d", n), CallId: fmt.Sprintf("call-%d", n),
			CallType: "default", State: "running", CreatedAt: time.Now(),
		})
	})
	mux.HandleFunc("POST /v1/agents/sessions/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "session-1" {
			close(stopEntered)
			<-releaseStop
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if socketSeq.Add(1) == 1 {
			_ = connection.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "ended"))
			return
		}
		_ = connection.WriteJSON(Frame{"type": "joined"})
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil || frame.Type() == "close" {
				return
			}
			secondSaid <- frame
		}
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})
	_, err := pipeline.Join(s.T().Context(), Call{Voice: true})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	// Wait for the first socket to close so Leave takes the stopOverHTTP path.
	for range pipeline.Events() {
	}

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()

	select {
	case <-stopEntered:
	case <-time.After(3 * time.Second):
		s.FailNow("Leave never called stopOverHTTP")
	}

	joinDone := make(chan error, 1)
	go func() {
		_, err := pipeline.Join(s.T().Context(), Call{Voice: true})
		joinDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseStop)

	s.Require().NoError(<-leaveDone)
	s.Require().NoError(<-joinDone)

	got := pipeline.Session()
	s.Require().NotNil(got)
	s.Equal("session-2", got.Id)

	event := next(s.T(), pipeline.Events())
	s.Equal("joined", event.Kind)

	s.Require().NoError(pipeline.Say("hello again", false))
	select {
	case frame := <-secondSaid:
		s.Equal("say", frame.Type())
		s.Equal("hello again", frame.String("text"))
	case <-time.After(3 * time.Second):
		s.FailNow("new session socket never received Say")
	}
}

// TestWatchRespectsContextCancellationWhileLeaving proves Watch stops waiting for an in-flight
// Leave when its own context expires, without dialling a second socket.
func (s *PipelineLifecycleSuite) TestWatchRespectsContextCancellationWhileLeaving() {
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	var sockets atomic.Int32
	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		sockets.Add(1)
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if frame.Type() == "close" {
				close(firstClosed)
				<-releaseHangup
				return
			}
		}
	})

	pipeline := Accelerated(Config{Backend: Backend{URL: backend.URL, CustomerID: "acme"}})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	// Commands while leaving are refused rather than sent after the close frame.
	s.Require().ErrorIs(pipeline.Say("too late", false), ErrNotOnCall)

	ctx, cancel := context.WithTimeout(s.T().Context(), 50*time.Millisecond)
	defer cancel()
	err := pipeline.Watch(ctx, &acceleration.Session{Id: "session-2", CallId: "call-2"})
	s.Require().ErrorIs(err, context.DeadlineExceeded)
	s.EqualValues(1, sockets.Load(), "Watch dialled a second socket while Leave was still in progress")

	close(releaseHangup)
	s.Require().NoError(<-leaveDone)
	s.Nil(pipeline.Session())
}

// TestJoinRespectsContextCancellationWhileLeaving proves Join stops waiting for an in-flight
// Leave when its own context expires, without creating a session on the router.
func (s *PipelineLifecycleSuite) TestJoinRespectsContextCancellationWhileLeaving() {
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if frame.Type() == "close" {
				close(firstClosed)
				<-releaseHangup
				return
			}
		}
	})

	pipeline := Accelerated(Config{Backend: Backend{URL: backend.URL, CustomerID: "acme"}})
	_, err := pipeline.Join(s.T().Context(), Call{Voice: true})
	s.Require().NoError(err)

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	ctx, cancel := context.WithTimeout(s.T().Context(), 50*time.Millisecond)
	defer cancel()
	_, err = pipeline.Join(ctx, Call{Voice: true})
	s.Require().ErrorIs(err, context.DeadlineExceeded)

	close(releaseHangup)
	s.Require().NoError(<-leaveDone)

	backend.mu.Lock()
	createdCount := len(backend.requests)
	backend.mu.Unlock()
	s.Equal(1, createdCount, "Join created a session while the previous Leave was still running")
}

// TestConcurrentLeaveWaitsForTheFirstLeaveToFinish proves a second concurrent Leave blocks
// until the first Leave finishes tearing down the session (or its own context cancels).
func (s *PipelineLifecycleSuite) TestConcurrentLeaveWaitsForTheFirstLeaveToFinish() {
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if frame.Type() == "close" {
				close(firstClosed)
				<-releaseHangup
				return
			}
		}
	})

	pipeline := Accelerated(Config{Backend: Backend{URL: backend.URL, CustomerID: "acme"}})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))

	firstLeave := make(chan error, 1)
	go func() {
		firstLeave <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	// A concurrent Leave with a short timeout must return DeadlineExceeded rather than nil
	// while the first Leave is still tearing down.
	shortCtx, cancel := context.WithTimeout(s.T().Context(), 40*time.Millisecond)
	defer cancel()
	s.Require().ErrorIs(pipeline.Leave(shortCtx), context.DeadlineExceeded)

	secondLeave := make(chan error, 1)
	go func() {
		secondLeave <- pipeline.Leave(s.T().Context())
	}()

	select {
	case err := <-secondLeave:
		s.FailNowf("second Leave returned before the first finished", "err=%v", err)
	case <-time.After(40 * time.Millisecond):
	}

	close(releaseHangup)
	s.Require().NoError(<-firstLeave)
	s.Require().NoError(<-secondLeave)
}

// TestAddToolsWaitingOnChangingAndInFlightDoesNotPolluteAfterLeave proves that an AddTools
// queued behind another tool change, and an AddTools in flight when Leave runs, neither send
// stale requests nor leave stale added/unsettled state on the pipeline.
func (s *PipelineLifecycleSuite) TestAddToolsWaitingOnChangingAndInFlightDoesNotPolluteAfterLeave() {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var addedTools []string

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions/{id}/tools", func(w http.ResponseWriter, r *http.Request) {
		var req acceleration.AddSessionToolsRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		var names []string
		if req.Tools != nil {
			for _, tool := range *req.Tools {
				names = append(names, tool.Name)
			}
		}
		mu.Lock()
		addedTools = append(addedTools, names...)
		mu.Unlock()
		if len(names) == 1 && names[0] == "tool_a" {
			close(firstEntered)
			<-releaseFirst
			// Fail with 502 so the in-flight AddTools would call unsettle without the fix.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":{"type":"unavailable","code":"bad_gateway","message":"gateway"}}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- pipeline.AddTools(s.T().Context(), s.toolRegistry("tool_a"))
	}()

	select {
	case <-firstEntered:
	case <-time.After(3 * time.Second):
		s.FailNow("first AddTools never reached the router")
	}

	// Queue a second AddTools behind p.changing while session-1 is still active.
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- pipeline.AddTools(s.T().Context(), s.toolRegistry("tool_b"))
	}()
	time.Sleep(40 * time.Millisecond)

	// Leave while tool_a is in flight and tool_b is blocked on p.changing.
	s.Require().NoError(pipeline.Leave(s.T().Context()))

	close(releaseFirst)
	s.Require().Error(<-firstErr)
	s.Require().ErrorIs(<-secondErr, ErrNotOnCall)

	s.False(pipeline.functionsFor("tool_a").Holds("tool_a"), "tool_a leaked into pipeline after Leave")
	s.False(pipeline.isUnsettled("tool_a"), "tool_a remained unsettled after Leave")
	s.False(pipeline.functionsFor("tool_b").Holds("tool_b"), "tool_b leaked into pipeline after Leave")
	s.False(pipeline.isUnsettled("tool_b"), "tool_b remained unsettled after Leave")

	mu.Lock()
	gotAdded := append([]string(nil), addedTools...)
	mu.Unlock()
	s.Equal([]string{"tool_a"}, gotAdded, "tool_b was sent to the router for a left session")
}

// TestRemoveToolWaitingOnChangingAndInFlightDoesNotPolluteAfterLeave proves that a RemoveTool
// queued behind another tool change, and a RemoveTool in flight when Leave runs, do not pollute
// withdrawn or unsettled state after Leave.
func (s *PipelineLifecycleSuite) TestRemoveToolWaitingOnChangingAndInFlightDoesNotPolluteAfterLeave() {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var removedTools []string

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /v1/agents/sessions/{id}/tools/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		mu.Lock()
		removedTools = append(removedTools, name)
		mu.Unlock()
		if name == "base_tool" {
			close(firstEntered)
			<-releaseFirst
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{
		Backend:   Backend{URL: server.URL, CustomerID: "acme"},
		Functions: s.toolRegistry("base_tool"),
	})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- pipeline.RemoveTool(s.T().Context(), "base_tool")
	}()
	<-firstEntered

	secondErr := make(chan error, 1)
	go func() {
		secondErr <- pipeline.RemoveTool(s.T().Context(), "other_tool")
	}()
	time.Sleep(40 * time.Millisecond)

	s.Require().NoError(pipeline.Leave(s.T().Context()))

	close(releaseFirst)
	s.Require().NoError(<-firstErr)
	s.Require().ErrorIs(<-secondErr, ErrNotOnCall)

	s.True(pipeline.functionsFor("base_tool").Holds("base_tool"), "base_tool remained withdrawn after Leave")
	s.False(pipeline.isUnsettled("base_tool"), "base_tool remained unsettled after Leave")

	mu.Lock()
	gotRemoved := append([]string(nil), removedTools...)
	mu.Unlock()
	s.Equal([]string{"base_tool"}, gotRemoved, "other_tool was sent to the router for a left session")
}

// TestTextSessionDoesNotReconnectWhenSocketDropsDuringLeave proves a resumable text session
// whose socket drops abnormally after Leave sent close does not start reconnecting.
func (s *PipelineLifecycleSuite) TestTextSessionDoesNotReconnectWhenSocketDropsDuringLeave() {
	var sockets atomic.Int32
	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		sockets.Add(1)
		_ = connection.WriteJSON(Frame{"type": "resumed", "cursor": "c.0", "complete": true})
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if frame.Type() == "close" {
				// Drop abnormally without CloseNormalClosure.
				_ = connection.Close()
				return
			}
		}
	})

	pipeline := Accelerated(Config{Backend: Backend{URL: backend.URL, CustomerID: "acme"}})
	pipeline.firstRetry = 10 * time.Millisecond
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1"}))

	s.Require().NoError(pipeline.Leave(s.T().Context()))
	s.EqualValues(1, sockets.Load(), "text session reconnected after Leave sent close")
}

// TestReconnectFinishingDuringLeaveDoesNotRestoreWatchingState proves a reconnect dial that
// completes while Leave is blocked in stopOverHTTP closes the socket instead of restoring the
// pipeline to Watching.
func (s *PipelineLifecycleSuite) TestReconnectFinishingDuringLeaveDoesNotRestoreWatchingState() {
	firstDropped := make(chan struct{})
	reconnectDialed := make(chan struct{})
	releaseReconnect := make(chan struct{})
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	var sockets atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions/{id}/stop", func(w http.ResponseWriter, _ *http.Request) {
		close(stopEntered)
		<-releaseStop
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		n := sockets.Add(1)
		if n == 2 {
			close(reconnectDialed)
			<-releaseReconnect
		}
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if n == 1 {
			_ = connection.WriteJSON(Frame{"type": "resumed", "cursor": "c.0", "complete": true})
			closes(connection, websocket.CloseServiceRestart, "restarting")
			close(firstDropped)
			return
		}
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})
	pipeline.firstRetry = 10 * time.Millisecond
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1"}))

	<-firstDropped
	<-reconnectDialed

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-stopEntered

	// Let the reconnect upgrade succeed while Leave is still blocked in stopOverHTTP.
	close(releaseReconnect)
	time.Sleep(40 * time.Millisecond)

	// Pipeline must still refuse commands while Leave is in progress.
	s.Require().ErrorIs(pipeline.Say("should not send", false), ErrNotOnCall)

	close(releaseStop)
	s.Require().NoError(<-leaveDone)
	s.Nil(pipeline.Session())
}

// TestConcurrentLifecycleAndToolChangesUnderRace exercises concurrent Watch, Leave, AddTools,
// RemoveTool and Say calls under the race detector and checks the pipeline settles cleanly.
func (s *PipelineLifecycleSuite) TestConcurrentLifecycleAndToolChangesUnderRace() {
	backend := newRouter(s.T(), hold)
	pipeline := Accelerated(Config{
		Backend:   Backend{URL: backend.URL, CustomerID: "acme"},
		Functions: s.toolRegistry("base_tool"),
	})

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Second)
	defer cancel()

	for i := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			toolName := fmt.Sprintf("dyn_%d", id)
			reg := s.toolRegistry(toolName)
			for step := range 10 {
				session := &acceleration.Session{
					Id:     fmt.Sprintf("session-%d-%d", id, step),
					CallId: fmt.Sprintf("call-%d-%d", id, step),
				}
				_ = pipeline.Watch(ctx, session)
				_ = pipeline.AddTools(ctx, reg)
				_ = pipeline.Say("ping", false)
				_ = pipeline.RemoveTool(ctx, toolName)
				_ = pipeline.Leave(ctx)
			}
		}(i)
	}
	wg.Wait()

	s.Require().NoError(pipeline.Leave(s.T().Context()))
	s.Nil(pipeline.Session())
	s.True(pipeline.functionsFor("base_tool").Holds("base_tool"))
	for i := range 4 {
		name := fmt.Sprintf("dyn_%d", i)
		s.False(pipeline.functionsFor(name).Holds(name))
		s.False(pipeline.isUnsettled(name))
	}
}
