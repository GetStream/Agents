package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
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

// waitBlockedIn waits until a goroutine is parked in state (e.g. "select" or "sync.Mutex.Lock")
// with frame in its stack trace and none of exclude present.
func (s *PipelineLifecycleSuite) waitBlockedIn(state, frame string, exclude ...string) {
	s.T().Helper()
	buf := make([]byte, 64*1024)
	s.Require().Eventually(func() bool {
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				for _, block := range strings.Split(string(buf[:n]), "\n\n") {
					header, _, _ := strings.Cut(block, "\n")
					if !strings.Contains(header, "["+state) || !strings.Contains(block, frame) {
						continue
					}
					excluded := false
					for _, ex := range exclude {
						if strings.Contains(block, ex) {
							excluded = true
							break
						}
					}
					if !excluded {
						return true
					}
				}
				return false
			}
			buf = make([]byte, len(buf)*2)
		}
	}, 3*time.Second, 2*time.Millisecond, "goroutine never parked in %s [%s]", frame, state)
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

	s.waitBlockedIn("select", "(*Pipeline).waitLeaving")
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

	s.waitBlockedIn("select", "(*Pipeline).ready")
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
	s.waitBlockedIn("sync.Mutex.Lock", "(*Pipeline).AddTools")

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
	s.waitBlockedIn("sync.Mutex.Lock", "(*Pipeline).RemoveTool")

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
	reconnectClosed := make(chan struct{})
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
		defer close(reconnectClosed)
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)
	var releaseReconnectOnce, releaseStopOnce sync.Once
	unblockReconnect := func() { releaseReconnectOnce.Do(func() { close(releaseReconnect) }) }
	unblockStop := func() { releaseStopOnce.Do(func() { close(releaseStop) }) }
	s.T().Cleanup(func() {
		unblockReconnect()
		unblockStop()
	})

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

	// Let the reconnect upgrade succeed while Leave is still blocked in stopOverHTTP;
	// reconnect must close the new socket immediately instead of restoring Watching.
	unblockReconnect()
	select {
	case <-reconnectClosed:
	case <-time.After(3 * time.Second):
		s.FailNow("reconnect socket was not closed during Leave")
	}

	// Pipeline must still refuse commands while Leave is in progress.
	s.Require().ErrorIs(pipeline.Say("should not send", false), ErrNotOnCall)

	unblockStop()
	s.Require().NoError(<-leaveDone)
	s.Nil(pipeline.Session())
}

// TestJoinAbandonsCreatedSessionWhenWatchFailsWhileLeaving proves JoinWith stops the session
// it just created on the router when Watch fails while an earlier Leave is still running.
func (s *PipelineLifecycleSuite) TestJoinAbandonsCreatedSessionWhenWatchFailsWhileLeaving() {
	createEntered := make(chan struct{})
	releaseCreate := make(chan struct{})
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	stopped := make(chan string, 2)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions", func(w http.ResponseWriter, _ *http.Request) {
		close(createEntered)
		<-releaseCreate
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(acceleration.Session{
			Id: "session-joined", CallId: "call-joined", CallType: "default",
			State: "running", CreatedAt: time.Now(),
		})
	})
	mux.HandleFunc("POST /v1/agents/sessions/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		stopped <- r.PathValue("id")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
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
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})

	joinCtx, cancel := context.WithTimeout(s.T().Context(), 200*time.Millisecond)
	defer cancel()
	joinDone := make(chan error, 1)
	go func() {
		_, err := pipeline.Join(joinCtx, Call{Voice: true})
		joinDone <- err
	}()
	<-createEntered

	// While Join's create request is in flight, watch and start leaving another session.
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-other", CallId: "call-other"}))
	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	// Release Join's create so it enters Watch while Leave is holding pipelineLeaving.
	close(releaseCreate)

	s.Require().ErrorIs(<-joinDone, context.DeadlineExceeded)
	select {
	case id := <-stopped:
		s.Equal("session-joined", id)
	case <-time.After(3 * time.Second):
		s.FailNow("router never received stop for the abandoned session")
	}

	close(releaseHangup)
	s.Require().NoError(<-leaveDone)
	s.Nil(pipeline.Session())
}

// TestInFlightAddTools4xxAfterLeaveDoesNotDeleteNewSessionsTool proves a 4xx response to an
// earlier session's AddTools does not remove a tool of the same name on a later session.
func (s *PipelineLifecycleSuite) TestInFlightAddTools4xxAfterLeaveDoesNotDeleteNewSessionsTool() {
	addEntered := make(chan struct{})
	releaseAdd := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions/{id}/tools", func(w http.ResponseWriter, _ *http.Request) {
		close(addEntered)
		<-releaseAdd
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"type":"conflict","code":"conflict","message":"already exists"}}`))
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
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-a", CallId: "call-a"}))
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	regA := s.toolRegistry("shared_tool")
	addErr := make(chan error, 1)
	go func() {
		addErr <- pipeline.AddTools(s.T().Context(), regA)
	}()
	<-addEntered

	// Leave session A and watch session B with its own tool of the same name.
	s.Require().NoError(pipeline.Leave(s.T().Context()))
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-b", CallId: "call-b"}))

	// Session A's in-flight AddTools still holds p.changing, so a real AddTools on session B
	// would serialize behind it. Seed pipeline.added directly to pin the isActiveGen guard on
	// the 4xx rollback path independently of p.changing.
	regB := s.toolRegistry("shared_tool")
	pipeline.mu.Lock()
	pipeline.added = map[string]*tools.Registry{"shared_tool": regB}
	pipeline.mu.Unlock()

	// Let session A's AddTools receive 409 Conflict; it must not delete session B's tool.
	close(releaseAdd)
	var refused *RouterError
	s.Require().ErrorAs(<-addErr, &refused)
	s.Equal(http.StatusConflict, refused.Status)
	s.Same(regB, pipeline.functionsFor("shared_tool"), "session A's 4xx deleted session B's added tool")
}

// TestWatchWhoseDialIsHeldDuringLeaveWaitsAndThenWatches proves a Watch whose socket dial
// finishes while an earlier session is leaving waits for that Leave instead of failing.
func (s *PipelineLifecycleSuite) TestWatchWhoseDialIsHeldDuringLeaveWaitsAndThenWatches() {
	dial2Entered := make(chan struct{})
	releaseDial2 := make(chan struct{})
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})
	secondSaid := make(chan Frame, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "session-2" {
			close(dial2Entered)
			<-releaseDial2
		}
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if id == "session-1" {
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
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	// Start Watch(session-2) while idle, holding its socket upgrade.
	watch2Done := make(chan error, 1)
	go func() {
		watch2Done <- pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-2", CallId: "call-2"})
	}()
	<-dial2Entered

	// Watch session-1 and start Leave on it so the pipeline enters pipelineLeaving.
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))
	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	// Release session-2's dial so Watch enters its state loop while pipelineLeaving is active.
	close(releaseDial2)
	s.waitBlockedIn("select", "(*Pipeline).Watch", "waitLeaving")

	// Finish Leave; Watch(session-2) must wake up and install session-2.
	close(releaseHangup)
	s.Require().NoError(<-leaveDone)
	s.Require().NoError(<-watch2Done)

	got := pipeline.Session()
	s.Require().NotNil(got)
	s.Equal("session-2", got.Id)

	s.Require().NoError(pipeline.Say("ready", false))
	select {
	case frame := <-secondSaid:
		s.Equal("say", frame.Type())
		s.Equal("ready", frame.String("text"))
	case <-time.After(3 * time.Second):
		s.FailNow("session-2 socket never received Say")
	}
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

// TestAddToolsAndRemoveToolSucceedWhileTextSessionIsReconnecting proves that AddTools and
// RemoveTool keep working while a resumable text session's socket is reconnecting.
func (s *PipelineLifecycleSuite) TestAddToolsAndRemoveToolSucceedWhileTextSessionIsReconnecting() {
	firstDropped := make(chan struct{})
	reconnectDialed := make(chan struct{})
	releaseReconnect := make(chan struct{})
	var sockets atomic.Int32
	var addedCount atomic.Int32
	var removedCount atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/sessions/{id}/tools", func(w http.ResponseWriter, _ *http.Request) {
		addedCount.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /v1/agents/sessions/{id}/tools/{name}", func(w http.ResponseWriter, _ *http.Request) {
		removedCount.Add(1)
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
		_ = connection.WriteJSON(Frame{"type": "resumed", "cursor": "c.1", "complete": true})
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReconnect) }) }
	s.T().Cleanup(release)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})
	pipeline.firstRetry = 10 * time.Millisecond
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1"}))
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	<-firstDropped
	<-reconnectDialed

	// While reconnect is in flight, pipeline is in pipelineReconnecting.
	s.Require().NoError(pipeline.AddTools(s.T().Context(), s.toolRegistry("dyn_tool")))
	s.EqualValues(1, addedCount.Load())
	s.True(pipeline.functionsFor("dyn_tool").Holds("dyn_tool"))

	s.Require().NoError(pipeline.RemoveTool(s.T().Context(), "dyn_tool"))
	s.EqualValues(1, removedCount.Load())
	s.False(pipeline.functionsFor("dyn_tool").Holds("dyn_tool"))

	release()
	event := next(s.T(), pipeline.Events())
	s.Equal("resumed", event.Kind)
}

// TestRunToolDropsResultWhenLeaveStartsWhileToolRuns proves runTool does not send a tool_result
// frame over a still-open socket once Leave has transitioned the pipeline to pipelineLeaving.
func (s *PipelineLifecycleSuite) TestRunToolDropsResultWhenLeaveStartsWhileToolRuns() {
	toolStarted := make(chan struct{})
	releaseTool := make(chan struct{})
	closeReceived := make(chan struct{})
	releaseHangup := make(chan struct{})
	serverDone := make(chan struct{})
	toolResultReceived := make(chan Frame, 1)

	reg := tools.NewRegistry()
	err := tools.Register(reg, "slow_tool", "Blocks until released", func(context.Context, struct{}) (any, error) {
		close(toolStarted)
		<-releaseTool
		return "done", nil
	})
	s.Require().NoError(err)

	backend := newRouter(s.T(), func(_ *testing.T, connection *websocket.Conn) {
		defer close(serverDone)
		_ = connection.WriteJSON(Frame{
			"type":      "tool_call",
			"id":        "tc-1",
			"name":      "slow_tool",
			"arguments": "{}",
		})
		for {
			var frame Frame
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			switch frame.Type() {
			case "close":
				close(closeReceived)
				// Keep the socket open until releaseHangup, then send a normal close frame so
				// ReadJSON drains any frame written by the client before EOF.
				go func() {
					<-releaseHangup
					_ = connection.WriteControl(
						websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, "ended"),
						time.Now().Add(time.Second),
					)
				}()
			case "tool_result":
				toolResultReceived <- frame
			}
		}
	})

	pipeline := Accelerated(Config{
		Backend:   Backend{URL: backend.URL, CustomerID: "acme"},
		Functions: reg,
	})
	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))

	select {
	case <-toolStarted:
	case <-time.After(3 * time.Second):
		s.FailNow("slow_tool never started")
	}

	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()

	select {
	case <-closeReceived:
	case <-time.After(3 * time.Second):
		s.FailNow("Leave never sent close")
	}

	// Finish slow_tool and wait for runTool to return while the socket is still open and Leave
	// is waiting in awaitHangup.
	close(releaseTool)
	pipeline.running.Wait()

	close(releaseHangup)
	s.Require().NoError(<-leaveDone)
	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		s.FailNow("server handler never finished reading socket")
	}

	select {
	case frame := <-toolResultReceived:
		s.FailNowf("runTool sent tool_result after Leave started", "frame=%v", frame)
	default:
	}
}

// TestWatchWhoseDialIsHeldDuringLeaveRespectsContextCancellation proves a Watch whose dial
// finishes while an earlier session is leaving closes its socket and returns ctx.Err() if its
// context cancels while waiting for Leave to finish.
func (s *PipelineLifecycleSuite) TestWatchWhoseDialIsHeldDuringLeaveRespectsContextCancellation() {
	dial2Entered := make(chan struct{})
	releaseDial2 := make(chan struct{})
	dial2Closed := make(chan struct{})
	firstClosed := make(chan struct{})
	releaseHangup := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/agents/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "session-2" {
			close(dial2Entered)
			<-releaseDial2
		}
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if id == "session-1" {
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
		defer close(dial2Closed)
		hold(s.T(), connection)
	})
	server := httptest.NewServer(mux)
	s.T().Cleanup(server.Close)

	pipeline := Accelerated(Config{Backend: Backend{URL: server.URL, CustomerID: "acme"}})
	s.T().Cleanup(func() { _ = pipeline.Leave(context.Background()) })

	watchCtx, cancelWatch := context.WithCancel(s.T().Context())
	defer cancelWatch()
	watch2Done := make(chan error, 1)
	go func() {
		watch2Done <- pipeline.Watch(watchCtx, &acceleration.Session{Id: "session-2", CallId: "call-2"})
	}()
	<-dial2Entered

	s.Require().NoError(pipeline.Watch(s.T().Context(), &acceleration.Session{Id: "session-1", CallId: "call-1"}))
	leaveDone := make(chan error, 1)
	go func() {
		leaveDone <- pipeline.Leave(s.T().Context())
	}()
	<-firstClosed

	close(releaseDial2)
	s.waitBlockedIn("select", "(*Pipeline).Watch", "waitLeaving")

	cancelWatch()
	s.Require().ErrorIs(<-watch2Done, context.Canceled)
	select {
	case <-dial2Closed:
	case <-time.After(3 * time.Second):
		s.FailNow("cancelled Watch did not close its dialled socket")
	}

	close(releaseHangup)
	s.Require().NoError(<-leaveDone)
	s.Nil(pipeline.Session())
}
