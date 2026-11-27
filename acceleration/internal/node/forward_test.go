package node

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ForwardSuite covers who a node lets a Forward in from: only a peer presenting a
// credential the node accepts reaches its handler. It also covers how a Forwarder keeps its
// connections to its peers.
type ForwardSuite struct {
	suite.Suite
	ctx context.Context
	// answered counts the requests that reached the node's own handler, and forwarded
	// those of them it was told came from a peer.
	answered  atomic.Int32
	forwarded atomic.Int32
	// handed counts those of them handed to the node's shard (HandedToShard), and marked those
	// still carrying the header that says so.
	handed atomic.Int32
	marked atomic.Int32
}

func TestForwardSuite(t *testing.T) {
	suite.Run(t, new(ForwardSuite))
}

func (s *ForwardSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	s.T().Cleanup(cancel)
	s.ctx = ctx
	s.answered.Store(0)
	s.forwarded.Store(0)
	s.handed.Store(0)
	s.marked.Store(0)
}

// node serves a node that accepts the given credentials, and returns where it listens.
func (s *ForwardSuite) node(accepted ...[]byte) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	return s.nodeOn(listener, accepted...)
}

// nodeOn serves a node that accepts the given credentials on listener, and returns where it listens.
func (s *ForwardSuite) nodeOn(listener net.Listener, accepted ...[]byte) string {
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.answered.Add(1)
		if Forwarded(r.Context()) {
			s.forwarded.Add(1)
		}
		if HandedToShard(r.Context()) {
			s.handed.Add(1)
		}
		if r.Header.Get(shardHeader) != "" {
			s.marked.Add(1)
		}
		w.WriteHeader(http.StatusTeapot)
	})
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: Serve(api, accepted)}}
	server.Start()
	s.T().Cleanup(server.Close)
	return server.Listener.Addr().String()
}

// ask forwards one request to the node at address, presenting credential.
func (s *ForwardSuite) ask(address string, credential []byte) (*ForwardResponse, error) {
	request, err := http.NewRequest(http.MethodPost, "/internal/scheduled-tasks/deliver", nil)
	s.Require().NoError(err)
	return NewForwarder(nil, credential).Ask(s.ctx, address, request, nil)
}

// refused checks that a call was turned away before the node's handler saw it.
func (s *ForwardSuite) refused(err error) {
	s.Equal(codes.Unauthenticated, status.Code(err), "error: %v", err)
	s.Zero(s.answered.Load(), "the node's handler ran")
}

// call invokes a method the Node service does not have, with metadata of its own.
func (s *ForwardSuite) call(address, method string, md metadata.MD) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })
	return conn.Invoke(metadata.NewOutgoingContext(s.ctx, md), method, &ForwardRequest{}, &ForwardResponse{})
}

func (s *ForwardSuite) TestAPeerPresentingTheCredentialIsAnswered() {
	address := s.node([]byte("current"))

	answer, err := s.ask(address, []byte("current"))
	s.Require().NoError(err)
	s.Equal(int32(http.StatusTeapot), answer.GetStatus())
	s.Equal(int32(1), s.forwarded.Load(), "the handler was not told the request came from a peer")
}

// A peer handing a request to the node's shard says so, and the node does not pass the mark on
// with whatever it carries the request on to.
func (s *ForwardSuite) TestARequestHandedToTheShardArrivesMarkedAndCarriesNoMarkOn() {
	address := s.node([]byte("current"))
	request, err := http.NewRequest(http.MethodPost, "/v1/chat/hooks/stream", nil)
	s.Require().NoError(err)

	answer, err := NewForwarder(nil, []byte("current")).Ask(ToShard(s.ctx, "europe-west4"), address, request, nil)

	s.Require().NoError(err)
	s.Equal(int32(http.StatusTeapot), answer.GetStatus())
	s.Equal(int32(1), s.handed.Load(), "the node was not told the request was handed to its shard")
	s.Zero(s.marked.Load(), "the mark stayed on the request, to be carried on with it")
}

// Anybody can send a header, so a request that arrived with the mark is carried on unmarked:
// only the peer handing it to a shard marks it.
func (s *ForwardSuite) TestAShardMarkOnTheRequestItselfIsNotCarriedOn() {
	address := s.node([]byte("current"))
	request, err := http.NewRequest(http.MethodPost, "/v1/chat/hooks/stream", nil)
	s.Require().NoError(err)
	request.Header.Set(shardHeader, "europe-west4")

	_, err = NewForwarder(nil, []byte("current")).Ask(s.ctx, address, request, nil)

	s.Require().NoError(err)
	s.Equal(int32(1), s.forwarded.Load())
	s.Zero(s.handed.Load(), "a mark the request arrived with was taken for a peer's")
}

func (s *ForwardSuite) TestANodeHoldingTwoKeysAnswersAPeerPresentingEither() {
	address := s.node([]byte("current"), []byte("older"))

	_, err := s.ask(address, []byte("older"))
	s.Require().NoError(err)
	_, err = s.ask(address, []byte("current"))
	s.Require().NoError(err)
	s.Equal(int32(2), s.answered.Load())
}

func (s *ForwardSuite) TestAForwardWithNoCredentialIsRefused() {
	address := s.node([]byte("current"))

	_, err := s.ask(address, nil)
	s.refused(err)
}

func (s *ForwardSuite) TestAForwardWithAnotherCredentialIsRefused() {
	address := s.node([]byte("current"))

	_, err := s.ask(address, []byte("another deployment's"))
	s.refused(err)
}

func (s *ForwardSuite) TestAForwardWithPartOfTheCredentialIsRefused() {
	address := s.node([]byte("current"))

	_, err := s.ask(address, []byte("curr"))
	s.refused(err)
}

func (s *ForwardSuite) TestANodeAcceptingNoCredentialRefusesEveryForward() {
	address := s.node()

	_, err := s.ask(address, nil)
	s.refused(err)
	_, err = s.ask(address, []byte("current"))
	s.refused(err)
}

func (s *ForwardSuite) TestAMethodTheNodeDoesNotHaveNeedsTheCredentialToo() {
	address := s.node([]byte("current"))

	err := s.call(address, "/node.Node/Other", nil)
	s.Equal(codes.Unauthenticated, status.Code(err), "error: %v", err)

	err = s.call(address, "/node.Node/Other", metadata.Pairs(credentialKey, hex.EncodeToString([]byte("current"))))
	s.Equal(codes.Unimplemented, status.Code(err), "error: %v", err)
	s.Zero(s.answered.Load(), "the node's handler ran")
}

func (s *ForwardSuite) TestANodeAcceptingAnEmptyCredentialStillRefusesAForwardWithNone() {
	address := s.node(nil, []byte("current"))

	_, err := s.ask(address, nil)
	s.refused(err)
}

func (s *ForwardSuite) TestACredentialSplitAcrossValuesIsRefused() {
	address := s.node([]byte("current"))
	half := hex.EncodeToString([]byte("current"))

	err := s.call(address, "/node.Node/Other", metadata.Pairs(credentialKey, half[:6], credentialKey, half[6:]))
	s.Equal(codes.Unauthenticated, status.Code(err), "error: %v", err)
}

func (s *ForwardSuite) TestTheCredentialSentTwiceIsRefused() {
	address := s.node([]byte("current"))
	sent := hex.EncodeToString([]byte("current"))

	err := s.call(address, "/node.Node/Other", metadata.Pairs(credentialKey, sent, credentialKey, sent))
	s.Equal(codes.Unauthenticated, status.Code(err), "error: %v", err)
}

// The largest body an operation reads, with the most headers net/http reads beside it.
func (s *ForwardSuite) TestTheLargestRequestANodeTakesIsCarried() {
	address := s.node([]byte("current"))
	request, err := http.NewRequest(http.MethodPost, "/v1/agents/sessions/id/respond", nil)
	s.Require().NoError(err)
	request.Header.Set("X-Padding", strings.Repeat("x", 1<<20))

	answer, err := NewForwarder(nil, []byte("current")).Ask(s.ctx, address, request, make([]byte, 12<<20))

	s.Require().NoError(err)
	s.Equal(int32(http.StatusTeapot), answer.GetStatus())
}

// Past 14 MiB a call is refused: nothing an operation takes is that large.
func (s *ForwardSuite) TestARequestLargerThanANodeTakesIsRefused() {
	address := s.node([]byte("current"))
	request, err := http.NewRequest(http.MethodPost, "/v1/agents/sessions/id/respond", nil)
	s.Require().NoError(err)

	_, err = NewForwarder(nil, []byte("current")).Ask(s.ctx, address, request, make([]byte, 15<<20))

	s.Equal(codes.ResourceExhausted, status.Code(err), "error: %v", err)
	s.Zero(s.answered.Load(), "the node's handler ran")
}

// An offer to upgrade to HTTP/2 is answered as an ordinary HTTP/1.1 request, before its body ends
// and as no socket: this body never ends.
func (s *ForwardSuite) TestAnOfferToUpgradeToHTTP2IsAnsweredAsAnOrdinaryRequest() {
	s.ordinary("POST")
}

func (s *ForwardSuite) TestAnOfferToUpgradeToHTTP2InThePrefacesMethodIsAnsweredAsAnOrdinaryRequest() {
	s.ordinary("PRI")
}

func (s *ForwardSuite) ordinary(method string) {
	offered := make(chan string, 1)
	server := httptest.NewServer(Serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offered <- r.Proto + " " + r.Header.Get("Upgrade")
		w.WriteHeader(http.StatusTeapot)
	}), nil))
	s.T().Cleanup(server.Close)
	connection, err := net.Dial("tcp", server.Listener.Addr().String())
	s.Require().NoError(err)
	defer connection.Close()
	s.Require().NoError(connection.SetDeadline(time.Now().Add(5 * time.Second)))

	_, err = connection.Write([]byte(method + " / HTTP/1.1\r\nHost: router\r\nContent-Length: 1000000\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\nUpgrade: H2C\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\npart"))
	s.Require().NoError(err)
	read := make([]byte, 64)
	n, err := connection.Read(read)

	s.Require().NoError(err)
	s.Contains(string(read[:n]), "418")
	s.Equal("HTTP/1.1 ", <-offered)
}

func (s *ForwardSuite) TestASocketsUpgradeReachesTheHandler() {
	offered := make(chan string, 1)
	served := Serve(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		offered <- r.Header.Get("Upgrade")
	}), nil)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Upgrade", "websocket")

	served.ServeHTTP(httptest.NewRecorder(), request)

	s.Equal("websocket", <-offered)
}

// What a request is said to hold is what is carried, whatever it held as it arrived.
func (s *ForwardSuite) TestACarriedRequestStatesTheLengthOfWhatIsCarried() {
	stated := make(chan string, 1)
	server := httptest.NewServer(Serve(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		stated <- r.Header.Get("Content-Length")
	}), [][]byte{[]byte("current")}))
	s.T().Cleanup(server.Close)
	request, err := http.NewRequest(http.MethodPost, "/", nil)
	s.Require().NoError(err)
	request.Header.Set("Content-Length", "100")

	_, err = NewForwarder(nil, []byte("current")).Ask(s.ctx, server.Listener.Addr().String(), request, []byte("four"))

	s.Require().NoError(err)
	s.Equal("4", <-stated)
	s.Equal("100", request.Header.Get("Content-Length"), "the caller's own request was changed")
}

func (s *ForwardSuite) TestACarriedRequestThatStatesNoLengthIsGivenNone() {
	stated := make(chan []string, 1)
	server := httptest.NewServer(Serve(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		stated <- r.Header.Values("Content-Length")
	}), [][]byte{[]byte("current")}))
	s.T().Cleanup(server.Close)
	request, err := http.NewRequest(http.MethodPost, "/", nil)
	s.Require().NoError(err)

	_, err = NewForwarder(nil, []byte("current")).Ask(s.ctx, server.Listener.Addr().String(), request, []byte("four"))

	s.Require().NoError(err)
	s.Empty(<-stated)
}

func (s *ForwardSuite) TestAnAnswerOfAnySizeIsCarriedBack() {
	whole := bytes.Repeat([]byte("a"), 16<<20)
	server := httptest.NewServer(Serve(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(whole)
	}), [][]byte{[]byte("current")}))
	s.T().Cleanup(server.Close)

	answer, err := s.ask(server.Listener.Addr().String(), []byte("current"))

	s.Require().NoError(err)
	s.True(bytes.Equal(whole, answer.GetBody()), "%d bytes came back", len(answer.GetBody()))
}

// A call with no credential is refused on its headers: this one never ends.
func (s *ForwardSuite) TestAForwardWithNoCredentialIsRefusedUnread() {
	address := s.node([]byte("current"))
	client := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	s.T().Cleanup(client.CloseIdleConnections)
	body, unfinished := io.Pipe()
	s.T().Cleanup(func() { _ = unfinished.Close() })
	request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "http://"+address+"/node.Node/Forward", body)
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/grpc")

	response, err := client.RoundTrip(request)

	s.Require().NoError(err)
	defer response.Body.Close()
	s.Equal("16", response.Header.Get("Grpc-Status"))
	s.Zero(s.answered.Load(), "the node's handler ran")
}

// A node that comes back at an address the forwarder failed to reach is reached on the next call.
// gRPC alone would keep failing calls to it until its reconnect wait ran out, up to 2 minutes.
func (s *ForwardSuite) TestANodeBackAtAnAddressThatFailedIsReached() {
	forwarder := NewForwarder(nil, []byte("current"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	address := listener.Addr().String()
	s.Require().NoError(listener.Close())
	request, err := http.NewRequest(http.MethodPost, "/internal/scheduled-tasks/deliver", nil)
	s.Require().NoError(err)
	_, err = forwarder.Ask(s.ctx, address, request, nil)
	s.Require().Equal(codes.Unavailable, status.Code(err), "error: %v", err)
	failed := forwarder.peers[address]

	listener, err = net.Listen("tcp", address)
	s.Require().NoError(err)
	s.nodeOn(listener, []byte("current"))
	answer, err := forwarder.Ask(s.ctx, address, request, nil)

	s.Require().NoError(err)
	s.Equal(int32(http.StatusTeapot), answer.GetStatus())
	s.Equal(connectivity.Shutdown, failed.GetState(), "the connection that failed was left to redial")
}

// A call to a node that is still being answered is not cut off by the next call to that node.
func (s *ForwardSuite) TestACallInFlightOutlivesTheNextCallToTheSameNode() {
	entered, release := make(chan struct{}), make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusTeapot)
	})
	server := httptest.NewServer(Serve(slow, [][]byte{[]byte("current")}))
	s.T().Cleanup(server.Close)
	address := server.Listener.Addr().String()
	forwarder := NewForwarder(nil, []byte("current"))
	slowRequest, err := http.NewRequest(http.MethodPost, "/slow", nil)
	s.Require().NoError(err)
	first := make(chan error, 1)
	go func() {
		_, err := forwarder.Ask(s.ctx, address, slowRequest, nil)
		first <- err
	}()
	select {
	case <-entered:
	case <-s.ctx.Done():
		s.FailNow("the first call never arrived")
	}

	request, err := http.NewRequest(http.MethodPost, "/fast", nil)
	s.Require().NoError(err)
	_, err = forwarder.Ask(s.ctx, address, request, nil)
	s.Require().NoError(err)
	close(release)

	s.NoError(<-first)
}

// Calls made at once to a node that is down all fail as Unavailable. The callers that hand a
// request to another shard try the next node, or do the work here, only on Unavailable, so a
// call cut with Canceled because another call closed the connection under it would answer 503.
func (s *ForwardSuite) TestCallsAtOnceToANodeThatIsDownAllFailAsUnavailable() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	address := listener.Addr().String()
	s.Require().NoError(listener.Close())
	forwarder := NewForwarder(nil, []byte("current"))

	var mu sync.Mutex
	codesSeen := map[codes.Code]int{}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				request, err := http.NewRequest(http.MethodPost, "/internal/scheduled-tasks/deliver", nil)
				s.NoError(err)
				_, err = forwarder.Ask(s.ctx, address, request, nil)
				mu.Lock()
				codesSeen[status.Code(err)]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	s.Equal(map[codes.Code]int{codes.Unavailable: 32 * 50}, codesSeen)
}

// A call still being answered is not cut off when its connection is replaced. A node that is
// shutting down tells its callers to go away but finishes the calls it has, so the next call
// redials, fails, and makes the one after it replace the connection. A cut call would be
// Unavailable, which a caller reads as never delivered, so it would do the work a second time.
func (s *ForwardSuite) TestACallInFlightOutlivesTheReplacementOfItsConnection() {
	entered, release := make(chan struct{}), make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusTeapot)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	address := listener.Addr().String()
	server := grpc.NewServer()
	RegisterNodeServer(server, &forwarded{api: slow})
	go func() { _ = server.Serve(listener) }()
	s.T().Cleanup(server.Stop)
	forwarder := NewForwarder(nil, []byte("current"))
	slowRequest, err := http.NewRequest(http.MethodPost, "/slow", nil)
	s.Require().NoError(err)
	first := make(chan error, 1)
	go func() {
		_, err := forwarder.Ask(s.ctx, address, slowRequest, nil)
		first <- err
	}()
	select {
	case <-entered:
	case <-s.ctx.Done():
		s.FailNow("the first call never arrived")
	}
	conn := s.peerOf(forwarder, address)

	// GracefulStop closes the listener, sends GOAWAY and waits for the slow call.
	go server.GracefulStop()
	s.Eventually(func() bool { return conn.GetState() != connectivity.Ready }, 5*time.Second, 10*time.Millisecond)
	request, err := http.NewRequest(http.MethodPost, "/fast", nil)
	s.Require().NoError(err)
	_, err = forwarder.Ask(s.ctx, address, request, nil)
	s.Require().Equal(codes.Unavailable, status.Code(err), "error: %v", err)
	s.Require().Equal(connectivity.TransientFailure, conn.GetState())
	_, err = forwarder.Ask(s.ctx, address, request, nil)
	s.Require().Equal(codes.Unavailable, status.Code(err), "error: %v", err)
	s.Require().NotSame(conn, s.peerOf(forwarder, address), "the failed connection was not replaced")
	close(release)

	s.NoError(<-first)
	s.Equal(connectivity.Shutdown, conn.GetState(), "the replaced connection was left open after its last call")
}

// peerOf returns the forwarder's connection to address, read under its lock.
func (s *ForwardSuite) peerOf(forwarder *Forwarder, address string) *peerConn {
	forwarder.mu.Lock()
	defer forwarder.mu.Unlock()
	return forwarder.peers[address]
}
