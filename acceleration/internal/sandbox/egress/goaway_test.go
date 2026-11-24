//go:build integration

package egress_test

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/egress"
)

// keptBody is the most of a request's body the proxy keeps to send it again (maxResentBody).
const keptBody = 1 << 20

// goAwayOrigin is an HTTP/2 host whose first connection answers its first request, once a DATA frame of its body
// has come and more is to follow, with a GOAWAY that names no stream as processed: last stream id 0 and NO_ERROR,
// the graceful close after which RFC 9113 section 6.8 lets a client send the request again. Every later connection
// is Go's HTTP/2 server, which answers 200 and keeps the SHA-256 of each body it read whole.
type goAwayOrigin struct {
	conns  atomic.Int32
	mu     sync.Mutex
	bodies []string
}

func (s *ProxySuite) goAwayOrigin() *goAwayOrigin {
	origin := &goAwayOrigin{}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{NextProtos: []string{http2.NextProtoTLS},
		Certificates: []tls.Certificate{sign(s.T(), s.upstreamCA, s.upstreamKey, "*.github.test", "github.test")}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = listener.Close() })
	server := &http2.Server{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		origin.mu.Lock()
		origin.bodies = append(origin.bodies, digest(body))
		origin.mu.Unlock()
		_, _ = io.WriteString(w, "processed")
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			first := origin.conns.Add(1) == 1
			go func() {
				defer conn.Close()
				// ServeConn reads the TLS version and ALPN of a finished handshake.
				if conn.(*tls.Conn).Handshake() != nil {
					return
				}
				if first {
					origin.goAway(conn)
					return
				}
				server.ServeConn(conn, &http2.ServeConnOpts{Handler: handler})
			}()
		}
	}()
	s.answerAt(listener.Addr().String())
	return origin
}

// goAway serves conn as an HTTP/2 host that starts a graceful shutdown in the middle of the first request's body,
// then reads on until the client closes, as a host draining its connection does.
func (o *goAwayOrigin) goAway(conn net.Conn) {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
		return
	}
	framer := http2.NewFramer(conn, conn)
	if framer.WriteSettings() != nil {
		return
	}
	sent := false
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() && framer.WriteSettingsAck() != nil {
				return
			}
		case *http2.DataFrame:
			if !sent && !frame.StreamEnded() {
				sent = true
				if framer.WriteGoAway(0, http2.ErrCodeNo, nil) != nil {
					return
				}
			}
		}
	}
}

func (o *goAwayOrigin) processed() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.bodies...)
}

// overHTTP2 has the proxy reach its hosts over HTTP/2, as the router's transport does with a host that offers it
// (http.DefaultTransport, which NewUnpooledClient clones, sets ForceAttemptHTTP2).
func (s *ProxySuite) overHTTP2(change ...func(*egress.Options)) {
	s.proxy.Close()
	s.proxy = httptest.NewServer(s.newProxy(s.store, append([]func(*egress.Options){func(options *egress.Options) {
		options.Transport.(*http.Transport).ForceAttemptHTTP2 = true
	}}, change...)...))
	s.T().Cleanup(s.proxy.Close)
}

// readsUploadPack grants api.github.test with git-upload-pack as a read, a POST the proxy forwards as it came.
func (s *ProxySuite) readsUploadPack() {
	grant := s.grant(s.placeholder, "api.github.test")
	grant.Reads = []core.SandboxRead{{Host: "api.github.test", Method: http.MethodPost, Path: "/*/*/git-upload-pack"}}
	s.put(s.credential, egress.Record{Session: s.session, Grants: []egress.Grant{grant}})
}

// unsized hides a body's length from net/http, which then sends it chunked.
type unsized struct{ io.Reader }

// TestARequestAnHTTP2HostLeftUnprocessedIsSentAgain: a host that sends GOAWAY in the middle of a forwarded body, and
// says it processed nothing, gets the request again on a new connection, and the sandbox its answer. On the base
// (18962cc3e) the sandbox got a 502 with no body, the proxy logged «got no answer it could read from a host», and
// Go sent nothing again: one connection, no request processed.
func (s *ProxySuite) TestARequestAnHTTP2HostLeftUnprocessedIsSentAgain() {
	s.readsUploadPack()
	for _, size := range []int{32 << 10, keptBody} {
		for _, chunked := range []bool{false, true} {
			for _, authorization := range []string{"Bearer " + s.placeholder, ""} {
				what := fmt.Sprint(size, " ", boolName("chunked", chunked), " ", authorization)
				s.overHTTP2()
				origin := s.goAwayOrigin()
				body := strings.Repeat("0032want ", size/9) + strings.Repeat("x", size%9)
				var reader io.Reader = strings.NewReader(body)
				if chunked {
					reader = unsized{reader}
				}
				request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "https://api.github.test/o/r.git/git-upload-pack", reader)
				s.Require().NoError(err)
				if authorization != "" {
					request.Header.Set("Authorization", authorization)
				}

				response, answer, err := s.send(s.sandbox(s.credential), request)

				s.Require().NoError(err, what)
				s.Equal(http.StatusOK, response.StatusCode, what)
				s.Equal("processed", answer, what)
				s.Equal([]string{digest([]byte(body))}, origin.processed(), what)
				s.Equal(int32(2), origin.conns.Load(), what)
			}
		}
	}
}

// TestARequestOverTheKeptSizeIsNotSentAgain is the bound: a body over keptBody streams as it comes, so the proxy
// cannot send it again, and the sandbox gets the 502 it got on the base (18962cc3e) for any body.
func (s *ProxySuite) TestARequestOverTheKeptSizeIsNotSentAgain() {
	s.readsUploadPack()
	for _, chunked := range []bool{false, true} {
		s.overHTTP2()
		origin := s.goAwayOrigin()
		var reader io.Reader = strings.NewReader(strings.Repeat("x", keptBody+1))
		if chunked {
			reader = unsized{reader}
		}
		request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "https://api.github.test/o/r.git/git-upload-pack", reader)
		s.Require().NoError(err)
		request.Header.Set("Authorization", "Bearer "+s.placeholder)

		response, answer, err := s.send(s.sandbox(s.credential), request)

		s.Require().NoError(err, boolName("chunked", chunked))
		s.Equal(http.StatusBadGateway, response.StatusCode, boolName("chunked", chunked))
		s.Empty(answer, boolName("chunked", chunked))
		s.Empty(origin.processed(), boolName("chunked", chunked))
		s.Equal(int32(1), origin.conns.Load(), boolName("chunked", chunked))
	}
	s.Contains(s.logs.String(), "got no answer it could read from a host")
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func boolName(name string, value bool) string {
	if value {
		return name
	}
	return "not " + name
}

// TestABodyReachesTheHostWhole: a body the proxy keeps and one over keptBody, which streams after the bytes read,
// reach the host as the sandbox sent them, sized or chunked.
func (s *ProxySuite) TestABodyReachesTheHostWhole() {
	s.readsUploadPack()
	for _, size := range []int{keptBody, keptBody + 1, 3 * keptBody} {
		for _, chunked := range []bool{false, true} {
			what := fmt.Sprint(size, " ", boolName("chunked", chunked))
			s.granted.forget()
			body := strings.Repeat("0123456789abcdef", size/16+1)[:size]
			var reader io.Reader = strings.NewReader(body)
			if chunked {
				reader = unsized{reader}
			}
			request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "https://api.github.test/o/r.git/git-upload-pack", reader)
			s.Require().NoError(err)
			request.Header.Set("Authorization", "Bearer "+s.placeholder)

			response, _, err := s.send(s.sandbox(s.credential), request)

			s.Require().NoError(err, what)
			s.Equal(http.StatusOK, response.StatusCode, what)
			requests := s.granted.requests()
			s.Require().Len(requests, 1, what)
			s.Equal(digest([]byte(body)), digest([]byte(requests[0].Body)), what)
		}
	}
}

// TestATrailerReachesTheHostAsOnTheBase: reading a body to its end fills its trailer, which ReverseProxy would then
// send on, a method override in it included, after the trailer check; the host gets what it got on the base
// (18962cc3e), where the body streamed (probe of this test there).
func (s *ProxySuite) TestATrailerReachesTheHostAsOnTheBase() {
	s.readsUploadPack()
	trailers := make(chan http.Header, 1)
	s.answering(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		trailers <- r.Trailer.Clone()
	})
	// The base's host got the names declared in Trailer, with no value, and no other.
	want := map[string]http.Header{"": nil, "X-Checksum": {"X-Checksum": nil}}
	for _, declared := range []string{"", "X-Checksum"} {
		conn := s.tunnel(&tls.Config{})
		head := "POST /o/r.git/git-upload-pack HTTP/1.1\r\nHost: api.github.test\r\nAuthorization: Bearer " + s.placeholder +
			"\r\nTransfer-Encoding: chunked\r\n"
		if declared != "" {
			head += "Trailer: " + declared + "\r\n"
		}
		_, err := io.WriteString(conn, head+"\r\n4\r\n0000\r\n0\r\nX-Checksum: abc\r\nX-HTTP-Method-Override: DELETE\r\n\r\n")
		s.Require().NoError(err)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		s.Require().NoError(err)
		_ = response.Body.Close()

		s.Equal(http.StatusOK, response.StatusCode, declared)
		s.Equal(want[declared], <-trailers, declared)
	}
}

// TestABodyCutShortIsNoRequest: a chunked body that breaks before its last chunk, at a chunk that is no chunk or
// where the sandbox stops sending, is no request the host may apply, so the sandbox gets the 502 it got on the base
// (18962cc3e), where the body streamed.
func (s *ProxySuite) TestABodyCutShortIsNoRequest() {
	s.readsUploadPack()
	for _, broken := range []string{"zz\r\n", ""} {
		s.granted.forget()
		conn := s.tunnel(&tls.Config{})
		_, err := io.WriteString(conn, "POST /o/r.git/git-upload-pack HTTP/1.1\r\nHost: api.github.test\r\nAuthorization: Bearer "+
			s.placeholder+"\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n0000\r\n"+broken)
		s.Require().NoError(err)
		if broken == "" {
			s.Require().NoError(conn.CloseWrite())
		}

		response, err := http.ReadResponse(bufio.NewReader(conn), nil)

		s.Require().NoError(err, broken)
		_ = response.Body.Close()
		s.Equal(http.StatusBadGateway, response.StatusCode, broken)
	}
}

// stalled opens a tunnel and sends a POST to path whose body is declared keptBody and stops one byte short.
func (s *ProxySuite) stalled(path string) *tls.Conn {
	conn := s.tunnel(&tls.Config{})
	_, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: api.github.test\r\nAuthorization: Bearer %s\r\nContent-Length: %d\r\n\r\n%s",
		path, s.placeholder, keptBody, strings.Repeat("x", keptBody-1))
	s.Require().NoError(err)
	return conn
}

// TestStalledBodiesKeepNoMoreThanTheBudget: with a budget of one keptBody, a tunnel whose body is declared keptBody
// and stops one byte short holds all of it, so a second such tunnel streams to the host as on the base (18962cc3e)
// instead of the proxy keeping a second body. On the head before the budget the host got nothing from the second
// tunnel (64 stalled tunnels held 1,282,816 bytes each, the base 261,248).
func (s *ProxySuite) TestStalledBodiesKeepNoMoreThanTheBudget() {
	s.readsUploadPack()
	s.overHTTP2(func(options *egress.Options) { options.KeptBodies = keptBody })
	arrived := make(chan string, 2)
	release := make(chan struct{})
	s.answering(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadFull(r.Body, make([]byte, 1))
		arrived <- r.URL.Path
		<-release
	})
	defer close(release)

	s.stalled("/o/a.git/git-upload-pack")
	s.stalled("/o/b.git/git-upload-pack")

	select {
	case path := <-arrived:
		s.Equal("/o/b.git/git-upload-pack", path)
	case <-time.After(5 * time.Second):
		s.Fail("the second stalled body was kept as well: the host got nothing from it")
	}
}

// TestTheBudgetOfKeptBodiesIsGivenBack: a request that was kept and answered leaves the whole budget to the next one,
// each on a host of its own so none rides the HTTP/2 connection of the one before. The budget is one keptBody, so
// a next request that is kept at all shows it is back.
func (s *ProxySuite) TestTheBudgetOfKeptBodiesIsGivenBack() {
	grant := s.grant(s.placeholder, "*.github.test")
	grant.Reads = []core.SandboxRead{{Host: "*.github.test", Method: http.MethodPost, Path: "/*/*/git-upload-pack"}}
	s.put(s.credential, egress.Record{Session: s.session, Grants: []egress.Grant{grant}})
	s.overHTTP2(func(options *egress.Options) { options.KeptBodies = keptBody })
	body := strings.Repeat("x", keptBody)

	deadline := time.Now().Add(5 * time.Second)
	for attempt := 0; ; attempt++ {
		origin := s.goAwayOrigin()
		request, err := http.NewRequestWithContext(s.ctx, http.MethodPost,
			fmt.Sprintf("https://h%d.github.test/o/r.git/git-upload-pack", attempt), strings.NewReader(body))
		s.Require().NoError(err)
		request.Header.Set("Authorization", "Bearer "+s.placeholder)
		response, _, err := s.send(s.sandbox(s.credential), request)
		s.Require().NoError(err)
		if response.StatusCode == http.StatusOK {
			s.Equal([]string{digest([]byte(body))}, origin.processed())
			if attempt == 0 {
				continue
			}
			return
		}
		s.Require().True(time.Now().Before(deadline), "the budget was never given back")
		time.Sleep(10 * time.Millisecond)
	}
}
