//go:build integration

package egress_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/egress"
)

// capped serves the test's proxy with a limit of max tunnels a sandbox holds open, and change, when
// given, applied after it.
func (s *ProxySuite) capped(max int, change ...func(*egress.Options)) *egress.Proxy {
	limit := func(options *egress.Options) { options.MaxTunnels = max }
	proxy := s.newProxy(s.store, append([]func(*egress.Options){limit}, change...)...)
	s.proxy = httptest.NewServer(proxy)
	s.T().Cleanup(s.proxy.Close)
	return proxy
}

// open is a CONNECT to target with credential: the proxy's status, its body, and the connection,
// which stays open until the test closes it or ends.
func (s *ProxySuite) open(target, credential string) (int, string, net.Conn) {
	conn, err := net.Dial("tcp", s.proxy.Listener.Addr().String())
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })
	s.Require().NoError(conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n", target, target,
		basic("sbx", credential))
	s.Require().NoError(err)
	// A one-byte reader keeps no byte past the answer from the test's reads of conn.
	response, err := http.ReadResponse(bufio.NewReaderSize(conn, 16), nil)
	s.Require().NoError(err)
	var body []byte
	// After a 200 the connection is the tunnel: only a refusal has a body to read.
	if response.StatusCode != http.StatusOK {
		body, err = io.ReadAll(response.Body)
		s.Require().NoError(err)
	}
	s.Require().NoError(conn.SetDeadline(time.Time{}))
	return response.StatusCode, string(body), conn
}

// opensAgain says a CONNECT to target by the test's sandbox gets its tunnel within a few seconds:
// a closed tunnel gives its place back once the proxy has seen it end.
func (s *ProxySuite) opensAgain(target string) {
	s.Eventually(func() bool {
		status, _, _ := s.open(target, s.credential)
		return status == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond, "the closed tunnel's place was not given back")
}

// TestASandboxHoldsAtMostItsLimitOfTunnels: tunnels to a granted host and to an allowed domain
// count together; past the limit either is refused with a 429 that says why, counted and logged
// with the session, and the host is not dialed.
func (s *ProxySuite) TestASandboxHoldsAtMostItsLimitOfTunnels() {
	s.capped(2)
	s.allow("outside.test")
	granted := s.requestCount("api.github.test", "CONNECT", "denied_tunnels", "4xx")
	blind := s.requestCount("other", "CONNECT", "denied_tunnels", "4xx")
	first, _, _ := s.open("api.github.test:443", s.credential)
	second, _, _ := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusOK, first)
	s.Require().Equal(http.StatusOK, second)

	toGranted, answer, _ := s.open("api.github.test:443", s.credential)
	toAllowed, _, _ := s.open("outside.test:443", s.credential)

	s.Equal(http.StatusTooManyRequests, toGranted)
	s.Equal(http.StatusTooManyRequests, toAllowed)
	s.Contains(answer, "egress: too many open tunnels: the sandbox holds 2, the most it may; close one and try again")
	s.Equal(granted+1, s.requestCount("api.github.test", "CONNECT", "denied_tunnels", "4xx"))
	s.Equal(blind+1, s.requestCount("other", "CONNECT", "denied_tunnels", "4xx"))
	s.Contains(s.logs.String(), "refused a tunnel past the sandbox's limit")
	s.Contains(s.logs.String(), "session="+s.session)
	s.Equal([]string{"outside.test:443"}, s.dialed(), "a refused tunnel dials nothing")
}

// TestAnotherSandboxOpensItsTunnelsWhileOneHoldsItsLimit: the limit is each sandbox's, so one at it
// keeps no other from opening one.
func (s *ProxySuite) TestAnotherSandboxOpensItsTunnelsWhileOneHoldsItsLimit() {
	s.capped(1)
	other := egress.NewCredential()
	s.put(other, egress.Record{Session: "session-" + uuid.NewString(),
		Grants: []egress.Grant{s.grant("va_ph_"+uuid.NewString(), "api.github.test")}})
	held, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusOK, held)
	past, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusTooManyRequests, past)

	status, _, _ := s.open("api.github.test:443", other)

	s.Equal(http.StatusOK, status)
}

// TestAGrantsTunnelTheSandboxClosesGivesItsPlaceBack: a host's ending a request is no end of the
// tunnel, which is the sandbox's connection to the proxy; the sandbox closing it is.
func (s *ProxySuite) TestAGrantsTunnelTheSandboxClosesGivesItsPlaceBack() {
	s.capped(1)
	status, _, conn := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusOK, status)
	past, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusTooManyRequests, past)

	s.Require().NoError(conn.Close())

	s.opensAgain("api.github.test:443")
}

// TestAGrantsTunnelThatSendsNothingInTimeGivesItsPlaceBack: the proxy closing a tunnel whose sandbox sends no
// handshake in time, as it closes one that sends no next request.
func (s *ProxySuite) TestAGrantsTunnelThatSendsNothingInTimeGivesItsPlaceBack() {
	s.capped(1, func(options *egress.Options) { options.HandshakeTimeout = 200 * time.Millisecond })
	status, _, conn := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusOK, status)
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, err := conn.Read(make([]byte, 1))
	s.Require().ErrorIs(err, io.EOF, "the proxy closes the silent tunnel")

	s.opensAgain("api.github.test:443")
}

// TestABlindTunnelTheSandboxClosesGivesItsPlaceBack: either end of a tunnel to an allowed domain
// closing ends it; here the sandbox's.
func (s *ProxySuite) TestABlindTunnelTheSandboxClosesGivesItsPlaceBack() {
	s.capped(1)
	s.allow("outside.test")
	status, _, conn := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusOK, status)
	past, _, _ := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusTooManyRequests, past)

	s.Require().NoError(conn.Close())

	s.opensAgain("outside.test:443")
}

// TestABlindTunnelTheHostClosesGivesItsPlaceBack: the host closing its end while the sandbox keeps
// its own open.
func (s *ProxySuite) TestABlindTunnelTheHostClosesGivesItsPlaceBack() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	s.answerAt(listener.Addr().String())
	s.capped(1)
	s.allow("outside.test")
	status, _, conn := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusOK, status)
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, err = conn.Read(make([]byte, 1))
	s.Require().ErrorIs(err, io.EOF, "the host's close reaches the sandbox")

	s.opensAgain("outside.test:443")
}

// TestAnIdleBlindTunnelGivesItsPlaceBack: a tunnel to an allowed domain that carries no byte
// within the idle timeout.
func (s *ProxySuite) TestAnIdleBlindTunnelGivesItsPlaceBack() {
	s.capped(1, func(options *egress.Options) { options.KeepAliveTimeout = 200 * time.Millisecond })
	s.allow("outside.test")
	status, _, conn := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusOK, status)
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, err := conn.Read(make([]byte, 1))
	s.Require().ErrorIs(err, io.EOF, "the proxy closes the idle tunnel")

	s.opensAgain("outside.test:443")
}

// TestABlindTunnelThatCannotBeDialedGivesItsPlaceBack: the 502 holds no tunnel.
func (s *ProxySuite) TestABlindTunnelThatCannotBeDialedGivesItsPlaceBack() {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	s.answerAt(closed.Addr().String())
	s.capped(1)
	s.allow("outside.test")
	status, _, _ := s.open("outside.test:443", s.credential)
	s.Require().Equal(http.StatusBadGateway, status)
	s.answerAt("")

	again, _, _ := s.open("outside.test:443", s.credential)

	s.Equal(http.StatusOK, again)
}

// TestATunnelThatCannotBeTakenOverGivesItsPlaceBack: a response writer that cannot hijack, for a
// granted host and then an allowed domain, each a 500 that holds no tunnel.
func (s *ProxySuite) TestATunnelThatCannotBeTakenOverGivesItsPlaceBack() {
	s.allow("outside.test")
	proxy := s.capped(1)
	for _, target := range []string{"api.github.test:443", "outside.test:443"} {
		request := httptest.NewRequest(http.MethodConnect, "http://"+target, nil)
		request.Host = target
		request.Header.Set("Proxy-Authorization", basic("sbx", s.credential))
		recorder := httptest.NewRecorder()

		proxy.ServeHTTP(recorder, request)

		s.Equal(http.StatusInternalServerError, recorder.Code, target)
	}

	status, _, _ := s.open("api.github.test:443", s.credential)

	s.Equal(http.StatusOK, status)
}

// TestAGrantsTunnelThatSpeaksPlainHTTPGivesBackOnlyItsPlace: a sandbox that sends plain HTTP in a
// granted tunnel has it closed by the TLS server twice (the 400 to the bare connection, then the
// server's own close); only its one place comes back.
func (s *ProxySuite) TestAGrantsTunnelThatSpeaksPlainHTTPGivesBackOnlyItsPlace() {
	s.capped(2)
	held, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusOK, held)
	for range 2 {
		status, _, conn := s.open("api.github.test:443", s.credential)
		s.Require().Equal(http.StatusOK, status)
		_, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: api.github.test\r\n\r\n")
		s.Require().NoError(err)
		s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
		answer, _ := io.ReadAll(conn)
		s.Require().Contains(string(answer), "400 Bad Request")
	}
	s.opensAgain("api.github.test:443")

	past, _, _ := s.open("api.github.test:443", s.credential)

	s.Equal(http.StatusTooManyRequests, past, "two tunnels are open, the limit")
}

// TestASandboxOfTheSameSessionOpensItsTunnelsWhileAnotherHoldsItsLimit: the count is the
// proxy credential's, one per sandbox, not the session's: two credentials of one session each
// hold their own limit.
func (s *ProxySuite) TestASandboxOfTheSameSessionOpensItsTunnelsWhileAnotherHoldsItsLimit() {
	s.capped(1)
	other := egress.NewCredential()
	s.put(other, egress.Record{Session: s.session, Grants: []egress.Grant{s.grant("va_ph_other", "api.github.test")}})
	held, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusOK, held)
	past, _, _ := s.open("api.github.test:443", s.credential)
	s.Require().Equal(http.StatusTooManyRequests, past)

	status, _, _ := s.open("api.github.test:443", other)

	s.Equal(http.StatusOK, status)
}
