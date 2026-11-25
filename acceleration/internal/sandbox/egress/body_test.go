//go:build integration

package egress_test

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/egress"
)

// bodyWait is the proxy's body timeout in these tests, so a body that stops arriving is cut off quickly.
const bodyWait = 300 * time.Millisecond

// cutOffAnswer is what the sandbox reads in the 408 of a body cut off after bodyWait.
const cutOffAnswer = "egress: the request body stopped arriving: no byte came for 300ms"

// waitingForBodies has the proxy cut off a body after bodyWait, change applied after.
func (s *ProxySuite) waitingForBodies(change ...func(*egress.Options)) {
	s.proxy.Close()
	s.proxy = httptest.NewServer(s.newProxy(s.store, append([]func(*egress.Options){func(options *egress.Options) {
		options.BodyTimeout = bodyWait
	}}, change...)...))
	s.T().Cleanup(s.proxy.Close)
}

// oneTunnel lets a sandbox hold one tunnel at a time, so a CONNECT tells whether the one before was given back.
func oneTunnel(options *egress.Options) { options.MaxTunnels = 1 }

// connects is the status of a CONNECT to host with credential, its tunnel closed at once.
func (s *ProxySuite) connects(credential, host string) int {
	conn, err := net.Dial("tcp", s.proxy.Listener.Addr().String())
	s.Require().NoError(err)
	defer conn.Close()
	s.Require().NoError(conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, err = fmt.Fprintf(conn, "CONNECT %s:443 HTTP/1.1\r\nHost: %s:443\r\nProxy-Authorization: %s\r\n\r\n",
		host, host, basic("sbx", credential))
	s.Require().NoError(err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	s.Require().NoError(err)
	return response.StatusCode
}

// givenBack waits until the sandbox, which may hold one tunnel (oneTunnel), opens one to host again. The tunnel it
// opens is given back as it closes, which the proxy sees later: the next tunnel is a new proxy's.
func (s *ProxySuite) givenBack(host, what string) {
	s.Eventually(func() bool { return s.connects(s.credential, host) == http.StatusOK }, 5*time.Second, 10*time.Millisecond,
		"%s: the tunnel is given back", what)
}

// bodyRead is one way the proxy reads a request body in a grant's tunnel: a check's read, resendable's, or the
// forward's as it streams.
type bodyRead struct {
	name, host, target, contentType string
	// sent is what the sandbox sends of a body declared declared bytes long; with declared -1 it is the first chunk
	// of a chunked body, and no chunk ends it. A check's is its JSON but the last byte, which the check alone would
	// refuse with a 403.
	sent     string
	declared int
	grant    func()
	// lfs is a Git LFS API request, refused with an LFS error; streamed is a body the forward reads.
	lfs, streamed bool
}

func (s *ProxySuite) bodyReads() []bodyRead {
	query := `{"query":"query { viewer { login } }"}`
	sql := `{"query":"SELECT 1"}`
	return []bodyRead{
		{name: "the GraphQL check", host: "api.github.test", target: "/graphql", contentType: "application/json",
			sent: query[:len(query)-1], declared: len(query), grant: func() { s.readGraphQL() }},
		{name: "the SQL check", host: "bigquery.github.test", target: "/bigquery/v2/projects/p/queries", contentType: "application/json",
			sent: sql[:len(sql)-1], declared: len(sql), grant: func() { s.readBigQuery() }},
		{name: "the LFS check", host: "github.test", target: "/o/r.git/info/lfs/objects/batch", contentType: "application/vnd.git-lfs+json",
			sent: lfsDownload[:len(lfsDownload)-1], declared: len(lfsDownload), grant: s.readLFS, lfs: true},
		{name: "a body kept to send again", host: "api.github.test", target: "/o/r.git/git-upload-pack",
			sent: strings.Repeat("x", keptBody-1), declared: keptBody, grant: s.readsUploadPack},
		{name: "a chunked body kept to send again", host: "api.github.test", target: "/o/r.git/git-upload-pack",
			sent: strings.Repeat("x", keptBody/2), declared: -1, grant: s.readsUploadPack},
		{name: "a body that streams", host: "api.github.test", target: "/o/r.git/git-upload-pack",
			sent: strings.Repeat("x", 2*keptBody), declared: 3 * keptBody, grant: s.readsUploadPack, streamed: true},
		{name: "a chunked body that streams", host: "api.github.test", target: "/o/r.git/git-upload-pack",
			sent: strings.Repeat("x", 2*keptBody), declared: -1, grant: s.readsUploadPack, streamed: true},
	}
}

// stop sends c's request in a tunnel of credential's and stops before its body ends.
func (s *ProxySuite) stop(credential string, c bodyRead) *tls.Conn {
	conn := s.tunnelFor(credential, c.host, &tls.Config{})
	head := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n", c.target, c.host, s.placeholder)
	if c.contentType != "" {
		head += "Content-Type: " + c.contentType + "\r\n"
	}
	var err error
	if c.declared < 0 {
		_, err = fmt.Fprintf(conn, "%sTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", head, len(c.sent), c.sent)
	} else {
		_, err = fmt.Fprintf(conn, "%sContent-Length: %d\r\n\r\n%s", head, c.declared, c.sent)
	}
	s.Require().NoError(err, c.name)
	return conn
}

// cutOff reads the proxy's answer in conn to c's stopped body: a 408 that closes the tunnel, then the tunnel's end.
func (s *ProxySuite) cutOff(conn *tls.Conn, c bodyRead) {
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	s.Require().NoError(err, c.name)
	answer, err := io.ReadAll(response.Body)
	s.Require().NoError(err, c.name)
	_ = response.Body.Close()
	s.Equal(http.StatusRequestTimeout, response.StatusCode, c.name)
	s.True(response.Close, "%s: the 408 says Connection: close", c.name)
	if c.lfs {
		s.Equal(lfsErrorBody(cutOffAnswer), string(answer), c.name)
	} else {
		s.Equal(cutOffAnswer+"\n", string(answer), c.name)
	}
	rest, err := io.ReadAll(reader)
	s.NoError(err, "%s: the proxy closes the tunnel", c.name)
	s.Empty(rest, c.name)
}

// TestABodyThatStopsArrivingIsCutOff: a body no byte of which comes for the body timeout, on every path that reads one
// (the three checks, resendable, and the forward as the body streams, sized or chunked), gets a 408 that closes the
// tunnel, no sooner than the timeout, and the tunnel is closed and given back. One cut off before the forward
// reaches no host and is counted as denied_request; one cut off as it streamed is counted under its decision, token,
// with its 408. On the base (aa4c2d4d9) no such body got an answer.
func (s *ProxySuite) TestABodyThatStopsArrivingIsCutOff() {
	for _, c := range s.bodyReads() {
		s.answerAt("")
		s.granted.forget()
		c.grant()
		s.waitingForBodies(oneTunnel)
		denied := s.requestCount(c.host, http.MethodPost, "denied_request", "4xx")
		forwarded := s.requestCount(c.host, http.MethodPost, "token", "4xx")
		reached := s.dialed()

		conn := s.stop(s.credential, c)
		stopped := time.Now()
		s.Equal(http.StatusTooManyRequests, s.connects(s.credential, c.host), "%s: the stalled body holds its tunnel", c.name)
		s.cutOff(conn, c)

		s.GreaterOrEqual(time.Since(stopped), bodyWait, c.name)
		s.givenBack(c.host, c.name)
		if c.streamed {
			s.Equal(forwarded+1, s.requestCount(c.host, http.MethodPost, "token", "4xx"), c.name)
			s.Equal(denied, s.requestCount(c.host, http.MethodPost, "denied_request", "4xx"), c.name)
			continue
		}
		s.Equal(reached, s.dialed(), "%s: no host is dialed", c.name)
		s.Equal(denied+1, s.requestCount(c.host, http.MethodPost, "denied_request", "4xx"), c.name)
		s.Equal(forwarded, s.requestCount(c.host, http.MethodPost, "token", "4xx"), c.name)
	}
	s.Contains(s.logs.String(), "cut off a request body that stopped arriving")
}

// TestABodyCutOffGivesItsKeptBytesBack: a body that stopped arriving while it held the whole budget of kept bodies
// gives it back once it is cut off, so the next request is kept and sent again after the host's GOAWAY. On the base
// (aa4c2d4d9) the stalled body held the budget for as long as its tunnel lived.
func (s *ProxySuite) TestABodyCutOffGivesItsKeptBytesBack() {
	s.readsUploadPack()
	s.overHTTP2(func(options *egress.Options) { options.KeptBodies, options.BodyTimeout = keptBody, bodyWait })
	kept := s.bodyReads()[3]

	// The server sends the 408 once the handler has returned, so its budget is back by then.
	s.cutOff(s.stop(s.credential, kept), kept)

	origin := s.goAwayOrigin()
	body := strings.Repeat("x", keptBody)
	request := s.request(http.MethodPost, "https://api.github.test/o/r.git/git-upload-pack", "Bearer "+s.placeholder, body)
	response, answer, err := s.send(s.sandbox(s.credential), request)
	s.Require().NoError(err)
	s.Equal(http.StatusOK, response.StatusCode)
	s.Equal("processed", answer)
	s.Equal([]string{digest([]byte(body))}, origin.processed())
}

// TestAStalledBodyLeavesAnotherSandboxsRequestsAlone: while one sandbox's bodies stop arriving in its tunnels, one in
// the GraphQL check and one that holds the whole budget of kept bodies, another sandbox's GraphQL query and its body
// to a POST read go through whole; the stalled ones are cut off after the timeout, each in its own tunnel.
func (s *ProxySuite) TestAStalledBodyLeavesAnotherSandboxsRequestsAlone() {
	grant := s.grant(s.placeholder, "api.github.test")
	grant.Reads = []core.SandboxRead{{Host: "api.github.test", Path: "/graphql", GraphQL: true},
		{Host: "api.github.test", Method: http.MethodPost, Path: "/*/*/git-upload-pack"}}
	s.put(s.credential, egress.Record{Session: s.session, Grants: []egress.Grant{grant}})
	other := egress.NewCredential()
	s.put(other, egress.Record{Session: "session-" + uuid.NewString(), Grants: []egress.Grant{grant}})
	s.waitingForBodies(func(options *egress.Options) { options.KeptBodies, options.BodyTimeout = keptBody, 2*time.Second })
	reads := s.bodyReads()
	graphQL, kept := reads[0], reads[3]

	stalled := []*tls.Conn{s.stop(s.credential, graphQL), s.stop(s.credential, kept)}
	since := time.Now()

	query := `{"query":"query { viewer { login } }"}`
	request := s.request(http.MethodPost, "https://api.github.test/graphql", "Bearer "+s.placeholder, query)
	request.Header.Set("Content-Type", "application/json")
	response, _, err := s.send(s.sandbox(other), request)
	s.Require().NoError(err)
	s.Equal(http.StatusOK, response.StatusCode)
	body := strings.Repeat("0032want ", keptBody/2/9)
	response, _, err = s.send(s.sandbox(other), s.request(http.MethodPost, "https://api.github.test/o/r.git/git-upload-pack",
		"Bearer "+s.placeholder, body))
	s.Require().NoError(err)
	s.Equal(http.StatusOK, response.StatusCode)
	s.Less(time.Since(since), 2*time.Second, "the other sandbox's requests went through while the bodies stalled")
	var bodies []string
	for _, got := range s.granted.requests() {
		bodies = append(bodies, got.Body)
	}
	s.Equal([]string{query, body}, bodies)

	for i, c := range []bodyRead{graphQL, kept} {
		message := strings.Replace(cutOffAnswer, "300ms", "2s", 1)
		reader := bufio.NewReader(stalled[i])
		response, err := http.ReadResponse(reader, nil)
		s.Require().NoError(err, c.name)
		answer, err := io.ReadAll(response.Body)
		s.Require().NoError(err, c.name)
		s.Equal(http.StatusRequestTimeout, response.StatusCode, c.name)
		s.Equal(message+"\n", string(answer), c.name)
	}
}

// paced is a body sent in pieces, gap apart: its bytes keep coming, each piece sooner than the body timeout, the
// whole body later.
type paced struct {
	pieces []string
	gap    time.Duration
	next   int
	rest   string
}

func (p *paced) Read(b []byte) (int, error) {
	if p.rest == "" {
		if p.next == len(p.pieces) {
			return 0, io.EOF
		}
		if p.next > 0 {
			time.Sleep(p.gap)
		}
		p.rest = p.pieces[p.next]
		p.next++
	}
	n := copy(b, p.rest)
	p.rest = p.rest[n:]
	return n, nil
}

// TestASlowSteadyBodyGoesThrough is the control: a body whose pieces come 100 ms apart, twice the body timeout or
// more in all, to the GraphQL check, kept to send again, or streamed past keptBody, sized or chunked, reaches the
// host whole and gets its answer: a 200, as on the base (aa4c2d4d9, probe of this test without the timeout there).
// Each piece is 8 KiB at least, over the client's 4 KiB write buffer, so its bytes leave when it is read.
func (s *ProxySuite) TestASlowSteadyBodyGoesThrough() {
	cases := []struct {
		name, target, contentType string
		pieces                    []string
		chunked                   bool
		grant                     func()
	}{
		{"the GraphQL check", "https://api.github.test/graphql", "application/json",
			append(append([]string{`{"query":"query { viewer { login } }"`}, repeat(strings.Repeat(" ", 8<<10), 7)...), "}"), false,
			func() { s.readGraphQL() }},
		{"a body kept to send again", "https://api.github.test/o/r.git/git-upload-pack", "",
			repeat(strings.Repeat("0032want ", (64<<10)/9), 8), false, s.readsUploadPack},
		{"a chunked body kept to send again", "https://api.github.test/o/r.git/git-upload-pack", "",
			repeat(strings.Repeat("0032want ", (64<<10)/9), 8), true, s.readsUploadPack},
		{"a body that streams", "https://api.github.test/o/r.git/git-upload-pack", "",
			repeat(strings.Repeat("x", keptBody/4), 12), false, s.readsUploadPack},
		{"a chunked body that streams", "https://api.github.test/o/r.git/git-upload-pack", "",
			repeat(strings.Repeat("x", keptBody/4), 12), true, s.readsUploadPack},
	}
	for _, c := range cases {
		c.grant()
		s.waitingForBodies()
		s.granted.forget()
		whole := strings.Join(c.pieces, "")
		request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, c.target, &paced{pieces: c.pieces, gap: bodyWait / 3})
		s.Require().NoError(err, c.name)
		if !c.chunked {
			request.ContentLength = int64(len(whole))
		}
		request.Header.Set("Authorization", "Bearer "+s.placeholder)
		if c.contentType != "" {
			request.Header.Set("Content-Type", c.contentType)
		}
		started := time.Now()

		response, _, err := s.send(s.sandbox(s.credential), request)

		s.Require().NoError(err, c.name)
		s.Equal(http.StatusOK, response.StatusCode, c.name)
		s.GreaterOrEqual(time.Since(started), 2*bodyWait, "%s: the body took longer than the timeout", c.name)
		requests := s.granted.requests()
		s.Require().Len(requests, 1, c.name)
		s.Equal(digest([]byte(whole)), digest([]byte(requests[0].Body)), c.name)
	}
}

func repeat(piece string, n int) []string {
	pieces := make([]string, n)
	for i := range pieces {
		pieces[i] = piece
	}
	return pieces
}

// TestAnAnswerLongerThanTheBodyTimeoutIsNotCut is the control of a body that arrived whole: a host that answers its
// header twice the body timeout after the body and then sends its answer in pieces 100 ms apart, to a body the GraphQL
// check read, one kept to send again and one that streamed, sized or chunked, gets its whole answer to the sandbox,
// as on the base (aa4c2d4d9, probe of this test without the timeout there). The server's read of the tunnel after a
// body's end must not wait under the body's deadline.
func (s *ProxySuite) TestAnAnswerLongerThanTheBodyTimeoutIsNotCut() {
	pieces := repeat(strings.Repeat("y", 8<<10), 10)
	s.answering(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		time.Sleep(2 * bodyWait)
		for _, piece := range pieces {
			_, _ = io.WriteString(w, piece)
			_ = http.NewResponseController(w).Flush()
			time.Sleep(bodyWait / 3)
		}
	})
	query := `{"query":"query { viewer { login } }"}`
	cases := []struct {
		name, target, contentType, body string
		chunked                         bool
		grant                           func()
	}{
		{"the GraphQL check", "https://api.github.test/graphql", "application/json", query, false, func() { s.readGraphQL() }},
		{"a body kept to send again", "https://api.github.test/o/r.git/git-upload-pack", "", strings.Repeat("x", 64<<10), false, s.readsUploadPack},
		{"a chunked body kept to send again", "https://api.github.test/o/r.git/git-upload-pack", "", strings.Repeat("x", 64<<10), true, s.readsUploadPack},
		{"a body that streams", "https://api.github.test/o/r.git/git-upload-pack", "", strings.Repeat("x", 2*keptBody), false, s.readsUploadPack},
		{"a chunked body that streams", "https://api.github.test/o/r.git/git-upload-pack", "", strings.Repeat("x", 2*keptBody), true, s.readsUploadPack},
	}
	for _, c := range cases {
		c.grant()
		s.waitingForBodies()
		var reader io.Reader = strings.NewReader(c.body)
		if c.chunked {
			reader = unsized{reader}
		}
		request, err := http.NewRequestWithContext(s.ctx, http.MethodPost, c.target, reader)
		s.Require().NoError(err, c.name)
		request.Header.Set("Authorization", "Bearer "+s.placeholder)
		if c.contentType != "" {
			request.Header.Set("Content-Type", c.contentType)
		}

		response, answer, err := s.send(s.sandbox(s.credential), request)

		s.Require().NoError(err, c.name)
		s.Equal(http.StatusOK, response.StatusCode, c.name)
		s.Equal(strings.Join(pieces, ""), answer, c.name)
	}
}

// TestARefusedRequestWhoseBodyStopsArrivingClosesItsTunnel: the server drains the rest of a refused POST's body before
// it sends the 403, to keep the tunnel open (net/http, response.WriteHeader), and a rest that stops arriving gets the
// body timeout for it, sized or chunked: then the 403 comes with Connection: close, and the tunnel closes. On the
// base (aa4c2d4d9) the drain waited for as long as the sandbox kept the tunnel open, and no 403 came.
func (s *ProxySuite) TestARefusedRequestWhoseBodyStopsArrivingClosesItsTunnel() {
	for _, declared := range []int{64, -1} {
		s.waitingForBodies(oneTunnel)
		refused := bodyRead{name: fmt.Sprint("a refused POST of ", declared), host: "api.github.test", target: "/repos/o/r/issues",
			contentType: "application/json", sent: `{"title":`, declared: declared}

		conn := s.stop(s.credential, refused)
		stopped := time.Now()
		reader := bufio.NewReader(conn)
		response, err := http.ReadResponse(reader, nil)
		s.Require().NoError(err, refused.name)
		_, _ = io.ReadAll(response.Body)

		s.Equal(http.StatusForbidden, response.StatusCode, refused.name)
		s.True(response.Close, "%s: the 403 says Connection: close", refused.name)
		s.GreaterOrEqual(time.Since(stopped), bodyWait, refused.name)
		rest, err := io.ReadAll(reader)
		s.NoError(err, "%s: the proxy closes the tunnel", refused.name)
		s.Empty(rest, refused.name)
		s.givenBack(refused.host, refused.name)
	}
}

// TestASandboxThatReadsAnAnswerSlowlyKeepsItsTunnel: a GET with no body, whose answer the sandbox reads later than
// the body timeout after the handler has returned, does not end the tunnel's next requests: the next two get their
// answers, with and without a token. The tunnel is a net.Pipe, whose write waits for the sandbox's read. On head
// 8812ba4d4 the second and third got 503 «grants cannot be read just now»; on the base they got 200.
func (s *ProxySuite) TestASandboxThatReadsAnAnswerSlowlyKeepsItsTunnel() {
	for _, authorization := range []string{"", "Authorization: Bearer " + s.placeholder + "\r\n"} {
		proxy := s.newProxy(s.store, func(options *egress.Options) { options.BodyTimeout = bodyWait })
		server, client := net.Pipe()
		go proxy.ServeTunnel(server, s.credential, "api.github.test", "api.github.test")
		conn := tls.Client(client, &tls.Config{ServerName: "api.github.test", RootCAs: s.trusted})
		s.T().Cleanup(func() { _ = conn.Close() })
		s.Require().NoError(conn.SetDeadline(time.Now().Add(10 * time.Second)))
		reader := bufio.NewReader(conn)
		for i := 0; i < 3; i++ {
			_, err := io.WriteString(conn, "GET /echo HTTP/1.1\r\nHost: api.github.test\r\n"+authorization+"\r\n")
			s.Require().NoError(err)
			if i == 0 && authorization == "" {
				time.Sleep(3 * bodyWait)
			}
			response, err := http.ReadResponse(reader, nil)
			s.Require().NoError(err)
			if i == 0 && authorization != "" {
				// The answer streams: its last byte, then the chunked body's end, is left for the server's final flush.
				one := make([]byte, 1)
				for one[0] != '}' {
					_, err := io.ReadFull(response.Body, one)
					s.Require().NoError(err)
				}
				time.Sleep(3 * bodyWait)
			}
			_, _ = io.ReadAll(response.Body)
			s.Equal(http.StatusOK, response.StatusCode, "authorization %q, request %d", authorization != "", i)
		}
	}
}
