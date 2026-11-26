//go:build integration

package egress_test

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/egress"
)

// TestOneSandboxsNewHostsLeaveAnotherSandboxsCertificatesKept: through real tunnels, a sandbox
// that names new hosts under a wildcard grant until the kept certificates are full takes no
// room from another sandbox: the other's host keeps its certificate, and its new host is kept.
func (s *ProxySuite) TestOneSandboxsNewHostsLeaveAnotherSandboxsCertificatesKept() {
	s.proxy = httptest.NewServer(s.newProxy(s.store, func(options *egress.Options) { options.MaxLeaves = 4 }))
	s.T().Cleanup(s.proxy.Close)
	churning := egress.NewCredential()
	s.put(churning, egress.Record{Session: s.session, Grants: []egress.Grant{s.grant("va_ph_"+uuid.NewString(), "*.github.test")}})
	s.put(s.credential, egress.Record{Session: s.session, Grants: []egress.Grant{s.grant(s.placeholder, "*.github.test")}})
	kept := s.serialAs(s.credential, "api.github.test")

	for i := range 10 {
		s.serialAs(churning, fmt.Sprintf("n%d.github.test", i))
	}
	uploads := s.serialAs(s.credential, "uploads.github.test")

	s.Equal(kept, s.serialAs(s.credential, "api.github.test"), "the other sandbox's host keeps its certificate")
	s.Equal(uploads, s.serialAs(s.credential, "uploads.github.test"), "the other sandbox's new host is kept")
	last := s.serialAs(churning, "n9.github.test")
	s.NotEqual(last, s.serialAs(churning, "n9.github.test"), "the sandbox that keeps the most gets no room for its new hosts")
}

// serialAs is the serial of the certificate a tunnel of credential's to host gets in its handshake.
func (s *ProxySuite) serialAs(credential, host string) string {
	conn, err := net.Dial("tcp", s.proxy.Listener.Addr().String())
	s.Require().NoError(err)
	defer func() { _ = conn.Close() }()
	s.Require().NoError(conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, err = fmt.Fprintf(conn, "CONNECT %s:443 HTTP/1.1\r\nHost: %s:443\r\nProxy-Authorization: %s\r\n\r\n",
		host, host, basic("sbx", credential))
	s.Require().NoError(err)
	// A one-byte reader keeps no byte of the handshake from tls.Client.
	response, err := http.ReadResponse(bufio.NewReaderSize(conn, 16), nil)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, response.StatusCode)
	client := tls.Client(conn, &tls.Config{ServerName: host, RootCAs: s.trusted})
	s.Require().NoError(client.Handshake())
	return client.ConnectionState().PeerCertificates[0].SerialNumber.String()
}
