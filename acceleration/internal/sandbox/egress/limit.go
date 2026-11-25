package egress

import (
	"net"
	"sync"
)

// maxTunnels caps the tunnels one sandbox holds open through this router at once, to granted
// hosts and to allowed domains together: every sandbox on the router shares its file descriptors,
// and an idle tunnel lives up to keepAliveTimeout. It is what Chromium lets one profile hold: 256
// is its soft cap of sockets per pool and the most connections through one proxy its
// MaxConnectionsPerProxy policy accepts (its default is 128), so a browser or a crawler in the
// sandbox stays under it (chromium/chromium at commit d839ed9cc2b32740f0136e209e80de640ac58789,
// net/socket/client_socket_pool_manager.cc: g_socket_soft_cap_per_pool, set_max_sockets_per_proxy_chain).
const maxTunnels = 256

// sandboxTunnels counts each sandbox's open tunnels by its proxy credential, which is the
// sandbox's own: the broker makes one per sandbox, where a session's sandboxes share its id.
type sandboxTunnels struct {
	max  int
	mu   sync.Mutex
	open map[string]int
}

// hold takes one of credential's tunnels, or says false when it holds max already.
func (s *sandboxTunnels) hold(credential string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open[credential] >= s.max {
		return false
	}
	s.open[credential]++
	return true
}

// release gives one of credential's tunnels back. A sandbox with none open leaves the map, so it
// holds only the sandboxes with a tunnel open now.
func (s *sandboxTunnels) release(credential string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open[credential] <= 1 {
		delete(s.open, credential)
		return
	}
	s.open[credential]--
}

// heldConn is the sandbox's end of a tunnel, which holds one of the sandbox's tunnels until it is
// closed. Every end of a tunnel closes it: either side closing, the idle timeout, or the tunnel's
// server ending it.
type heldConn struct {
	net.Conn
	release func()
	once    sync.Once
}

// held is conn holding one of credential's tunnels, which a hold took.
func (s *sandboxTunnels) held(conn net.Conn, credential string) net.Conn {
	return &heldConn{Conn: conn, release: func() { s.release(credential) }}
}

// Close gives the tunnel back, once however many times it is called: io.Closer leaves a second
// Close undefined, and net/http closes the connection of a TLS handshake that failed (the sandbox
// sent plain HTTP) and then the tls.Conn around it closes it again (net/http server.go, the
// RecordHeaderError branch; crypto/tls conn.go, Conn.Close).
func (c *heldConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
