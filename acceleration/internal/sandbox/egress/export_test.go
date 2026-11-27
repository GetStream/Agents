package egress

import "net"

// ServeTunnel is tunnel, for tests that serve a tunnel over a conn of their own (net.Pipe, whose writes wait for a read).
func (p *Proxy) ServeTunnel(conn net.Conn, credential, host, label string) {
	p.tunnel(conn, tunnel{credential: credential, host: host, label: label})
}

// KeptBodies is how many bytes of the kept-body budget the proxy has taken now, for tests that wait until a request
// holds its share.
func (p *Proxy) KeptBodies() int64 { return p.keptBodies.Load() }
