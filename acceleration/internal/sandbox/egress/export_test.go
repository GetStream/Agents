package egress

import "net"

// ServeTunnel is tunnel, for tests that serve a tunnel over a conn of their own (net.Pipe, whose writes wait for a read).
func (p *Proxy) ServeTunnel(conn net.Conn, credential, host, label string) {
	p.tunnel(conn, tunnel{credential: credential, host: host, label: label})
}
