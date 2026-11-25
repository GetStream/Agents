package egress

import (
	"bytes"
	"io"
	"net/http"
)

// maxKeptBodies is how much request body the proxy keeps in memory across every tunnel at once, 64 MiB: a sixteenth
// of the router container's 1 GiB memory limit (infra/charts/accelerate/values.yaml:402 in GetStream/chat, master
// deaa400660; its goMemLimit, :406, is 900 MiB). A tunnel's body counts from before it is read, by its declared
// length (maxResentBody when it has none), until its request is answered, so a sandbox that stalls its bodies pins
// no more than this. Past it a body streams, as it did before AI-3463.
const maxKeptBodies = 64 << 20

// maxResentBody is the most of a request's body the proxy keeps to send it again, 1 MiB: what it already holds of
// one request's body to read it (maxGraphQLBody, maxLFSBody, maxQueryBody; AI-2033). A larger body streams to the
// host as it comes.
const maxResentBody = 1 << 20

// resendable keeps r's body in memory when it is maxResentBody at most and the proxy's budget of kept bodies
// (maxKeptBodies) has room for it, and gives r a GetBody that hands out those bytes again. An HTTP/2 host that sends
// GOAWAY names the last request it processed (RFC 9113 section 6.8), and Go's transport sends a later one again on a
// new connection only when its body has a GetBody or is empty (shouldRetryRequest,
// net/http/internal/http2/transport.go, go1.27.0): without it the sandbox gets a 502. The returned func gives the
// budget back; the caller runs it when the request is done, on every path.
//
// A request with a GetBody already (a check kept its body, as the GraphQL, SQL and LFS ones do), a larger body, one
// cut short, or one the budget has no room for goes on as it came, the bytes read first, and with no GetBody. The
// trailer stays as the checks before left it: reading a body to its end fills r.Trailer, which ReverseProxy sends
// on, and no trailer the proxy did not check may reach the host.
func (p *Proxy) resendable(r *http.Request) (release func()) {
	release = func() {}
	if r.GetBody != nil {
		return release
	}
	// What the body may take: its declared length, or the most kept when it has none.
	reserve := int64(maxResentBody)
	if r.ContentLength >= 0 {
		reserve = r.ContentLength
	}
	if reserve > maxResentBody || !p.reserveBody(reserve) {
		return release
	}
	release = func() { p.keptBodies.Add(-reserve) }
	checked := r.Trailer.Clone()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxResentBody+1))
	r.Trailer = checked
	if err != nil || len(body) > maxResentBody {
		r.Body = rest{Reader: io.MultiReader(bytes.NewReader(body), r.Body), Closer: r.Body}
		return release
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return release
}

// reserveBody takes n bytes of the kept-body budget, or says it has no room.
func (p *Proxy) reserveBody(n int64) bool {
	for {
		held := p.keptBodies.Load()
		if held+n > p.keptBudget {
			return false
		}
		if p.keptBodies.CompareAndSwap(held, held+n) {
			return true
		}
	}
}

// rest is a body whose first bytes were read already: they come first, then the rest of the body it closes.
type rest struct {
	io.Reader
	io.Closer
}
