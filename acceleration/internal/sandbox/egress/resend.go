package egress

import (
	"bytes"
	"io"
	"net/http"
)

// maxResentBody is the most of a request's body the proxy keeps to send it again, 1 MiB: what it already holds of
// one request's body to read it (maxGraphQLBody, maxLFSBody, maxQueryBody; AI-2033). A larger body streams to the
// host as it comes.
const maxResentBody = 1 << 20

// resendable keeps r's body in memory when it is maxResentBody at most, and gives r a GetBody that hands out those
// bytes again. An HTTP/2 host that sends GOAWAY names the last request it processed (RFC 9113 section 6.8), and
// Go's transport sends a later one again on a new connection only when its body has a GetBody or is empty
// (shouldRetryRequest, net/http/internal/http2/transport.go, go1.27.0): without it the sandbox gets a 502.
//
// A larger body, or one cut short, goes on as it came, the bytes read first, and with no GetBody. The trailer
// stays as the checks before left it: reading a body to its end fills r.Trailer, which ReverseProxy sends on, and
// no trailer the proxy did not check may reach the host.
func resendable(r *http.Request) {
	checked := r.Trailer.Clone()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxResentBody+1))
	r.Trailer = checked
	if err != nil || len(body) > maxResentBody {
		r.Body = rest{Reader: io.MultiReader(bytes.NewReader(body), r.Body), Closer: r.Body}
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
}

// rest is a body whose first bytes were read already: they come first, then the rest of the body it closes.
type rest struct {
	io.Reader
	io.Closer
}
