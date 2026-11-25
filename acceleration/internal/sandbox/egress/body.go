package egress

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// errBodyStalled is a request body in which no byte came for bodyTimeout.
var errBodyStalled = errors.New("egress: the request body stopped arriving")

// timedBody is a request body whose every read waits timeout at most for a byte, by the tunnel's read deadline. A
// read that waits longer cuts the body off: it fails with errBodyStalled, and the deadline is left passed, so the
// server's drain of the rest of the body fails at once, and the server answers with Connection: close and closes
// the tunnel (net/http, response.WriteHeader and finishRequest): what the sandbox sends next could be the rest of
// the body, which it would read as a request.
type timedBody struct {
	io.ReadCloser
	timeout time.Duration
	// deadline sets the tunnel's read deadline (http.ResponseController.SetReadDeadline).
	deadline func(time.Time) error
	mu       sync.Mutex
	// ended is a body read to its end or to an error, or one whose handler returned: the tunnel's read deadline is
	// no longer this body's to set.
	ended, stalled bool
}

// timeBody puts a timedBody in place of r's body, when it has one, and returns it.
func (p *Proxy) timeBody(w http.ResponseWriter, r *http.Request) *timedBody {
	body := &timedBody{ReadCloser: r.Body, timeout: p.bodyTimeout, deadline: http.NewResponseController(w).SetReadDeadline}
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = body
	}
	return body
}

func (b *timedBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if !b.ended {
		_ = b.deadline(time.Now().Add(b.timeout))
	}
	b.mu.Unlock()
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if errors.Is(err, os.ErrDeadlineExceeded) {
		b.stalled = true
		return n, errBodyStalled
	}
	if err != nil {
		// The body's end starts the server's background read of the tunnel (net/http, connReader), which a later
		// read must not put under this deadline: the host's answer can take far longer, and that read failing
		// cancels the request.
		b.ended = true
	}
	return n, err
}

// stop gives the tunnel's read deadline back to the server once the handler returns. A body not read to its end,
// such as a refused request's, gets one timeout more in all for the server's drain of its rest (net/http reads up to
// 256 KiB of it to keep the tunnel open, response.finishRequest): past it the drain fails, and the server closes the
// tunnel. The server then reads the next request under its own deadlines, and a read of the body the transport
// still makes, for a host that answered before the body ended, sets none.
func (b *timedBody) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.ended && !b.stalled {
		_ = b.deadline(time.Now().Add(b.timeout))
	}
	b.ended = true
}

// cut says whether the body stopped arriving.
func (b *timedBody) cut() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stalled
}

// cutOff answers 408 when r's body stopped arriving (RFC 9110 section 15.5.9, as nginx's client_body_timeout does),
// and says whether it did; the server closes the tunnel after it (timedBody). A body cut off before the forward is
// counted as denied_request; one cut off as it streamed to the host is counted by the forward (forwarded), under its
// own decision.
func (p *Proxy) cutOff(w http.ResponseWriter, r *http.Request, t tunnel, logger *slog.Logger, body *timedBody,
	forwarded bool) bool {
	if !body.cut() {
		return false
	}
	logger.Warn("the sandbox egress proxy cut off a request body that stopped arriving", "method", loggedMethod(r.Method),
		"timeout", p.bodyTimeout)
	message := fmt.Sprintf("egress: the request body stopped arriving: no byte came for %s", p.bodyTimeout)
	if forwarded {
		http.Error(w, message, http.StatusRequestTimeout)
		return true
	}
	refuseRequest(w, r, t.label, decisionDeniedRequest, http.StatusRequestTimeout, message)
	return true
}
