package api

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/egress"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// How an inbound connector event ended (receiveEvent), the outcome of connectorEvents.
const (
	// eventHandled: verified, and a signal in it was acted on or an agent answers a message.
	eventHandled = "handled"
	// eventUnhandled: verified and answered 200, with nothing in it the router acted on: no
	// message, every message skipped, or none an agent answers.
	eventUnhandled = "unhandled"
	// eventRefused: answered 401 or 404, the connector known: unverified, or nothing here
	// verifies its events.
	eventRefused = "refused"
	// eventFailed: answered 500, so the provider delivers it again.
	eventFailed = "failed"
	// eventChallenge: a verified handshake, answered with its challenge.
	eventChallenge = "challenge"
)

// connectorEvents counts only events whose connector the router has a definition of: the id a
// request's path names before that is the sender's choice, without a bound.
var connectorEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "connector_events_total",
	Help:      "Inbound connector events, such as a Slack bot's message, by connector and how they ended: handled, unhandled, refused, failed or challenge (api receiveEvent)",
}, []string{"connector", "outcome"})

// countEvent counts one inbound event of connector id.
func countEvent(id, outcome string) {
	connectorEvents.WithLabelValues(store.ConnectorLabel(id), outcome).Inc()
}

// unmatchedRoute is the route label of a request no route served: a 404, a 405 for an unknown
// method, or a CORS preflight, which withCORS answers before the mux.
const unmatchedRoute = "unmatched"

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "http_requests_total",
		Help: "Requests the API answered, by method, route template (unmatched when no route served it) and status class: " +
			"1xx is a socket upgraded, 5xx also a panic nothing answered for. A request a peer node was handed is counted " +
			"on the node it reached first (api withMetrics)",
	}, []string{"method", "route", "status"})
	// internalRequests is httpRequests for the node-to-node routes, in a family of its own: the Datadog
	// monitor api_errors, the api_availability SLO and the Grafana 5xx panels read router_http_requests_total
	// over every route but /health and the connection proxy, so a peer's refusal counted there would page
	// as a customer-facing 5xx. A new label would not keep it out of them: the chart's include_labels
	// is an allow-list, so Datadog would fold the label away and sum the two series.
	internalRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "internal_http_requests_total",
		Help: "Requests a peer node handed this one over a node-to-node /internal/ route, by method, route template and " +
			"status class: not the customer API, so router_http_requests_total leaves them out (api withMetrics)",
	}, []string{"method", "route", "status"})
	httpRequestSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "router",
		Name:      "http_request_seconds",
		Help: "How long the API took to begin its answer, as its Server-Timing header says, by method, route template and " +
			"status class. A socket and a streamed answer are left out: what they last is a connection's time, not a response's",
		// Prometheus' defaults, 5 ms to 10 s: an answer slower than 10 s is slow whatever its exact time.
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	httpStatusSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "router",
		Name:      "api_latency_seconds",
		Help: "How long the API took to begin its answer, by status class only: router_http_request_seconds without the " +
			"method and route labels, so a cheap SLO can read API latency without them. A socket and a streamed answer " +
			"are left out",
		// The same buckets as router_http_request_seconds.
		Buckets: prometheus.DefBuckets,
	}, []string{"status"})
)

// internalPrefix starts every node-to-node route: the paths of server.go's scheduledPeerPath,
// chatInboundPeerPath, threadTurnPeerPath, unansweredPeerPath and machinePeerPath. They are only
// served to a forwarded request (node.Forwarded), and no customer route has it.
const internalPrefix = "/internal/"

// withMetrics counts each request the API answers in httpRequests and times the answers that
// are a response in httpRequestSeconds and, by status class alone, in httpStatusSeconds. A
// request a peer sent over an /internal/ route is counted in internalRequests alone.
//
// It sits inside withTrace, which seeds the route context the mux fills in: the route label is
// the template the request matched in routes, since its path carries session and config ids, a
// series each. It sits outside withCustomer, withQuota and onOwningNode, so a refusal and a
// request carried to the node running its session are counted too, the latter once: the node
// it was handed to leaves it to this one.
func withMetrics(routes *chi.Mux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The entry node counts a forwarded request, except one for a node-to-node /internal/ route:
		// no entry node saw that, and it is not the customer API, so it is counted here, apart.
		if node.Forwarded(r.Context()) && !strings.HasPrefix(findRoute(routes, r, r.Method), internalPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		metered := &meteredResponse{loggedResponse: loggedResponse{ResponseWriter: w}, started: time.Now()}
		finished := false
		// Deferred so a panic is counted as well: withRequestLog answers it and panics again.
		defer func() { metered.observe(routes, r, finished) }()
		next.ServeHTTP(metered, r)
		finished = true
	})
}

// meteredResponse is a loggedResponse that also notes when the answer began and whether it was
// streamed.
type meteredResponse struct {
	loggedResponse
	started  time.Time
	answered time.Duration
	begun    bool
	streamed bool
}

func (m *meteredResponse) WriteHeader(code int) {
	m.begin()
	m.loggedResponse.WriteHeader(code)
}

func (m *meteredResponse) Write(body []byte) (int, error) {
	m.begin()
	return m.loggedResponse.Write(body)
}

// FlushError marks the answer streamed: a handler flushes to send part of an answer before the
// rest exists. http.ResponseController reaches it through the writers' Unwrap.
func (m *meteredResponse) FlushError() error {
	m.streamed = true
	return http.NewResponseController(m.ResponseWriter).Flush()
}

// begin notes the answer starting, the moment withTiming stamps Server-Timing. An answer
// nothing wrote begins when the handler returns, as net/http then sends it.
func (m *meteredResponse) begin() {
	if m.begun {
		return
	}
	m.begun = true
	m.answered = time.Since(m.started)
}

// observe records the request once it has been answered, or once it panicked.
func (m *meteredResponse) observe(routes *chi.Mux, r *http.Request, finished bool) {
	m.begin()
	status := m.status()
	// What panicked outside withRequestLog is answered by nothing, which net/http sends as an
	// empty 200 once withSentry has recovered; it is the server's failure all the same.
	if !finished && m.code == 0 && !m.hijacked {
		status = http.StatusInternalServerError
	}
	method, route, class := egress.MethodLabel(r.Method), routeLabel(routes, r), strconv.Itoa(status/100)+"xx"
	if node.Forwarded(r.Context()) {
		internalRequests.WithLabelValues(method, route, class).Inc()
		return
	}
	httpRequests.WithLabelValues(method, route, class).Inc()
	if m.hijacked || m.streamed {
		return
	}
	httpRequestSeconds.WithLabelValues(method, route, class).Observe(m.answered.Seconds())
	httpStatusSeconds.WithLabelValues(class).Observe(m.answered.Seconds())
}

// routeLabel is the template of the route in routes that served r. A request answered before it
// reached the mux, such as one withQuota refused or onOwningNode could not carry, is looked up
// there, so its refusal is the route's rather than lumped in with the paths nothing serves.
func routeLabel(routes *chi.Mux, r *http.Request) string {
	if pattern := chi.RouteContext(r.Context()).RoutePattern(); pattern != "" {
		return pattern
	}
	if pattern := findRoute(routes, r, r.Method); pattern != "" {
		return pattern
	}
	return unmatchedRoute
}

// findRoute is the pattern routes has for method and the path r asks for, or "".
func findRoute(routes *chi.Mux, r *http.Request, method string) string {
	// The path the mux itself routes by (chi's Mux.routeHTTP).
	path := r.URL.RawPath
	if path == "" {
		path = r.URL.Path
	}
	return routes.Find(chi.NewRouteContext(), method, path)
}

// tokenSegment is what a route's secret path segment is called in a pattern, and what a log or
// a trace shows in its place.
const tokenSegment = "{token}"

// hiddenSegments are the path segments a log or a trace shows by their name: a secret, and an end
// user's id, which is often an email and which forgetting the user cannot take out of a log.
var hiddenSegments = []string{tokenSegment, "{user_id}"}

// hiddenIn is the segments of pattern a log or a trace shows by name: hiddenSegments, or the
// {id} of a customer's policy, which for a user's is <app>/<user id>.
func hiddenIn(pattern string) []string {
	if pattern == customerPolicyPath {
		return []string{"{id}"}
	}
	return hiddenSegments
}

// hidesSegment reports whether pattern has a segment of hiddenIn.
func hidesSegment(pattern string) bool {
	return slices.ContainsFunc(hiddenIn(pattern), func(segment string) bool { return strings.Contains(pattern, segment) })
}

// loggedPath is the path of r with every segment of hiddenIn its route's pattern
// replaced by its name, so a log line or a trace never holds a secret or an end user's id the
// URL carries. A method the route
// does not serve still hides it, as the caller sent the token all the same. A path no pattern
// matches, or one a wildcard matches, is as sent.
func loggedPath(routes *chi.Mux, r *http.Request) string {
	pattern := chi.RouteContext(r.Context()).RoutePattern()
	if pattern == "" {
		for _, method := range []string{r.Method, http.MethodGet, http.MethodPost, http.MethodPut,
			http.MethodPatch, http.MethodDelete, http.MethodHead} {
			found := findRoute(routes, r, method)
			if pattern == "" {
				pattern = found
			}
			if hidesSegment(found) {
				pattern = found
				break
			}
		}
	}
	if !hidesSegment(pattern) {
		return r.URL.Path
	}
	raw := r.URL.RawPath
	if raw == "" {
		raw = r.URL.Path
	}
	segments, patterned := strings.Split(raw, "/"), strings.Split(pattern, "/")
	if len(segments) != len(patterned) {
		return r.URL.Path
	}
	for i, segment := range patterned {
		if slices.Contains(hiddenIn(pattern), segment) {
			segments[i] = segment
		}
	}
	return strings.Join(segments, "/")
}
