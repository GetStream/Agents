package keypool

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/alerts"
)

// unnamedKeyVar is the key_var of a pool no FromEnv has named: one from New, or from keys
// passed in that no provider reads from a variable.
const unnamedKeyVar = "unknown"

// The owner of a refused key: the router's own, read from a variable (Env), or a customer's,
// such as their own model's key or the key they keep for a vendor (BYOK).
const (
	ourOwner      = "ours"
	customerOwner = "customer"
)

// key_var is the variable a pool's keys are read from (DEEPGRAM_API_KEY), a constant in the
// code, status the refused response's HTTP status (refused, rejectsKey) or a socket refusal's
// name (QuotaExceeded, InvalidKey, OutOfFunds), owner ours or customer, and product and model
// the routing modality and the catalog model a request was made for (WithCaller): all fixed sets.
// A customer's own key leaves model empty, as the customer names their own models. A key is
// never a label.
var refusedAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "keypool_refused_attempts_total",
	Help:      "Requests and handshakes a provider refused on a pooled key, by the variable the keys come from, the refusal's HTTP status (every 401 and 402, a 400 or 403 whose body says the key is not valid, and 429), spend_limit for a 429 that says the account is past its spend cap (Anthropic's enforced_spend_limit_reached), or, for a socket frame or close after the handshake, quota_exceeded, invalid_key (a key the provider does not take) or 402 (a frame for want of funds), owner: ours for a key the router read from its variable, customer for a customer's own (their model's key, or one they keep for the vendor), and the product (llm, tts, stt, sts, image and the other modalities) and model the request was made for, empty when the request named none, the model also for a customer's own key; a 429 or a spent quota moves to the next key or, on the last, is handed back to fail over, any other 400, 401, 402 or 403 counted is handed back at once",
}, []string{"key_var", "status", "owner", "product", "model"})

// Alerts are the conditions of a provider refusing one of the router's own keys, one alert per
// key variable and status at most every limit of the loop that runs them, such as
// ELEVENLABS_API_KEY/quota_exceeded: a key is never named. Its numbers list the products and the
// models the refused requests were made for. A customer's own key refused is not
// the operator's to fix and raises none. It does not page: the request moves to the next key or
// fails over, and the key is to top up or replace.
func Alerts(limit time.Duration) []alerts.Condition {
	return []alerts.Condition{{Name: "key_refused", Level: alerts.Warn,
		Items:    alerts.IncreasedPer(refusedAttempts, limit, []string{"key_var", "status"}, map[string]string{"owner": ourOwner}, "owner", "product", "model"),
		Describe: describeRefusal}}
}

// callerKey is the context key of the caller WithCaller names.
type callerKey struct{}

// caller is the product and the model a request is made for.
type caller struct{ product, model string }

// WithCaller is ctx for requests made for product, a routing modality such as tts, and model,
// the catalog model such as gemini-2.5-flash-preview-tts: a refusal of a pooled key under ctx is
// counted under them, and key_refused names them. Neither may be a key.
func WithCaller(ctx context.Context, product, model string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller{product: product, model: model})
}

// refusalReasons say what each status of refusedAttempts means, for the person who acts on the
// alert. The statuses are the ones refusedAttempts documents.
var refusalReasons = map[string]string{
	strconv.Itoa(http.StatusBadRequest):      "key not valid, replace it",
	strconv.Itoa(http.StatusUnauthorized):    "key not valid or its quota spent, replace it or top it up",
	outOfFundsStatus:                         "out of funds, top up the account",
	strconv.Itoa(http.StatusForbidden):       "key not valid, replace it",
	strconv.Itoa(http.StatusTooManyRequests): "rate limited or spend cap reached, wait, add keys or raise the cap",
	spendLimitStatus:                         "spend cap reached, raise the cap or wait for it to reset",
	quotaExceededStatus:                      "quota spent, wait for it to reset or raise it",
	invalidKeyStatus:                         "key not valid, replace it",
}

// describeRefusal is the reason for an item of Alerts, key_var/status. A status with no reason,
// or an item of another shape, gets none.
func describeRefusal(item string) string {
	_, status, _ := strings.Cut(item, "/")
	return refusalReasons[status]
}

// The vendor metrics are kept only for a pool whose keys were read from a variable (Env), so
// a customer's own key is never counted with the router's: key_var is that variable, and key
// the key's place in the pool from 0, never the key. limit is the vendor's own name of the
// figure, from the fixed set headroomHeaders and RateLimits read.
var (
	rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "vendor_rate_limited_total",
		Help:      "Requests and websocket handshakes a vendor refused with 429 on one of the router's own keys, by the variable the keys were read from and the key's place in the pool; Anthropic's 429 for a spent monthly cap (enforced_spend_limit_reached) is not a rate limit and is not counted. Each attempt counts, also one the pool moved past to the next key. A variable the router serves from start-up that holds no key has one series, key none, which stays at 0",
	}, []string{"key_var", "key"})
	headroom = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "router",
		Name:      "vendor_ratelimit_remaining",
		Help:      "What the vendor last said is left before its rate limit, on one of the router's own keys: the value of a response header named by limit (OpenAI x-ratelimit-remaining-requests, -tokens and -project-tokens; Anthropic anthropic-ratelimit-requests-remaining, -tokens-, -input-tokens- and -output-tokens-remaining), ElevenLabs' maximum-concurrent-requests less current-concurrent-requests as concurrent-requests, or OpenAI Realtime's rate_limits.updated event as requests and tokens. A vendor that reports none has no series",
	}, []string{"key_var", "key", "limit"})
)

// headroomHeaders are the response headers that say what is left of a rate limit, as their
// vendor documents them, each read under its canonical form so that reading allocates
// nothing. limit is the header's own name.
//   - OpenAI: https://developers.openai.com/api/docs/guides/rate-limits
//   - Anthropic: https://platform.claude.com/docs/en/api/rate-limits#response-headers
var headroomHeaders = []struct{ header, limit string }{
	{"X-Ratelimit-Remaining-Requests", "x-ratelimit-remaining-requests"},
	{"X-Ratelimit-Remaining-Tokens", "x-ratelimit-remaining-tokens"},
	{"X-Ratelimit-Remaining-Project-Tokens", "x-ratelimit-remaining-project-tokens"},
	{"Anthropic-Ratelimit-Requests-Remaining", "anthropic-ratelimit-requests-remaining"},
	{"Anthropic-Ratelimit-Tokens-Remaining", "anthropic-ratelimit-tokens-remaining"},
	{"Anthropic-Ratelimit-Input-Tokens-Remaining", "anthropic-ratelimit-input-tokens-remaining"},
	{"Anthropic-Ratelimit-Output-Tokens-Remaining", "anthropic-ratelimit-output-tokens-remaining"},
}

// ElevenLabs reports how many requests run on the account and how many may, not what is left:
// https://elevenlabs.io/docs/overview/models#concurrency-and-priority
const (
	currentConcurrentHeader = "Current-Concurrent-Requests"
	maximumConcurrentHeader = "Maximum-Concurrent-Requests"
	concurrentLimit         = "concurrent-requests"
)

// spendCapMarker is what Anthropic's 429 for an organization past its monthly spend cap says,
// a refusal no wait ends:
// https://platform.claude.com/docs/en/api/rate-limits#reaching-your-spend-cap
var spendCapMarker = []byte("enforced_spend_limit_reached")

// spendCapped reports whether a response is a 429 that says the account is past its spend cap.
// The body is read only far enough to tell, and is left for the caller to read whole.
func spendCapped(response *http.Response) bool {
	return response.StatusCode == http.StatusTooManyRequests && bytes.Contains(peek(response), spendCapMarker)
}

// refusalStatus is the status a refused response is counted under: its HTTP status, except a
// spend cap, which is spendLimitStatus so it is not taken for a rate limit's rest.
func refusalStatus(response *http.Response) string {
	if spendCapped(response) {
		return spendLimitStatus
	}
	return strconv.Itoa(response.StatusCode)
}

// noKey is the key label of the one series served for a variable that holds no key. No 429 is
// counted into it: a 429 is counted only on a key read from the variable.
const noKey = "none"

// ServeRateLimited serves the vendor rate-limit refusal counter at 0 for each key read from
// each of names, under the labels a 429 on it is counted with, before any pool over those keys
// is built: an alert on 429s then reads 0, not no data. A variable that holds no key is served
// one series, key noKey, so every vendor of names has one. It reads each variable as Env does,
// but names no pool by it, so a pool keeps the name the first provider to read its keys gives it.
func ServeRateLimited(names ...string) {
	for _, name := range names {
		n := len(Parse(numbered(name)...))
		if n == 0 {
			rateLimited.WithLabelValues(name, noKey)
		}
		zeroRateLimited(name, n)
	}
}

// zeroRateLimited makes the refusal counter's series of the first n keys read from keyVar,
// which stays at 0 until a 429 on that key is counted.
func zeroRateLimited(keyVar string, n int) {
	for index := range n {
		rateLimited.WithLabelValues(keyVar, strconv.Itoa(index))
	}
}

// observe counts a rate-limit refusal of key and keeps the headroom the response reports, on a
// pool whose keys were read from a variable.
func (r *ring) observe(key string, response *http.Response) {
	source, index, ok := r.own(key)
	if !ok {
		return
	}
	if response.StatusCode == http.StatusTooManyRequests && !spendCapped(response) {
		rateLimited.WithLabelValues(source, index).Inc()
	}
	for _, h := range headroomHeaders {
		if value, ok := headerNumber(response.Header, h.header); ok {
			headroom.WithLabelValues(source, index, h.limit).Set(value)
		}
	}
	maximum, hasMaximum := headerNumber(response.Header, maximumConcurrentHeader)
	current, hasCurrent := headerNumber(response.Header, currentConcurrentHeader)
	if hasMaximum && hasCurrent {
		headroom.WithLabelValues(source, index, concurrentLimit).Set(maximum - current)
	}
}

// Remaining keeps what a vendor said, after the handshake, is left of its limit on key, for a
// socket protocol that reports it in a frame. limit must come from a fixed set the caller
// names, as a label. A value that is not a count is dropped.
func (p *Pool) Remaining(key, limit string, value float64) {
	source, index, ok := p.own(key)
	if !ok || !count(value) {
		return
	}
	headroom.WithLabelValues(source, index, limit).Set(value)
}

// own is the variable the pool's keys were read from and key's place among them, when the
// keys were read from one and key is one of them.
func (r *ring) own(key string) (string, string, bool) {
	r.mu.Lock()
	source := r.source
	r.mu.Unlock()
	index := slices.Index(r.keys, key)
	if source == "" || index < 0 {
		return "", "", false
	}
	return source, strconv.Itoa(index), true
}

// headerNumber is the count a header holds, from its first value: a vendor that sends
// something else has not said how much is left.
func headerNumber(header http.Header, name string) (float64, bool) {
	values := header[name]
	if len(values) == 0 {
		return 0, false
	}
	value, err := strconv.ParseFloat(values[0], 64)
	return value, err == nil && count(value)
}

// count reports whether value can be a count of what is left: finite and not negative.
func count(value float64) bool {
	return value >= 0 && !math.IsInf(value, 1)
}
