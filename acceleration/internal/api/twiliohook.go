package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/redis/rueidis"

	phonetwilio "github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
)

// twilioCallHookPath is the voice URL a Twilio number is to be pointed at. Twilio asks it what
// to do with each call to the number.
const twilioCallHookPath = "/v1/phone/hooks/twilio"

// twilioCallLimit caps the form Twilio posts, which is a few hundred bytes
// (https://www.twilio.com/docs/voice/twiml, "Twilio's request to your application").
const twilioCallLimit = 64 << 10

// twilioCallKey prefixes the Redis key that marks a call's CallSid as answered. The key holds
// only the CallSid, which names the call at Twilio and no customer.
const twilioCallKey = "phone:twilio:call:"

// twilioCallAnsweredFor is how long an answered CallSid is remembered, so how long a copy of a
// signed call is refused. Twilio's signature holds no time
// (https://www.twilio.com/docs/usage/webhooks/webhooks-security), so after it a copy is answered
// again. A week is far past any call: Twilio ends one after 4 hours, or 24 with its longer limit
// turned on (timeLimit, https://www.twilio.com/docs/voice/twiml/dial), and it bounds the keys to
// one week's calls.
const twilioCallAnsweredFor = 7 * 24 * time.Hour

// twilioCallSidLimit is the longest CallSid that is marked. A Call's Sid is 34 characters
// (^CA[0-9a-fA-F]{32}$, https://www.twilio.com/docs/voice/api/call-resource), so 64 bytes keeps
// every key small; a call with a longer one, or none, is hung up.
const twilioCallSidLimit = 64

// twilioCallMarkTimeout bounds marking or forgetting a CallSid, as twilioAlertMarkTimeout does an
// alert's: one SET of one key is answered in well under a millisecond by a Redis that is up.
const twilioCallMarkTimeout = time.Second

// twilioPlans are the TwiML answers to a Twilio call.
var twilioPlans = callPlans{
	vendor: "twilio", renderDial: phonetwilio.RenderInbound,
	renderReject: phonetwilio.RenderReject, renderHangup: phonetwilio.RenderHangup,
}

// receiveTwilioCall answers a call to one of the router's Twilio numbers with TwiML that dials
// that number's Stream trunk, its credentials in the answer's body.
//
// Once the request is known to be Twilio's, every failure is a hangup or a reject rather than an
// HTTP error, as for a Telnyx call: Twilio would otherwise play its own error to the caller.
//
// Each CallSid is answered once: a signed call has no time in it, so a copy of it would get the
// trunk's credentials again. Twilio sends a call again only when it got no answer: by default
// after a failed connect or TLS handshake, and with the URL's overrides after a read timeout or a
// 5xx (https://www.twilio.com/docs/usage/webhooks/webhooks-connection-overrides). So the CallSid
// is marked before the answer and forgotten when the answer did not reach Twilio, and a copy of a
// call whose answer did is hung up.
func (s *Server) receiveTwilioCall(w http.ResponseWriter, r *http.Request) {
	if !s.twilioConfigured() {
		// Without the token anyone could ask this path for a number's trunk credentials.
		writeError(w, notFound("twilio calls are not configured"))
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, twilioCallLimit))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, payloadTooLarge("that call is too large"))
		return
	}
	if err != nil {
		writeError(w, invalidRequest("could not read that call"))
		return
	}
	// Twilio signs the form, so one that cannot be read cannot be shown to be Twilio's.
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeError(w, invalidRequest("that is not a form"))
		return
	}
	if err := s.twilioSigned(r, twilioCallHookPath, form); err != nil {
		s.logger.Warn("refused a twilio call webhook", "reason", err.Error())
		writeError(w, unauthenticated("that is not a call from Twilio"))
		return
	}

	logger := s.logger.With("call_sid", form.Get("CallSid"))
	if s.twilioAccountSID == "" {
		// Otherwise a signed call that names no account would match.
		twilioPlans.hangUp(w, logger, "hung up a twilio call: TWILIO_ACCOUNT_SID is not set")
		return
	}
	// The token signs for one account, but a call naming another is not this router's to answer
	// even so: its numbers are bought and attached under TWILIO_ACCOUNT_SID.
	if account := form.Get("AccountSid"); account != s.twilioAccountSID {
		twilioPlans.hangUp(w, logger, "hung up a twilio call on another account than TWILIO_ACCOUNT_SID",
			"account_sid", account)
		return
	}
	if direction := form.Get("Direction"); direction != "inbound" {
		twilioPlans.hangUp(w, logger, "hung up a twilio call that is not inbound", "direction", direction)
		return
	}
	to := form.Get("To")
	if to == "" {
		twilioPlans.hangUp(w, logger, "hung up a twilio call that names no number")
		return
	}
	sid := form.Get("CallSid")
	if s.twilioCallSids == nil {
		// Otherwise a copy of a signed call would be answered with the trunk's credentials again.
		twilioPlans.hangUp(w, logger, "hung up a twilio call: there is no Redis to answer each call once")
		return
	}
	if sid == "" || len(sid) > twilioCallSidLimit {
		twilioPlans.hangUp(w, logger, "hung up a twilio call that names no CallSid, or one too long to mark")
		return
	}
	first, err := s.markTwilioCall(r.Context(), sid)
	if err != nil {
		twilioPlans.hangUp(w, logger, "hung up a twilio call: could not mark it answered", "error", err)
		return
	}
	if !first {
		twilioPlans.hangUp(w, logger, "hung up a twilio call that was answered already")
		return
	}
	s.answerCall(w, r, logger, twilioPlans, to)
	// Twilio, gone before the answer reached it, sends the call again; that copy is answered.
	if err := http.NewResponseController(w).Flush(); err != nil || r.Context().Err() != nil {
		s.forgetTwilioCall(r.Context(), logger, sid)
	}
}

// markTwilioCall marks the CallSid as answered for twilioCallAnsweredFor, and reports whether it
// was not marked already.
func (s *Server) markTwilioCall(ctx context.Context, sid string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, twilioCallMarkTimeout)
	defer cancel()
	mark := s.twilioCallSids.B().Set().Key(twilioCallKey + sid).Value("1").Nx().Ex(twilioCallAnsweredFor).Build()
	err := s.twilioCallSids.Do(ctx, mark).Error()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	return err == nil, err
}

// forgetTwilioCall drops the mark of a call whose answer did not reach Twilio. Twilio may have
// hung up, which ends the request's context, so the mark is dropped without it.
func (s *Server) forgetTwilioCall(ctx context.Context, logger *slog.Logger, sid string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), twilioCallMarkTimeout)
	defer cancel()
	if err := s.twilioCallSids.Do(ctx, s.twilioCallSids.B().Del().Key(twilioCallKey+sid).Build()).Error(); err != nil {
		logger.Warn("could not forget a twilio call whose answer did not reach Twilio; its retry is hung up",
			"error", err)
		return
	}
	logger.Warn("the answer to a twilio call did not reach Twilio; its retry is answered")
}
