package api

import (
	"errors"
	"io"
	"net/http"
	"net/url"

	phonetwilio "github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
)

// twilioCallHookPath is the voice URL a Twilio number is to be pointed at. Twilio asks it what
// to do with each call to the number.
const twilioCallHookPath = "/v1/phone/hooks/twilio"

// twilioCallLimit caps the form Twilio posts, which is a few hundred bytes
// (https://www.twilio.com/docs/voice/twiml, "Twilio's request to your application").
const twilioCallLimit = 64 << 10

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
	s.answerCall(w, r, logger, twilioPlans, to)
}
