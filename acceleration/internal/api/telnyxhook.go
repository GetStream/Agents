package api

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	phonetelnyx "github.com/GetStream/Vision-Agents/acceleration/internal/phone/telnyx"
	"github.com/GetStream/Vision-Agents/acceleration/internal/telnyxsig"
)

// telnyxCallHookPath is the voice URL of the Telnyx TeXML application every attached
// Telnyx number is on. Telnyx asks it what to do with each call to one of them.
const telnyxCallHookPath = "/v1/phone/hooks/telnyx"

// telnyxPlans are the TeXML answers to a Telnyx call.
var telnyxPlans = callPlans{
	vendor: "telnyx", renderDial: phonetelnyx.RenderInbound,
	renderReject: phonetelnyx.RenderReject, renderHangup: phonetelnyx.RenderHangup,
}

// telnyxCallLimit caps the form Telnyx posts, which is a few hundred bytes.
const telnyxCallLimit = 64 << 10

// receiveTelnyxCall answers a call to one of the numbers on the TeXML application with
// TeXML that dials that number's Stream trunk.
//
// Once the request is known to be Telnyx's, every failure is a hangup rather than an HTTP
// error: Telnyx would otherwise play its own error to the caller, and a hangup is also
// what stops one number's call from ever being sent anywhere but its own trunk.
func (s *Server) receiveTelnyxCall(w http.ResponseWriter, r *http.Request) {
	if s.telnyxPublicKey == nil {
		// Without the key anyone could ask this path for a number's trunk credentials.
		writeError(w, notFound("telnyx calls are not configured"))
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, telnyxCallLimit))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, payloadTooLarge("that call is too large"))
		return
	}
	if err != nil {
		writeError(w, invalidRequest("could not read that call"))
		return
	}
	err = telnyxsig.Verify(s.telnyxPublicKey,
		r.Header.Get(telnyxsig.TimestampHeader), r.Header.Get(telnyxsig.SignatureHeader), body, time.Now())
	if err != nil {
		s.logger.Warn("refused a telnyx call webhook", "reason", err.Error())
		writeError(w, unauthenticated("that is not a call from Telnyx"))
		return
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		telnyxPlans.hangUp(w, s.logger, "hung up a telnyx call whose form could not be read")
		return
	}
	logger := s.logger.With("call_sid", form.Get("CallSid"))
	if direction := form.Get("Direction"); direction != "inbound" {
		telnyxPlans.hangUp(w, logger, "hung up a telnyx call that is not inbound", "direction", direction)
		return
	}
	if s.telnyxConnectionID == "" {
		// Otherwise a signed call that names no application would match.
		telnyxPlans.hangUp(w, logger, "hung up a telnyx call: TELNYX_CONNECTION_ID is not set")
		return
	}
	// Dev and hosted routers share one Telnyx account, so a call for the other router's
	// application is signed with the same key. Its numbers are not in this database, but the
	// check keeps that from being the only thing that stops it.
	if connection := form.Get("ConnectionId"); connection != s.telnyxConnectionID {
		telnyxPlans.hangUp(w, logger, "hung up a telnyx call on another TeXML application than TELNYX_CONNECTION_ID",
			"connection_id", connection)
		return
	}
	to := form.Get("To")
	if to == "" {
		telnyxPlans.hangUp(w, logger, "hung up a telnyx call that names no number")
		return
	}
	s.answerCall(w, r, logger, telnyxPlans, to)
}
