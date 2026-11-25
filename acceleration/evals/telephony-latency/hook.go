package main

import (
	"log/slog"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/twiliosig"
)

// hookLimit caps the form Twilio posts to the hook, which is a few hundred bytes
// (https://www.twilio.com/docs/voice/twiml, "Twilio's request to your application").
const hookLimit = 64 << 10

// TwilioHook answers Twilio's call webhook as the router does, with the bench as the trunk: a
// Twilio number is pointed at the webhook, so the bench serves it to be told where calls go.
type TwilioHook struct {
	// Token is the Auth Token Twilio signs the webhook with.
	Token string
	// PublicURL is where Twilio reaches the bench's webhook, which the signature is over.
	PublicURL string
	// Bridges is where each number's calls go, by number.
	Bridges map[string]phone.Bridge
	Log     *slog.Logger
}

// ServeHTTP answers a signed call to one of the bench's numbers with TwiML dialing the bench,
// and any other signed call with a hangup.
func (h *TwilioHook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, hookLimit)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "that is not a form", http.StatusBadRequest)
		return
	}
	signature := r.Header.Get(twiliosig.SignatureHeader)
	if err := twiliosig.Verify(h.Token, h.PublicURL+phone.TwilioCallHookPath, r.PostForm, signature); err != nil {
		http.Error(w, "that is not a call from Twilio", http.StatusUnauthorized)
		return
	}

	plan := twilio.RenderHangup()
	if bridge, ok := h.Bridges[r.PostForm.Get("To")]; ok {
		dial, err := twilio.RenderInbound(bridge)
		if err != nil {
			h.Log.Error("telephony-latency: render a twilio call", "to", r.PostForm.Get("To"), "error", err)
		} else {
			plan = dial
		}
	}
	w.Header().Set("Content-Type", plan.ContentType)
	_, _ = w.Write(plan.Body)
}
