package vendors

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
)

// TwilioCallAnswers are the TwiML answers to Twilio's call webhook. They are here, in the
// registry, so the API that serves the webhook imports no vendor package.
var TwilioCallAnswers = CallAnswers{
	Dial: twilio.RenderInbound, Reject: twilio.RenderReject, Hangup: twilio.RenderHangup,
}

// CallAnswers are the answers a vendor's call webhook gives, each in that vendor's dialect.
type CallAnswers struct {
	Dial   func(phone.Bridge) (phone.Plan, error)
	Reject func() phone.Plan
	Hangup func() phone.Plan
}
