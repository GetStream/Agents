package twilio

import (
	"encoding/xml"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
)

// twimlContentType is one of the types Twilio reads a webhook's answer as TwiML from
// (https://www.twilio.com/docs/voice/twiml, "Twilio's request to your application").
const twimlContentType = "application/xml"

// RenderInbound answers Twilio's webhook for a call to one of our numbers: dial that number's
// Stream trunk. The trunk's credentials are in the answer's body, which is the only place they go.
func RenderInbound(bridge phone.Bridge) (phone.Plan, error) {
	instructions, err := dialBridge(bridge)
	if err != nil {
		return phone.Plan{}, err
	}
	return phone.Plan{ContentType: twimlContentType, Body: []byte(instructions)}, nil
}

// RenderReject refuses a call without answering it, so the caller hears it rejected and no
// second of it is billed. Twilio only leaves a call unanswered when Reject is its first verb
// (https://www.twilio.com/docs/voice/twiml/reject).
func RenderReject() phone.Plan {
	return phone.Plan{ContentType: twimlContentType, Body: []byte(xml.Header + `<Response><Reject reason="rejected"/></Response>`)}
}

// RenderHangup ends a call there is nowhere to send.
func RenderHangup() phone.Plan {
	return phone.Plan{ContentType: twimlContentType, Body: []byte(xml.Header + "<Response><Hangup/></Response>")}
}
