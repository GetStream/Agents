package channels

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

// WebhookCheckSuite covers the GET a provider checks a line's webhook with.
type WebhookCheckSuite struct{ suite.Suite }

func TestWebhookCheckSuite(t *testing.T) {
	suite.Run(t, new(WebhookCheckSuite))
}

func checkQuery(mode, token, challenge string) url.Values {
	return url.Values{"hub.mode": {mode}, "hub.verify_token": {token}, "hub.challenge": {challenge}}
}

func (s *WebhookCheckSuite) TestAWhatsAppLineEchoesMetasChallenge() {
	for _, token := range []string{"pick-anything", "Clé d'accès 🔑 & 100%"} {
		for _, challenge := range []string{"0", "1158201444", "18446744073709551616"} {
			answer := webhookCheck(Account{Kind: WhatsApp, Challenge: token}, checkQuery("subscribe", token, challenge))

			s.Equal(Answer{Status: http.StatusOK, Text: challenge}, answer)
		}
	}
}

func (s *WebhookCheckSuite) TestACheckTheLineDoesNotAgreeToIsRefused() {
	line := Account{Kind: WhatsApp, Challenge: "pick-anything"}
	for name, check := range map[string]struct {
		line  Account
		query url.Values
	}{
		"another mode":             {line, checkQuery("unsubscribe", "pick-anything", "42")},
		"no mode":                  {line, checkQuery("", "pick-anything", "42")},
		"another token":            {line, checkQuery("subscribe", "guessed", "42")},
		"the start of the token":   {line, checkQuery("subscribe", "pick", "42")},
		"no token":                 {line, checkQuery("subscribe", "", "42")},
		"no challenge":             {line, checkQuery("subscribe", "pick-anything", "")},
		"a challenge in words":     {line, checkQuery("subscribe", "pick-anything", "nonce-42")},
		"a challenge of markup":    {line, checkQuery("subscribe", "pick-anything", "<b>42</b>")},
		"digits of another script": {line, checkQuery("subscribe", "pick-anything", "٤٢")},
		"a line with no token":     {Account{Kind: WhatsApp}, checkQuery("subscribe", "", "42")},
		"an sms line":              {Account{Kind: SMS}, checkQuery("subscribe", "", "42")},
		"an imessage line":         {Account{Kind: IMessage}, checkQuery("subscribe", "", "42")},
		"an sms line's token":      {Account{Kind: SMS, Challenge: "t"}, checkQuery("subscribe", "t", "42")},
		"an imessage line's token": {Account{Kind: IMessage, Challenge: "t"}, checkQuery("subscribe", "t", "42")},
	} {
		answer := webhookCheck(check.line, check.query)

		s.Equal(http.StatusForbidden, answer.Status, name)
		s.Empty(answer.Text, name)
		s.Equal(failure("this line does not agree to that webhook check"), answer.Body, name)
	}
}
