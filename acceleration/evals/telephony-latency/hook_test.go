package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/twiliosig"
)

type TwilioHookSuite struct {
	suite.Suite
	hook   *TwilioHook
	server *httptest.Server
}

func TestTwilioHookSuite(t *testing.T) {
	suite.Run(t, new(TwilioHookSuite))
}

func (s *TwilioHookSuite) SetupTest() {
	s.hook = &TwilioHook{
		Token: "bench-token", PublicURL: "http://203.0.113.7:8080",
		Bridges: map[string]phone.Bridge{"+15125550100": {URI: "sip:+15125550100@203.0.113.7:5060"}},
		Log:     slog.New(slog.DiscardHandler),
	}
	s.server = httptest.NewServer(s.hook)
}

func (s *TwilioHookSuite) TearDownTest() { s.server.Close() }

// post sends body to the hook with signature, as Twilio does, and answers the status and body.
func (s *TwilioHookSuite) post(body, signature string) (int, string) {
	request, err := http.NewRequest(http.MethodPost, s.server.URL+phone.TwilioCallHookPath, strings.NewReader(body))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set(twiliosig.SignatureHeader, signature)
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, string(answered)
}

// signed is the signature Twilio sends with form, over the bench's public URL and the path.
func (s *TwilioHookSuite) signed(form url.Values) string {
	return twiliosig.Sign(s.hook.Token, s.hook.PublicURL+phone.TwilioCallHookPath, form)
}

func (s *TwilioHookSuite) TestASignedCallToABenchNumberDialsTheBench() {
	form := url.Values{"To": {"+15125550100"}, "Direction": {"inbound"}}

	status, body := s.post(form.Encode(), s.signed(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Dial><Sip>sip:+15125550100@203.0.113.7:5060</Sip></Dial>")
}

func (s *TwilioHookSuite) TestASignedCallToAnotherNumberIsHungUp() {
	form := url.Values{"To": {"+15125550199"}, "Direction": {"inbound"}}

	status, body := s.post(form.Encode(), s.signed(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Hangup/>")
	s.NotContains(body, "<Dial>")
}

func (s *TwilioHookSuite) TestACallNotSignedOverTheBenchsURLIsRefused() {
	form := url.Values{"To": {"+15125550100"}}
	for name, signature := range map[string]string{
		"unsigned":      "",
		"another token": twiliosig.Sign("another-token", s.hook.PublicURL+phone.TwilioCallHookPath, form),
		"another URL":   twiliosig.Sign(s.hook.Token, "http://198.51.100.1:8080"+phone.TwilioCallHookPath, form),
	} {
		s.Run(name, func() {
			status, body := s.post(form.Encode(), signature)

			s.Equal(http.StatusUnauthorized, status)
			s.NotContains(body, "sip:")
		})
	}
}

func (s *TwilioHookSuite) TestABodyThatIsNoFormOrTooLargeIsRefused() {
	for name, body := range map[string]string{
		"no form":   "To=%zz",
		"too large": "To=" + strings.Repeat("1", hookLimit),
	} {
		s.Run(name, func() {
			status, _ := s.post(body, "")

			s.Equal(http.StatusBadRequest, status)
		})
	}
}
