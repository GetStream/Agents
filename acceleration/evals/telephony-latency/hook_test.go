package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
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

// A number whose bridge cannot be dialled is hung up rather than answered with an empty plan.
func (s *TwilioHookSuite) TestASignedCallToANumberWithABadBridgeIsHungUp() {
	s.hook.Bridges["+15125550100"] = phone.Bridge{URI: "https://203.0.113.7/not-sip"}
	form := url.Values{"To": {"+15125550100"}, "Direction": {"inbound"}}

	status, body := s.post(form.Encode(), s.signed(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Hangup/>")
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

// The bench serves the hook at the URL it returns until it is closed, and only with a token.
func (s *TwilioHookSuite) TestTheBenchServesTheHookWhereItSaysUntilClosed() {
	bridges := map[string]phone.Bridge{"+15125550100": {URI: "sip:+15125550100@127.0.0.1:5060"}}
	host := netip.MustParseAddr("127.0.0.1")
	log := slog.New(slog.DiscardHandler)
	s.T().Setenv(twiliosig.AuthTokenEnvVar, "")
	_, _, err := serveTwilioHook("127.0.0.1:0", host, bridges, log)
	s.ErrorContains(err, twiliosig.AuthTokenEnvVar)

	s.T().Setenv(twiliosig.AuthTokenEnvVar, "bench-token")
	_, _, err = serveTwilioHook("127.0.0.1", host, bridges, log)
	s.Error(err, "an address with no port")
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer taken.Close()
	_, _, err = serveTwilioHook(taken.Addr().String(), host, bridges, log)
	s.ErrorContains(err, "serve twilio's call webhook")

	free, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	address := free.Addr().String()
	s.Require().NoError(free.Close())
	publicURL, server, err := serveTwilioHook(address, host, bridges, log)
	s.Require().NoError(err)
	s.Equal("http://"+address, publicURL)

	form := url.Values{"To": {"+15125550100"}}
	request, err := http.NewRequest(http.MethodPost, publicURL+phone.TwilioCallHookPath, strings.NewReader(form.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set(twiliosig.SignatureHeader, twiliosig.Sign("bench-token", publicURL+phone.TwilioCallHookPath, form))
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	body, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	s.Contains(string(body), "<Sip>sip:+15125550100@127.0.0.1:5060</Sip>")

	s.Require().NoError(server.Close())
	_, err = http.Post(publicURL+phone.TwilioCallHookPath, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	s.Error(err, "nothing serves the hook once it is closed")
}
