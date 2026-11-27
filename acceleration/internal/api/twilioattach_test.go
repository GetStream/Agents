//go:build integration

package api

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/telnyx"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/twiliosig"
)

// A number attached at Twilio is pointed at this router's call webhook, by POST, and a call
// Twilio signs over that voice URL is answered there with the number's trunk.
func (s *TwilioHookSuite) TestANumberAttachedAtTwilioIsAnsweredAtTheVoiceURLTwilioWasGiven() {
	vendor := newFakeVendor(s.e164)
	defer vendor.Close()
	service := s.attacher(vendor, attacherSettings{
		token: s.twilioAuthToken, account: s.twilioAccountSID, publicURL: s.publicURL, sealed: true,
	})
	s.hold(s.e164, "twilio")

	attached, err := service.Attach(context.Background(), phone.Attachment{CustomerID: s.customerID(), E164: s.e164})
	s.Require().NoError(err)

	configured := vendor.posted()
	s.Require().Len(configured, 1)
	voiceURL := configured[0].Get("VoiceUrl")
	s.Equal(strings.TrimRight(s.publicURL, "/")+"/v1/phone/hooks/twilio", voiceURL)
	s.Equal(http.MethodPost, configured[0].Get("VoiceMethod"))
	s.NotContains(voiceURL, "#", "no connection overrides: the webhook answers each CallSid once")
	s.Require().NotEmpty(attached.Bridge.Password)
	for _, value := range configured[0] {
		s.NotContains(strings.Join(value, " "), attached.Bridge.Password)
	}

	parsed, err := url.Parse(voiceURL)
	s.Require().NoError(err)
	form := s.line.call(s.e164)
	request, err := http.NewRequest(http.MethodPost, s.server.URL+parsed.Path, strings.NewReader(form.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set(twiliosig.SignatureHeader, twiliosig.Sign(s.twilioAuthToken, voiceURL, form))
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, response.StatusCode, string(body))
	var answer struct {
		Dial struct {
			Sip struct {
				URI      string `xml:",chardata"`
				Username string `xml:"username,attr"`
				Password string `xml:"password,attr"`
			} `xml:"Sip"`
		} `xml:"Dial"`
	}
	s.Require().NoError(xml.Unmarshal(body, &answer), string(body))
	s.Equal(attached.Bridge.URI, answer.Dial.Sip.URI)
	s.Equal(attached.Bridge.Username, answer.Dial.Sip.Username)
	s.Equal(attached.Bridge.Password, answer.Dial.Sip.Password)
}

// Without what the router needs to answer the call webhook, a Twilio number is not attached:
// nothing is made at Stream, the app's call hook included, and Twilio is asked nothing.
func (s *TwilioHookSuite) TestATwilioNumberIsNotAttachedWithoutWhatItsCallWebhookNeeds() {
	ready := attacherSettings{token: s.twilioAuthToken, account: s.twilioAccountSID, publicURL: s.publicURL, sealed: true}
	for name, test := range map[string]struct {
		settings attacherSettings
		refusal  string
	}{
		"no webhook token": {settings: ready.without(func(a *attacherSettings) { a.token = "" }), refusal: "TWILIO_WEBHOOK_AUTH_TOKEN"},
		"no public URL":    {settings: ready.without(func(a *attacherSettings) { a.publicURL = "" }), refusal: "ROUTER_PUBLIC_URL"},
		"a plain http URL": {settings: ready.without(func(a *attacherSettings) { a.publicURL = "http://router.example.test" }), refusal: "https"},
		"no account SID":   {settings: ready.without(func(a *attacherSettings) { a.account = "" }), refusal: "TWILIO_ACCOUNT_SID"},
		"no keyring":       {settings: ready.without(func(a *attacherSettings) { a.sealed = false }), refusal: "stored trunk"},
	} {
		s.Run(name, func() {
			e164 := s.utils.number()
			vendor := newFakeVendor(e164)
			defer vendor.Close()
			service := s.attacher(vendor, test.settings)
			s.hold(e164, "twilio")
			trunks := s.chat.Trunks()
			s.chat.SetEventHooks(s.chat.Key)

			_, err := service.Attach(context.Background(), phone.Attachment{CustomerID: s.customerID(), E164: e164})

			s.ErrorContains(err, test.refusal)
			s.Empty(vendor.asked(), "twilio was asked nothing")
			s.Equal(trunks, s.chat.Trunks(), "no trunk was made at Stream")
			s.Empty(s.chat.EventHooks(s.chat.Key), "the app's call hook was not written")
			number, err := s.store.PhoneNumbers.GetByE164(context.Background(), s.customerID(), e164)
			s.Require().NoError(err)
			s.Nil(number.AttachedAt)
		})
	}
}

// A number attached already is not pointed anywhere again, so attaching it again still answers
// with what it has where the webhook's settings are gone: programs send it on every start.
func (s *TwilioHookSuite) TestAnAttachedTwilioNumberAttachedAgainWithoutTheWebhookTokenAnswers() {
	vendor := newFakeVendor(s.e164)
	defer vendor.Close()
	ready := attacherSettings{token: s.twilioAuthToken, account: s.twilioAccountSID, publicURL: s.publicURL, sealed: true}
	s.hold(s.e164, "twilio")
	first, err := s.attacher(vendor, ready).Attach(context.Background(), phone.Attachment{CustomerID: s.customerID(), E164: s.e164})
	s.Require().NoError(err)
	asked := len(vendor.asked())

	unready := s.attacher(vendor, ready.without(func(a *attacherSettings) { a.token, a.publicURL = "", "" }))
	again, err := unready.Attach(context.Background(), phone.Attachment{CustomerID: s.customerID(), E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Len(vendor.asked(), asked, "twilio is not told anything new")
}

// Telnyx numbers are attached as before: the number is put on the TeXML application, with no
// public URL needed. The requests are those base 52435fe23 sent.
func (s *TwilioHookSuite) TestATelnyxNumberIsAttachedAsBefore() {
	vendor := newFakeVendor(s.e164)
	defer vendor.Close()
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	for i := range config.Vendors {
		config.Vendors[i].Credentials = nil
	}
	registry := vendors.Registry(config)
	registry.Register("telnyx", func() (phone.Provider, error) {
		return telnyx.New(telnyx.Options{APIKey: "k", ConnectionID: "texml-1", BaseURL: vendor.URL})
	})
	sealer, err := auth.NewSealer(suiteTrunkKEK)
	s.Require().NoError(err)
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: registry, Store: s.store, Stream: phone.NewStreamFromClient(s.chat.Client), Sealer: sealer,
		Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.hold(s.e164, "telnyx")

	_, err = service.Attach(context.Background(), phone.Attachment{CustomerID: s.customerID(), E164: s.e164})

	s.Require().NoError(err)
	s.Equal([]string{
		"GET /v2/phone_numbers?filter%5Bphone_number%5D=" + url.QueryEscape(s.e164),
		`PATCH /v2/phone_numbers/PN1 {"connection_id":"texml-1"}`,
	}, vendor.asked())
}

// attacherSettings are what a deployment has for its Twilio numbers.
type attacherSettings struct {
	token, account, publicURL string
	// sealed is a deployment with a keyring.
	sealed bool
}

// without is the settings with change made to a copy.
func (a attacherSettings) without(change func(*attacherSettings)) attacherSettings {
	change(&a)
	return a
}

// attacher is a phone service attaching the suite's numbers in its Stream app, with Twilio at
// vendor and the settings given.
func (s *TwilioHookSuite) attacher(vendor *fakeVendor, settings attacherSettings) *phone.Service {
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	// The suites share one process, so the credentials are handed to the provider instead.
	for i := range config.Vendors {
		config.Vendors[i].Credentials = nil
	}
	registry := vendors.Registry(config)
	registry.Register("twilio", func() (phone.Provider, error) {
		return twilio.New(twilio.Options{
			AccountSID: settings.account, AuthToken: "api-secret", WebhookAuthToken: settings.token, BaseURL: vendor.URL,
		})
	})
	var sealer *auth.Sealer
	if settings.sealed {
		sealer, err = auth.NewSealer(suiteTrunkKEK)
		s.Require().NoError(err)
	}
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: registry, Store: s.store, Stream: phone.NewStreamFromClient(s.chat.Client), Sealer: sealer,
		PublicURL: settings.publicURL, Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	return service
}

// hold records the number as the suite's app's, bought from vendor.
func (s *TwilioHookSuite) hold(e164, vendor string) {
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &store.PhoneNumber{
		E164: e164, Vendor: vendor, Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
}

// fakeVendor is Twilio's and Telnyx's number APIs holding one number, recording every request.
type fakeVendor struct {
	*httptest.Server
	mu       sync.Mutex
	requests []string
	forms    []url.Values
}

func newFakeVendor(e164 string) *fakeVendor {
	vendor := &fakeVendor{}
	vendor.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		vendor.mu.Lock()
		asked := r.Method + " " + r.URL.RequestURI()
		if len(body) > 0 {
			asked += " " + string(body)
		}
		vendor.requests = append(vendor.requests, asked)
		if r.Method == http.MethodPost {
			form, _ := url.ParseQuery(string(body))
			vendor.forms = append(vendor.forms, form)
		}
		vendor.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		listed := []map[string]string{{"sid": "PN1", "id": "PN1", "phone_number": e164}}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"incoming_phone_numbers": listed, "data": listed, "sid": "PN1", "phone_number": e164,
		})
	}))
	return vendor
}

// asked is every request the vendor got, as method, path and query, then the body.
func (v *fakeVendor) asked() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.requests...)
}

// posted is the form of every POST the vendor got.
func (v *fakeVendor) posted() []url.Values {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]url.Values(nil), v.forms...)
}
