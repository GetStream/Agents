package routing

import (
	"context"
	"crypto/rand"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool/keypooltest"
)

// KeyCallerSuite asks a vendor that refuses every request with a 402, as one refuses a key out
// of funds, on a pool of the router's own keys, read from a variable of the test's own.
type KeyCallerSuite struct {
	suite.Suite
	keyVar string
	pool   *keypool.Pool
	vendor *httptest.Server
}

func TestKeyCallerSuite(t *testing.T) {
	suite.Run(t, new(KeyCallerSuite))
}

func (s *KeyCallerSuite) SetupTest() {
	s.vendor = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
	}))
	s.T().Cleanup(s.vendor.Close)
	s.keyVar = "KEY_CALLER_" + rand.Text() + "_API_KEY"
	s.T().Setenv(s.keyVar, "key-caller-"+rand.Text())
	s.pool = keypool.FromEnv("", s.keyVar)
}

// ask sends one request on the pool under ctx, which the vendor refuses.
func (s *KeyCallerSuite) ask(ctx context.Context) error {
	response, err := s.pool.Send(ctx, func(key string) (*http.Response, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.vendor.URL, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		return s.vendor.Client().Do(request)
	})
	if err != nil {
		return err
	}
	return response.Body.Close()
}

// refused is the refusals counted under the suite's variable, by model/owner/product/status.
func (s *KeyCallerSuite) refused() map[string]float64 {
	return keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", s.keyVar)
}

// askingProvider asks the vendor when it starts, as a socket provider dials.
type askingProvider struct {
	stubProvider
	ask func(context.Context) error
}

func (a *askingProvider) Start(ctx context.Context) error { return a.ask(ctx) }

// A provider the router starts is started for the router's modality and the model, so a refusal
// of its key, then or later on the context it keeps, names them.
func (s *KeyCallerSuite) TestAProviderTheRouterStartsNamesItsModalityAndModel() {
	registry := NewRegistry[*askingProvider]()
	registry.Register("vendor", func(Spec) (*askingProvider, error) { return &askingProvider{ask: s.ask}, nil })
	router, err := New(Options[*askingProvider]{
		Modality: TTS,
		Config: ModalityConfig{Providers: []ProviderConfig{
			{Provider: "vendor", Model: "voice-1", Languages: []string{"en"}, Realtime: true, DataPolicy: discard},
		}},
		Registry: registry,
		Logger:   slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)

	provider, _, err := router.Select(s.T().Context(), Request{CustomerID: "acme", Target: "vendor/voice-1"})

	s.Require().NoError(err)
	s.Require().NoError(provider.Close())
	s.Equal(map[string]float64{"voice-1/ours/tts/402": 1}, s.refused())
}

// Work asked for through Call is asked for the recorder's modality and the meter's model; with
// no recorder there is no modality to name.
func (s *KeyCallerSuite) TestWorkAskedForThroughCallNamesTheRecordersModalityAndTheModel() {
	recorder := NewRecorder(Image, nil, nil, nil, slog.New(slog.DiscardHandler))
	s.T().Cleanup(recorder.Close)
	config := ProviderConfig{Provider: "vendor", Model: "picture-1"}
	for _, meter := range []Meter{{Recorder: recorder, Config: config}, {Config: config}} {
		_, err := Call(s.T().Context(), meter, func(ctx context.Context) (asked, error) { return asked{}, s.ask(ctx) })
		s.Require().NoError(err)
	}

	s.Equal(map[string]float64{"picture-1/ours/image/402": 1, "picture-1/ours//402": 1}, s.refused())
}
