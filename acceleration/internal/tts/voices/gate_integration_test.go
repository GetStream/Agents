//go:build integration

package voices

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/blob"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// GateSuite brings voices under the real enforcer, with two vendors' clones answered in process.
type GateSuite struct {
	suite.Suite
	ctx   context.Context
	money *policytest.Money
	owner routing.Owner
	// asked is what each vendor was sent, in order.
	asked map[string][]string
	// entries are the prices the test's work is billed at.
	entries []pricing.Entry
	// enforced says the suite's enforcer refuses, rather than runs in shadow or off.
	enforced bool
	bucket   *blob.Bucket
	service  *Service
	voice    store.Voice
}

func TestGateSuite(t *testing.T) { suite.Run(t, new(GateSuite)) }

func (s *GateSuite) SetupTest() {
	s.entries, s.enforced = prices.Vendor().Entries, true
	s.start()
}

// start runs the suite's enforcer and service, and records a voice of a customer with credit.
func (s *GateSuite) start(change ...func(*policy.Settings)) {
	s.ctx = context.Background()
	s.money = policytest.Start(s.T(), "voices_gate", change...)
	s.asked = map[string][]string{}
	bucket, err := blob.Open(s.ctx, "file://"+s.T().TempDir())
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(bucket.Close()) })
	s.bucket = bucket
	s.service = s.gatedBy(s.money.Enforcer)
	s.own(1_000_000)
}

// own makes the test's customer one with credit and a recorded voice.
func (s *GateSuite) own(credit int64) {
	if s.enforced {
		s.owner = s.money.Owner(s.T(), credit)
	} else {
		app := uuid.NewString()
		s.owner = routing.Owner{CustomerID: app, Attribution: routing.Attribution{OrganizationID: "org-" + app}}
		s.Require().NoError(s.money.Store.AppOrganizations.Upsert(s.ctx,
			&store.AppOrganization{AppID: app, OrganizationID: s.owner.OrganizationID}))
		s.money.Usage.Suspend(s.T(), s.owner.OrganizationID, "review")
		s.money.Enforcer.Join(s.ctx, app, s.owner.OrganizationID, usage.DefaultTier)
	}
	voice, err := s.service.Create(s.ctx, s.owner.CustomerID, "founder", "")
	s.Require().NoError(err)
	s.voice = voice
	s.Require().NoError(s.record())
}

// gatedBy is a service gated by enforcer, over elevenlabs and fish.
func (s *GateSuite) gatedBy(enforcer *policy.Enforcer) *Service {
	serve := func(provider string) string {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.asked[provider] = append(s.asked[provider], r.Method+" "+r.URL.Path)
			_, _ = w.Write([]byte(`{"voice_id":"el-1","_id":"fish-1","state":"trained"}`))
		}))
		s.T().Cleanup(server.Close)
		return server.URL
	}
	cloners := NewRegistry()
	eleven, err := NewElevenLabs(ElevenLabsOptions{APIKey: "unused", BaseURL: serve("elevenlabs")})
	s.Require().NoError(err)
	cloners.Register("elevenlabs", eleven)
	second, err := NewFish(FishOptions{APIKey: "unused", BaseURL: serve("fish")})
	s.Require().NoError(err)
	cloners.Register("fish", second)
	list, err := pricing.NewList("vendor", pricing.AllAtCost(s.entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	service, err := NewService(Options{
		Store: s.money.Store, Bucket: s.bucket, Cloners: cloners, Recorder: s.money.Recorder(routing.TTS, list), Gate: enforcer,
		Models: []routing.ProviderConfig{
			{Provider: "another", Model: "drama-3",
				DataPolicy: options.DataHandling{TrainsOnData: options.ClaimYes, Retention: options.RetentionUnspecified}},
			{Provider: "elevenlabs", Model: "eleven_v4",
				DataPolicy: options.DataHandling{TrainsOnData: options.ClaimYes, Retention: options.RetentionUnspecified}},
			{Provider: "elevenlabs", Model: "eleven_flash_v2_5",
				DataPolicy: options.DataHandling{TrainsOnData: options.ClaimYes, Retention: options.RetentionUnspecified}},
			{Provider: "fish", Model: "drama-3",
				DataPolicy: options.DataHandling{TrainsOnData: options.ClaimNo, Retention: options.RetentionNone}},
		},
	})
	s.Require().NoError(err)
	return service
}

func (s *GateSuite) record() error {
	return s.service.AddSample(s.ctx, s.owner.CustomerID, s.voice.ID, Sample{Name: "a.wav", ContentType: "audio/wav", Content: []byte("speech")})
}

func (s *GateSuite) prepare(providers ...string) error {
	return s.service.Prepare(s.ctx, s.owner.CustomerID, s.voice.ID, providers)
}

func (s *GateSuite) speak(provider string) error {
	_, err := s.service.Speak(s.ctx, s.owner.CustomerID, s.voice.ID, provider, "hello")
	return err
}

// ready is the providers the voice can be spoken through.
func (s *GateSuite) ready() []string {
	bindings, err := s.service.Bindings(s.ctx, s.voice.ID)
	s.Require().NoError(err)
	var ready []string
	for _, binding := range bindings {
		if binding.State == store.VoiceReady {
			ready = append(ready, binding.Provider)
		}
	}
	return ready
}

func (s *GateSuite) suspend() {
	s.money.Usage.Suspend(s.T(), s.owner.OrganizationID, "review")
	s.Require().Eventually(func() bool {
		_, err := s.money.Enforcer.Admit(s.ctx, s.owner)
		return errors.Is(err, policy.ErrSuspended)
	}, 5*time.Second, 5*time.Millisecond, "the suspension is read")
}

func (s *GateSuite) save(document store.PolicyDocument) {
	s.Require().NoError(s.money.Enforcer.Save(s.ctx, store.ScopeOrganization, s.owner.OrganizationID, document))
}

func (s *GateSuite) asks() int { return len(s.asked["elevenlabs"]) + len(s.asked["fish"]) }

func (s *GateSuite) samples() int {
	samples, err := s.service.Samples(s.ctx, s.voice.ID)
	s.Require().NoError(err)
	return len(samples)
}

func (s *GateSuite) TestASuspendedOrganizationNeitherRecordsNorClonesNorHearsAVoice() {
	s.Require().NoError(s.prepare())
	before := s.asks()
	s.suspend()

	s.ErrorIs(s.record(), policy.ErrSuspended)
	s.ErrorIs(s.prepare(), policy.ErrSuspended)
	s.ErrorIs(s.prepare("fish"), policy.ErrSuspended)
	s.ErrorIs(s.speak("elevenlabs"), policy.ErrSuspended)

	s.Equal(1, s.samples())
	s.Equal(before, s.asks(), "no vendor is asked")
	s.ElementsMatch([]string{"elevenlabs", "fish"}, s.ready(), "what was cloned before stays usable")
}

func (s *GateSuite) TestARestoredOrganizationHearsTheVoiceItCloned() {
	s.Require().NoError(s.prepare())
	s.suspend()
	s.Require().ErrorIs(s.prepare(), policy.ErrSuspended)
	s.money.Usage.Restore(s.T(), s.owner.OrganizationID)

	s.Eventually(func() bool { return s.speak("elevenlabs") == nil }, 5*time.Second, 20*time.Millisecond)
}

func (s *GateSuite) TestWithNoCreditAVoiceIsClonedForFreeAndNotHeard() {
	s.own(0)

	s.Require().NoError(s.record(), "a recording costs nothing")
	s.Require().NoError(s.prepare("elevenlabs"), "nor does a clone")
	before := s.asks()
	s.ErrorIs(s.speak("elevenlabs"), policy.ErrNoCredits)
	s.Equal(before, s.asks())
}

func (s *GateSuite) TestACloneThatCostsMoneyNeedsCredit() {
	s.entries = slices.Clone(s.entries)
	for i, entry := range s.entries {
		if entry.Provider == "elevenlabs" && entry.Model == "voice_clone" {
			s.entries[i].Free = false
			s.entries[i].Rates = map[pricing.Meter]pricing.Rate{pricing.Clones: {USD: 1, Per: 1}}
		}
	}
	s.service = s.gatedBy(s.money.Enforcer)
	s.own(0)

	s.ErrorIs(s.prepare("elevenlabs"), policy.ErrNoCredits)
	s.Empty(s.asked["elevenlabs"])
	s.Require().NoError(s.prepare(), "the vendor that clones for free has it")
	s.Equal([]string{"fish"}, s.ready())
}

// spokenAt prices the model a preview speaks with as entry, or not at all.
func (s *GateSuite) spokenAt(entry ...pricing.Entry) {
	s.entries = slices.DeleteFunc(slices.Clone(s.entries), func(entry pricing.Entry) bool { return entry.Model == "eleven_flash_v2_5" })
	s.entries = append(s.entries, entry...)
	s.service = s.gatedBy(s.money.Enforcer)
}

func (s *GateSuite) TestALineThatCostsNothingIsHeardWithNoCreditAndNotWhenSuspended() {
	s.spokenAt(pricing.Entry{Modality: "tts", Provider: "elevenlabs", Model: "eleven_flash_v2_5", Region: pricing.RegionUS, Free: true})
	s.own(0)
	s.Require().NoError(s.prepare("elevenlabs"))

	s.NoError(s.speak("elevenlabs"))
	s.suspend()
	s.ErrorIs(s.speak("elevenlabs"), policy.ErrSuspended)
}

func (s *GateSuite) TestAnUnpricedLineNeedsCredit() {
	s.spokenAt()
	s.Require().NoError(s.prepare("elevenlabs"))
	s.NoError(s.speak("elevenlabs"))

	s.own(0)
	s.Require().NoError(s.prepare("elevenlabs"))
	s.ErrorIs(s.speak("elevenlabs"), policy.ErrNoCredits)
}

func (s *GateSuite) TestAnUnpricedCloneIsMadeForACustomerWithCredit() {
	var priced []pricing.Entry
	for _, entry := range s.entries {
		if entry.Model != "voice_clone" {
			priced = append(priced, entry)
		}
	}
	s.entries = priced
	s.service = s.gatedBy(s.money.Enforcer)

	s.Require().NoError(s.prepare())
	s.ElementsMatch([]string{"elevenlabs", "fish"}, s.ready())
}

func (s *GateSuite) TestAVoiceIsClonedOnlyAtVendorsWhoseModelsAreAllowed() {
	s.save(store.PolicyDocument{AllowedModels: &[]string{"fish/drama-3"}})

	s.Require().NoError(s.prepare())

	s.Equal([]string{"fish"}, s.ready())
	s.Empty(s.asked["elevenlabs"])
	s.ErrorIs(s.prepare("fish", "elevenlabs"), routing.ErrModelNotAllowed)
	s.Len(s.asked["fish"], 1, "a named vendor that is refused refuses them all")
	s.NoError(s.speak("fish"))
}

func (s *GateSuite) TestAVoiceIsHeardOnlyOnAModelThatIsAllowed() {
	s.Require().NoError(s.prepare())
	before := s.asks()
	s.save(store.PolicyDocument{AllowedModels: &[]string{"elevenlabs/eleven_v4"}})

	s.ErrorIs(s.speak("elevenlabs"), routing.ErrModelNotAllowed, "a preview speaks eleven_flash_v2_5")
	s.Equal(before, s.asks())
	s.NoError(s.prepare("elevenlabs"), "another of the vendor's models may speak in it")
}

func (s *GateSuite) TestNoModelAllowedClonesNowhere() {
	s.save(store.PolicyDocument{AllowedModels: &[]string{}})

	s.ErrorIs(s.prepare(), routing.ErrModelNotAllowed)
	s.Zero(s.asks())
	s.Empty(s.ready())
}

func (s *GateSuite) TestAVoiceGoesOnlyToVendorsTheDataPolicyPermits() {
	s.Require().NoError(s.prepare())
	before := len(s.asked["elevenlabs"])
	no := false
	s.save(store.PolicyDocument{DataPolicy: store.PolicyDataPolicy{AllowTraining: &no, Retention: options.RetentionNone}})

	s.ErrorContains(s.speak("elevenlabs"), "data policy")
	s.ErrorContains(s.prepare("elevenlabs"), "data policy")
	s.Require().NoError(s.prepare())
	s.Len(s.asked["elevenlabs"], before)
	s.NoError(s.speak("fish"))
	s.ElementsMatch([]string{"elevenlabs", "fish"}, s.ready(), "an earlier clone is kept")
}

func (s *GateSuite) TestARouterThatHasReadNothingRefusesASuspendedOrganization() {
	s.Require().NoError(s.prepare())
	s.suspend()
	before := s.asks()
	client, err := usage.NewClient(usage.ClientOptions{URL: s.money.Usage.URL, Token: s.money.Usage.Token})
	s.Require().NoError(err)
	cold, err := policy.New(s.money.Store, policy.Settings{Usage: client, FirstRead: 5 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Require().NoError(err)
	defer cold.Close()
	s.service = s.gatedBy(cold)

	s.ErrorIs(s.speak("elevenlabs"), policy.ErrSuspended)
	s.Equal(before, s.asks())
}

func (s *GateSuite) TestInShadowMoneyRefusesNothing() {
	s.enforced = false
	s.start(func(settings *policy.Settings) { settings.Mode = config.LimitsShadow })
	s.Require().Eventually(func() bool {
		_, read := s.money.Enforcer.Balance(s.owner.CustomerID, s.owner.OrganizationID)
		return read
	}, 5*time.Second, 5*time.Millisecond, "the suspension is read")
	s.admitsEverything()
}

func (s *GateSuite) TestWithLimitsOffMoneyRefusesNothing() {
	s.enforced = false
	s.start(func(settings *policy.Settings) { settings.Mode = config.LimitsOff })
	s.admitsEverything()
}

func (s *GateSuite) admitsEverything() {
	s.NoError(s.record())
	s.NoError(s.prepare())
	s.NoError(s.speak("elevenlabs"))
	s.True(strings.HasPrefix(s.asked["fish"][0], "POST"))
}
