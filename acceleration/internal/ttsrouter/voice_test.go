package ttsrouter

import (
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/breeze"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/cartesia"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/elevenlabs"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/fish"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/gemini"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/inworld"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/mock"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/s2pro"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/speechify"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/voices"
)

// vendors is every text-to-speech provider DefaultRegistry and RegisterMock build, so the rule
// is held for each voice a socket can be served by.
var vendors = []string{
	elevenlabs.ProviderName, cartesia.ProviderName, inworld.ProviderName, fish.ProviderName,
	breeze.ProviderName, s2pro.ProviderName, gemini.ProviderName, speechify.ProviderName,
	mock.ProviderName,
}

// voicedStub is a stub that speaks in the voice it was built with, or in a default of its
// own when it was given none, as every vendor's adapter reports it (tts.Voiced).
type voicedStub struct {
	*stubTTS
	voice string
}

func (v *voicedStub) Voice() string { return v.voice }

// defaultVoice is what a stub built for vendor speaks in when it was given no voice.
func defaultVoice(vendor string) string { return "default-" + vendor }

// startVoiced opens a session on vendor alone, behind resolver, and returns the stub serving
// it.
func (s *TTSRouterSuite) startVoiced(vendor string, resolver routing.VoiceResolver, voice string) (*Session, *voicedStub) {
	var built *voicedStub
	registry := NewRegistry()
	registry.Register(vendor, func(spec routing.Spec) (tts.TTS, error) {
		built = &voicedStub{stubTTS: newStubTTS(), voice: spec.Voice}
		if built.voice == "" {
			built.voice = defaultVoice(vendor)
		}
		return built, nil
	})
	config := routing.ModalityConfig{Providers: []routing.ProviderConfig{{
		Provider: vendor, Model: "voice-1", Languages: []string{"en"}, Realtime: true,
		Price: routing.Price{PerMillionChars: 50},
	}}}
	router, err := New(Options{Config: config, Registry: registry, Voices: resolver})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: vendor + "/voice-1", Voice: voice})
	s.Require().NoError(err, vendor)
	s.T().Cleanup(func() { _ = session.Close() })
	return session, built
}

// A voice named for one utterance was never resolved nor checked as this customer's, so one
// that is not the session's is refused before any vendor is asked for it, whoever serves.
func (s *TTSRouterSuite) TestAnUtteranceInAVoiceThatIsNotTheSessionsIsRefusedBeforeTheVendor() {
	for _, vendor := range vendors {
		session, provider := s.startVoiced(vendor, ownVoices{}, "founder")
		s.Require().Equal("el-founder", session.Voice(), vendor)

		for _, voice := range []string{"el-globex", "EL-FOUNDER", "el-founder ", "Founder", defaultVoice(vendor)} {
			err := session.Synthesize(tts.Request{ID: "u1", Text: "hello", Voice: voice, Final: true})

			s.ErrorIs(err, routing.ErrRequest, vendor+" "+voice)
			s.ErrorContains(err, "open a new session", vendor+" "+voice)
		}
		s.Empty(provider.said, vendor+": no utterance in another voice may reach the vendor")
	}
}

// A client may send the session's voice again on each utterance, as the start frame asked
// for it or as the session reported it. The vendor then speaks the voice it was built with
// and is asked for no other.
func (s *TTSRouterSuite) TestAnUtteranceNamingTheSessionsVoiceIsSpokenInIt() {
	for _, vendor := range vendors {
		session, provider := s.startVoiced(vendor, ownVoices{}, "founder")

		for i, voice := range []string{"", "founder", "el-founder"} {
			s.Require().NoError(session.Synthesize(tts.Request{ID: "u" + voice, Text: "hello", Voice: voice, Final: true}),
				vendor+" "+voice)
			s.Require().Len(provider.said, i+1, vendor+" "+voice)
			s.Empty(provider.said[i].Voice, vendor+" "+voice)
		}
		s.Equal("el-founder", provider.Voice(), vendor)
	}
}

// A session opened with no voice speaks the vendor's default, which is its voice as well; a
// voice of the customer's own the start frame never asked for is still another voice.
func (s *TTSRouterSuite) TestASessionOpenedWithNoVoiceTakesOnlyItsDefault() {
	for _, vendor := range vendors {
		session, provider := s.startVoiced(vendor, ownVoices{}, "")

		err := session.Synthesize(tts.Request{ID: "u1", Text: "hello", Voice: "founder", Final: true})
		s.ErrorIs(err, routing.ErrRequest, vendor)
		s.Empty(provider.said, vendor)

		s.Require().NoError(session.Synthesize(tts.Request{ID: "u2", Text: "hello", Voice: defaultVoice(vendor), Final: true}))
		s.Require().Len(provider.said, 1, vendor)
		s.Empty(provider.said[0].Voice, vendor)
	}
}

// A Cartesia id is held to one spelling (voiceid.Canonical), so another case of the
// session's own id is another string and refused, unless the start frame asked it that way.
func (s *TTSRouterSuite) TestACartesiaUtteranceInAnotherSpellingOfTheSessionsVoiceIsRefused() {
	upper := strings.ToUpper(cartesiaVoice)
	mixed := "A0e99841-438c-4a64-b679-aE501e7d6091"

	session, provider := s.startVoiced(cartesia.ProviderName, voices.NewResolver(nil, nil), cartesiaVoice)
	for _, voice := range []string{upper, mixed, strings.ReplaceAll(cartesiaVoice, "-", "")} {
		err := session.Synthesize(tts.Request{ID: "u1", Text: "hello", Voice: voice, Final: true})
		s.ErrorIs(err, routing.ErrRequest, voice)
	}
	s.Empty(provider.said)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u2", Text: "hello", Voice: cartesiaVoice, Final: true}))

	asked, provider := s.startVoiced(cartesia.ProviderName, voices.NewResolver(nil, nil), upper)
	s.Require().Equal(cartesiaVoice, asked.Voice(), "the vendor is handed the one spelling")
	s.Require().ErrorIs(asked.Synthesize(tts.Request{ID: "u1", Text: "hello", Voice: mixed, Final: true}), routing.ErrRequest)
	s.Require().NoError(asked.Synthesize(tts.Request{ID: "u2", Text: "hello", Voice: upper, Final: true}))
	s.Require().Len(provider.said, 1)
	s.Empty(provider.said[0].Voice, "the vendor speaks the spelling it was handed, not the one asked")
}
