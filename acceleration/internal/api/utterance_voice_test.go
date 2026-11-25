//go:build integration

package api

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/voices"
)

// suiteVoices is the resolver cmd/router builds, over the suite's database, which opens only
// after the routers are configured.
type suiteVoices struct{ suite *RouterSuite }

func (v suiteVoices) ResolveVoice(ctx context.Context, customerID, provider, voice string) (string, error) {
	return voices.NewResolver(v.suite.store, nil).ResolveVoice(ctx, customerID, provider, voice)
}

// UtteranceVoiceSuite is the text-to-speech socket with a voice named on an utterance, where
// the app has a voice of its own, founder, and another customer has one too, each prepared
// for the stub provider.
type UtteranceVoiceSuite struct {
	RouterSuite
	// own is what the stub provider knows the app's founder as, and theirs the other
	// customer's voice.
	own, theirs string
}

func TestUtteranceVoiceSuite(t *testing.T) {
	suite := new(UtteranceVoiceSuite)
	suite.ttsVoices = suiteVoices{suite: &suite.RouterSuite}
	runSuite(t, suite)
}

func (s *UtteranceVoiceSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.own = s.prepared(s.customerID())
	s.theirs = s.prepared("globex-" + rand.Text())
}

// The other customer's voice is refused when a socket is opened in it, so it must be
// refused when named on one utterance of a socket opened in the app's own.
func (s *UtteranceVoiceSuite) TestAnUtteranceInAnotherCustomersVoiceIsRefusedAndNotSpoken() {
	opened := s.serverClient.opens("/v1/tts/stream")
	s.Require().NoError(opened.WriteJSON(frame{"type": "start", "target": "en-low-latency", "voice": s.theirs}))
	s.Contains(s.await(opened, "error")["error"], "not one of this customer's voices")

	speaking := s.speaking("founder")
	refused, after := "refused "+rand.Text(), "after "+rand.Text()

	s.Require().NoError(speaking.WriteJSON(frame{"type": "speak", "id": "s1", "text": refused, "voice": s.theirs}))
	s.Contains(s.await(speaking, "error")["error"], "open a new session")
	s.Require().NoError(speaking.WriteJSON(frame{"type": "speak", "id": "s2", "text": after}))
	s.await(speaking, "synthesis_complete")

	s.NotContains(s.voice.spoken(), refused)
	s.Contains(s.voice.spoken(), after, "the socket goes on in its own voice")
}

// A client sends the session's voice again on each utterance, as the start frame named it or
// as the started frame reported it.
func (s *UtteranceVoiceSuite) TestAnUtteranceInTheSessionsOwnVoiceIsSpoken() {
	speaking := s.speaking("founder")

	for _, voice := range []string{"founder", s.own} {
		line := "line " + rand.Text()
		s.Require().NoError(speaking.WriteJSON(frame{"type": "speak", "id": rand.Text(), "text": line, "voice": voice}))
		s.await(speaking, "synthesis_complete")
		s.Contains(s.voice.spoken(), line, voice)
	}
}

// speaking opens a text-to-speech socket in voice and waits for it to start in the app's own.
func (s *UtteranceVoiceSuite) speaking(voice string) *websocket.Conn {
	connection := s.serverClient.opens("/v1/tts/stream")
	s.Require().NoError(connection.WriteJSON(frame{"type": "start", "target": "en-low-latency", "voice": voice}))
	s.Require().Equal(s.own, s.await(connection, "started")["voice"])
	return connection
}

// prepared stores a voice named founder for customerID, ready at the stub provider, and
// returns what the provider knows it as.
func (s *UtteranceVoiceSuite) prepared(customerID string) string {
	ctx := context.Background()
	voice := store.Voice{CustomerID: customerID, Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(ctx, &voice))
	external := "stub-" + rand.Text()
	s.Require().NoError(s.store.VoiceBindings.Upsert(ctx, &store.VoiceBinding{
		VoiceID: voice.ID, Provider: "stub", ExternalID: external, State: store.VoiceReady,
	}))
	return external
}
