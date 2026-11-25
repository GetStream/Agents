//go:build integration

package store

import (
	"crypto/rand"
	"errors"
	"strings"
)

func (s *StoreSuite) TestPreparingAVoiceTwiceLeavesOneAnswerPerProvider() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	first := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "old", State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &first))
	second := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "new", State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &second))

	bindings, err := s.store.VoiceBindings.Select(s.ctx, voice.ID)
	s.Require().NoError(err)
	s.Require().Len(bindings, 1, "two answers to which id to use would be one too many")
	s.Equal("new", bindings[0].ExternalID)
}

func (s *StoreSuite) TestOnlyAReadyBindingIsHandedToASession() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	ready := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "el-1", State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &ready))
	failed := VoiceBinding{VoiceID: voice.ID, Provider: "cartesia", Error: "too quiet", State: VoiceFailed}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &failed))

	external, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
	s.Require().NoError(err)
	s.Equal("el-1", external)

	_, err = s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "cartesia")
	s.True(errors.Is(err, ErrNoVoice),
		"speaking in the wrong voice is worse than not speaking through this provider")
}

func (s *StoreSuite) TestABindingOfADeletedVoiceIsNoLongerHandedOut() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "el-1", State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))
	s.Require().NoError(s.store.Voices.Delete(s.ctx, "acme", voice.ID))

	_, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
	s.True(errors.Is(err, ErrNoVoice))
}

// A failed binding's error is a provider's refusal, up to 2048 bytes of its body, which can
// echo the key it was sent, and the caller reads it back: it is kept without the key, and cut
// as the requests table cuts an error. One with no key is kept as it was.
func (s *StoreSuite) TestAFailedBindingsErrorIsKeptWithoutTheKeyItEchoed() {
	key := "sk-test-" + rand.Text()
	for failure, kept := range map[string]string{
		"elevenlabs: 401 Unauthorized\nAuthorization: Bearer " + key: "elevenlabs: 401 Unauthorized\n[redacted]",
		"elevenlabs: too quiet":   "elevenlabs: too quiet",
		strings.Repeat("x", 9000): strings.Repeat("x", 2048) + "… [truncated]",
	} {
		voice := Voice{CustomerID: "acme", Name: "founder " + rand.Text()}
		s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

		binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", State: VoiceFailed, Error: failure}
		s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))

		bindings, err := s.store.VoiceBindings.Select(s.ctx, voice.ID)
		s.Require().NoError(err)
		s.Require().Len(bindings, 1)
		s.Equal(kept, bindings[0].Error)
	}
}

func (s *StoreSuite) TestAnInvalidVoiceBindingIsNotWritten() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	var invalid *ValidationError
	binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", State: "lost"}
	s.Require().ErrorAs(s.store.VoiceBindings.Upsert(s.ctx, &binding), &invalid)
	s.Equal("state", invalid.Field)
	bindings, err := s.store.VoiceBindings.Select(s.ctx, voice.ID)
	s.Require().NoError(err)
	s.Empty(bindings)
}

// A ready binding is cached for every session that speaks in the voice, so a binding that
// fails afterwards has to stop being handed out at once.
func (s *StoreSuite) TestABindingThatFailsAfterBeingReadyIsNoLongerHandedOut() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	ready := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "el-1", State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &ready))
	external, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
	s.Require().NoError(err)
	s.Equal("el-1", external)

	failed := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", State: VoiceFailed, Error: "revoked"}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &failed))

	_, err = s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
	s.ErrorIs(err, ErrNoVoice)
	_, err = s.store.VoiceBindings.GetReady(s.ctx, "globex", voice.ID, "elevenlabs")
	s.ErrorIs(err, ErrNoVoice, "another customer's voice is not handed out")
}

func (s *StoreSuite) TestAProvidersVoiceIdIsTheCustomersWhoseVoiceItIs() {
	owner, other := newID(), newID()
	voice := Voice{CustomerID: owner, Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	external := "el-" + newID()
	binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: external, State: VoiceReady}
	s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))
	s.Require().NoError(s.store.Voices.Delete(s.ctx, owner, voice.ID))

	elsewhere, err := s.store.VoiceBindings.Elsewhere(s.ctx, other, "elevenlabs", external)
	s.Require().NoError(err)
	s.True(elsewhere, "a deleted voice stays its customer's")
	elsewhere, err = s.store.VoiceBindings.Elsewhere(s.ctx, owner, "elevenlabs", external)
	s.Require().NoError(err)
	s.False(elsewhere)
	elsewhere, err = s.store.VoiceBindings.Elsewhere(s.ctx, other, "cartesia", external)
	s.Require().NoError(err)
	s.False(elsewhere, "the same id at another provider is another voice")

	mine, err := s.store.VoiceBindings.SelectByCustomer(s.ctx, owner)
	s.Require().NoError(err)
	s.Require().Len(mine, 1)
	s.Equal(external, mine[0].ExternalID)
	theirs, err := s.store.VoiceBindings.SelectByCustomer(s.ctx, other)
	s.Require().NoError(err)
	s.Empty(theirs)
}

func (s *StoreSuite) TestVoiceBindingQueryBudgets() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	for range 20 {
		binding := VoiceBinding{VoiceID: voice.ID, Provider: "p" + newID(), ExternalID: "x", State: VoiceReady}
		s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))
	}
	binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "el-1", State: VoiceReady}

	s.Equal(3+s.redisTrips(1), s.countQueries(func() { s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding)) }), "begin, the write, its transaction's id when there is a cache, and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(1, s.countQueries(func() {
		bindings, err := s.store.VoiceBindings.Select(s.ctx, voice.ID)
		s.Require().NoError(err)
		s.Len(bindings, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.VoiceBindings.Elsewhere(s.ctx, "globex", "elevenlabs", "el-1")
		s.Require().NoError(err)
	}), "Elsewhere")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.VoiceBindings.SelectByCustomer(s.ctx, "acme")
		s.Require().NoError(err)
	}), "SelectByCustomer")
	if s.store.cache == nil {
		s.Equal(1, s.countQueries(func() {
			_, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
			s.Require().NoError(err)
		}))
		return
	}
	for range 2 {
		_, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
		s.Require().NoError(err)
	}
	s.Equal(0, s.countQueries(func() {
		s.Equal(0, s.countRedis(func() {
			_, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
			s.Require().NoError(err)
		}))
	}))
	s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding)) }))
}
