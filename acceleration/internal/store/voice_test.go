//go:build integration

package store

import (
	"errors"
	"time"
)

func (s *StoreSuite) TestAVoiceIsReadBackByIdAndByTheNameItWasGiven() {
	voice := Voice{CustomerID: "acme", Name: "founder", Description: "the one from the ad"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	byID, err := s.store.Voices.Get(s.ctx, "acme", voice.ID)
	s.Require().NoError(err)
	s.Equal("founder", byID.Name)
	s.Equal("the one from the ad", byID.Description)

	byName, err := s.store.Voices.GetByName(s.ctx, "acme", "founder")
	s.Require().NoError(err)
	s.Equal(voice.ID, byName.ID)
}

func (s *StoreSuite) TestANameNoVoiceHoldsIsCachedForAMinuteOnly() {
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &Voice{CustomerID: "acme", Name: "founder"}))
	_, err := s.store.Voices.GetByName(s.ctx, "acme", "founder")
	s.Require().NoError(err)
	_, err = s.store.Voices.GetByName(s.ctx, "acme", "narrator")
	s.Require().ErrorIs(err, ErrNoVoice)

	s.Positive(s.expiryOf(voiceNameKey("acme", "narrator")), "the miss is cached")
	s.LessOrEqual(s.expiryOf(voiceNameKey("acme", "narrator")), time.Minute)
	s.Greater(s.expiryOf(voiceNameKey("acme", "founder")), 59*time.Minute, "a voice found is still cached for the hour")
}

func (s *StoreSuite) TestOneCustomersVoiceIsNotAnothersToRead() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	_, err := s.store.Voices.Get(s.ctx, "globex", voice.ID)
	s.True(errors.Is(err, ErrNoVoice), "a voice id guessed at by another customer is not found")
}

func (s *StoreSuite) TestADeletedVoiceStopsBeingFound() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	s.Require().NoError(s.store.Voices.Delete(s.ctx, "acme", voice.ID))

	_, err := s.store.Voices.Get(s.ctx, "acme", voice.ID)
	s.True(errors.Is(err, ErrNoVoice))
}

func (s *StoreSuite) TestADeletedNameCanBeUsedAgain() {
	first := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &first))
	s.Require().NoError(s.store.Voices.Delete(s.ctx, "acme", first.ID))

	second := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &second),
		"the name is only taken while the voice it belongs to is still there")
}

func (s *StoreSuite) TestAnInvalidVoiceIsNotWritten() {
	var invalid *ValidationError
	s.Require().ErrorAs(s.store.Voices.Insert(s.ctx, &Voice{CustomerID: "acme"}), &invalid)
	s.Equal("name", invalid.Field)

	listed, err := s.store.Voices.Select(s.ctx, "acme")
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestARenamedVoiceIsNoLongerFoundUnderItsOldName() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	_, err := s.store.Voices.GetByName(s.ctx, "acme", "founder")
	s.Require().NoError(err)
	_, err = s.store.Voices.GetByName(s.ctx, "acme", "narrator")
	s.ErrorIs(err, ErrNoVoice)

	voice.Name = "narrator"
	s.Require().NoError(s.store.Voices.Update(s.ctx, &voice))

	_, err = s.store.Voices.GetByName(s.ctx, "acme", "founder")
	s.ErrorIs(err, ErrNoVoice)
	found, err := s.store.Voices.GetByName(s.ctx, "acme", "narrator")
	s.Require().NoError(err)
	s.Equal(voice.ID, found.ID)
	stolen := voice
	stolen.CustomerID = "globex"
	s.ErrorIs(s.store.Voices.Update(s.ctx, &stolen), ErrNoVoice)
	s.ErrorIs(s.store.Voices.Delete(s.ctx, "globex", voice.ID), ErrNoVoice)
}

func (s *StoreSuite) TestVoiceQueryBudgets() {
	for range 20 {
		s.Require().NoError(s.store.Voices.Insert(s.ctx, &Voice{CustomerID: "acme", Name: "v" + newID()}))
	}
	voice := Voice{CustomerID: "acme", Name: "founder"}

	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.Voices.Select(s.ctx, "acme")
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Voices.GetByName(s.ctx, "acme", "founder")
		s.Require().NoError(err)
	}))
	voice.Description = "the one from the ad"
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Voices.Update(s.ctx, &voice)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	for _, provider := range []string{"elevenlabs", "cartesia"} {
		binding := VoiceBinding{VoiceID: voice.ID, Provider: provider, ExternalID: "x", State: VoiceReady}
		s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))
	}
	if s.store.cache != nil {
		for range 2 {
			_, err := s.store.Voices.Get(s.ctx, "acme", voice.ID)
			s.Require().NoError(err)
		}
		s.Equal(0, s.countQueries(func() {
			s.Equal(0, s.countRedis(func() {
				_, err := s.store.Voices.Get(s.ctx, "acme", voice.ID)
				s.Require().NoError(err)
			}))
		}))
		s.Equal(3, s.countQueries(func() {
			s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.Voices.Delete(s.ctx, "acme", voice.ID)) }),
				"forgetting the voice, its name and every provider's binding is one round trip")
		}), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
		return
	}
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Voices.Delete(s.ctx, "acme", voice.ID)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
}
