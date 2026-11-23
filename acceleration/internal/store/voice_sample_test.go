//go:build integration

package store

func (s *StoreSuite) TestSamplesComeBackInTheOrderTheyWereGiven() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	for _, key := range []string{"one.wav", "two.wav", "three.wav"} {
		sample := VoiceSample{VoiceID: voice.ID, ObjectKey: key, Bytes: 10}
		s.Require().NoError(s.store.VoiceSamples.Insert(s.ctx, &sample))
	}

	samples, err := s.store.VoiceSamples.Select(s.ctx, voice.ID)
	s.Require().NoError(err)
	s.Require().Len(samples, 3)
	s.Equal("one.wav", samples[0].ObjectKey)
	s.Equal("three.wav", samples[2].ObjectKey)
}

func (s *StoreSuite) TestAnInvalidVoiceSampleIsNotWritten() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))

	var invalid *ValidationError
	s.Require().ErrorAs(s.store.VoiceSamples.Insert(s.ctx, &VoiceSample{VoiceID: voice.ID, Bytes: 10}), &invalid)
	s.Equal("object_key", invalid.Field)
	samples, err := s.store.VoiceSamples.Select(s.ctx, voice.ID)
	s.Require().NoError(err)
	s.Empty(samples)
}

func (s *StoreSuite) TestVoiceSampleQueryBudgets() {
	voice := Voice{CustomerID: "acme", Name: "founder"}
	s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
	for range 20 {
		s.Require().NoError(s.store.VoiceSamples.Insert(s.ctx, &VoiceSample{VoiceID: voice.ID, ObjectKey: newID() + ".wav"}))
	}

	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.VoiceSamples.Insert(s.ctx, &VoiceSample{VoiceID: voice.ID, ObjectKey: "last.wav"}))
	}))
	s.Equal(1, s.countQueries(func() {
		samples, err := s.store.VoiceSamples.Select(s.ctx, voice.ID)
		s.Require().NoError(err)
		s.Len(samples, 21)
	}))
}
