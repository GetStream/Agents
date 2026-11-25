package voiceid_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/cartesia"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/elevenlabs"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/voiceid"
)

type VoiceIDSuite struct {
	suite.Suite
}

func TestVoiceID(t *testing.T) {
	suite.Run(t, new(VoiceIDSuite))
}

// The ids every vendor has handed out or documents: refusing one breaks a real voice.
func (s *VoiceIDSuite) TestRealIdsPass() {
	for _, id := range []string{
		elevenlabs.DefaultVoiceID,
		"21m00Tcm4TlvDq8ikWAM", // the docs' example
		"cVOjvbxC2c4mOyGcx3AH", // a Voice Library voice
		"pNInz6obpgDQGcFmaJgB",
		"el-77",  // the clone ids our fakes answer with
		"a_b",    // '_' is unreserved
		"Rachel", // a name still goes to ElevenLabs, which refuses it as before
		strings.Repeat("a", 128),
	} {
		s.NoError(voiceid.Check(elevenlabs.ProviderName, id), id)
		s.NoError(voiceid.ElevenLabs(id), id)
	}
	for _, id := range []string{
		cartesia.DefaultVoiceID,
		"a0e99841-438c-4a64-b679-ae501e7d6091",
		"5619d38c-cf51-4d8e-9575-48f61a280413",
	} {
		s.NoError(voiceid.Check(cartesia.ProviderName, id), id)
		s.NoError(voiceid.Cartesia(id), id)
	}
}

func (s *VoiceIDSuite) TestAnElevenLabsIdThatCouldLeaveItsPathSegmentIsRefused() {
	for _, id := range []string{
		"",
		"..",
		".",
		"a.b",
		"abc/def",
		"abc?x=1",
		"abc#x",
		"abc%2Fdef",
		"abc def",
		" abc",
		"abc\n",
		"abc\t",
		"abc\x00",
		"abc&model_id=x",
		"abc;x",
		"ab~c",
		"abc+def",
		"абв", // letters, but not ASCII ones
		strings.Repeat("a", 129),
	} {
		err := voiceid.Check(elevenlabs.ProviderName, id)
		s.ErrorIs(err, voiceid.ErrInvalid, "%q", id)
		s.ErrorIs(voiceid.ElevenLabs(id), voiceid.ErrInvalid, "%q", id)
	}
}

func (s *VoiceIDSuite) TestACartesiaIdThatIsNotACanonicalUuidIsRefused() {
	for _, id := range []string{
		"",
		"x",
		"ct-42",
		"A0E99841-438C-4A64-B679-AE501E7D6091", // the same voice in upper case
		"a0e99841-438c-4a64-b679-ae501e7d609A",
		"a0e99841438c4a64b679ae501e7d6091", // the same voice unhyphenated
		"{a0e99841-438c-4a64-b679-ae501e7d6091}",
		"urn:uuid:a0e99841-438c-4a64-b679-ae501e7d6091",
		"a0e99841-438c-4a64-b679-ae501e7d6091/../x",
		"a0e99841-438c-4a64-b679-ae501e7d6091?x=1",
		"a0e99841-438c-4a64-b679-ae501e7d6091#x",
		"a0e99841-438c-4a64-b679-ae501e7d6091 ",
		"a0e99841-438c-4a64-b679-ae501e7d60911",
		"a0e99841-438c-4a64-b679-ae501e7d609",
		"g0e99841-438c-4a64-b679-ae501e7d6091",
		"..",
	} {
		s.ErrorIs(voiceid.Check(cartesia.ProviderName, id), voiceid.ErrInvalid, "%q", id)
		s.ErrorIs(voiceid.Cartesia(id), voiceid.ErrInvalid, "%q", id)
	}
}

func (s *VoiceIDSuite) TestTheErrorNamesTheProviderAndTheId() {
	s.EqualError(voiceid.Check(elevenlabs.ProviderName, "a/b"), `voiceid: not a voice id of elevenlabs: "a/b"`)
}

// A provider that puts its voice in a body or an escaped query has no rule: its voices are
// names as well as ids (Inworld's Ashley, Gemini's Kore), as on the base.
func (s *VoiceIDSuite) TestAnyIdPassesForAProviderWithNoRule() {
	for _, provider := range []string{"inworld", "gemini", "fish", "speechify", "", "ElevenLabs"} {
		for _, id := range []string{"Ashley", "a/b?c#d", "..", "", "A0E99841-438C-4A64-B679-AE501E7D6091"} {
			s.NoError(voiceid.Check(provider, id), "%s %q", provider, id)
		}
	}
}
