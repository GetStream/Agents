package sts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
)

type STSSuite struct {
	suite.Suite
}

func TestSTSSuite(t *testing.T) {
	suite.Run(t, new(STSSuite))
}

func (s *STSSuite) TestCapabilitiesExpressOnlyWhatTheyDeclare() {
	all := Capabilities{
		Text: true, Tools: true, InputTranscript: true, OutputTranscript: true,
		SemanticTurns: true, ManualTurns: true, Endpointing: true, FullDuplex: true,
	}
	for _, term := range []options.Term{
		options.SemanticTurns, options.FullDuplex, options.ManualTurns, options.Endpointing, options.Tools,
		options.TextInput, options.InputTranscript, options.OutputTranscript,
	} {
		s.True(all.Expresses(term), "%s should be expressible when declared", term)
		s.False(Capabilities{}.Expresses(term), "%s should be refused when not declared", term)
	}
	s.False(all.Expresses(options.Diarize), "a transcriber's term means nothing to a speech-to-speech model")
}

func (s *STSSuite) TestAcceptsTreatsAudioAndTextAsImplicit() {
	sees := Capabilities{InputModalities: []string{options.ModalityImage}}
	s.True(sees.Accepts("audio"))
	s.True(sees.Accepts("text"))
	s.True(sees.Accepts(options.ModalityImage))
	s.False(Capabilities{}.Accepts(options.ModalityImage))
}

func (s *STSSuite) TestATurnMeasuresTheWaitFromWhenTheCallerStopped() {
	heardAt := time.Now().Add(-200 * time.Millisecond)
	turn := NewTurn("resp_1", 3, heardAt)

	chunk := turn.Chunk(audio.PcmData{Samples: make([]int16, 2400), SampleRate: 24_000, Channels: 1})
	s.Equal("resp_1", chunk.ResponseID)
	s.Equal(3, chunk.Generation)
	s.Equal(0, chunk.Index)

	second := turn.Chunk(audio.PcmData{Samples: make([]int16, 2400), SampleRate: 24_000, Channels: 1})
	s.Equal(1, second.Index)
	s.Equal(200, turn.SentMs())

	complete := turn.Complete("openai", "gpt-realtime-2", false, Usage{InputTokens: 10, OutputTokens: 20})
	s.Equal("resp_1", complete.ResponseID)
	s.Equal(3, complete.Generation)
	s.InDelta(200, complete.AudioDurationMs, 0.001)
	s.GreaterOrEqual(complete.TimeToFirstByteMs, 200.0, "the wait started when the caller stopped, not when the reply began")
	s.Equal(int64(10), complete.Usage.InputTokens)
	s.False(complete.Interrupted)
}

// Catches a wait measured as seconds times a thousand: 2.235932 s comes out as
// 2235.9320000000002 ms in float64, and the wait is a whole number of the microseconds it is
// measured in.
func (s *STSSuite) TestAWaitCarriesNoFloatNoise() {
	heardAt := time.Now()
	wait := 2235932 * time.Microsecond
	s.NotEqual(2235.932, wait.Seconds()*1000, "seconds times a thousand carries float noise")
	turn := NewTurn("resp_1", 1, heardAt)
	turn.firstByteAt = heardAt.Add(wait)

	s.Equal(2235.932, turn.Complete("openai", "gpt-realtime-2", false, Usage{}).TimeToFirstByteMs)
}

func (s *STSSuite) TestAReplyNobodyWaitedForReportsNoWait() {
	turn := NewTurn("", 1, time.Time{})
	another := NewTurn("", 1, time.Time{})
	s.NotEmpty(turn.ID, "a vendor without response ids gets a generated ID")
	s.NotEqual(turn.ID, another.ID, "a fresh session cannot reuse an earlier generation's id")

	turn.Chunk(audio.PcmData{Samples: make([]int16, 240), SampleRate: 24_000, Channels: 1})
	complete := turn.Complete("gemini", "live", true, Usage{})
	s.Zero(complete.TimeToFirstByteMs)
	s.True(complete.Interrupted)
}
