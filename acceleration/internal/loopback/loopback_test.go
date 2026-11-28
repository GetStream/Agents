package loopback

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

type LoopbackSuite struct {
	suite.Suite
}

func TestLoopbackSuite(t *testing.T) {
	suite.Run(t, new(LoopbackSuite))
}

// half a second of audio, which is long enough that pacing it is measurable and short
// enough that a test waiting for it is not.
func speech(ms int) audio.PcmData {
	return audio.PcmData{
		Samples:    make([]int16, stt.SampleRate*ms/1000),
		SampleRate: stt.SampleRate,
		Channels:   1,
	}
}

func (s *LoopbackSuite) TestSpeechIsHeldOntoUntilItWouldHaveBeenHeard() {
	edge := New(Options{PlayoutAhead: 100 * time.Millisecond})

	started := time.Now()
	s.Require().NoError(edge.PublishAudio(speech(500)))
	took := time.Since(started)

	// Half a second of speech, of which 100ms may be waiting to be heard: publishing has
	// to hold onto the rest rather than hand it over at once. An edge that swallowed it
	// would leave the agent believing it had finished talking before anybody heard it.
	s.Greater(took, 300*time.Millisecond)
	s.True(edge.Talking(), "the last of it has not been heard yet")
}

func (s *LoopbackSuite) TestAnEdgeIsQuietOnceEverythingHasBeenHeard() {
	// Allowing more to be queued than is published means this returns at once, so what is
	// asserted afterwards is the playhead rather than the sleep.
	edge := New(Options{PlayoutAhead: time.Second})

	s.Require().NoError(edge.PublishAudio(speech(150)))

	s.True(edge.Talking(), "speech that has not finished being heard is still speech")
	s.Eventually(func() bool { return !edge.Talking() }, time.Second, 10*time.Millisecond,
		"the floor is given up once the last of it has been heard")
}

func (s *LoopbackSuite) TestSomebodyOnTheOtherEndHearsWhatTheAgentSaid() {
	var heard int
	edge := New(Options{
		PlayoutAhead: time.Second,
		Published:    func(pcm audio.PcmData) { heard += len(pcm.Samples) },
	})

	s.Require().NoError(edge.PublishAudio(speech(100)))

	s.Equal(stt.SampleRate*100/1000, heard)
}

func (s *LoopbackSuite) TestAMicrophoneCarriesTheCallEvenWhenNobodyIsTalking() {
	edge := New(Options{})
	mic := edge.Microphone(stt.Participant{ID: "caller"}, audio.PcmData{})
	defer mic.Stop()

	// A real call carries the room, so an agent waiting for audio to arrive is not left
	// waiting between sentences.
	var packets int
	deadline := time.After(500 * time.Millisecond)
	for packets < 3 {
		select {
		case <-edge.Audio():
			packets++
		case <-deadline:
			s.FailNowf("the call went silent", "got %d packets", packets)
		}
	}
	s.Equal(3, packets)
}

func (s *LoopbackSuite) TestPlayingAnUtteranceReturnsWhenTheSpeakerStoppedTalking() {
	edge := New(Options{Inbound: 256})
	mic := edge.Microphone(stt.Participant{ID: "caller"}, audio.PcmData{})
	defer mic.Stop()

	// Drain, because a microphone whose packets nobody takes stops being paced by the
	// clock and starts being paced by the reader.
	go func() {
		for range edge.Audio() {
		}
	}()

	started := time.Now()
	at := mic.Play(speech(200))

	s.Greater(time.Since(started), 150*time.Millisecond,
		"an utterance is spliced into the call at the rate it is spoken")
	s.False(at.IsZero())
}

func (s *LoopbackSuite) TestAnEdgeThatHasLeftTellsTheAgentTheCallIsOver() {
	edge := New(Options{})

	s.Require().NoError(edge.Leave())

	_, open := <-edge.Audio()
	s.False(open, "the channel closing is what says the call ended")
	// Leaving twice is what a teardown that already happened looks like.
	s.Require().NoError(edge.Leave())
}

// onFakeClock runs body in a synctest bubble, where the edge's pacing takes no real time.
func (s *LoopbackSuite) onFakeClock(body func()) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		body()
	})
}

// listener is somebody on the other end of the call, keeping what they heard as it came.
type listener struct {
	mu    sync.Mutex
	heard []audio.PcmData
}

func (l *listener) hear(pcm audio.PcmData) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.heard = append(l.heard, pcm)
}

// count is how many chunks have been heard so far.
func (l *listener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.heard)
}

// within is how many samples of speech, and of quiet, were heard from the first chunk of
// speech after the first n chunks to the last one.
func (l *listener) within(n int) (speech, quiet int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	first, last := -1, -1
	for i := n; i < len(l.heard); i++ {
		if slices.ContainsFunc(l.heard[i].Samples, func(sample int16) bool { return sample != 0 }) {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return 0, 0
	}
	for _, pcm := range l.heard[first : last+1] {
		for _, sample := range pcm.Samples {
			if sample != 0 {
				speech++
			} else {
				quiet++
			}
		}
	}
	return speech, quiet
}

// quietSince is how many samples of quiet were heard after the first n chunks.
func (l *listener) quietSince(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	var quiet int
	for _, pcm := range l.heard[n:] {
		for _, sample := range pcm.Samples {
			if sample == 0 {
				quiet++
			}
		}
	}
	return quiet
}

func loud(ms int) audio.PcmData {
	pcm := speech(ms)
	for i := range pcm.Samples {
		pcm.Samples[i] = 8000
	}
	return pcm
}

func (s *LoopbackSuite) TestSomebodyOnTheOtherEndHearsTheQuietBetweenWhatTheAgentSays() {
	s.onFakeClock(func() {
		ear := &listener{}
		edge := New(Options{PlayoutAhead: 100 * time.Millisecond, Published: ear.hear})
		s.Require().NoError(edge.Join(context.Background()))
		// Joining again is the same call, not a second stream of quiet over the first.
		s.Require().NoError(edge.Join(context.Background()))
		defer func() { s.Require().NoError(edge.Leave()) }()

		// A transcriber hears a run of speech end by the quiet after it: with nothing
		// after the agent's last word, the run would only end when it next spoke.
		time.Sleep(time.Second)
		s.InDelta(stt.SampleRate, ear.quietSince(0), float64(stt.SampleRate)/10,
			"a second of the call is a second of quiet")
		ear.mu.Lock()
		defer ear.mu.Unlock()
		for _, pcm := range ear.heard {
			s.Equal(stt.SampleRate, pcm.SampleRate, "quiet is in the call's own format")
			s.Equal(1, pcm.Channels)
		}
	})
}

func (s *LoopbackSuite) TestNoQuietIsHeardInsideWhatTheAgentSays() {
	s.onFakeClock(func() {
		ear := &listener{}
		edge := New(Options{PlayoutAhead: 100 * time.Millisecond, Published: ear.hear})
		s.Require().NoError(edge.Join(context.Background()))
		defer func() { s.Require().NoError(edge.Leave()) }()
		time.Sleep(100 * time.Millisecond)
		before := ear.count()

		// Speech is handed over ahead of being heard, so quiet sent while it plays would
		// land between its words, and split one sentence into two runs.
		s.Require().NoError(edge.PublishAudio(loud(500)))
		s.Require().NoError(edge.PublishAudio(loud(500)))
		for edge.Talking() {
			time.Sleep(10 * time.Millisecond)
		}

		speech, quiet := ear.within(before)
		s.Equal(stt.SampleRate, speech)
		s.Zero(quiet)
		time.Sleep(100 * time.Millisecond)
		s.Positive(ear.quietSince(before), "the quiet comes back once the speech has been heard")
	})
}

func (s *LoopbackSuite) TestNothingIsHeardOnceTheCallIsLeft() {
	s.onFakeClock(func() {
		ear := &listener{}
		edge := New(Options{Published: ear.hear})
		s.Require().NoError(edge.Join(context.Background()))
		// Off the packet clock, so leaving does not race a packet going out.
		time.Sleep(110 * time.Millisecond)

		s.Require().NoError(edge.Leave())
		left := ear.count()
		s.Positive(left)
		time.Sleep(time.Second)

		s.Equal(left, ear.count(), "the other end hangs up with the agent")
	})
}

func (s *LoopbackSuite) TestAnEdgeWithNobodyListeningJoinsAndLeaves() {
	s.onFakeClock(func() {
		edge := New(Options{})
		s.Require().NoError(edge.Join(context.Background()))
		// Packets of quiet with nobody to hand them to would have nothing to call.
		time.Sleep(100 * time.Millisecond)
		s.Require().NoError(edge.Leave())
	})
}

// overlapping is a listener that notes whether it was ever handed two packets at once, and
// takes a moment over each, as a transcriber's write does.
type overlapping struct {
	taking     atomic.Int32
	overlapped atomic.Bool
}

func (o *overlapping) hear(audio.PcmData) {
	if o.taking.Add(1) > 1 {
		o.overlapped.Store(true)
	}
	time.Sleep(2 * time.Millisecond)
	o.taking.Add(-1)
}

func (s *LoopbackSuite) TestTheAgentsSpeechAndTheQuietAreHandedOverOneAtATime() {
	// On the real clock: a goroutine waiting on a lock does not let a fake one move.
	ear := &overlapping{}
	edge := New(Options{PlayoutAhead: 40 * time.Millisecond, Published: ear.hear})
	s.Require().NoError(edge.Join(context.Background()))
	defer func() { s.Require().NoError(edge.Leave()) }()

	// Short sentences with pauses between them, so the edge keeps going from talking to
	// quiet and back: packets handed over together would reach a transcriber out of order.
	for i := range 60 {
		s.Require().NoError(edge.PublishAudio(loud(20)))
		time.Sleep(time.Duration(i%4) * 10 * time.Millisecond)
	}

	s.False(ear.overlapped.Load(), "a listener was handed speech and quiet at the same time")
}

func (s *LoopbackSuite) TestLeavingDoesNotWaitOnAListenerThatIsStuck() {
	s.onFakeClock(func() {
		taking, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		edge := New(Options{Published: func(audio.PcmData) {
			once.Do(func() { close(taking) })
			<-release
		}})
		s.Require().NoError(edge.Join(context.Background()))
		<-taking

		// A transcriber whose write has stalled must not keep the call from ending: Leave
		// waiting on it would deadlock this bubble.
		s.Require().NoError(edge.Leave())
		_, open := <-edge.Audio()
		s.False(open)
		close(release)
	})
}

func (s *LoopbackSuite) TestNoQuietWaitingBehindAStuckListenerGoesOutOnceTheCallIsLeft() {
	// On the real clock: the quiet waits on the lock the stuck speech holds.
	taking, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var quietAfter atomic.Int32
	var released atomic.Bool
	edge := New(Options{Published: func(pcm audio.PcmData) {
		if slices.ContainsFunc(pcm.Samples, func(sample int16) bool { return sample != 0 }) {
			once.Do(func() { close(taking) })
			<-release
			return
		}
		if released.Load() {
			quietAfter.Add(1)
		}
	}})
	s.Require().NoError(edge.Join(context.Background()))
	go func() { _ = edge.PublishAudio(loud(20)) }()
	<-taking
	time.Sleep(60 * time.Millisecond)

	s.Require().NoError(edge.Leave())
	released.Store(true)
	close(release)
	time.Sleep(100 * time.Millisecond)

	s.Zero(quietAfter.Load(), "a packet that waited through the hang-up is not sent after it")
}
