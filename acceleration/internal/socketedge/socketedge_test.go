package socketedge

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

type SocketEdgeSuite struct {
	suite.Suite

	edge *Edge

	mu      sync.Mutex
	sent    [][]byte
	cleared int
}

func TestSocketEdgeSuite(t *testing.T) {
	suite.Run(t, new(SocketEdgeSuite))
}

func (s *SocketEdgeSuite) SetupTest() {
	s.sent, s.cleared = nil, 0
	edge, err := New(Options{
		SampleRate: 24_000,
		Caller:     stt.Participant{ID: "caller"},
		Send: func(pcm []byte) error {
			s.mu.Lock()
			s.sent = append(s.sent, pcm)
			s.mu.Unlock()
			return nil
		},
		Cleared: func() {
			s.mu.Lock()
			s.cleared++
			s.mu.Unlock()
		},
	})
	s.Require().NoError(err)
	s.edge = edge
	s.Require().NoError(s.edge.Join(context.Background()))
	s.T().Cleanup(func() { _ = s.edge.Leave() })
}

// speech is that much of the agent's voice at the rate a TTS provider hands it over.
func speech(duration time.Duration) audio.PcmData {
	samples := make([]int16, 16_000*int(duration/time.Millisecond)/1000)
	for i := range samples {
		samples[i] = 1000
	}
	return audio.PcmData{Samples: samples, SampleRate: 16_000, Channels: 1}
}

// sentMs is how much speech has gone out, at the socket's 24 kHz.
func (s *SocketEdgeSuite) sentMs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, frame := range s.sent {
		total += len(frame) / 2
	}
	return total * 1000 / 24_000
}

func (s *SocketEdgeSuite) TestSpeechGoesOutAtThePaceItIsHeard() {
	started := time.Now()
	s.Require().NoError(s.edge.PublishAudio(speech(200 * time.Millisecond)))

	s.Require().Eventually(func() bool { return s.sentMs() >= 200 }, 2*time.Second, 5*time.Millisecond)
	s.GreaterOrEqual(time.Since(started), 180*time.Millisecond, "200 ms of speech sent faster than it is heard")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Len(s.sent[0], 24_000*20/1000*2, "a frame is 20 ms of 16-bit samples at the socket's rate")
}

func (s *SocketEdgeSuite) TestPublishingWaitsWhileMuchIsAlreadyQueued() {
	started := time.Now()
	s.Require().NoError(s.edge.PublishAudio(speech(time.Second)))

	s.GreaterOrEqual(time.Since(started), 500*time.Millisecond,
		"a second of speech was taken at once, leaving nothing to drop when the caller cuts in")
}

func (s *SocketEdgeSuite) TestDroppingSpeechStopsItWithinAFrame() {
	go func() { _ = s.edge.PublishAudio(speech(2 * time.Second)) }()
	s.Require().Eventually(func() bool { return s.sentMs() >= 100 }, time.Second, 5*time.Millisecond)

	s.edge.DropSpeech()
	at := s.sentMs()
	time.Sleep(200 * time.Millisecond)

	s.LessOrEqual(s.sentMs()-at, 20, "speech went on after it was dropped")
	s.False(s.edge.SpeechPending())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Equal(1, s.cleared)
}

func (s *SocketEdgeSuite) TestTheCallersAudioReachesTheAgentAt16kHz() {
	frame := audio.PcmData{Samples: make([]int16, 24_000*20/1000), SampleRate: 24_000, Channels: 1}

	s.Require().NoError(s.edge.Hear(frame.Bytes()))

	heard := <-s.edge.Audio()
	s.Equal("caller", heard.Participant.ID)
	s.Equal(stt.SampleRate, heard.Audio.SampleRate)
	s.Len(heard.Audio.Samples, stt.SampleRate*20/1000)
}

func (s *SocketEdgeSuite) TestLeavingEndsTheCallBothWays() {
	s.Require().NoError(s.edge.Leave())

	_, open := <-s.edge.Audio()
	s.False(open, "the agent was not told the call is over")
	s.ErrorIs(s.edge.Hear(make([]byte, 640)), ErrLeft)
	s.ErrorIs(s.edge.PublishAudio(speech(20*time.Millisecond)), ErrLeft)
}

// The edge is one call, held once: joining it again after leaving would start a second
// playout on a call already hung up.
func (s *SocketEdgeSuite) TestAnEdgeThatHasLeftIsNotJoinedAgain() {
	s.False(s.edge.Left())
	s.Require().NoError(s.edge.Leave())

	s.ErrorIs(s.edge.Join(context.Background()), ErrLeft)
	s.True(s.edge.Left())
	s.ErrorIs(s.edge.PublishAudio(speech(20*time.Millisecond)), ErrLeft)
}

// Joining a call already joined changes nothing: there is still one playout, and leaving
// stops it once.
func (s *SocketEdgeSuite) TestJoiningTwiceIsOneCall() {
	s.Require().NoError(s.edge.Join(context.Background()))

	s.NoError(s.edge.Leave())
	s.True(s.edge.Left())
}

// unjoined is a 16 kHz edge that sends nowhere and has not been joined.
func (s *SocketEdgeSuite) unjoined() *Edge {
	edge, err := New(Options{Send: func([]byte) error { return nil }})
	s.Require().NoError(err)
	return edge
}

// An edge left before it was joined has no playout to wait for: leaving returns.
func (s *SocketEdgeSuite) TestLeavingAnEdgeNeverJoinedReturns() {
	edge := s.unjoined()
	left := make(chan error, 1)

	go func() { left <- edge.Leave() }()

	select {
	case err := <-left:
		s.NoError(err)
	case <-time.After(2 * time.Second):
		s.Fail("Leave waits for a playout that never started")
	}
	s.ErrorIs(edge.Join(context.Background()), ErrLeft)
}

// A voice started while the same voice is stopped joins, asks and leaves the edge at once:
// whichever wins, the call is held at most once and hung up once.
func (s *SocketEdgeSuite) TestJoinRacingLeaveHangsUpOnce() {
	for range 200 {
		edge := s.unjoined()
		left := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); _ = edge.Join(context.Background()) }()
		go func() { defer wg.Done(); _ = edge.Join(context.Background()) }()
		go func() { defer wg.Done(); _ = edge.Left() }()
		go func() { left <- edge.Leave() }()
		wg.Wait()

		select {
		case err := <-left:
			s.Require().NoError(err)
		case <-time.After(2 * time.Second):
			s.Require().Fail("Leave hangs")
		}
		s.Require().True(edge.Left())
		s.Require().ErrorIs(edge.Join(context.Background()), ErrLeft)
	}
}

// The socket's read loop hears frames while a DELETE voice leaves on another goroutine: a
// frame heard as the call ends is refused, never sent on the closed audio channel.
func (s *SocketEdgeSuite) TestHearRacingLeaveNeverSendsOnAClosedChannel() {
	for i := range 3000 {
		edge := s.unjoined()
		if i%2 == 0 {
			s.Require().NoError(edge.Join(context.Background()))
		}
		drained := make(chan struct{})
		go func() {
			for range edge.Audio() {
			}
			close(drained)
		}()
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for edge.Hear(make([]byte, 640)) == nil {
				}
			}()
		}
		s.Require().NoError(edge.Leave())
		wg.Wait()
		<-drained
	}
}

// A frame waiting for room on the audio channel, which nothing reads, is released by leaving,
// so Leave returns whether or not the edge was joined.
func (s *SocketEdgeSuite) TestLeavingReleasesAHearWaitingToSend() {
	for _, join := range []bool{true, false} {
		edge := s.unjoined()
		if join {
			s.Require().NoError(edge.Join(context.Background()))
		}
		heard := make(chan error, 1)
		go func() {
			for {
				if err := edge.Hear(make([]byte, 640)); err != nil {
					heard <- err
					return
				}
			}
		}()
		s.Require().Eventually(func() bool { return len(edge.inbound) == cap(edge.inbound) }, 2*time.Second, time.Millisecond)

		left := make(chan error, 1)
		go func() { left <- edge.Leave() }()
		select {
		case err := <-left:
			s.Require().NoError(err)
		case <-time.After(2 * time.Second):
			s.Require().Fail("Leave waits for a Hear that is never released")
		}
		s.Require().ErrorIs(<-heard, ErrLeft)
	}
}

// carriedRates are the rates a caller's audio comes in at, from a phone line's 8 kHz to
// a browser's 192 kHz.
var carriedRates = []int{8_000, 11_025, 12_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000, 88_200, 96_000, 192_000}

// edgeAt is an edge joined at the rate given, and what it has sent so far.
func (s *SocketEdgeSuite) edgeAt(rate int) (*Edge, func() [][]byte) {
	var mu sync.Mutex
	var sent [][]byte
	edge, err := New(Options{
		SampleRate: rate,
		Caller:     stt.Participant{ID: "caller"},
		Send: func(pcm []byte) error {
			mu.Lock()
			sent = append(sent, pcm)
			mu.Unlock()
			return nil
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(edge.Join(context.Background()))
	s.T().Cleanup(func() { _ = edge.Leave() })
	return edge, func() [][]byte {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(sent)
	}
}

// The control: 20 ms of the caller at any rate the socket carries reaches the agent as 20 ms
// at 16 kHz, and the agent's speech goes out in 20 ms frames at the socket's rate.
func (s *SocketEdgeSuite) TestEveryCarriedRateIsHeardAndSpokenIn20msFrames() {
	for _, rate := range carriedRates {
		s.Run(strconv.Itoa(rate), func() {
			edge, sent := s.edgeAt(rate)
			frame := audio.PcmData{Samples: make([]int16, rate*20/1000), SampleRate: rate, Channels: 1}

			s.Require().NoError(edge.Hear(frame.Bytes()))
			heard := <-edge.Audio()
			s.Require().NoError(edge.PublishAudio(speech(20 * time.Millisecond)))

			// 20 ms at 11.025 kHz is 220.5 samples, so the whole-sample resample may be one short.
			s.InDelta(stt.SampleRate*20/1000, len(heard.Audio.Samples), 1)
			s.Require().Eventually(func() bool { return len(sent()) > 0 }, 2*time.Second, 5*time.Millisecond)
			s.Len(sent()[0], rate*20/1000*2, "a frame is 20 ms of 16-bit samples at the socket's rate")
		})
	}
}

// A rate the socket does not carry is refused before any frame is resampled at it, and zero
// still means 16 kHz.
func (s *SocketEdgeSuite) TestNewRefusesARateTheSocketDoesNotCarry() {
	for _, rate := range []int{1, 7_999, 192_001, math.MaxInt32, math.MaxInt, -1, -16_000, math.MinInt} {
		s.Run(strconv.Itoa(rate), func() {
			edge, err := New(Options{SampleRate: rate, Send: func([]byte) error { return nil }})

			s.Error(err)
			s.Nil(edge)
		})
	}
	edge := s.unjoined()
	s.Require().NoError(edge.Hear(make([]byte, stt.SampleRate*20/1000*2)))
	s.Len((<-edge.Audio()).Audio.Samples, stt.SampleRate*20/1000, "20 ms at 16 kHz is heard as 20 ms")
}

// A send failure in playout ends the call cleanly: Left reports true, queued speech is
// cleared, Audio closes, and subsequent PublishAudio, Hear, and Join calls all fail with ErrLeft.
func (s *SocketEdgeSuite) TestPlayoutSendFailureEndsTheCallCleanly() {
	edge, err := New(Options{
		SampleRate: 16_000,
		Caller:     stt.Participant{ID: "caller"},
		Send:       func([]byte) error { return errors.New("write failed") },
	})
	s.Require().NoError(err)
	s.Require().NoError(edge.Join(s.T().Context()))
	s.T().Cleanup(func() { _ = edge.Leave() })

	// Publish more than one 20ms frame so queued speech remains when the first send fails.
	s.Require().NoError(edge.PublishAudio(speech(200 * time.Millisecond)))

	s.Require().Eventually(func() bool { return edge.Left() }, 2*time.Second, 5*time.Millisecond,
		"send failure must transition the edge to left")
	s.False(edge.SpeechPending(), "queued speech must be cleared on send failure")

	select {
	case _, open := <-edge.Audio():
		s.False(open, "audio channel must be closed on send failure")
	case <-time.After(2 * time.Second):
		s.Fail("audio channel was not closed on send failure")
	}

	s.ErrorIs(edge.PublishAudio(speech(20*time.Millisecond)), ErrLeft)
	s.ErrorIs(edge.Join(s.T().Context()), ErrLeft)
	for range 50 {
		s.Require().ErrorIs(edge.Hear(make([]byte, 640)), ErrLeft)
	}
	s.NoError(edge.Leave())
}

// A send failure in playout unblocks a PublishAudio waiting for queue room and a Hear waiting
// for buffer room, returning ErrLeft to both and closing Audio once Hear finishes.
func (s *SocketEdgeSuite) TestPlayoutSendFailureReleasesBlockedPublishAndHear() {
	inSend := make(chan struct{})
	releaseSend := make(chan struct{})
	var once sync.Once
	edge, err := New(Options{
		SampleRate: 16_000,
		Caller:     stt.Participant{ID: "caller"},
		Send: func([]byte) error {
			once.Do(func() { close(inSend) })
			<-releaseSend
			return errors.New("write failed")
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(edge.Join(s.T().Context()))
	s.T().Cleanup(func() { _ = edge.Leave() })

	// Block a PublishAudio caller waiting on drained (> 400ms ahead).
	pubErr := make(chan error, 1)
	go func() { pubErr <- edge.PublishAudio(speech(time.Second)) }()

	select {
	case <-inSend:
	case <-time.After(2 * time.Second):
		s.Require().Fail("playout never reached Send")
	}

	// Fill inbound and block a Hear caller on inbound <- ...
	hearErr := make(chan error, 1)
	go func() {
		for {
			if err := edge.Hear(make([]byte, 640)); err != nil {
				hearErr <- err
				return
			}
		}
	}()
	s.Require().Eventually(func() bool { return len(edge.inbound) == cap(edge.inbound) }, 2*time.Second, time.Millisecond)

	close(releaseSend)

	select {
	case err := <-pubErr:
		s.ErrorIs(err, ErrLeft)
	case <-time.After(2 * time.Second):
		s.Fail("blocked PublishAudio did not unblock on send failure")
	}
	select {
	case err := <-hearErr:
		s.ErrorIs(err, ErrLeft)
	case <-time.After(2 * time.Second):
		s.Fail("blocked Hear did not unblock on send failure")
	}

	s.True(edge.Left())
	s.False(edge.SpeechPending())
	drained := make(chan struct{})
	go func() {
		for range edge.Audio() {
		}
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		s.Fail("audio channel was not closed after send failure")
	}
}

// Publishing before Join fails immediately instead of deadlocking on a playout that has not
// started, and leaves the edge joinable.
func (s *SocketEdgeSuite) TestPublishAudioBeforeJoinReturnsErrorInsteadOfDeadlocking() {
	edge := s.unjoined()

	pubErr := make(chan error, 1)
	go func() { pubErr <- edge.PublishAudio(speech(time.Second)) }()
	select {
	case err := <-pubErr:
		s.ErrorIs(err, ErrNotJoined)
	case <-time.After(time.Second):
		s.Require().Fail("PublishAudio before Join deadlocked")
	}

	s.ErrorIs(edge.PublishAudio(speech(20*time.Millisecond)), ErrNotJoined)
	s.False(edge.SpeechPending(), "unjoined PublishAudio must not queue speech")

	s.Require().NoError(edge.Join(s.T().Context()))
	s.Require().NoError(edge.PublishAudio(speech(20 * time.Millisecond)))
	s.NoError(edge.Leave())
}

// Every concurrent Leave waits until playout has finished its last Send and Audio is closed.
func (s *SocketEdgeSuite) TestConcurrentLeaveCallsWaitForPlayoutToFinish() {
	inSend := make(chan struct{})
	releaseSend := make(chan struct{})
	var sendDone atomic.Bool
	var once sync.Once
	edge, err := New(Options{
		SampleRate: 16_000,
		Caller:     stt.Participant{ID: "caller"},
		Send: func([]byte) error {
			once.Do(func() { close(inSend) })
			<-releaseSend
			sendDone.Store(true)
			return nil
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(edge.Join(s.T().Context()))

	s.Require().NoError(edge.PublishAudio(speech(40 * time.Millisecond)))
	select {
	case <-inSend:
	case <-time.After(2 * time.Second):
		s.Require().Fail("playout never reached Send")
	}

	const callers = 3
	results := make(chan error, callers)
	for range callers {
		go func() { results <- edge.Leave() }()
	}

	// None of the concurrent Leave callers may return while Send is still in flight.
	select {
	case <-results:
		close(releaseSend)
		s.Require().Fail("a concurrent Leave returned before playout finished Send")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSend)
	for range callers {
		select {
		case err := <-results:
			s.NoError(err)
		case <-time.After(2 * time.Second):
			s.Require().Fail("Leave did not return after Send finished")
		}
	}
	s.True(sendDone.Load(), "Send must have finished before Leave returned")
	_, open := <-edge.Audio()
	s.False(open, "audio channel must be closed before Leave returns")
	edge.mu.Lock()
	finalState := edge.state
	edge.mu.Unlock()
	s.Equal(stateClosed, finalState)
}

// While Leave is waiting on an in-flight Send (stateClosing), Left already reports true and
// Join, PublishAudio, and Hear all refuse with ErrLeft.
func (s *SocketEdgeSuite) TestOperationsDuringClosingReportLeft() {
	inSend := make(chan struct{})
	releaseSend := make(chan struct{})
	var once sync.Once
	edge, err := New(Options{
		SampleRate: 16_000,
		Caller:     stt.Participant{ID: "caller"},
		Send: func([]byte) error {
			once.Do(func() { close(inSend) })
			<-releaseSend
			return nil
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(edge.Join(s.T().Context()))
	s.Require().NoError(edge.PublishAudio(speech(60 * time.Millisecond)))

	select {
	case <-inSend:
	case <-time.After(2 * time.Second):
		s.Require().Fail("playout never reached Send")
	}

	leaveDone := make(chan error, 1)
	go func() { leaveDone <- edge.Leave() }()

	// Wait until Leave has entered stateClosing.
	s.Require().Eventually(func() bool {
		edge.mu.Lock()
		defer edge.mu.Unlock()
		return edge.state == stateClosing
	}, 2*time.Second, time.Millisecond)

	s.True(edge.Left())
	s.False(edge.SpeechPending())
	s.ErrorIs(edge.Join(s.T().Context()), ErrLeft)
	s.ErrorIs(edge.PublishAudio(speech(20*time.Millisecond)), ErrLeft)
	s.ErrorIs(edge.Hear(make([]byte, 640)), ErrLeft)

	close(releaseSend)
	select {
	case err := <-leaveDone:
		s.NoError(err)
	case <-time.After(2 * time.Second):
		s.Require().Fail("Leave did not return after Send finished")
	}
	edge.mu.Lock()
	finalState := edge.state
	edge.mu.Unlock()
	s.Equal(stateClosed, finalState)
}

// Leave racing a playout send failure waits for full teardown and never deadlocks or panics,
// including when Leave has already transitioned to stateClosing before Send returns its error.
func (s *SocketEdgeSuite) TestLeaveRacingPlayoutSendFailureCompletesCleanly() {
	var panicked atomic.Bool
	ctx := goroutine.WithEnd(s.T().Context(), slog.New(slog.DiscardHandler), func() {
		panicked.Store(true)
	})

	// Deterministic case: Leave enters stateClosing before Send returns an error.
	{
		inSend := make(chan struct{})
		releaseSend := make(chan struct{})
		var once sync.Once
		edge, err := New(Options{
			SampleRate: 16_000,
			Caller:     stt.Participant{ID: "caller"},
			Send: func([]byte) error {
				once.Do(func() { close(inSend) })
				<-releaseSend
				return errors.New("write failed")
			},
		})
		s.Require().NoError(err)
		s.Require().NoError(edge.Join(ctx))
		s.Require().NoError(edge.PublishAudio(speech(60 * time.Millisecond)))

		select {
		case <-inSend:
		case <-time.After(2 * time.Second):
			s.Require().Fail("playout never reached Send")
		}

		leaveDone := make(chan error, 1)
		go func() { leaveDone <- edge.Leave() }()
		s.Require().Eventually(func() bool {
			edge.mu.Lock()
			defer edge.mu.Unlock()
			return edge.state == stateClosing
		}, 2*time.Second, time.Millisecond)

		close(releaseSend)
		select {
		case err := <-leaveDone:
			s.Require().NoError(err)
		case <-time.After(2 * time.Second):
			s.Require().Fail("Leave deadlocked when Send failed during stateClosing")
		}
		s.Require().True(edge.Left())
		s.Require().False(edge.SpeechPending())
		_, open := <-edge.Audio()
		s.Require().False(open)
	}

	// Concurrent race case: Leave and Send failure race without ordering.
	for range 20 {
		inSend := make(chan struct{})
		releaseSend := make(chan struct{})
		var once sync.Once
		edge, err := New(Options{
			SampleRate: 16_000,
			Caller:     stt.Participant{ID: "caller"},
			Send: func([]byte) error {
				once.Do(func() { close(inSend) })
				<-releaseSend
				return errors.New("write failed")
			},
		})
		s.Require().NoError(err)
		s.Require().NoError(edge.Join(ctx))
		s.Require().NoError(edge.PublishAudio(speech(60 * time.Millisecond)))

		select {
		case <-inSend:
		case <-time.After(2 * time.Second):
			s.Require().Fail("playout never reached Send")
		}

		leave1 := make(chan error, 1)
		leave2 := make(chan error, 1)
		go func() { leave1 <- edge.Leave() }()
		go func() { leave2 <- edge.Leave() }()
		close(releaseSend)

		for _, ch := range []chan error{leave1, leave2} {
			select {
			case err := <-ch:
				s.Require().NoError(err)
			case <-time.After(2 * time.Second):
				s.Require().Fail("Leave deadlocked racing send failure")
			}
		}
		s.Require().True(edge.Left())
		s.Require().False(edge.SpeechPending())
		_, open := <-edge.Audio()
		s.Require().False(open)
	}

	s.False(panicked.Load(), "playout panicked while racing Leave")
}

// A blocked PublishAudio returns ErrLeft even when drained is signalled right before Send fails.
func (s *SocketEdgeSuite) TestBlockedPublishAudioReturnsErrLeftWhenSendFailsImmediately() {
	for range 20 {
		edge, err := New(Options{
			SampleRate: 16_000,
			Caller:     stt.Participant{ID: "caller"},
			Send:       func([]byte) error { return errors.New("write failed") },
		})
		s.Require().NoError(err)
		s.Require().NoError(edge.Join(s.T().Context()))

		err = edge.PublishAudio(speech(time.Second))
		s.Require().ErrorIs(err, ErrLeft)
		s.Require().NoError(edge.Leave())
	}
}
