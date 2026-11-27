package tts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// perSentenceVoice stands in for a provider that takes each sentence as its own request.
// A test sends its events by hand, in whatever order the case needs.
type perSentenceVoice struct {
	emitter      *Emitter
	refuse       map[string]bool
	onSynthesize func(Request) error
	onInterrupt  func() error
	interrupts   atomic.Int64
}

func (v *perSentenceVoice) Start(context.Context) error { return nil }

func (v *perSentenceVoice) Synthesize(request Request) error {
	if v.onSynthesize != nil {
		return v.onSynthesize(request)
	}
	if v.refuse[request.ID] {
		return errors.New("refused")
	}
	return nil
}

func (v *perSentenceVoice) Interrupt() error {
	v.interrupts.Add(1)
	if v.onInterrupt != nil {
		return v.onInterrupt()
	}
	return nil
}
func (v *perSentenceVoice) Events() <-chan Event { return v.emitter.Events() }
func (v *perSentenceVoice) Close() error         { v.emitter.Close(); return nil }
func (v *perSentenceVoice) Provider() string     { return "stub" }
func (v *perSentenceVoice) Model() string        { return "stub-model" }
func (v *perSentenceVoice) Streaming() bool      { return false }
func (v *perSentenceVoice) Prompt() string       { return "" }
func (v *perSentenceVoice) Performs() bool       { return false }
func (v *perSentenceVoice) Voice() string        { return "Kore" }

type InOrderSuite struct {
	suite.Suite
	provider *perSentenceVoice
	voice    TTS
}

func TestInOrderSuite(t *testing.T) {
	suite.Run(t, new(InOrderSuite))
}

func (s *InOrderSuite) SetupTest() {
	s.provider = &perSentenceVoice{emitter: NewEmitter(64), refuse: map[string]bool{}}
	s.voice = InOrder(context.Background(), s.provider)
	s.T().Cleanup(func() { _ = s.voice.Close() })
}

func (s *InOrderSuite) say(ids ...string) {
	for _, id := range ids {
		s.Require().NoError(s.voice.Synthesize(Request{ID: id, Text: id, Final: true}))
	}
}

func (s *InOrderSuite) chunk(id string, index int) {
	s.provider.emitter.Send(AudioChunk{SynthesisID: id, Index: index, Audio: speech(20)})
}

func (s *InOrderSuite) complete(id string) {
	s.provider.emitter.Send(SynthesisComplete{SynthesisID: id})
}

// next reads the next event the wrapper passes on.
func (s *InOrderSuite) next() Event {
	select {
	case event := <-s.voice.Events():
		return event
	case <-time.After(2 * time.Second):
		s.FailNow("nothing was passed on")
		return nil
	}
}

// heard reads n events, naming each as the sentence and what it was.
func (s *InOrderSuite) heard(n int) []string {
	var names []string
	for range n {
		names = append(names, name(s.next()))
	}
	return names
}

// quiet reports whether nothing more is passed on for a moment.
func (s *InOrderSuite) quiet() bool {
	select {
	case event := <-s.voice.Events():
		s.Failf("expected nothing", "got %s", name(event))
		return false
	case <-time.After(50 * time.Millisecond):
		return true
	}
}

// entries counts the sentences the wrapper still keeps.
func (s *InOrderSuite) entries() int {
	o := s.voice.(*inOrder)
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}

// holdsCompletion reports whether a sentence waiting in the queue already has its completion.
func (s *InOrderSuite) holdsCompletion(id string) bool {
	o := s.voice.(*inOrder)
	o.mu.Lock()
	defer o.mu.Unlock()
	idx := o.indexActiveLocked(id)
	return idx >= 0 && o.entries[idx].state == entryQueuedCompleted
}

func name(event Event) string {
	switch typed := event.(type) {
	case SynthesisStarted:
		return typed.SynthesisID + " started"
	case AudioChunk:
		return typed.SynthesisID + " audio"
	case SynthesisComplete:
		if typed.Interrupted {
			return typed.SynthesisID + " interrupted"
		}
		return typed.SynthesisID + " done"
	case Error:
		return typed.SynthesisID + " error"
	}
	return "other"
}

func (s *InOrderSuite) TestSentencesAreHeardInTheOrderTheyWereAskedFor() {
	s.say("a", "b", "c")

	s.chunk("b", 0)
	s.chunk("a", 0)
	s.chunk("c", 0)
	s.chunk("b", 1)
	s.complete("c")
	s.complete("b")
	s.chunk("a", 1)
	s.complete("a")

	s.Equal([]string{
		"a audio", "a audio", "a done",
		"b audio", "b audio", "b done",
		"c audio", "c done",
	}, s.heard(8))
}

func (s *InOrderSuite) TestTheSentenceBeingHeardIsPassedOnAsItArrives() {
	s.say("a", "b")

	s.chunk("a", 0)

	s.Equal([]string{"a audio"}, s.heard(1), "the head must not wait for anything")
}

func (s *InOrderSuite) TestAStartIsPassedOnStraightAway() {
	s.say("a", "b")

	s.provider.emitter.Send(SynthesisStarted{SynthesisID: "b"})

	s.Equal([]string{"b started"}, s.heard(1))
}

func (s *InOrderSuite) TestABargeInSilencesEverythingButStillSettlesEverySentence() {
	s.say("a", "b", "c")
	s.chunk("a", 0)
	s.chunk("b", 0)
	s.complete("b")
	s.chunk("c", 0)
	s.Equal([]string{"a audio"}, s.heard(1))

	s.Require().NoError(s.voice.Interrupt())
	s.chunk("a", 1)
	s.complete("a")
	s.chunk("c", 1)
	s.complete("c")

	s.ElementsMatch([]string{"a interrupted", "b interrupted", "c interrupted"}, s.heard(3),
		"a sentence nobody heard in full is settled as interrupted, and nothing more is heard")
	s.True(s.quiet())
	s.Equal(int64(1), s.provider.interrupts.Load(), "the provider is stopped too")
}

func (s *InOrderSuite) TestTheNextTurnIsNotHeldBehindOneABargeInAbandoned() {
	s.say("a")
	s.chunk("a", 0)
	s.Equal([]string{"a audio"}, s.heard(1))
	s.Require().NoError(s.voice.Interrupt())

	s.say("d")
	s.chunk("d", 0)
	s.complete("d")

	s.Equal([]string{"d audio", "d done"}, s.heard(2))

	s.complete("a")
	s.Equal([]string{"a interrupted"}, s.heard(1))
}

func (s *InOrderSuite) TestAFailedSentenceLetsTheNextOneBeHeard() {
	s.say("a", "b")
	s.chunk("b", 0)
	s.complete("b")
	s.provider.emitter.Send(Error{SynthesisID: "a", Err: errors.New("boom")})

	s.Equal([]string{"a error"}, s.heard(1), "an error is passed on before the end it explains")
	s.complete("a")

	s.Equal([]string{"a done", "b audio", "b done"}, s.heard(3))
}

func (s *InOrderSuite) TestASentenceTheProviderRefusesDoesNotHoldUpTheNext() {
	s.provider.refuse["a"] = true
	s.Require().Error(s.voice.Synthesize(Request{ID: "a", Text: "a", Final: true}))
	s.say("b")

	s.chunk("b", 0)

	s.Equal([]string{"b audio"}, s.heard(1))
}

func (s *InOrderSuite) TestASecondSentenceUnderAnIdStillBeingSpokenIsRefused() {
	s.say("a", "b")

	s.Require().Error(s.voice.Synthesize(Request{ID: "b", Text: "b again", Final: true}))
	s.Require().Error(s.voice.Synthesize(Request{ID: "b", Text: "more", Final: false}),
		"nor does it take more text once its last piece was sent")
	s.say("c")
	s.complete("b")
	s.complete("a")
	s.complete("c")

	s.Equal([]string{"a done", "b done", "c done"}, s.heard(3),
		"the sentences asked for after it are still heard")
}

func (s *InOrderSuite) TestASentenceSentInPiecesHoldsOnePlace() {
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "there.", Final: true}))
	s.say("b")

	s.complete("b")
	s.complete("a")

	s.Equal([]string{"a done", "b done"}, s.heard(2))
}

func (s *InOrderSuite) TestAnIdIsFreeAgainOnceItsSentenceIsHeard() {
	s.say("a")
	s.complete("a")
	s.Equal([]string{"a done"}, s.heard(1))

	s.say("a")
	s.chunk("a", 0)

	s.Equal([]string{"a audio"}, s.heard(1))
}

func (s *InOrderSuite) TestAnIdIsFreeAgainOnceItsSentenceIsHeardBehindAnother() {
	s.say("a", "b")
	s.complete("b")
	s.complete("a")
	s.Equal([]string{"a done", "b done"}, s.heard(2))

	s.say("b")
	s.chunk("b", 0)

	s.Equal([]string{"b audio"}, s.heard(1))
}

func (s *InOrderSuite) TestAnIdIsFreeAgainOnceTheProviderRefusedIt() {
	s.provider.refuse["a"] = true
	s.Require().Error(s.voice.Synthesize(Request{ID: "a", Text: "a", Final: true}))
	s.provider.refuse["a"] = false

	s.say("a")
	s.chunk("a", 0)

	s.Equal([]string{"a audio"}, s.heard(1))
}

func (s *InOrderSuite) TestAnIdIsFreeAgainAfterABargeIn() {
	s.say("a")
	s.Require().NoError(s.voice.Interrupt())
	s.complete("a")
	s.Equal([]string{"a interrupted"}, s.heard(1))

	s.say("a")
	s.chunk("a", 0)

	s.Equal([]string{"a audio"}, s.heard(1))
}

func (s *InOrderSuite) TestAnIdSentOnlyInPiecesIsFreeAfterABargeIn() {
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
	s.Require().NoError(s.voice.Interrupt())

	s.say("a", "b")
	s.chunk("a", 0)
	s.complete("a")
	s.complete("b")

	s.Equal([]string{"a audio", "a done", "b done"}, s.heard(3),
		"the provider threw the pieces away and sent no completion for them")
}

func (s *InOrderSuite) TestASentenceAskedForAfterTheProviderClosedReachesTheProvider() {
	s.say("a")
	s.provider.refuse["a"] = true
	s.Require().NoError(s.provider.Close())
	for range s.voice.Events() {
	}

	err := s.voice.Synthesize(Request{ID: "a", Text: "a", Final: true})

	s.ErrorContains(err, "refused", "the provider answers, not a sentence that can never finish")
}

func (s *InOrderSuite) TestASentenceWithoutAnIdIsStillKeptInOrder() {
	s.say("a")
	s.Require().NoError(s.voice.Synthesize(Request{Text: "unnamed", Final: true}))
	s.say("c")

	s.chunk("c", 0)
	s.complete("c")
	s.complete("a")

	s.Equal([]string{"a done"}, s.heard(1))
	s.True(s.quiet(), "c waits behind the unnamed sentence asked for before it")
}

func (s *InOrderSuite) TestWhatIsStillHeldWhenTheProviderClosesIsPassedOn() {
	s.say("a", "b")
	s.chunk("b", 0)
	s.complete("b")

	s.Require().NoError(s.voice.Close())

	s.Equal([]string{"b audio", "b done"}, s.heard(2))
	_, open := <-s.voice.Events()
	s.False(open, "the events close with the provider")
}

func (s *InOrderSuite) TestItSpeaksInTheProvidersVoice() {
	voiced, ok := s.voice.(Voiced)
	s.Require().True(ok)
	s.Equal("Kore", voiced.Voice())
	s.False(s.voice.Streaming())
}

func (s *InOrderSuite) TestInterruptedCompletionsAreDeliveredBeforeTheEventsChannelCloses() {
	for i := range 50 {
		provider := &perSentenceVoice{emitter: NewEmitter(64), refuse: map[string]bool{}}
		voice := InOrder(s.T().Context(), provider)

		s.Require().NoError(voice.Synthesize(Request{ID: "a", Text: "a", Final: true}))
		s.Require().NoError(voice.Synthesize(Request{ID: "b", Text: "b", Final: true}))
		provider.emitter.Send(AudioChunk{SynthesisID: "b", Index: 0, Audio: speech(20)})
		provider.emitter.Send(SynthesisComplete{SynthesisID: "b"})

		// Wait until relay has processed b's completion by observing a's audio pass through.
		provider.emitter.Send(AudioChunk{SynthesisID: "a", Index: 0, Audio: speech(20)})
		select {
		case ev := <-voice.Events():
			s.Equal("a audio", name(ev))
		case <-time.After(2 * time.Second):
			s.FailNow("timed out waiting for a audio")
		}

		s.Require().NoError(voice.Interrupt())
		s.Require().NoError(voice.Close())

		var got []string
		for ev := range voice.Events() {
			got = append(got, name(ev))
		}
		s.Require().Equal([]string{"b interrupted"}, got, "iteration %d dropped interrupted completion on close", i)
	}
}

func (s *InOrderSuite) TestFailedFirstChunkReleasesHeldSentenceBeforeTheEventsChannelCloses() {
	for i := range 50 {
		started := make(chan struct{})
		release := make(chan struct{})
		provider := &perSentenceVoice{
			emitter: NewEmitter(64),
			onSynthesize: func(req Request) error {
				if req.ID == "a" {
					close(started)
					<-release
					return errors.New("refused")
				}
				return nil
			},
		}
		voice := InOrder(s.T().Context(), provider)

		errCh := make(chan error, 1)
		go func() {
			errCh <- voice.Synthesize(Request{ID: "a", Text: "a", Final: true})
		}()
		<-started

		s.Require().NoError(voice.Synthesize(Request{ID: "b", Text: "b", Final: true}))
		provider.emitter.Send(AudioChunk{SynthesisID: "b", Index: 0, Audio: speech(20)})
		provider.emitter.Send(SynthesisComplete{SynthesisID: "b"})
		// Synchronize relay so b's events are held before a fails.
		provider.emitter.Send(SynthesisStarted{SynthesisID: "b"})
		select {
		case ev := <-voice.Events():
			s.Equal("b started", name(ev))
		case <-time.After(2 * time.Second):
			s.FailNow("timed out waiting for b started")
		}

		close(release)
		s.Require().Error(<-errCh)
		s.Require().NoError(voice.Close())

		var got []string
		for ev := range voice.Events() {
			got = append(got, name(ev))
		}
		s.Require().Equal([]string{"b audio", "b done"}, got, "iteration %d dropped released sentence on close", i)
	}
}

func (s *InOrderSuite) TestInterruptWithFullEmitterBufferDoesNotDeadlock() {
	for i := range 20 {
		provider := &perSentenceVoice{emitter: NewEmitter(64), refuse: map[string]bool{}}
		voice := InOrder(s.T().Context(), provider)

		s.Require().NoError(voice.Synthesize(Request{ID: "a", Text: "a", Final: true}))
		s.Require().NoError(voice.Synthesize(Request{ID: "b", Text: "b", Final: true}))
		provider.emitter.Send(AudioChunk{SynthesisID: "b", Index: 0, Audio: speech(20)})
		provider.emitter.Send(SynthesisComplete{SynthesisID: "b"})

		// Fill the 64-event emitter buffer completely without reading from voice.Events().
		for j := range 64 {
			provider.emitter.Send(AudioChunk{SynthesisID: "a", Index: j, Audio: speech(20)})
		}
		s.Require().Eventually(func() bool {
			return len(voice.Events()) == 64
		}, 2*time.Second, time.Millisecond, "emitter buffer did not fill on iteration %d", i)

		// Send one more event so relay is actively blocked writing to the full buffer.
		provider.emitter.Send(AudioChunk{SynthesisID: "a", Index: 64, Audio: speech(20)})

		// Interrupt must not block even though this goroutine is the only consumer of voice.Events().
		done := make(chan error, 1)
		go func() {
			done <- voice.Interrupt()
		}()
		select {
		case err := <-done:
			s.Require().NoError(err)
		case <-time.After(2 * time.Second):
			s.FailNow("Interrupt deadlocked with full emitter buffer")
		}

		s.Require().NoError(voice.Close())

		var got []string
		for ev := range voice.Events() {
			got = append(got, name(ev))
		}
		s.Require().GreaterOrEqual(len(got), 65, "iteration %d lost buffered events", i)
		s.Require().Equal("b interrupted", got[len(got)-1], "iteration %d lost interrupted completion", i)
	}
}

func (s *InOrderSuite) TestReusingAnIdImmediatelyAfterABargeInIsNotSwallowedByTheDroppedTurn() {
	s.say("a")
	s.chunk("a", 0)
	s.Equal([]string{"a audio"}, s.heard(1))

	s.Require().NoError(s.voice.Interrupt())

	// The new "a" is already speaking when the cancelled request's completion arrives.
	s.say("a", "b")
	s.chunk("a", 0)
	s.complete("a")
	s.chunk("a", 1)
	s.complete("a")
	s.chunk("b", 0)
	s.complete("b")

	s.Equal([]string{"a audio", "a interrupted", "a audio", "a done", "b audio", "b done"}, s.heard(6))
}

func (s *InOrderSuite) TestReusingAnIdBehindAnotherSentenceAfterABargeInIsHeldAndHeard() {
	s.say("a")
	s.Require().NoError(s.voice.Interrupt())

	s.say("b", "a")
	s.complete("a")
	s.chunk("a", 0)
	s.complete("a")
	s.chunk("b", 0)
	s.complete("b")

	s.Equal([]string{"a interrupted", "b audio", "b done", "a audio", "a done"}, s.heard(5))
}

// A provider cancels its request on a barge-in and sends that request's completion at once,
// while a new request under the same id still waits on the network.
func (s *InOrderSuite) TestAReusedIdIsHeardWhenTheBargedInRequestSettlesFirst() {
	s.say("a")
	s.chunk("a", 0)
	s.Equal([]string{"a audio"}, s.heard(1))
	s.Require().NoError(s.voice.Interrupt())

	s.say("a", "b")
	s.provider.emitter.Send(SynthesisComplete{SynthesisID: "a", Interrupted: true})
	s.chunk("a", 0)
	s.complete("a")
	s.chunk("b", 0)
	s.complete("b")

	s.Equal([]string{"a interrupted", "a audio", "a done", "b audio", "b done"}, s.heard(5))
}

func (s *InOrderSuite) TestBargeInsUnderOneIdLeaveNothingBehind() {
	for range 100 {
		s.say("a")
		s.Require().NoError(s.voice.Interrupt())
		s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
		s.complete("a")
		s.Equal([]string{"a interrupted"}, s.heard(1))
		s.Require().NoError(s.voice.Interrupt())
	}

	s.Zero(s.entries(), "every sentence settled, so none is still waited on")
}

// The session bills a sentence by its completion, so one that came in for a request the
// provider then refused is still passed on.
func (s *InOrderSuite) TestARefusedSentenceStillPassesOnACompletionItWasHolding() {
	s.say("a")
	s.provider.onSynthesize = func(request Request) error {
		s.complete("b")
		s.Require().Eventually(func() bool { return s.holdsCompletion("b") }, 2*time.Second, time.Millisecond)
		return errors.New("nothing to say")
	}

	s.Require().Error(s.voice.Synthesize(Request{ID: "b", Text: "", Final: true}))

	s.Equal([]string{"b interrupted"}, s.heard(1))
}

func (s *InOrderSuite) TestAFailedSecondChunkKeepsTheFirstChunkInOrderAndAllowsRetry() {
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
	s.say("b")

	s.provider.refuse["a"] = true
	s.Require().Error(s.voice.Synthesize(Request{ID: "a", Text: "world.", Final: true}))
	s.provider.refuse["a"] = false

	// b finished first, but must remain held because a's first chunk is still in flight.
	s.chunk("b", 0)
	s.complete("b")
	s.True(s.quiet(), "b must wait behind a's in-flight first chunk")

	// a's first chunk audio still passes through as head, and a's final chunk can be retried.
	s.chunk("a", 0)
	s.Equal([]string{"a audio"}, s.heard(1))
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "world.", Final: true}))
	s.chunk("a", 1)
	s.complete("a")

	s.Equal([]string{"a audio", "a done", "b audio", "b done"}, s.heard(4))
}

func (s *InOrderSuite) TestAFailedNonFinalSecondChunkKeepsTheFirstChunkInOrder() {
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
	s.say("b")

	s.provider.refuse["a"] = true
	s.Require().Error(s.voice.Synthesize(Request{ID: "a", Text: "middle ", Final: false}))
	s.provider.refuse["a"] = false

	s.chunk("b", 0)
	s.complete("b")
	s.True(s.quiet(), "b must wait behind a's in-flight first chunk")

	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "world.", Final: true}))
	s.complete("a")
	s.Equal([]string{"a done", "b audio", "b done"}, s.heard(3))
}

func (s *InOrderSuite) TestAFailedFinalChunkAfterPromotionKeepsTheEntryStreaming() {
	started := make(chan struct{})
	release := make(chan struct{})
	s.provider.onSynthesize = func(req Request) error {
		if req.ID == "b" && req.Final {
			close(started)
			<-release
			return errors.New("refused")
		}
		return nil
	}

	s.say("a")
	s.Require().NoError(s.voice.Synthesize(Request{ID: "b", Text: "Hello ", Final: false}))

	// Start b's Final chunk while b is queued behind a.
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.voice.Synthesize(Request{ID: "b", Text: "world.", Final: true})
	}()
	<-started

	// Complete a so b is promoted to head while b's Final request is still in flight.
	s.complete("a")
	s.Equal([]string{"a done"}, s.heard(1))

	// Now fail b's Final request. b must remain at head in streaming state, not erased and not stuck closed.
	close(release)
	s.Require().Error(<-errCh)

	s.provider.onSynthesize = nil
	s.say("c")
	s.chunk("c", 0)
	s.complete("c")
	s.True(s.quiet(), "c must wait behind b")

	s.Require().NoError(s.voice.Synthesize(Request{ID: "b", Text: "world.", Final: true}))
	s.chunk("b", 0)
	s.complete("b")
	s.Equal([]string{"b audio", "b done", "c audio", "c done"}, s.heard(4))
}

func (s *InOrderSuite) TestInterruptDuringFailedFinalChunkDoesNotLeaveADroppedEntryBehind() {
	started := make(chan struct{})
	release := make(chan struct{})
	s.provider.onSynthesize = func(req Request) error {
		if req.ID == "a" && req.Final {
			close(started)
			<-release
			return errors.New("refused")
		}
		return nil
	}

	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.voice.Synthesize(Request{ID: "a", Text: "world.", Final: true})
	}()
	<-started

	// Interrupt runs while Final is in flight, then Final fails.
	s.Require().NoError(s.voice.Interrupt())
	close(release)
	s.Require().Error(<-errCh)

	// No dropped entry remains for "a", so an untracked completion passes through un-interrupted.
	s.complete("a")
	s.Equal([]string{"a done"}, s.heard(1))
}

func (s *InOrderSuite) TestASecondBargeInKeepsWaitingForAnEarlierDroppedCompletion() {
	s.say("a")
	s.Require().NoError(s.voice.Interrupt())

	s.say("b")
	s.Require().NoError(s.voice.Interrupt())

	s.chunk("a", 0)
	s.complete("a")
	s.chunk("b", 0)
	s.complete("b")

	s.Equal([]string{"a interrupted", "b interrupted"}, s.heard(2))
}

func (s *InOrderSuite) TestAFailedFinalChunkOnAQueuedSentenceAllowsRetry() {
	s.say("a")
	s.Require().NoError(s.voice.Synthesize(Request{ID: "b", Text: "Hello ", Final: false}))

	s.provider.refuse["b"] = true
	s.Require().Error(s.voice.Synthesize(Request{ID: "b", Text: "world.", Final: true}))
	s.provider.refuse["b"] = false

	// b is still queued in streaming state, so retrying Final must succeed.
	s.Require().NoError(s.voice.Synthesize(Request{ID: "b", Text: "world.", Final: true}))
	s.chunk("b", 0)
	s.complete("b")
	s.complete("a")

	s.Equal([]string{"a done", "b audio", "b done"}, s.heard(3))
}

func (s *InOrderSuite) TestAQueuedStreamingSentenceSpeaksImmediatelyOncePromotedToHead() {
	s.say("a")
	s.Require().NoError(s.voice.Synthesize(Request{ID: "b", Text: "Hello ", Final: false}))
	s.chunk("b", 0)
	s.complete("a")

	s.Equal([]string{"a done", "b audio"}, s.heard(2))

	// b is now head in streaming state: subsequent chunks pass straight through without waiting for Final.
	s.chunk("b", 1)
	s.Equal([]string{"b audio"}, s.heard(1))
}

func (s *InOrderSuite) TestProviderClosingInsideInterruptStillDeliversSettledCompletions() {
	for i := range 50 {
		var provider *perSentenceVoice
		provider = &perSentenceVoice{
			emitter: NewEmitter(64),
			refuse:  map[string]bool{},
			onInterrupt: func() error {
				return provider.Close()
			},
		}
		voice := InOrder(s.T().Context(), provider)

		s.Require().NoError(voice.Synthesize(Request{ID: "a", Text: "a", Final: true}))
		s.Require().NoError(voice.Synthesize(Request{ID: "b", Text: "b", Final: true}))
		provider.emitter.Send(AudioChunk{SynthesisID: "b", Index: 0, Audio: speech(20)})
		provider.emitter.Send(SynthesisComplete{SynthesisID: "b"})
		provider.emitter.Send(AudioChunk{SynthesisID: "a", Index: 0, Audio: speech(20)})
		select {
		case ev := <-voice.Events():
			s.Equal("a audio", name(ev))
		case <-time.After(2 * time.Second):
			s.FailNow("timed out waiting for a audio")
		}

		s.Require().NoError(voice.Interrupt())

		var got []string
		for ev := range voice.Events() {
			got = append(got, name(ev))
		}
		s.Require().Equal([]string{"b interrupted"}, got, "iteration %d dropped settled completion when provider closed during interrupt", i)
	}
}

func (s *InOrderSuite) TestAMultiChunkSentenceRefusesMoreTextAndSettlesInterruptedAfterFinal() {
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "Hello ", Final: false}))
	s.Require().NoError(s.voice.Synthesize(Request{ID: "a", Text: "world.", Final: true}))

	err := s.voice.Synthesize(Request{ID: "a", Text: "more", Final: false})
	s.Require().ErrorIs(err, ErrIDSpoken)

	s.Require().NoError(s.voice.Interrupt())
	s.chunk("a", 0)
	s.complete("a")

	s.Equal([]string{"a interrupted"}, s.heard(1))
}

func (s *InOrderSuite) TestInterruptDeliversSettledCompletionWithoutClosingOrFurtherProviderEvents() {
	s.say("a", "b")
	s.chunk("b", 0)
	s.complete("b")
	s.chunk("a", 0)
	s.Equal([]string{"a audio"}, s.heard(1))

	s.Require().NoError(s.voice.Interrupt())

	s.Equal([]string{"b interrupted"}, s.heard(1))
}

func (s *InOrderSuite) TestFailedFirstChunkAtHeadPromotesHeldSentenceWithoutClosing() {
	started := make(chan struct{})
	release := make(chan struct{})
	s.provider.onSynthesize = func(req Request) error {
		if req.ID == "a" {
			close(started)
			<-release
			return errors.New("refused")
		}
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.voice.Synthesize(Request{ID: "a", Text: "a", Final: true})
	}()
	<-started

	s.say("b")
	s.chunk("b", 0)
	s.complete("b")
	s.provider.emitter.Send(SynthesisStarted{SynthesisID: "b"})
	s.Equal([]string{"b started"}, s.heard(1))

	close(release)
	s.Require().Error(<-errCh)

	s.Equal([]string{"b audio", "b done"}, s.heard(2))
}

func (s *InOrderSuite) TestConcurrentSynthesizeInterruptAndCloseNeverRacesOrDrops() {
	provider := &perSentenceVoice{emitter: NewEmitter(64), refuse: map[string]bool{}}
	voice := InOrder(s.T().Context(), provider)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 30 {
			id := fmt.Sprintf("s-%d", i%3)
			if err := voice.Synthesize(Request{ID: id, Text: id, Final: true}); err == nil {
				provider.emitter.Send(AudioChunk{SynthesisID: id, Index: 0, Audio: speech(20)})
				provider.emitter.Send(SynthesisComplete{SynthesisID: id})
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 10 {
			_ = voice.Interrupt()
		}
	}()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range voice.Events() {
		}
	}()

	wg.Wait()
	s.Require().NoError(voice.Close())
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		s.FailNow("events channel did not close")
	}
}
