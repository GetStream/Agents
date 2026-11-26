package ttsrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/google/uuid"
)

// Session is a live voice attached to one customer. It forwards the provider's events
// untouched and records a stat row per completed synthesis on the way past.
type Session struct {
	// call is the context of the call. The goroutines of the session start under it.
	call     context.Context
	provider tts.TTS
	// config is the routing identity of the provider. Stats and health are keyed by it,
	// so a provider registered under a different name still aggregates coherently.
	config   routing.ProviderConfig
	owner    routing.Owner
	recorder *routing.Recorder
	logger   *slog.Logger
	// asked is the voice the session was opened with, as the caller spelled it, which is
	// what a client naming the session's voice on each utterance sends again.
	asked string

	events chan tts.Event
	// forwarded closes once every event the provider sent has been recorded, and stopped once
	// the session is closed.
	forwarded chan struct{}
	stopped   chan struct{}

	mu sync.Mutex
	// inFlight tracks utterances that have not been settled into a stat row yet.
	inFlight map[string]*utterance
	// sentences is what was sent under each id and not settled yet, oldest first. Text for an
	// id joins its last sentence until that has ended; after, or once it settled, it is a
	// sentence of its own. Each completion for the id settles the oldest.
	sentences map[string][]*pending
	// finished is the most recent utterances settled, oldest first in finishedOrder. A
	// completion for one with nothing sent under it since, that has not started again, is a
	// repeat and records nothing. Only the last finishedKept are kept.
	finished      map[string]struct{}
	finishedOrder []string

	closeOnce sync.Once
}

// finishedKept is how many settled utterances a session remembers. Late text belongs to the
// last few, well inside it.
const finishedKept = 256

// miscounted is the provider and model names whose adapter was seen counting characters other
// than those sent, each logged once a process.
var miscounted sync.Map

// pending is the text sent under an id for one synthesis of it, until that settles.
type pending struct {
	// characters is what was handed to the provider, which is what it is billed for, whatever
	// the adapter counted.
	characters int64
	// reserved is the usage id its characters are held under, which its row is recorded under.
	reserved uuid.UUID
	// ended is set once no more text joins it, which is what the voice does with that text.
	// A voice that takes each sentence as its own request completes once for each final text,
	// so its final text ends it. A streaming voice completes once for the utterance and speaks
	// text sent after the final text in it, so only a barge-in ends it before it settles.
	ended bool
	// abandoned is set once it has the timing line of a sentence that ended with no
	// completion, so it never has a second.
	abandoned bool
}

// utterance is what the session remembers about one synthesis until it completes.
type utterance struct {
	// startedAt is when the provider accepted it, so the stat row is stamped with when
	// the customer asked rather than when the audio finished.
	startedAt time.Time
	// errorCode is set when the provider reported a failure for this synthesis. It turns
	// the completion into a failed row rather than adding a second one.
	errorCode string
	// errorMessage is what that failure said, for the synthesis's timing line.
	errorMessage string
	// firstByteAt is when its first audio reached the session, for the timing line of a
	// synthesis that never completes. Zero until then.
	firstByteAt time.Time
}

func newSession(
	ctx context.Context,
	provider tts.TTS,
	config routing.ProviderConfig,
	owner routing.Owner,
	recorder *routing.Recorder,
	logger *slog.Logger,
) *Session {
	// A voice that takes each sentence as its own request works on them side by side, and
	// heard as it arrives their audio is spliced together.
	if !provider.Streaming() {
		provider = tts.InOrder(ctx, provider)
	}
	session := &Session{
		call:      ctx,
		provider:  provider,
		config:    config,
		owner:     owner,
		recorder:  recorder,
		logger:    logger,
		events:    make(chan tts.Event, 64),
		forwarded: make(chan struct{}),
		stopped:   make(chan struct{}),
		inFlight:  map[string]*utterance{},
		sentences: map[string][]*pending{},
		finished:  map[string]struct{}{},
	}
	goroutine.Go(ctx, session.forward)
	return session
}

// Synthesize sends text to the selected provider. A request with no id is given one here
// rather than by the provider, so what is sent can be counted against the utterance it
// settles as.
//
// What an utterance's text costs is reserved before it is sent, under the id its row is
// recorded under, and raised as more of its text follows. A refusal is answered and that text
// is not sent. Of an utterance already speaking, the text sent before is still spoken, settled
// and billed: the final text's refusal still ends it, and after any other the caller does, with
// a final text or an empty one. Money that could not be checked (policy.Transient) lets an
// utterance already speaking finish on what it reserved. Text for an id whose sentence ended or
// settled is a new sentence, as the voice speaks it: it is reserved on its own and refused as a
// first text is.
//
// A voice named for one utterance must be the session's: the one it was opened with, as asked
// or as the provider was handed it once resolved and checked as this customer's. The text then
// goes to the provider with no voice, so it speaks the one it was built with. Any other voice
// was never resolved nor checked, so it is refused and nothing is sent.
func (s *Session) Synthesize(request tts.Request) error {
	if request.Voice != "" {
		if request.Voice != s.asked && request.Voice != s.Voice() {
			return routing.Refused(stack.Wrap(fmt.Errorf(
				"tts: the session speaks in voice %q, open a new session for %q", s.Voice(), request.Voice)))
		}
		request.Voice = ""
	}
	if request.ID == "" {
		request.ID = tts.NewSynthesisID()
	}
	characters := int64(utf8.RuneCountInString(request.Text))
	s.mu.Lock()
	queue := s.sentences[request.ID]
	speaking := len(queue) > 0 && !queue[len(queue)-1].ended
	var current *pending
	if speaking {
		current = queue[len(queue)-1]
	} else {
		current = &pending{reserved: uuid.Must(uuid.NewV7())}
		s.sentences[request.ID] = append(queue, current)
	}
	// Counted before it is sent, so a completion that comes straight back finds it.
	current.characters += characters
	current.ended = request.Final && !s.provider.Streaming()
	total, reserved := current.characters, current.reserved
	s.mu.Unlock()

	var err error
	if characters > 0 {
		err = s.reserve(reserved, total, characters)
		if speaking && policy.Transient(err) {
			err = nil
		}
	}
	unpaid := err != nil
	if err == nil {
		err = s.provider.Synthesize(request)
	}
	if err != nil {
		// Refused, it was never sent: what is held goes back to what was.
		s.mu.Lock()
		current.characters -= characters
		left := current.characters
		// Its last text refused for money, what was sent of it is still to be spoken.
		finish := unpaid && left > 0 && request.Final
		current.ended = finish && !s.provider.Streaming()
		if !speaking || left <= 0 {
			s.forget(request.ID, current)
		}
		s.mu.Unlock()
		switch {
		case characters <= 0:
		case !speaking || left <= 0:
			s.recorder.Release(reserved)
		default:
			_ = s.recorder.Reserve(s.config, s.owner, reserved, pricing.Quantities{pricing.Characters: left}, routing.ReserveTTL)
		}
		if finish {
			request.Text = ""
			if failed := s.provider.Synthesize(request); failed != nil {
				// Not told it was over, it is still the caller's to end.
				s.mu.Lock()
				current.ended = false
				s.mu.Unlock()
				err = errors.Join(err, failed)
			}
		}
	}
	return err
}

// reserve holds what total characters of an utterance cost under its usage id, added of them
// new. What an id holds is no reason to refuse it more, so what an utterance already speaking
// adds is checked first under an id of its own, beside what the utterance holds.
func (s *Session) reserve(reserved uuid.UUID, total, added int64) error {
	if total > added {
		more := uuid.Must(uuid.NewV7())
		err := s.recorder.Reserve(s.config, s.owner, more, pricing.Quantities{pricing.Characters: added}, routing.ReserveTTL)
		s.recorder.Release(more)
		if err != nil {
			return err
		}
	}
	return s.recorder.Reserve(s.config, s.owner, reserved, pricing.Quantities{pricing.Characters: total}, routing.ReserveTTL)
}

// forget takes a sentence that was never sent out of its id's. The caller holds mu.
func (s *Session) forget(id string, refused *pending) {
	queue := slices.DeleteFunc(s.sentences[id], func(queued *pending) bool { return queued == refused })
	if len(queue) == 0 {
		delete(s.sentences, id)
		return
	}
	s.sentences[id] = queue
}

// Interrupt drops audio in flight so the agent can stop mid-sentence. Every sentence sent
// so far ends, so text for its id after this is a sentence of its own. The voice completes
// each of them, but one that takes each sentence as its own request throws away a sentence
// whose final text was not sent and never completes it, so what that held is let go now.
func (s *Session) Interrupt() error {
	streaming := s.provider.Streaming()
	var dropped []uuid.UUID
	var cut []abandonment
	s.mu.Lock()
	for id, queue := range s.sentences {
		var kept []*pending
		for i, sentence := range queue {
			if !streaming && !sentence.ended {
				dropped = append(dropped, sentence.reserved)
				if abandoned, ok := s.abandon(id, sentence, i == 0); ok {
					cut = append(cut, abandoned)
				}
				continue
			}
			sentence.ended = true
			kept = append(kept, sentence)
		}
		if len(kept) == 0 {
			delete(s.sentences, id)
		} else {
			s.sentences[id] = kept
		}
	}
	s.mu.Unlock()
	for _, reserved := range dropped {
		s.recorder.Release(reserved)
	}
	for _, abandoned := range cut {
		s.logAbandoned(abandoned, "interrupted")
	}
	return s.provider.Interrupt()
}

// Events returns the provider's events. The channel closes when the session closes.
func (s *Session) Events() <-chan tts.Event { return s.events }

// Provider is the provider serving this session.
func (s *Session) Provider() string { return s.config.Provider }

// Model is the model serving this session.
func (s *Session) Model() string { return s.config.Model }

// Config is the routing identity of the provider serving this session.
func (s *Session) Config() routing.ProviderConfig { return s.config }

// Voice is the voice speaking, the provider's default included. Empty when the provider
// cannot say.
func (s *Session) Voice() string {
	if voiced, ok := s.provider.(tts.Voiced); ok {
		return voiced.Voice()
	}
	return ""
}

// Streaming reports whether the provider accepts partial text deltas.
func (s *Session) Streaming() bool { return s.provider.Streaming() }

// Prompt is what the model writing this voice's lines should be told about it.
func (s *Session) Prompt() string { return s.provider.Prompt() }

// Performs reports whether the voice acts bracketed directions rather than reading them.
func (s *Session) Performs() bool { return s.provider.Performs() }

// Quote is what that usage of this session's provider costs, priced as its row is, so a
// caller can report a cost without reaching for the router's prices.
func (s *Session) Quote(usage routing.Usage) pricing.Quote { return s.recorder.Quote(s.config, usage) }

// Close ends the session. The event channel closes once the provider's events are drained.
// What is still reserved for utterances that never completed is let go once they are.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stopped)
		err = s.provider.Close()
		goroutine.Go(s.call, func() {
			<-s.forwarded
			s.mu.Lock()
			sentences := s.sentences
			s.sentences = map[string][]*pending{}
			s.mu.Unlock()
			for _, queue := range sentences {
				for _, unsettled := range queue {
					s.recorder.Release(unsettled.reserved)
				}
			}
		})
	})
	return err
}

// forward relays provider events and records statistics as they pass.
func (s *Session) forward() {
	defer close(s.forwarded)
	defer close(s.events)
	// Once the voice's events end, because the session was closed or the voice ended them on
	// its own, a sentence it has not completed never will be.
	defer s.abandonUnsettled()

	for event := range s.provider.Events() {
		s.observe(event)
		select {
		case s.events <- event:
			continue
		default:
		}
		// Once the session is closed a caller that stopped reading drops what is left, which
		// is still recorded, rather than holding the provider's events, and what they settle,
		// forever.
		select {
		case s.events <- event:
		case <-s.stopped:
		}
	}
}

func (s *Session) observe(event tts.Event) {
	switch typed := event.(type) {
	case tts.SynthesisStarted:
		s.mu.Lock()
		s.inFlight[typed.SynthesisID] = &utterance{startedAt: typed.At}
		s.mu.Unlock()

	case tts.AudioChunk:
		s.mu.Lock()
		if current := s.inFlight[typed.SynthesisID]; current != nil && current.firstByteAt.IsZero() {
			current.firstByteAt = time.Now()
		}
		s.mu.Unlock()

	case tts.SynthesisComplete:
		// One utterance is one unit of billable work, the way one turn is for
		// speech-to-text. An interrupted one still produced audio and still cost money.
		settled, repeated := s.settle(typed.SynthesisID)
		if repeated {
			return
		}
		characters := typed.Characters
		if settled.sent {
			characters = settled.characters
			if characters != typed.Characters {
				s.warnMiscounted(typed.Characters, characters)
			}
		}
		stat := routing.Stat{
			Owner:       s.owner,
			UsageID:     settled.reserved,
			StartedAt:   settled.startedAt,
			OperationID: typed.SynthesisID,
			Purpose:     "speech",
			DurationMs:  float64(time.Since(settled.startedAt).Microseconds()) / 1000,
			Usage: routing.Usage{
				Characters: characters,
				AudioMs:    int64(typed.AudioDurationMs),
			},
			// Time to first byte is what the listener actually waited for; the rest of
			// the audio arrives while they are already hearing it.
			LatencyMs: typed.TimeToFirstByteMs,
			Success:   settled.errorCode == "",
			ErrorCode: settled.errorCode,
		}
		s.recorder.Record(s.config, stat)
		s.logTiming(stat, settled.errorMessage)

	case tts.Error:
		// A failure that names an utterance is settled by that utterance's completion, so
		// one synthesis is still one row. Anything else is a session-level failure.
		message := errorMessage(typed)
		if typed.SynthesisID != "" && s.fail(typed.SynthesisID, errorCode(typed), message) {
			return
		}
		stat := routing.Stat{
			Owner:     s.owner,
			StartedAt: time.Now().UTC(),
			Success:   false,
			ErrorCode: errorCode(typed),
		}
		s.recorder.Record(s.config, stat)
		s.logTiming(stat, message)
	}
}

// logTiming writes the "speech synthesis timing" line for a row just recorded: a synthesis,
// or a failure of the session that no synthesis settles. A failover is a session of its own,
// so each voice asked has its own line. A failure of the session has no synthesis, so no
// operation and nothing timed: those fields are left out rather than logged as zero.
func (s *Session) logTiming(stat routing.Stat, message string) {
	logged := routing.LoggedCall(s.owner.CallID, s.owner, s.config)
	synthesis := stat.OperationID != ""
	if synthesis {
		logged = append(logged, "operation", stat.OperationID)
	}
	logged = append(logged, "characters", stat.Characters, "audio_ms", stat.AudioMs)
	if synthesis {
		logged = append(logged, "ttfb_ms", stat.LatencyMs, "duration_ms", stat.DurationMs)
	}
	logged = append(logged, "cost_micros", s.recorder.QuoteStat(s.config, stat).CostMicros,
		"success", stat.Success, "error_code", stat.ErrorCode)
	if message != "" {
		logged = append(logged, "error", store.SafeLogText(message))
	}
	s.logger.Info("speech synthesis timing", logged...)
}

// abandonment is a sentence sent that ended with no completion, so with no row, as its
// timing line tells it.
type abandonment struct {
	id         string
	characters int64
	// utterance is what the voice reported of it; started is false when it never started.
	utterance utterance
	started   bool
}

// abandon is the timing line of a sentence of id that ends with no completion, once: ok is
// false when it already has one. first is whether it is id's oldest sentence, which a start
// and a failure of id belong to, as they do when one settles. The caller holds mu.
func (s *Session) abandon(id string, sentence *pending, first bool) (abandonment, bool) {
	if sentence.abandoned {
		return abandonment{}, false
	}
	sentence.abandoned = true
	abandoned := abandonment{id: id, characters: sentence.characters}
	if current, ok := s.inFlight[id]; ok && first {
		abandoned.utterance, abandoned.started = *current, true
	}
	return abandoned, true
}

// abandonUnsettled logs every sentence the voice had not completed when its events ended,
// with the code the voice reported for it, or "closed" when it reported none.
// Their reservations are let go at Close, as before.
func (s *Session) abandonUnsettled() {
	var left []abandonment
	s.mu.Lock()
	for id, queue := range s.sentences {
		for i, sentence := range queue {
			if abandoned, ok := s.abandon(id, sentence, i == 0); ok {
				left = append(left, abandoned)
			}
		}
	}
	s.mu.Unlock()
	for _, abandoned := range left {
		// The code the voice reported for it, when it did, is what its row would say.
		code := "closed"
		if reported := abandoned.utterance.errorCode; reported != "" {
			code = reported
		}
		s.logAbandoned(abandoned, code)
	}
}

// logAbandoned writes the timing line of a sentence that ended with no completion, failed
// with code. What was not measured is left out: the audio, a time to first byte when none
// arrived, a duration when it never started. No row is written for it, so it has no
// cost_micros either; what was reserved for it is let go.
func (s *Session) logAbandoned(abandoned abandonment, code string) {
	logged := append(routing.LoggedCall(s.owner.CallID, s.owner, s.config),
		"operation", abandoned.id, "characters", abandoned.characters)
	if started := abandoned.utterance.startedAt; abandoned.started {
		if first := abandoned.utterance.firstByteAt; !first.IsZero() {
			logged = append(logged, "ttfb_ms", milliseconds(first.Sub(started)))
		}
		logged = append(logged, "duration_ms", milliseconds(time.Since(started)))
	}
	logged = append(logged, "success", false, "error_code", code)
	if message := abandoned.utterance.errorMessage; message != "" {
		logged = append(logged, "error", store.SafeLogText(message))
	}
	s.logger.Info("speech synthesis timing", logged...)
}

func milliseconds(elapsed time.Duration) float64 { return float64(elapsed.Microseconds()) / 1000 }

// errorMessage is what a provider's failure said. An adapter may report one with no error
// under it, which says nothing.
func errorMessage(failure tts.Error) string {
	if failure.Err == nil {
		return ""
	}
	return failure.Err.Error()
}

// fail marks an utterance as failed, reporting whether it was still in flight.
func (s *Session) fail(synthesisID, code, message string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.inFlight[synthesisID]
	if !ok {
		return false
	}
	// The first failure is the one that explains the utterance.
	if current.errorCode == "" {
		current.errorCode, current.errorMessage = code, message
	}
	return true
}

// settled is an utterance taken out of flight, and the characters sent for it.
type settled struct {
	utterance
	characters int64
	// sent is false for an utterance that never went through Synthesize here, whose
	// characters only the adapter counted.
	sent bool
	// reserved is the usage id its characters were reserved under, nil when none were.
	reserved uuid.UUID
}

// settle takes an utterance out of flight with its oldest sentence, so a late duplicate
// completion cannot bill twice: one for an utterance already settled that has not started
// again and has nothing sent under it since is reported repeated, and records nothing. An
// utterance the session never saw start is stamped with now.
func (s *Session) settle(synthesisID string) (settled, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.inFlight[synthesisID]
	queue := s.sentences[synthesisID]
	if _, known := s.finished[synthesisID]; known && !ok && len(queue) == 0 {
		return settled{}, true
	}
	var characters int64
	var reserved uuid.UUID
	sent := len(queue) > 0
	if sent {
		characters, reserved = queue[0].characters, queue[0].reserved
		if len(queue) == 1 {
			delete(s.sentences, synthesisID)
		} else {
			s.sentences[synthesisID] = queue[1:]
		}
	}
	if _, known := s.finished[synthesisID]; !known {
		s.finished[synthesisID] = struct{}{}
		s.finishedOrder = append(s.finishedOrder, synthesisID)
		if len(s.finishedOrder) > finishedKept {
			delete(s.finished, s.finishedOrder[0])
			s.finishedOrder = s.finishedOrder[1:]
		}
	}
	if !ok {
		return settled{utterance: utterance{startedAt: time.Now().UTC()}, characters: characters, sent: sent, reserved: reserved}, false
	}
	delete(s.inFlight, synthesisID)
	return settled{
		utterance:  utterance{startedAt: current.startedAt.UTC(), errorCode: current.errorCode, errorMessage: current.errorMessage},
		characters: characters,
		sent:       sent,
		reserved:   reserved,
	}, false
}

// warnMiscounted logs, once a process for each provider and model, an adapter counting
// characters other than those it was sent.
func (s *Session) warnMiscounted(counted, sent int64) {
	if _, logged := miscounted.LoadOrStore(s.config.Name(), true); logged {
		return
	}
	s.logger.Warn("the voice counted characters other than those it was sent; the row bills what was sent",
		"provider", s.config.Provider, "model", s.config.Model, "counted", counted, "sent", sent)
}

func errorCode(failure tts.Error) string {
	if failure.Fatal {
		return "provider_fatal"
	}
	if failure.Context != "" {
		return failure.Context
	}
	return "provider_error"
}
