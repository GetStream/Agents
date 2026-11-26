package sttrouter

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// Session is a live transcription attached to one customer. It forwards the provider's
// events untouched and records a stat row per completed turn on the way past. A vendor
// that refuses the session part way through hands it to the next candidate.
type Session struct {
	// mu guards provider, config and closed: a failover replaces the provider while the
	// caller is still sending audio and may close the session.
	mu       sync.Mutex
	provider stt.STT
	// config is the routing identity of the provider. Stats and health are keyed by it,
	// so a provider registered under a different name still aggregates coherently.
	config   routing.ProviderConfig
	closed   bool
	owner    routing.Owner
	recorder *routing.Recorder
	// reopen starts the next candidate, none of those named in refused. Nil leaves a
	// refusal the end of the session.
	reopen func(refused []string) (stt.STT, routing.ProviderConfig, error)
	// cancel ends the context reopen starts the next candidate under. Nil when there is no
	// such context.
	cancel context.CancelFunc
	// call is the context of the call. The goroutines of the session start under it.
	call context.Context

	events chan stt.Event
	// spent is set once the session's money ran out (unpaid), for good: a failover clears failed.
	spent atomic.Bool
	// failed is set once the provider reports a fatal error. The caller has been told, and
	// the provider hears nothing more, so later audio is dropped rather than failing once
	// per frame.
	failed atomic.Bool
	// readAhead asks forward to read what the provider sent before a write to it failed,
	// and closes the channel it is sent once it has. done closes when forward returns.
	readAhead chan chan struct{}
	done      chan struct{}

	// streamed is the audio each provider has taken since its last settled turn, which is what
	// a turn the vendor did not measure is billed for, and what a provider measured by the
	// router is billed for whatever it reports. It is kept by provider, so a failover bills
	// each vendor the audio that vendor took. Guarded by mu.
	streamed map[stt.STT]*streamedAudio
	// lease holds a minute of audio while the session streams, so the other routers count
	// it before its turns are recorded. It follows the model serving the session (swap). Guarded
	// by mu.
	lease *routing.Lease

	closeOnce sync.Once
}

// leaseRenew is how often a session's lease is renewed, for three times as long.
var leaseRenew = routing.LeaseRenew

// leaseMinute is what a minute of streaming is held at.
var leaseMinute = pricing.Quantities{pricing.AudioMs: 60_000}

// streamedAudio is audio a provider took and no row has billed yet, and what its timing line
// says it took in all, from when.
type streamedAudio struct {
	config  routing.ProviderConfig
	ms      float64
	total   float64
	firstAt time.Time
}

func newSession(
	ctx context.Context,
	provider stt.STT,
	config routing.ProviderConfig,
	owner routing.Owner,
	recorder *routing.Recorder,
	reopen func(refused []string) (stt.STT, routing.ProviderConfig, error),
	cancel context.CancelFunc,
) *Session {
	session := &Session{
		call:     ctx,
		provider: provider,
		config:   config,
		owner:    owner,
		recorder: recorder,
		reopen:   reopen,
		cancel:   cancel,
		events:   make(chan stt.Event, 64),
		// Unbuffered, so only a forward still running takes a request, and it answers it.
		readAhead: make(chan chan struct{}),
		done:      make(chan struct{}),
		streamed:  map[stt.STT]*streamedAudio{},
	}
	session.lease = session.leaseOf(config)
	// Stamped here, as audio may reach the provider before forward runs.
	started := time.Now()
	goroutine.Go(ctx, func() { session.forward(started) })
	return session
}

// ProcessAudio streams audio to the selected provider, and drops it once the provider has
// failed for good. Once the session's money runs out it answers why, once, and ends it.
func (s *Session) ProcessAudio(pcm stt.PcmData, participant stt.Participant) error {
	if s.failed.Load() || s.spent.Load() {
		return nil
	}
	s.mu.Lock()
	provider, config, lease := s.provider, s.config, s.lease
	s.mu.Unlock()
	if err := s.unpaid(lease); err != nil {
		return err
	}
	err := provider.ProcessAudio(pcm, participant)
	if err == nil {
		s.took(provider, config, pcm.DurationMs())
	}
	if err != nil && s.toldOf(provider) {
		// The provider failed while this frame was on its way to it. The caller hears of
		// that failure, or of the next model taking over, once, and not again for the frame.
		return nil
	}
	return err
}

// maxRefusals is how many renewals of its lease in a row, LeaseRenew apart, are refused before a
// session ends: one alone may come from another reservation that is soon let go.
const maxRefusals = 2

// unpaid is why the session may take no more audio: maxRefusals renewals of its lease in a row
// were refused because money ran out, or one because the organization is suspended. It ends the
// session, which still transcribes and bills the audio it took, so its events close after the
// last transcript. Money that could not be checked (policy.Transient) counts for nothing.
func (s *Session) unpaid(lease *routing.Lease) error {
	err := refusedFor(lease)
	if err == nil {
		return nil
	}
	if !s.spent.CompareAndSwap(false, true) {
		return nil
	}
	// Off the caller's goroutine, since closing may wait on the vendor for the last words.
	goroutine.Go(s.call, func() { _ = s.Close() })
	return stack.Wrap(err)
}

// refusedFor is the refusal of the lease that ends a session, nil while none does.
func refusedFor(lease *routing.Lease) error {
	err, refusals, _ := lease.Refusals()
	if refusals >= maxRefusals || errors.Is(err, policy.ErrSuspended) {
		return err
	}
	return nil
}

// Unpaid reports whether the session's money ran out: it ended for that (unpaid), or its next
// audio ends it.
func (s *Session) Unpaid() bool {
	s.mu.Lock()
	lease := s.lease
	s.mu.Unlock()
	return s.spent.Load() || refusedFor(lease) != nil
}

// RefusedSince is when the first of the refusals of the session's lease in a row was, zero when
// the last it was granted.
func (s *Session) RefusedSince() time.Time {
	s.mu.Lock()
	lease := s.lease
	s.mu.Unlock()
	_, _, since := lease.Refusals()
	return since
}

// toldOf reports whether the caller hears of provider's failure, or of the next model
// taking over, from the session's events. The vendor's refusal can reach the provider just
// before a write fails, or wait behind events the caller has not read yet, so forward is
// first asked to read that far.
func (s *Session) toldOf(provider stt.STT) bool {
	read := make(chan struct{})
	select {
	case s.readAhead <- read:
		<-read
	case <-s.done:
	}
	return s.failed.Load() || provider != s.current()
}

// Events returns the provider's events. The channel closes when the session closes.
func (s *Session) Events() <-chan stt.Event { return s.events }

// Provider is the provider serving this session.
func (s *Session) Provider() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config.Provider
}

// Model is the model serving this session.
func (s *Session) Model() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config.Model
}

// SetKeyterms tells the transcriber which words to expect, where it can be told mid-session,
// and does nothing where it cannot.
func (s *Session) SetKeyterms(terms []string) error {
	retuner, ok := s.current().(stt.Retuner)
	if !ok {
		return nil
	}
	return retuner.SetKeyterms(terms)
}

// Finalize has the provider serving the session write its final of the run of speech in
// progress now (stt.Finalizer), and says whether it can be told to: one that cannot settles
// the run when it hears it end.
func (s *Session) Finalize() (bool, error) {
	finalizer, ok := s.current().(stt.Finalizer)
	if !ok {
		return false, nil
	}
	return true, finalizer.Finalize()
}

// Serving is the provider serving the session now, for what a caller can tell one provider
// that the session has no word for. A failover replaces it, and the next knows nothing of it.
func (s *Session) Serving() stt.STT { return s.current() }

// current is the provider serving the session, which a failover replaces.
func (s *Session) current() stt.STT {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.provider
}

// Close ends the session, and the start of a next candidate still under way. The event
// channel closes once the provider's events are drained.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		provider, lease := s.provider, s.lease
		s.mu.Unlock()
		err = provider.Close()
		// After the provider has closed: a provider started under this context may hold its
		// socket by it, and closing may still wait on the vendor for the last words.
		if s.cancel != nil {
			s.cancel()
		}
		lease.Stop()
	})
	return err
}

// forward relays provider events and records statistics as they pass. A refusal the next
// candidate takes over from is not passed on: the caller hears of the Failover instead.
// When no candidate takes over, the caller hears the refusal, as if none had been asked.
func (s *Session) forward(startedAt time.Time) {
	defer close(s.done)
	defer close(s.events)

	current := &relay{provider: s.provider, config: s.config, startedAt: startedAt}
	var refused []string
	for {
		event, open := s.next(current)
		if !open {
			// Before the events close, so whoever waits on them waits for the last row.
			s.billUnsettled(nil, current)
			s.logTiming(current)
			return
		}
		if failure, ok := event.(stt.Error); ok && failure.Fatal && !transportDrop(failure) && s.reopen != nil {
			from := current.config.Name()
			refused = append(refused, from)
			if next, nextConfig, err := s.reopenAnswering(refused); err == nil && s.swap(current.provider, next, nextConfig) {
				routing.CountHandover(routing.STT, current.config, failure.Err)
				s.billUnsettled(current.provider, current)
				s.logTiming(current)
				// Before the Failover is passed on: a write that fails meanwhile went to next.
				current = &relay{provider: next, config: nextConfig, startedAt: time.Now()}
				s.pass(current, stt.Failover{Provider: nextConfig.Provider, Model: nextConfig.Model, From: from, Err: failure.Err})
				continue
			}
		}
		s.pass(current, event)
	}
}

// reopenAnswering starts the next candidate, and answers a failed write while it does: the
// session has failed already, so the write is dropped rather than waiting for the dial. On
// the socket the writer is the loop that reads the caller, which must see a hang-up and
// close the session, ending the dial (router.go, Start).
func (s *Session) reopenAnswering(refused []string) (stt.STT, routing.ProviderConfig, error) {
	type opened struct {
		provider stt.STT
		config   routing.ProviderConfig
		err      error
	}
	result := make(chan opened, 1)
	goroutine.Go(s.call, func() {
		next, config, err := s.reopen(refused)
		result <- opened{provider: next, config: config, err: err}
	})
	for {
		select {
		case opened := <-result:
			return opened.provider, opened.config, opened.err
		case read := <-s.readAhead:
			close(read)
		}
	}
}

// relay is the provider forward reads, and what it has read of it ahead of the caller.
type relay struct {
	provider stt.STT
	config   routing.ProviderConfig
	// ahead is what was read to answer a failed write and not passed on yet. It stops at a
	// fatal error, so nothing the provider sends after one is read.
	ahead []stt.Event

	// What the provider's timing line says, kept by forward alone: when it started serving,
	// when its first transcript was read, what its turns and its unsettled audio cost (a failed
	// row bills no audio), and the fatal error that ended it.
	startedAt         time.Time
	firstTranscriptAt time.Time
	costMicros        int64
	errorCode         string
	errorMessage      string
}

// next returns the provider's next event, observed, and reads ahead for a failed write
// while it waits.
func (s *Session) next(current *relay) (stt.Event, bool) {
	for {
		if len(current.ahead) > 0 {
			event := current.ahead[0]
			current.ahead = current.ahead[1:]
			return event, true
		}
		select {
		case event, open := <-current.provider.Events():
			if !open {
				return nil, false
			}
			s.observe(current, event)
			return event, true
		case read := <-s.readAhead:
			s.readAheadOf(current)
			close(read)
		}
	}
}

// pass hands the caller an event, and reads ahead for a failed write while the caller has
// not taken it.
func (s *Session) pass(current *relay, event stt.Event) {
	for {
		select {
		case s.events <- event:
			return
		case read := <-s.readAhead:
			s.readAheadOf(current)
			close(read)
		}
	}
}

// readAheadOf observes what the provider has sent and forward has not read, up to a fatal
// error, which marks the session failed.
func (s *Session) readAheadOf(current *relay) {
	for !s.failed.Load() {
		select {
		case event, open := <-current.provider.Events():
			if !open {
				return
			}
			s.observe(current, event)
			current.ahead = append(current.ahead, event)
		default:
			return
		}
	}
}

// swap puts next in place of the provider that refused, and closes that one. A session
// closed while next was starting has closed the refusing provider already, so next is
// closed instead and the swap does not happen.
func (s *Session) swap(refusing, next stt.STT, config routing.ProviderConfig) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = next.Close()
		return false
	}
	s.provider, s.config = next, config
	if s.lease == nil {
		// The model that refused billed nothing, so nothing was held.
		s.lease = s.leaseOf(config)
	} else {
		s.lease.Move(config)
	}
	s.mu.Unlock()
	s.failed.Store(false)

	// Off this goroutine, since closing may wait on the vendor for the last words of the
	// call: ElevenLabs's Close flushes for up to FlushTimeout (elevenlabs.go, flush).
	goroutine.Go(s.call, func() { _ = refusing.Close() })
	return true
}

// leaseOf holds a minute of audio on the model, nil when it bills nothing.
func (s *Session) leaseOf(config routing.ProviderConfig) *routing.Lease {
	return routing.StartLeaseEvery(s.recorder, config, s.owner, "stt", uuid.NewString(), leaseMinute, leaseRenew, 3*leaseRenew)
}

func (s *Session) observe(current *relay, event stt.Event) {
	switch typed := event.(type) {
	case stt.Transcript:
		if current.firstTranscriptAt.IsZero() {
			current.firstTranscriptAt = time.Now()
		}
		// Only settled transcripts are billable work; interim ones are revisions of a
		// turn that has not finished yet.
		if !typed.Final() {
			return
		}
		streamedMs := s.claim(current.provider)
		stat := routing.Stat{
			Owner:     s.owner,
			StartedAt: time.Now().UTC(),
			Usage:     routing.Usage{AudioMs: int64(typed.AudioDurationMs)},
			LatencyMs: typed.ProcessingTimeMs,
			Success:   true,
		}
		switch {
		case stt.MeteringOf(current.provider) == stt.MeteredByRouter:
			// The vendor bills the audio it was sent, so what was streamed since the last
			// turn settled is the turn's bill, the silence before it included.
			stat.AudioMs = streamedMs
			stat.MeasuredByRouter()
		case stat.AudioMs == 0:
			// The vendor did not say how much audio the turn took: it is billed for what was
			// streamed since the last turn settled, and marked, so the adapter gets fixed.
			stat.AudioMs = streamedMs
			stat.Estimated()
		}
		s.recorder.Record(current.config, stat)
		current.costMicros += s.recorder.QuoteStat(current.config, stat).CostMicros

	case stt.Error:
		stat := routing.Stat{
			Owner:     s.owner,
			StartedAt: time.Now().UTC(),
			Success:   false,
			ErrorCode: errorCode(typed),
			// The vendor's own words are what tell an account it refuses from a dropped
			// socket.
			ErrorMessage: typed.Err.Error(),
		}
		if typed.Fatal {
			// Set before the error is passed on, so a caller who has read it sends no audio
			// that still reaches the provider.
			s.failed.Store(true)
			// Only a refusal the vendor sent marks the model unavailable. Providers report a
			// socket that dropped as a fatal error with the context read or write, and the
			// next session can still reach the model.
			stat.Fatal = !transportDrop(typed)
			// The first is what ended the provider's part of the session.
			if current.errorCode == "" {
				current.errorCode, current.errorMessage = stat.ErrorCode, stat.ErrorMessage
			}
		}
		s.recorder.Record(current.config, stat)
	}
}

// logTiming writes the "speech transcription timing" line for what one provider served of
// the session: all of it, or what it served until it refused the session and the next
// candidate took over, which then has a line of its own. Time to first byte runs from the
// first audio the provider took to the first transcript read from it, interim ones included.
func (s *Session) logTiming(current *relay) {
	var audioMs float64
	var firstAudioAt time.Time
	s.mu.Lock()
	if streamed := s.streamed[current.provider]; streamed != nil {
		audioMs, firstAudioAt = streamed.total, streamed.firstAt
	}
	s.mu.Unlock()
	logged := append(routing.LoggedCall(s.owner.CallID, s.owner, current.config), "audio_ms", int64(audioMs))
	// No transcript, or one read before any audio was taken, answers none of it.
	if !firstAudioAt.IsZero() && !current.firstTranscriptAt.Before(firstAudioAt) {
		logged = append(logged, "ttfb_ms", milliseconds(current.firstTranscriptAt.Sub(firstAudioAt)))
	}
	logged = append(logged, "duration_ms", milliseconds(time.Since(current.startedAt)),
		"cost_micros", current.costMicros, "success", current.errorCode == "", "error_code", current.errorCode)
	if current.errorMessage != "" {
		logged = append(logged, "error", store.SafeLogText(current.errorMessage))
	}
	slog.Info("speech transcription timing", logged...)
}

func milliseconds(elapsed time.Duration) float64 { return float64(elapsed.Microseconds()) / 1000 }

// took counts audio a provider accepted, against the config it was routed under.
func (s *Session) took(provider stt.STT, config routing.ProviderConfig, ms float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	streamed := s.streamed[provider]
	if streamed == nil {
		streamed = &streamedAudio{config: config, firstAt: time.Now()}
		s.streamed[provider] = streamed
	}
	streamed.ms += ms
	streamed.total += ms
}

// claim is the whole milliseconds of audio the provider took since it was last billed, which
// the row being recorded bills. The fraction left over waits for the next row, so the rows of
// a session add up to what it streamed.
func (s *Session) claim(provider stt.STT) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	streamed := s.streamed[provider]
	if streamed == nil {
		return 0
	}
	whole := math.Floor(streamed.ms)
	streamed.ms -= whole
	return int64(whole)
}

// billUnsettled records the audio the providers the router measures took and no turn has
// billed: what such a vendor heard after its last turn, when it refuses the session or the
// session ends, which it bills every second of. Nil bills every provider's. A vendor that
// measures itself reports what it bills on its turns, so what it took after them is not
// billed. The row is the router's own measure and times nothing the provider did. What a row
// of current's provider costs is added to current's.
func (s *Session) billUnsettled(only stt.STT, current *relay) {
	s.mu.Lock()
	var providers []stt.STT
	for provider := range s.streamed {
		if only == nil || provider == only {
			providers = append(providers, provider)
		}
	}
	s.mu.Unlock()
	for _, provider := range providers {
		if stt.MeteringOf(provider) != stt.MeteredByRouter {
			continue
		}
		s.mu.Lock()
		config := s.streamed[provider].config
		s.mu.Unlock()
		ms := s.claim(provider)
		if ms <= 0 {
			continue
		}
		stat := routing.Stat{Owner: s.owner, StartedAt: time.Now().UTC(), Usage: routing.Usage{AudioMs: ms}, Success: true, Unmeasured: true}
		stat.MeasuredByRouter()
		s.recorder.Record(config, stat)
		if provider == current.provider {
			current.costMicros += s.recorder.QuoteStat(config, stat).CostMicros
		}
	}
}

// transportDrop is a read or write that failed without the vendor refusing the session. A
// close frame whose code says the vendor refused it, such as Muse's 1008 "Ingress below
// real-time", is not a drop. Any other close, and any network error, is: the next session
// can still reach the model.
//
// The refusal codes are RFC 6455 section 7.4.1 (IANA WebSocket Close Code registry): 1002
// protocol error, 1003 unsupported data, 1007 invalid payload, 1008 policy violation, 1009
// message too big, 1010 mandatory extension. 4000-4999 is private use, so the vendor
// defines it; the only one documented by our vendors is Together's 4503 no_healthy_workers
// (docs.together.ai/docs/inference/transcription/streaming.md:103, read 2026-10-20), a
// refusal. The codes left out are drops: 1005 and 1006 (no frame), 1011 to 1014 (Muse's
// "Max session duration reached" and "Rate limited", dev.meta.ai/docs/api-reference/voice/
// realtime.md:119-120) and AssemblyAI's 3006 inactivity and 3008 3-hour limit, which are
// the session ending, not the model refusing.
func transportDrop(failure stt.Error) bool {
	if failure.Context != "read" && failure.Context != "write" {
		return false
	}
	var closed *websocket.CloseError
	if !errors.As(failure.Err, &closed) {
		return true
	}
	switch code := closed.Code; {
	case code == websocket.CloseProtocolError, code == websocket.CloseUnsupportedData,
		code >= websocket.CloseInvalidFramePayloadData && code <= websocket.CloseMandatoryExtension,
		code >= 4000 && code <= 4999:
		return false
	}
	return true
}

func errorCode(failure stt.Error) string {
	if failure.Fatal {
		return "provider_fatal"
	}
	if failure.Context != "" {
		return failure.Context
	}
	return "provider_error"
}
