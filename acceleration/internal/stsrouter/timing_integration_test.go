//go:build integration

package stsrouter

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
)

// TestEachSessionLogsWhatTheRowsItStoredCost runs one conversation per way a vendor bills, each
// over the same socket, through the built-in price list into the suite's database. Each line's
// cost is what that session's own stored rows cost, the rows of the models it handed work to
// included and a reply given away left out, and the socket still counts every row of each.
func (s *MeteringSuite) TestEachSessionLogsWhatTheRowsItStoredCost() {
	logs := capturedTiming(s.T())
	socket := &routing.Spent{}
	var stored []store.Request

	for _, tc := range []struct {
		name   string
		model  string
		script func(*Session, *stubSTS)
		// stored checks the session wrote each kind of row its line adds up.
		stored func([]store.Request)
	}{
		{
			name:  "billed by the second, its reply free and its reasoning on a text model",
			model: "openai/gpt-live-1",
			script: func(session *Session, provider *stubSTS) {
				s.Require().NoError(session.ProcessAudio(heard(1000), sts.Participant{}))
				provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
				provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 640})
				provider.emitter.Send(sts.SessionUsage{Seconds: 13})
				provider.emitter.Send(sts.BackendUsage{ResponseID: "resp_1", Model: "gpt-5.6-luna",
					Usage: llm.Usage{InputTokens: 890, OutputTokens: 31}})
				provider.emitter.Send(sts.BackendUsage{ResponseID: "resp_1", Model: "gpt-5.6-luna",
					Usage: llm.Usage{InputTokens: 890, OutputTokens: 31}})
				provider.emitter.Send(sts.SessionUsage{Seconds: 29, Final: true})
			},
			stored: func(rows []store.Request) {
				free := s.only(rows, func(row store.Request) bool { return row.Attributes[routing.AttributeFreeCostMicros] != "" })
				s.Zero(free.CostMicros)
				s.Positive(s.mustInt(free.Attributes[routing.AttributeFreeCostMicros]), "a reply that would have cost something")
				s.Positive(s.only(rows, func(row store.Request) bool { return row.Model == "gpt-5.6-luna" }).CostMicros)
				s.Len(s.all(rows, func(row store.Request) bool { return row.Meters[pricing.SessionSeconds] != 0 }), 2)
			},
		},
		{
			name:  "billed by the second, hung up on before the vendor's last report",
			model: "openai/gpt-live-1",
			script: func(session *Session, _ *stubSTS) {
				// The router's clock bills the seconds of a session nobody reported, from one
				// second up.
				time.Sleep(time.Second)
			},
			stored: func(rows []store.Request) {
				unreported := s.only(rows, func(row store.Request) bool { return row.Meters[pricing.SessionSeconds] != 0 })
				s.Equal("true", unreported.Attributes[routing.AttributeUsageEstimated])
				s.Positive(unreported.CostMicros)
			},
		},
		{
			name:  "billed by its replies, with a transcriber of its own and a failure",
			model: "openai/gpt-realtime-2.1",
			script: func(session *Session, provider *stubSTS) {
				provider.emitter.Send(sts.TranscriptionUsage{ItemID: "item_1", Model: "gpt-4o-mini-transcribe",
					Usage: sts.Usage{InputTokens: 33, InputAudioTokens: 33, OutputTokens: 16, OutputTextTokens: 16}})
				provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
				provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 380,
					Usage: sts.Usage{InputTokens: 120, InputAudioTokens: 100, InputTextTokens: 20, OutputTokens: 60, OutputAudioTokens: 60}})
				provider.emitter.Send(sts.Error{Err: io.ErrUnexpectedEOF, Context: "read", Fatal: true})
			},
			stored: func(rows []store.Request) {
				s.Positive(s.only(rows, func(row store.Request) bool { return row.Model == "gpt-4o-mini-transcribe" }).CostMicros)
				s.Positive(s.only(rows, func(row store.Request) bool { return row.Success && row.Model == "gpt-realtime-2.1" }).CostMicros)
				s.Zero(s.only(rows, func(row store.Request) bool { return !row.Success }).CostMicros, "a failure is not billed")
			},
		},
		{
			name:  "billed by the caller's audio, the caller heard after the last reply",
			model: "xai/grok-voice-3.0",
			script: func(session *Session, provider *stubSTS) {
				s.Require().NoError(session.ProcessAudio(heard(2000), sts.Participant{}))
				provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
				provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 510,
					Usage: sts.Usage{AudioBilled: true, BilledAudioMs: 2000, InputTokens: 7, OutputTokens: 119}})
				s.settledReply(session, "r1")
				s.Require().NoError(session.ProcessAudio(heard(1000), sts.Participant{}))
			},
			stored: func(rows []store.Request) {
				trailing := s.only(rows, func(row store.Request) bool { return row.Attributes[routing.AttributeMeasuredBy] != "" })
				s.EqualValues(1000, trailing.AudioMs)
				s.Positive(trailing.CostMicros)
			},
		},
	} {
		s.Run(tc.name, func() {
			customer := uuid.NewString()
			config := s.builtIn(tc.model)
			provider := newStubSTS()
			provider.capabilities, _ = capabilitiesFor(config.Provider, config.Model)
			recorder := routing.NewRecorder(routing.STS, s.store, s.live, s.prices, slog.New(slog.NewTextHandler(io.Discard, nil)))
			owner := routing.Owner{CustomerID: customer, AgentID: agentID, Attribution: attributed, Spent: socket}
			session := newSession(context.Background(), provider, config, owner, recorder, recorder.For(routing.LLM))

			tc.script(session, provider)
			s.closed(session, recorder)

			rows := s.rows(customer)
			tc.stored(rows)
			var cost int64
			for _, row := range rows {
				cost += row.CostMicros
			}
			s.Positive(cost)
			lines := timingLines(s.T(), logs, session.id)
			s.Require().Len(lines, 1)
			s.Equal(float64(cost), lines[0]["cost_micros"], "what this session's stored rows cost, and no other session's")
			stored = append(stored, rows...)
		})
	}

	var cost int64
	for _, row := range stored {
		cost += row.CostMicros
	}
	micros, rows, _ := socket.Total()
	s.Equal([]int64{cost, int64(len(stored))}, []int64{micros, rows}, "the socket counts every row of every session")
}

// A device-side socket, and an agent's own voice, open a session with no Spent in their context.
// The session's line still says what its stored rows cost.
func (s *MeteringSuite) TestASessionOnASocketWithNoSpentLogsItsRowsCost() {
	logs := capturedTiming(s.T())
	customer := uuid.NewString()
	config := s.builtIn("openai/gpt-realtime-2.1")
	provider := newStubSTS()
	provider.capabilities, _ = capabilitiesFor(config.Provider, config.Model)
	recorder := routing.NewRecorder(routing.STS, s.store, s.live, s.prices, slog.New(slog.NewTextHandler(io.Discard, nil)))
	session := newSession(context.Background(), provider, config, routing.Owner{CustomerID: customer, AgentID: agentID, Attribution: attributed},
		recorder, recorder.For(routing.LLM))
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 380,
		Usage: sts.Usage{InputTokens: 120, InputAudioTokens: 100, InputTextTokens: 20, OutputTokens: 60, OutputAudioTokens: 60}})
	s.closed(session, recorder)

	var cost int64
	for _, row := range s.rows(customer) {
		cost += row.CostMicros
	}
	s.Positive(cost)
	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.Equal(float64(cost), lines[0]["cost_micros"])
}

// only is the one row that matches.
func (s *MeteringSuite) only(rows []store.Request, match func(store.Request) bool) store.Request {
	found := s.all(rows, match)
	s.Require().Len(found, 1)
	return found[0]
}

func (s *MeteringSuite) all(rows []store.Request, match func(store.Request) bool) []store.Request {
	var found []store.Request
	for _, row := range rows {
		if match(row) {
			found = append(found, row)
		}
	}
	return found
}

func (s *MeteringSuite) mustInt(text string) int64 {
	n, err := strconv.ParseInt(text, 10, 64)
	s.Require().NoError(err)
	return n
}
