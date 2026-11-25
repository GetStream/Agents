//go:build integration

package store

import (
	"crypto/rand"
	"strings"
	"time"
)

// A case's error is the agent's when it stopped answering, which can be a provider's text
// echoing the key it was sent: it is kept without the key, and cut as the requests table cuts
// an error. One with no key is kept as it was.
func (s *StoreSuite) TestACasesErrorIsKeptWithoutTheKeyItEchoed() {
	key := "sk-test-" + rand.Text()
	for failure, kept := range map[string]string{
		"simulation: the agent did not answer: 401 Unauthorized\nAuthorization: Bearer " + key: "simulation: the agent did not answer: 401 Unauthorized\n[redacted]",
		"simulation: the agent did not answer: 503 Service Unavailable":                        "simulation: the agent did not answer: 503 Service Unavailable",
		strings.Repeat("x", 9000): strings.Repeat("x", 2048) + "… [truncated]",
	} {
		run := s.startedRun("app", time.Minute)
		cases, err := s.store.SimulationCases.Select(s.ctx, run.ID)
		s.Require().NoError(err)
		kase := cases[0]
		kase.State, kase.Error = SimulationErrored, failure

		s.Require().NoError(s.store.SimulationCases.Finish(s.ctx, kase))

		cases, err = s.store.SimulationCases.Select(s.ctx, run.ID)
		s.Require().NoError(err)
		s.Equal(kept, cases[0].Error)
	}
}

// A tool's result in a transcript can be a provider's text echoing the key it was sent: it is
// kept without the key, and the transcript the caller passed in (the judge's) is left as it
// was. A result with no key is kept as it was.
func (s *StoreSuite) TestATranscriptsToolResultIsKeptWithoutTheKeyItEchoed() {
	key := "sk-test-" + rand.Text()
	for result, kept := range map[string]string{
		"401 Unauthorized\nAuthorization: Bearer " + key: "401 Unauthorized\n[redacted]",
		`{"status":"shipped","eta":"2026-11-09"}`:        `{"status":"shipped","eta":"2026-11-09"}`,
		strings.Repeat("x", 9000):                        strings.Repeat("x", 2048) + "… [truncated]",
	} {
		run := s.startedRun("app", time.Minute)
		cases, err := s.store.SimulationCases.Select(s.ctx, run.ID)
		s.Require().NoError(err)
		kase := cases[0]
		kase.State = SimulationPassed
		kase.Transcript = []SimulationLine{{Text: "where is it", Tools: []SimulationTool{
			{Name: "lookup", Arguments: "{}", Result: "first"}, {Name: "lookup", Arguments: "{}", Result: result},
		}}}

		s.Require().NoError(s.store.SimulationCases.Finish(s.ctx, kase))

		cases, err = s.store.SimulationCases.Select(s.ctx, run.ID)
		s.Require().NoError(err)
		s.Require().Len(cases[0].Transcript, 1)
		s.Equal([]string{"first", kept}, []string{cases[0].Transcript[0].Tools[0].Result, cases[0].Transcript[0].Tools[1].Result})
		s.Equal(result, kase.Transcript[0].Tools[1].Result, "the caller's transcript was changed")
	}
}

func (s *StoreSuite) TestACasesConversationIsRecordedWhenItStartsAndFinishes() {
	run := s.startedRun("app", time.Minute)
	cases, err := s.store.SimulationCases.Select(s.ctx, run.ID)
	s.Require().NoError(err)
	s.Require().Len(cases, 1)
	s.Equal(SimulationPending, cases[0].State)

	s.Require().NoError(s.store.SimulationCases.Start(s.ctx, cases[0].ID, "call-1"))
	cases, err = s.store.SimulationCases.Select(s.ctx, run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationRunning, cases[0].State)
	s.Equal("call-1", cases[0].CallID)

	kase := cases[0]
	kase.State, kase.Turns, kase.Ended = SimulationPassed, 4, EndedComplete
	s.Require().NoError(s.store.SimulationCases.Finish(s.ctx, kase))
	cases, err = s.store.SimulationCases.Select(s.ctx, run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationPassed, cases[0].State)
	s.Equal(4, cases[0].Turns)
	s.NotNil(cases[0].FinishedAt)
}

func (s *StoreSuite) TestSimulationCaseQueryBudgets() {
	simulation := s.simulation("app")
	run := SimulationRun{CustomerID: "app", SimulationID: simulation.ID, Mode: SimulationText}
	cases := make([]SimulationCase, maxVariations)
	for i := range cases {
		cases[i] = SimulationCase{Variation: i, Scenario: simulation.Scenario}
	}
	s.Require().NoError(s.store.SimulationRuns.Start(s.ctx, &run, cases, time.Minute))

	s.Equal(1, s.countQueries(func() {
		read, err := s.store.SimulationCases.Select(s.ctx, run.ID)
		s.Require().NoError(err)
		s.Len(read, maxVariations)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SimulationCases.Start(s.ctx, cases[0].ID, "call-1")) }))
	cases[0].State = SimulationFailed
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SimulationCases.Finish(s.ctx, cases[0])) }))
}
