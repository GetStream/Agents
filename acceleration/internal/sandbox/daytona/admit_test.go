//go:build integration

package daytona

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sdk "github.com/daytona/clients/sdk-go/pkg/daytona"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
)

// unpaid is the refusal the tests' Admit hooks answer with.
var unpaid = errors.New("the organization has no credit left")

// stallingRuns passes requests to the test's Daytona, but holds each code run while stall is
// set, until let is closed or its request gives up, as code that runs a while does. The
// in-process Daytona answers one request at a time, so a run held there would hold the stop too.
type stallingRuns struct {
	stall atomic.Bool
	// in, when set, is the one sandbox whose runs are held.
	in      string
	entered chan struct{}
	let     chan struct{}
}

func newStallingRuns() *stallingRuns {
	runs := &stallingRuns{entered: make(chan struct{}, 1), let: make(chan struct{})}
	runs.stall.Store(true)
	return runs
}

func (t *stallingRuns) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.stall.Load() && strings.HasSuffix(r.URL.Path, "/process/code-run") &&
		(t.in == "" || strings.Contains(r.URL.Path, "/"+t.in+"/")) {
		t.entered <- struct{}{}
		select {
		case <-t.let:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	return http.DefaultTransport.RoundTrip(r)
}

// failingStops passes requests to the test's Daytona, but fails each stop while fail is set: when
// lost is set, after Daytona stopped the sandbox, as an answer lost on its way back; otherwise
// before Daytona hears of it, as a stop Daytona could not make.
type failingStops struct {
	fail, lost atomic.Bool
}

func (t *failingStops) RoundTrip(r *http.Request) (*http.Response, error) {
	if !t.fail.Load() || r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/stop") {
		return http.DefaultTransport.RoundTrip(r)
	}
	if t.lost.Load() {
		answer, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		_ = answer.Body.Close()
	}
	return nil, errors.New("connection reset by peer")
}

// finishingRuns passes requests to the test's Daytona, but holds the answer of each code run,
// which Daytona ran, until the run's request is cut, as code that finished in the moment its
// owner was refused. ran is told of each.
type finishingRuns struct {
	ran chan struct{}
}

func (t *finishingRuns) RoundTrip(r *http.Request) (*http.Response, error) {
	answer, err := http.DefaultTransport.RoundTrip(r)
	if err != nil || !strings.HasSuffix(r.URL.Path, "/process/code-run") {
		return answer, err
	}
	body, err := io.ReadAll(answer.Body)
	_ = answer.Body.Close()
	if err != nil {
		return nil, err
	}
	answer.Body = io.NopCloser(bytes.NewReader(body))
	t.ran <- struct{}{}
	<-r.Context().Done()
	return answer, nil
}

// refusable is a sandbox on the test's Daytona, metered by the test and idling after an hour,
// whose owner Admit refuses while refused is set, with its options changed.
func (s *LifecycleSuite) refusable(refused *atomic.Bool, change func(*Options)) *Sandbox {
	options := Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Hour},
		Meter: s.meter, Logger: slog.New(slog.NewTextHandler(s.logs, nil)),
		Admit: func(context.Context, bool) error {
			if refused.Load() {
				return unpaid
			}
			return nil
		}}
	if change != nil {
		change(&options)
	}
	box, err := New(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	return box
}

// stops is how many times the sandbox was asked to stop.
func (s *LifecycleSuite) stops(id string) int {
	count := 0
	for _, call := range s.daytona.Calls() {
		if call == "POST /api/sandbox/"+id+"/stop" {
			count++
		}
	}
	return count
}

// running runs code in box on runs, which holds it, and is the run's error once it ends. The
// run is cut when the test ends, so one a test failed to end does not outlive it.
func (s *LifecycleSuite) running(box *Sandbox, runs *stallingRuns) <-chan error {
	ctx, cancel := context.WithCancel(s.ctx)
	s.T().Cleanup(cancel)
	ran := make(chan error, 1)
	go func() {
		_, err := box.Run(ctx, "print(1)", nil)
		ran <- err
	}()
	select {
	case <-runs.entered:
	case err := <-ran:
		s.Require().NoError(err, "the run ended before its code ran")
	case <-time.After(5 * time.Second):
		s.FailNow("the code never ran")
	}
	return ran
}

func (s *LifecycleSuite) TestASandboxStoppedForItsOwnersMoneyGivesBackItsSlot() {
	slots := s.slotsFor(1)
	var refused atomic.Bool
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute},
		Meter: s.meter, Slots: slots, Logger: slog.New(slog.NewTextHandler(s.logs, nil)),
		Admit: func(context.Context, bool) error {
			if refused.Load() {
				return unpaid
			}
			return nil
		}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	s.run(box)
	s.Require().Equal(1, s.held(slots))
	refused.Store(true)

	_, err = box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
	s.Zero(s.held(slots), "given back once stopped, as at an idle stop, so another of the organization's may run")
}

func (s *LifecycleSuite) TestARefusalWhileCodeRunsEndsTheRunAndStopsTheSandbox() {
	runs := newStallingRuns()
	var refused atomic.Bool
	// The run may take 30 minutes, the most a config allows, and the timer that admits an idle
	// sandbox waits for it: only the admission while it runs can end it sooner.
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Hour},
		RunTimeout: 30 * time.Minute, Meter: s.meter, HTTPClient: &http.Client{Transport: runs},
		Logger: slog.New(slog.NewTextHandler(s.logs, nil)), AdmitEvery: 50 * time.Millisecond,
		Admit: func(context.Context, bool) error {
			if refused.Load() {
				return unpaid
			}
			return nil
		}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	ran := s.running(box, runs)

	refused.Store(true)

	select {
	case err := <-ran:
		s.Require().ErrorIs(err, unpaid, "the refusal is what the model is told")
	case <-time.After(5 * time.Second):
		s.FailNow("the run went on after its owner was refused")
	}
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Stopped, held.State, "stopped rather than left to bill until it idles")
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
}

func (s *LifecycleSuite) TestARefusalWhileCodeRunsInAClosedSandboxLeavesItToBeDeleted() {
	runs := newStallingRuns()
	var refused atomic.Bool
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(),
		Lifecycle: sandbox.Lifecycle{IdleStop: 300 * time.Millisecond}, Timeout: 500 * time.Millisecond, RunTimeout: 30 * time.Minute,
		Meter: s.meter, HTTPClient: &http.Client{Transport: runs}, Logger: slog.New(slog.NewTextHandler(s.logs, nil)),
		AdmitEvery: 50 * time.Millisecond,
		Admit: func(context.Context, bool) error {
			if refused.Load() {
				return unpaid
			}
			return nil
		}})
	s.Require().NoError(err)
	ran := s.running(box, runs)
	s.Require().ErrorIs(box.Close(), context.DeadlineExceeded, "the run still holds the sandbox")
	s.daytona.RefuseDeletes(true)

	refused.Store(true)

	select {
	case err := <-ran:
		s.Require().ErrorIs(err, unpaid)
	case <-time.After(5 * time.Second):
		s.FailNow("the run went on after its owner was refused")
	}
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned")
	s.daytona.RefuseDeletes(false)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted by its timer once it idled, as any orphan")
	s.NotContains(s.daytona.Calls(), "POST /api/sandbox/sandbox-1/stop", "not stopped, which would leave it to Daytona's timer")
}

func (s *LifecycleSuite) TestARunGoesOnWhileItsOwnersMoneyOnlyCannotBeChecked() {
	runs := newStallingRuns()
	runs.stall.Store(false)
	var unchecked atomic.Bool
	var whileRunning atomic.Int32
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Hour},
		Meter: s.meter, HTTPClient: &http.Client{Transport: runs}, AdmitEvery: 20 * time.Millisecond,
		// As metered's hook does, money that could not be checked refuses only a sandbox not
		// running yet.
		Admit: func(_ context.Context, running bool) error {
			if !unchecked.Load() {
				return nil
			}
			if !running {
				return unpaid
			}
			whileRunning.Add(1)
			return nil
		}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	s.run(box)
	unchecked.Store(true)
	runs.stall.Store(true)
	ran := s.running(box, runs)
	// The code runs, holding the gate the timer waits for: only the run's watch admits now.
	before := whileRunning.Load()
	s.Require().Eventually(func() bool { return whileRunning.Load() >= before+2 }, 5*time.Second, 10*time.Millisecond,
		"admitted again while the code runs")

	close(runs.let)

	s.Require().NoError(<-ran)
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Started, held.State)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container"}, s.meter.all(), "never stopped")
}

func (s *LifecycleSuite) TestASandboxNobodyAdmitsRunsAsBefore() {
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Hour},
		Meter: s.meter, AdmitEvery: time.Nanosecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })

	ran, err := box.Run(s.ctx, "print(1)", nil)

	s.Require().NoError(err, "a run outlasting AdmitEvery many times over asks no Admit it has not got")
	s.Equal("ran\n", ran.Output)
}

func (s *LifecycleSuite) TestARunningSandboxIsAdmittedAgainEveryMinuteUnlessToldOtherwise() {
	synctest.Test(s.T(), func(t *testing.T) {
		var mu sync.Mutex
		var admitted []time.Duration
		var box *Sandbox
		start := time.Now()
		box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Hour},
			Admit: func(context.Context, bool) error {
				mu.Lock()
				defer mu.Unlock()
				admitted = append(admitted, time.Since(start))
				if len(admitted) == 2 {
					// The timer holds the gate: taken as gone, the sandbox's timer is armed no more.
					box.gone = true
				}
				return nil
			}})
		require.NoError(t, err)
		box.box = &sdk.Sandbox{ID: "sandbox-1"}
		box.gate <- struct{}{}
		box.done(t.Context())

		time.Sleep(time.Hour)
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, []time.Duration{time.Minute, 2 * time.Minute}, admitted)
	})
}

func (s *LifecycleSuite) TestARefusedSandboxWhoseStopsAnswerWasLostIsBilledAsStoppedAndStartsAgain() {
	stops := &failingStops{}
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) { o.HTTPClient = &http.Client{Transport: stops} })
	s.run(box)
	stops.fail.Store(true)
	stops.lost.Store(true)
	refused.Store(true)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Require().Equal(daytonatest.Stopped, held.State, "Daytona stopped it, though its answer never came")
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, s.meter.all(), "billed as stopped, as it is")
	refused.Store(false)
	ran, err := box.Run(s.ctx, "print(3)", nil)
	s.Require().NoError(err, "started again once its owner may pay")
	s.Equal("ran\n", ran.Output)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "started"}, s.meter.all())
	s.Equal([]string{"sandbox-1: print(1)", "sandbox-1: print(3)"}, s.daytona.Ran())
}

func (s *LifecycleSuite) TestARefusedSandboxDaytonaWouldNotStopIsBilledAsRunningAndStoppedAtItsNextAdmission() {
	stops := &failingStops{}
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) { o.HTTPClient = &http.Client{Transport: stops} })
	s.run(box)
	stops.fail.Store(true)
	refused.Store(true)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Require().Equal(daytonatest.Started, held.State)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container"}, s.meter.all(), "billed as running, as it still is")
	stops.fail.Store(false)
	_, err = box.Run(s.ctx, "print(3)", nil)
	s.Require().ErrorIs(err, unpaid)
	held, _ = s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Stopped, held.State, "stopped at its next admission")
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, s.meter.all())
}

func (s *LifecycleSuite) TestARefusedSandboxSomeoneElseDeletedIsForgottenAndNotStoppedAgain() {
	var refused atomic.Bool
	box := s.refusable(&refused, nil)
	s.run(box)
	s.daytona.Gone("sandbox-1")
	refused.Store(true)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone"}, s.meter.all(), "nothing bills once it is gone")
	_, err = box.Run(s.ctx, "print(3)", nil)
	s.Require().ErrorIs(err, unpaid)
	s.Equal(1, s.stops("sandbox-1"), "a sandbox that is gone is not stopped again")
	refused.Store(false)
	s.run(box)
	s.Equal([]string{"sandbox-1", "sandbox-2"}, s.daytona.Created(), "another is made once its owner may pay")
}

func (s *LifecycleSuite) TestARefusedSandboxThatWasStoppingIsBilledAsStopped() {
	stops := &failingStops{}
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) { o.HTTPClient = &http.Client{Transport: stops} })
	s.run(box)
	s.daytona.SetState("sandbox-1", "stopping")
	stops.fail.Store(true)
	refused.Store(true)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, s.meter.all(),
		"a stop whose wait ran out leaves it stopping, which bills as stopped")
}

func (s *LifecycleSuite) TestARefusedSandboxFoundDestroyedIsForgotten() {
	stops := &failingStops{}
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) { o.HTTPClient = &http.Client{Transport: stops} })
	s.run(box)
	s.daytona.SetState("sandbox-1", daytonatest.Destroyed)
	stops.fail.Store(true)
	refused.Store(true)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone"}, s.meter.all())
}

func (s *LifecycleSuite) TestARunAdmittedAfterARefusalWhileCodeRanLeavesTheSandboxRunning() {
	runs := newStallingRuns()
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) {
		o.HTTPClient, o.RunTimeout, o.AdmitEvery = &http.Client{Transport: runs}, 30*time.Minute, 50*time.Millisecond
	})
	ran := s.running(box, runs)
	refused.Store(true)
	s.Require().ErrorIs(<-ran, unpaid)
	refused.Store(false)
	runs.stall.Store(false)

	s.run(box)

	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Started, held.State, "the refusal stopped it once, not after every run since")
	s.Equal(1, s.stops("sandbox-1"))
}

func (s *LifecycleSuite) TestARunMadeAgainInANewSandboxIsWatchedToo() {
	runs := newStallingRuns()
	runs.in = "sandbox-2"
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) {
		o.HTTPClient, o.RunTimeout, o.AdmitEvery = &http.Client{Transport: runs}, 30*time.Minute, 50*time.Millisecond
	})
	s.run(box)
	s.daytona.Gone("sandbox-1")
	ran := s.running(box, runs)

	refused.Store(true)

	select {
	case err := <-ran:
		s.Require().ErrorIs(err, unpaid)
	case <-time.After(5 * time.Second):
		s.FailNow("the run in the sandbox made again went on after its owner was refused")
	}
	held, _ := s.daytona.Sandbox("sandbox-2")
	s.Equal(daytonatest.Stopped, held.State)
}

func (s *LifecycleSuite) TestASandboxThatIdlesBeforeItsNextAdmissionIsStoppedForIdling() {
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) {
		o.Lifecycle, o.AdmitEvery = sandbox.Lifecycle{IdleStop: 200 * time.Millisecond}, time.Hour
	})

	s.run(box)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
}

func (s *LifecycleSuite) TestARunThatFinishedAsItsOwnerWasRefusedKeepsItsResultAndStopsTheSandbox() {
	runs := &finishingRuns{ran: make(chan struct{}, 1)}
	var refused atomic.Bool
	box := s.refusable(&refused, func(o *Options) {
		o.HTTPClient, o.AdmitEvery = &http.Client{Transport: runs}, 20*time.Millisecond
	})
	s.daytona.Writes("/tmp/out.txt", []byte("written"))
	type answer struct {
		result sandbox.Result
		err    error
	}
	ran := make(chan answer, 1)
	go func() {
		result, err := box.Run(s.ctx, "print(1)", []string{"/tmp/out.txt"})
		ran <- answer{result, err}
	}()
	select {
	case <-runs.ran:
	case <-time.After(5 * time.Second):
		s.FailNow("the code never ran")
	}

	refused.Store(true)

	var got answer
	select {
	case got = <-ran:
	case <-time.After(5 * time.Second):
		s.FailNow("the run never ended")
	}
	s.Require().NoError(got.err, "the code ran, so the model is told what it did")
	s.Equal("ran\n", got.result.Output)
	s.Equal([]sandbox.File{{Name: "out.txt", MIME: "text/plain", Data: []byte("written")}}, got.result.Files,
		"its files are read before the sandbox stops")
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Stopped, held.State, "stopped all the same")
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, s.meter.all())
}
