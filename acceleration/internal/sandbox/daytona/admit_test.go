//go:build integration

package daytona

import (
	"context"
	"errors"
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
	stall   atomic.Bool
	entered chan struct{}
	let     chan struct{}
}

func newStallingRuns() *stallingRuns {
	runs := &stallingRuns{entered: make(chan struct{}, 1), let: make(chan struct{})}
	runs.stall.Store(true)
	return runs
}

func (t *stallingRuns) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.stall.Load() && strings.HasSuffix(r.URL.Path, "/process/code-run") {
		t.entered <- struct{}{}
		select {
		case <-t.let:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	return http.DefaultTransport.RoundTrip(r)
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
		box.done()

		time.Sleep(time.Hour)
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, []time.Duration{time.Minute, 2 * time.Minute}, admitted)
	})
}
