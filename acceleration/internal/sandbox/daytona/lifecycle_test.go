package daytona

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sdk "github.com/daytona/clients/sdk-go/pkg/daytona"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
)

// told is what a sandbox told its meter, in order.
type told struct {
	mu   sync.Mutex
	said []string
}

func (t *told) add(said string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.said = append(t.said, said)
}

func (t *told) Created(r Reserved) {
	t.add(fmt.Sprintf("created %s %g vCPU %g GiB %g GiB %s", r.ID, r.CPU, r.MemoryGB, r.DiskGB, r.Class))
}
func (t *told) Stopped()  { t.add("stopped") }
func (t *told) Started()  { t.add("started") }
func (t *told) Orphaned() { t.add("orphaned") }
func (t *told) Gone()     { t.add("gone") }

func (t *told) all() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.said)
}

// lockedLog is a log a sandbox's timers write while the test reads it.
type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// LifecycleSuite runs sandboxes through the states Daytona keeps them in, against a Daytona
// in process.
type LifecycleSuite struct {
	suite.Suite
	ctx     context.Context
	daytona *daytonatest.Server
	meter   *told
	logs    *lockedLog
}

func TestLifecycleSuite(t *testing.T) {
	suite.Run(t, new(LifecycleSuite))
}

func (s *LifecycleSuite) SetupTest() {
	s.ctx = context.Background()
	s.daytona = daytonatest.Start(s.T())
	s.meter = &told{}
	s.logs = &lockedLog{}
}

// box is a sandbox on the test's Daytona, metered by the test, on lifecycle.
func (s *LifecycleSuite) box(lifecycle sandbox.Lifecycle, labels map[string]string) *Sandbox {
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: lifecycle, Labels: labels, Meter: s.meter,
		Logger: slog.New(slog.NewTextHandler(s.logs, nil))})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	return box
}

// stalled is a broker whose BeforeRun waits until let is closed, so a run holds its sandbox
// for as long as a test needs.
type stalled struct {
	entered chan struct{}
	let     chan struct{}
	once    sync.Once
}

// release lets the run go on.
func (b *stalled) release() { b.once.Do(func() { close(b.let) }) }

func (b *stalled) Prepare(context.Context, []sandbox.Grant) (sandbox.Mount, error) {
	return sandbox.Mount{}, nil
}

func (b *stalled) BeforeRun(ctx context.Context) (map[string]string, error) {
	b.entered <- struct{}{}
	select {
	case <-b.let:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *stalled) AfterRun(context.Context) error { return nil }

func (b *stalled) Close(context.Context) error { return nil }

// busy is a sandbox on lifecycle with a run inside it until the broker's let is closed. Close
// gives up waiting for that run after half a second, which a create on the test's Daytona takes
// far less than.
func (s *LifecycleSuite) busy(lifecycle sandbox.Lifecycle) (*Sandbox, *stalled) {
	broker := &stalled{entered: make(chan struct{}, 1), let: make(chan struct{})}
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: lifecycle, Meter: s.meter,
		Broker: broker, Timeout: 500 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(s.logs, nil))})
	s.Require().NoError(err)
	box.check = time.Minute // the stalled refresh outlasts Close's wait, as a long run of code does
	ran := make(chan error, 1)
	go func() {
		_, err := box.Run(s.ctx, "print(1)", nil)
		ran <- err
	}()
	s.T().Cleanup(func() {
		broker.release()
		s.NoError(<-ran, "the run itself")
	})
	select {
	case <-broker.entered:
	case err := <-ran:
		s.Require().NoError(err, "the run ended before it held the sandbox")
	}
	return box, broker
}

func (s *LifecycleSuite) run(box *Sandbox) {
	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)
}

// told waits for the meter to have been told want. A poll that outlives a failed wait reads the
// meter it was given, not the suite's field, which the next test's SetupTest replaces.
func (s *LifecycleSuite) told(want ...string) {
	meter := s.meter
	s.EventuallyWithT(func(c *assert.CollectT) { assert.Equal(c, want, meter.all()) }, 5*time.Second, 10*time.Millisecond,
		"what the meter was told")
}

func (s *LifecycleSuite) TestASandboxIsCreatedWithLabelsAndALifecycle() {
	labels := map[string]string{"stream.env": "test", "stream.router": "shard-a", "stream.organization": "org-1",
		"stream.app": "app-1", "stream.session": "session-1"}
	box := s.box(sandbox.Lifecycle{IdleStop: 15 * time.Minute, StoppedDelete: time.Hour}, labels)

	s.run(box)

	s.Require().Len(s.daytona.Creations(), 1)
	created := s.daytona.Creations()[0]
	s.EqualValues(15, created["autoStopInterval"], "Daytona stops it when the meter does")
	s.EqualValues(60, created["autoDeleteInterval"], "and deletes it an hour after")
	s.NotContains(created, "autoArchiveInterval", "it is deleted long before Daytona would archive it")
	held, ok := s.daytona.Sandbox("sandbox-1")
	s.Require().True(ok)
	for name, value := range labels {
		s.Equal(value, held.Labels[name], name)
	}
	s.Equal("python", held.Labels["code-toolbox-language"], "Daytona's own label is kept")
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container"}, s.meter.all(),
		"billed from the create, at the size Daytona says it made")
}

func (s *LifecycleSuite) TestALifecycleIsInWholeMinutes() {
	box := s.box(sandbox.Lifecycle{IdleStop: 90 * time.Second, StoppedDelete: time.Second}, nil)

	s.run(box)

	created := s.daytona.Creations()[0]
	s.EqualValues(2, created["autoStopInterval"], "rounded up, so Daytona never stops it before the meter does")
	s.EqualValues(1, created["autoDeleteInterval"])
}

func (s *LifecycleSuite) TestWithoutALifecycleDaytonasIdleStopIsKept() {
	box := s.box(sandbox.Lifecycle{}, nil)

	s.run(box)

	created := s.daytona.Creations()[0]
	s.EqualValues(15, created["autoStopInterval"])
	s.NotContains(created, "autoDeleteInterval", "Daytona never deletes a stopped sandbox on its own")
}

func (s *LifecycleSuite) TestABusySandboxIsNotReadBeforeEachRun() {
	box := s.box(sandbox.Lifecycle{IdleStop: time.Minute}, nil)

	for range 3 {
		s.run(box)
	}

	s.Equal([]string{"POST /api/sandbox"}, s.daytona.Calls(), "a sandbox in use is as it was left")
}

func (s *LifecycleSuite) TestAStoppedSandboxIsStartedAgain() {
	box := s.box(sandbox.Lifecycle{IdleStop: 200 * time.Millisecond}, nil)
	s.run(box)
	s.daytona.Stop("sandbox-1")
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().NoError(err, "the next run after Daytona stopped it")
	s.Equal([]string{"sandbox-1: print(1)", "sandbox-1: print(2)"}, s.daytona.Ran(), "in the same sandbox, files and all")
	s.Contains(s.daytona.Calls(), "POST /api/sandbox/sandbox-1/start")
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "started")
	generation, err := box.Ready(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, generation, "the same sandbox")
}

func (s *LifecycleSuite) TestADeletedSandboxIsMadeAgain() {
	box := s.box(sandbox.Lifecycle{IdleStop: 200 * time.Millisecond}, nil)
	s.run(box)
	s.daytona.Gone("sandbox-1")
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")

	generation, err := box.Ready(s.ctx)

	s.Require().NoError(err)
	s.Equal(2, generation, "a new sandbox, holding nothing written into the last")
	s.run(box)
	s.Equal([]string{"sandbox-1: print(1)", "sandbox-2: print(1)"}, s.daytona.Ran())
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone", "created sandbox-2 1 vCPU 1 GiB 3 GiB container")
}

func (s *LifecycleSuite) TestASandboxDeletedWhileBusyIsMadeAgainAndTheRunRetried() {
	box := s.box(sandbox.Lifecycle{IdleStop: time.Minute}, nil)
	s.run(box)
	s.daytona.Gone("sandbox-1")

	ran, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().NoError(err, "a sandbox someone else deleted while in use is made again")
	s.Equal("ran\n", ran.Output)
	s.Equal([]string{"sandbox-1: print(1)", "sandbox-2: print(2)"}, s.daytona.Ran(), "the run tried once more, in the new one")
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone", "created sandbox-2 1 vCPU 1 GiB 3 GiB container"},
		s.meter.all(), "the old sandbox's meter stopped")
	s.Contains(s.logs.String(), `msg="a sandbox in use was deleted by someone else; making another" sandbox=sandbox-1`)

	s.Require().NoError(box.Write(s.ctx, "data/in.csv", []byte("a,b")))
	s.daytona.Gone("sandbox-2")
	s.Require().NoError(box.Write(s.ctx, "data/in.csv", []byte("c,d")))
	s.Equal(map[string][]byte{"data/in.csv": []byte("c,d")}, s.daytona.Files("sandbox-3"), "a write is retried the same way")
	generation, err := box.Ready(s.ctx)
	s.Require().NoError(err)
	s.Equal(3, generation)
}

func (s *LifecycleSuite) TestASandboxInAFailedStateIsDeletedAndMadeAgain() {
	for _, state := range []string{"error", "build_failed", "unknown"} {
		s.SetupTest()
		box := s.box(sandbox.Lifecycle{IdleStop: 200 * time.Millisecond}, nil)
		s.run(box)
		s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
		s.daytona.SetState("sandbox-1", state)

		_, err := box.Run(s.ctx, "print(2)", nil)

		s.Require().NoError(err, state)
		s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "%s: deleted, not started", state)
		s.NotContains(s.daytona.Calls(), "POST /api/sandbox/sandbox-1/start", state)
		s.Equal([]string{"sandbox-1: print(1)", "sandbox-2: print(2)"}, s.daytona.Ran(), state)
		s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone", "created sandbox-2 1 vCPU 1 GiB 3 GiB container")
		s.Contains(s.logs.String(), "deleted a sandbox in a state it cannot run code in; making another\" sandbox=sandbox-1 state="+state)
	}
}

func (s *LifecycleSuite) TestASandboxIsDeletedWhenTheRunItsCloseGaveUpOnEnds() {
	box, run := s.busy(sandbox.Lifecycle{IdleStop: time.Hour})

	s.ErrorIs(box.Close(), context.DeadlineExceeded, "the run still holds the sandbox")
	s.Empty(s.daytona.Deleted(), "nothing is deleted under a run")
	run.release()

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted as the run ended, long before it idled")
}

func (s *LifecycleSuite) TestASandboxNotDeletedWhenTheRunItsCloseGaveUpOnEndsIsDeletedOnceIdle() {
	box, run := s.busy(sandbox.Lifecycle{IdleStop: 300 * time.Millisecond, StoppedDelete: time.Hour})
	s.Error(box.Close())
	s.daytona.RefuseDeletes(true)
	run.release()

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned")
	s.daytona.RefuseDeletes(false)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted once it idled, by its own timer")
	// The warning is written before the gate is let go, so by the timer's delete it is there.
	s.Contains(s.logs.String(), `msg="could not close a sandbox once the run its Close gave up on ended"`)
}

func (s *LifecycleSuite) TestAnOrphanIsDeletedOnceIdleWhenTheDeleteAfterItsRunOutlastedTheIdleStop() {
	deletes := &stallingDeletes{}
	broker := &stalled{entered: make(chan struct{}, 1), let: make(chan struct{})}
	// The delete after the run stalls for the whole 500 ms Timeout, past the 300 ms IdleStop, so
	// the timer the run armed fires while that delete holds the sandbox.
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(),
		Lifecycle: sandbox.Lifecycle{IdleStop: 300 * time.Millisecond, StoppedDelete: time.Hour}, Meter: s.meter, Broker: broker,
		Timeout: 500 * time.Millisecond, HTTPClient: &http.Client{Transport: deletes}, Logger: slog.New(slog.NewTextHandler(s.logs, nil))})
	s.Require().NoError(err)
	box.check = time.Minute // the stalled refresh outlasts Close's wait, as a long run of code does
	ran := make(chan error, 1)
	go func() {
		_, err := box.Run(s.ctx, "print(1)", nil)
		ran <- err
	}()
	<-broker.entered

	s.Error(box.Close())
	deletes.stall.Store(true)
	broker.release()
	s.NoError(<-ran)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted by its timer, armed again after the stalled delete")
}

// stallingDeletes passes requests to the test's Daytona, but holds the first delete after stall
// is set until its request gives up.
type stallingDeletes struct{ stall atomic.Bool }

func (t *stallingDeletes) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodDelete && t.stall.CompareAndSwap(true, false) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (s *LifecycleSuite) TestATimerDueWhileTheGateIsHeldFiresOnceItIsLetGo() {
	synctest.Test(s.T(), func(t *testing.T) {
		meter := &told{}
		box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(),
			Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute, StoppedDelete: time.Hour}, Meter: meter})
		require.NoError(t, err)
		box.box = &sdk.Sandbox{ID: "sandbox-1"}
		box.phase = stateRunning
		box.gate <- struct{}{}
		box.done(t.Context())
		// Work takes the gate and holds it past the idle stop, as the delete closeLater makes
		// after a run can, so the timer the run armed fires and finds the gate held. The work
		// then arms the timer again, due at once, before it lets the gate go.
		box.gate <- struct{}{}
		time.Sleep(2 * time.Minute)
		box.arm()
		armed := box.epoch
		synctest.Wait()
		<-box.gate
		synctest.Wait()

		require.Equal(t, []string{"stopped"}, meter.all(), "stopped once, by the timer armed last")
		box.gate <- struct{}{}
		// The timer that waited for the gate finds itself replaced and sets no timer: one that
		// did would replace the next waiting one in turn, a chain of wake-ups that do nothing.
		// Only the stop arms the next point of the lifecycle.
		assert.Equal(t, armed+1, box.epoch, "timers armed after the gate was let go")
		box.timer.Stop()
		<-box.gate
	})
}

// The run ends with half of Close's wait left, so Close takes the sandbox with time to delete it. A
// run that ends in the last milliseconds of the wait leaves Close too little time, and the delete
// fails and the sandbox is left an orphan, for its timer to delete.
func (s *LifecycleSuite) TestASandboxIsDeletedByCloseWhenItsRunEndsWhileCloseWaits() {
	box, run := s.busy(sandbox.Lifecycle{IdleStop: time.Hour})
	ending := time.AfterFunc(box.timeout/2, run.release)
	defer ending.Stop()

	s.NoError(box.Close())

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted by Close, once, as the run ended")
}

func (s *LifecycleSuite) TestAnIdleSandboxIsBilledAsStoppedThenGone() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 200 * time.Millisecond}, nil)

	s.run(box)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone")
	held, _ := s.daytona.Sandbox("sandbox-1")
	s.Equal(daytonatest.Started, held.State, "the meter keeps Daytona's clock; it deletes nothing itself")
}

func (s *LifecycleSuite) TestASandboxDaytonaDidNotStopIsReported() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond}, nil)
	box.grace = 100 * time.Millisecond

	s.run(box)

	logs := s.logs
	s.Eventually(func() bool { return strings.Contains(logs.String(), "Daytona has not stopped a sandbox") },
		5*time.Second, 10*time.Millisecond)
	s.Contains(s.logs.String(), `level=WARN msg="Daytona has not stopped a sandbox that is billed as stopped" sandbox=sandbox-1 billed_as=stopped state=started`)
}

func (s *LifecycleSuite) TestClosingDeletesTheSandboxAndEndsItsBill() {
	box := s.box(sandbox.Lifecycle{IdleStop: time.Minute}, nil)
	s.run(box)

	s.Require().NoError(box.Close())

	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "gone"}, s.meter.all())
}

func (s *LifecycleSuite) TestASandboxDaytonaDeletedIsClosedWithoutComplaint() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 100 * time.Millisecond}, nil)
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone")
	s.daytona.Gone("sandbox-1")

	s.NoError(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone"}, s.meter.all(), "gone once")
}

func (s *LifecycleSuite) TestAnOrphanIsDeletedOnceIdle() {
	box := s.box(sandbox.Lifecycle{IdleStop: 300 * time.Millisecond, StoppedDelete: time.Hour}, nil)
	s.run(box)
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close(), "Daytona would not delete it")
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned")
	s.daytona.RefuseDeletes(false)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "deleted once it idled, by its own timer")
}

func (s *LifecycleSuite) TestAnOrphanDaytonaWillNotDeleteIsBilledUntilDaytonaDeletesIt() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 200 * time.Millisecond}, nil)
	s.run(box)
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "stopped", "gone")
	s.Contains(s.logs.String(), "could not delete a sandbox whose session closed")
}

func (s *LifecycleSuite) TestClosingAGoneSandboxWhenDeleteFailsDoesNotEmitOrphaned() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 100 * time.Millisecond}, nil)
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone")
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())

	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone"}, s.meter.all(),
		"a sandbox already billed as gone must not be billed as orphaned")
	s.Nil(box.box, "closed cleanly with no abandoned sandbox")
	s.NoError(box.Close(), "a second Close is a no-op once closed")
}

func (s *LifecycleSuite) TestAnOrphanedStoppedSandboxWithoutStoppedDeleteRetriesDeletion() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond}, nil)
	box.retryBase = 50 * time.Millisecond
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned"}, s.meter.all())
	s.daytona.RefuseDeletes(false)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "retried and deleted once Daytona recovers")
}

func (s *LifecycleSuite) TestAnOrphanedStoppedSandboxRetriesDeletionBeforeStoppedDelete() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: time.Hour}, nil)
	box.retryBase = 50 * time.Millisecond
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned"}, s.meter.all())
	s.daytona.RefuseDeletes(false)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "retried and deleted long before StoppedDelete")
}

func (s *LifecycleSuite) TestAnOrphanedStoppedSandboxRetriesOnBackoffUntilStoppedDeleteExpires() {
	synctest.Test(s.T(), func(t *testing.T) {
		meter := &told{}
		deletes := &countingDeletes{}
		box, err := New(Options{APIKey: "key-1", APIURL: "http://127.0.0.1:9/api",
			Lifecycle: sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 350 * time.Millisecond}, Meter: meter,
			HTTPClient: &http.Client{Transport: deletes},
			Logger:     slog.New(slog.DiscardHandler)})
		require.NoError(t, err)
		box.retryBase = 60 * time.Millisecond
		_, err = box.Ready(t.Context())
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, []string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, meter.all())

		require.Error(t, box.Close())
		time.Sleep(time.Second)
		synctest.Wait()

		assert.Equal(t, []string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned", "gone"}, meter.all())
		// Close at +100ms, retries at +160ms and +280ms, and the last at the +450ms expiry.
		assert.EqualValues(t, 4, deletes.n.Load(), "retried deletion on backoff before StoppedDelete expired")
		require.NoError(t, box.Close(), "closed cleanly once StoppedDelete expired")
		assert.Nil(t, box.box, "sandbox reference cleared once StoppedDelete expired")
	})
}

func (s *LifecycleSuite) TestAnOrphanedStoppedSandboxBacksOffExponentially() {
	synctest.Test(s.T(), func(t *testing.T) {
		meter := &told{}
		box, err := New(Options{APIKey: "key-1", APIURL: "http://127.0.0.1:9/api",
			Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute}, Meter: meter,
			HTTPClient: &http.Client{Transport: failingDeletes{}},
			Logger:     slog.New(slog.DiscardHandler)})
		require.NoError(t, err)
		_, err = box.Ready(t.Context())
		require.NoError(t, err)
		// Advance past IdleStop so the sandbox transitions to stopped.
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, []string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped"}, meter.all())

		require.Error(t, box.Close())
		require.Equal(t, []string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned"}, meter.all())

		// Expect retries at +30s, +60s, +120s, +240s, +480s, then capped at +900s (15m).
		wantDelays := []time.Duration{
			30 * time.Second,
			time.Minute,
			2 * time.Minute,
			4 * time.Minute,
			8 * time.Minute,
			15 * time.Minute,
			15 * time.Minute,
		}
		for _, want := range wantDelays {
			box.gate <- struct{}{}
			got := time.Until(box.retryAt)
			<-box.gate
			require.Equal(t, want, got)
			time.Sleep(want)
			synctest.Wait()
		}

		box.gate <- struct{}{}
		box.timer.Stop()
		<-box.gate
	})
}

// failingDeletes is an in-memory HTTP transport (no sockets, safe inside synctest) that creates
// a started sandbox and fails every delete.
type failingDeletes struct{}

func (failingDeletes) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sandbox") {
		body := `{"id":"sandbox-1","organizationId":"org-test","name":"sandbox-1","user":"daytona","env":{},"labels":{},"public":false,"networkBlockAll":false,"target":"us","cpu":1,"gpu":0,"memory":1,"disk":3,"state":"started","sandboxClass":"container","toolboxProxyUrl":"http://daytona.test/toolbox"}`
		return &http.Response{
			StatusCode: http.StatusCreated,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
	return nil, fmt.Errorf("daytona unavailable")
}

// countingDeletes is failingDeletes that counts the deletes it fails.
type countingDeletes struct {
	failingDeletes
	n atomic.Int32
}

func (c *countingDeletes) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodDelete {
		c.n.Add(1)
	}
	return c.failingDeletes.RoundTrip(r)
}

func (s *LifecycleSuite) TestClosingAnOrphanedStoppedSandboxTwiceEmitsOrphanedOnce() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: time.Hour}, nil)
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.Error(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned"}, s.meter.all(),
		"a repeated failed Close does not emit orphaned again")

	s.daytona.RefuseDeletes(false)
	s.NoError(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "orphaned", "gone"}, s.meter.all())
	s.Nil(box.box)
}

func (s *LifecycleSuite) TestAnOrphanedRunningSandboxRetriesAtIdleStopAndThenOnBackoffWhenStopped() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: time.Hour}, nil)
	box.retryBase = 50 * time.Millisecond
	s.run(box)
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "stopped")
	s.daytona.RefuseDeletes(false)

	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "stopped", "gone")
	s.Equal([]string{"sandbox-1"}, s.daytona.Deleted(), "retried on backoff after entering orphaned stopped")
}

func (s *LifecycleSuite) TestRevivingAGoneSandboxDaytonaDeletedDoesNotEmitGoneTwice() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 100 * time.Millisecond}, nil)
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone")
	s.daytona.Gone("sandbox-1")

	generation, err := box.Ready(s.ctx)

	s.Require().NoError(err)
	s.Equal(2, generation)
	s.Equal([]string{
		"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone",
		"created sandbox-2 1 vCPU 1 GiB 3 GiB container",
	}, s.meter.all(), "gone once before creating the replacement sandbox")
}

func (s *LifecycleSuite) TestRevivingAGoneSandboxStillAliveBillsAgain() {
	box := s.box(sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: 100 * time.Millisecond}, nil)
	s.run(box)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone")
	s.daytona.Stop("sandbox-1")

	generation, err := box.Ready(s.ctx)

	s.Require().NoError(err)
	s.Equal(1, generation, "same sandbox restarted when Daytona had not deleted it yet")
	s.Equal([]string{
		"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped", "gone",
		"created sandbox-1 1 vCPU 1 GiB 3 GiB container",
	}, s.meter.all(), "billed as created again rather than started after gone")
}

// countingSlots is an in-memory sandbox.Slots that tracks active slots.
type countingSlots struct {
	held atomic.Int32
}

func (c *countingSlots) Take(context.Context, bool) (func(), error) {
	c.held.Add(1)
	var once sync.Once
	return func() { once.Do(func() { c.held.Add(-1) }) }, nil
}

func (s *LifecycleSuite) TestAnOrphanedRunningSandboxReleasesItsSlotWhenStoppedForIdling() {
	slots := &countingSlots{}
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(),
		Lifecycle: sandbox.Lifecycle{IdleStop: 100 * time.Millisecond, StoppedDelete: time.Hour},
		Meter:     s.meter, Slots: slots, Logger: slog.New(slog.NewTextHandler(s.logs, nil))})
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		s.daytona.RefuseDeletes(false)
		_ = box.Close()
	})
	s.run(box)
	s.Require().Equal(int32(1), slots.held.Load())
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "stopped")
	s.Equal(int32(0), slots.held.Load(), "slot released when idle orphan stops")
}

func (s *LifecycleSuite) TestClosingAnOrphanedRunningSandboxTwiceEmitsOrphanedOnce() {
	box := s.box(sandbox.Lifecycle{IdleStop: time.Hour, StoppedDelete: time.Hour}, nil)
	s.run(box)
	s.daytona.RefuseDeletes(true)

	s.Error(box.Close())
	s.Error(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned"}, s.meter.all(),
		"a repeated failed Close on an orphaned running sandbox does not emit orphaned again")

	s.daytona.RefuseDeletes(false)
	s.NoError(box.Close())
	s.Equal([]string{"created sandbox-1 1 vCPU 1 GiB 3 GiB container", "orphaned", "gone"}, s.meter.all())
}
