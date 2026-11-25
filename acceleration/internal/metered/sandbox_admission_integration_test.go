//go:build integration

package metered

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// plenty is credit no test spends.
const plenty = 1_000_000_000

// idle is how soon the tests' sandboxes idle, so a test can take one to have stopped.
const idle = 100 * time.Millisecond

// money is a real enforcer reading a usage service of the test's own, with the settings changed.
func (s *MeteredSuite) money(change ...func(*policy.Settings)) *policytest.Money {
	return policytest.Start(s.T(), "metered_sandboxes", change...)
}

// admitting makes sandboxes on server admitted by money, idling after idle, a slice of each
// lasting every, in the owner's own organization where keys holds their key.
func (s *MeteredSuite) admitting(money *policytest.Money, server *daytonatest.Server, every time.Duration, keys routing.CustomerKeys) *Sandboxes {
	return NewSandboxes(SandboxesOptions{Recorder: s.recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: idle},
		APIKey: "key-1", APIURL: server.APIURL(), Keys: keys, Every: every, Logger: slog.New(slog.DiscardHandler)})
}

// payer is an app of an organization of its own with credit micros, once money has read it, in
// a session of its own, whose rows rows reads.
func (s *MeteredSuite) payer(money *policytest.Money, credit int64) routing.Owner {
	owner := money.Owner(s.T(), credit)
	owner.SessionID = "session-" + uuid.NewString()
	s.owner = owner
	return owner
}

// refusing waits until money refuses the owner's billable work with kind.
func (s *MeteredSuite) refusing(money *policytest.Money, owner routing.Owner, kind error) {
	s.Require().Eventually(func() bool {
		admission, err := money.Enforcer.Admit(s.ctx, owner)
		return errors.Is(err, kind) || errors.Is(admission.Spend, kind)
	}, 5*time.Second, 10*time.Millisecond, "the organization's standing is read")
}

// opened is a sandbox of owner's.
func (s *MeteredSuite) opened(sandboxes *Sandboxes, owner routing.Owner) sandbox.Sandbox {
	box, err := sandboxes.Open(s.ctx, owner, SandboxSpec{})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	return box
}

// ran runs code in box, which must run it.
func (s *MeteredSuite) ran(box sandbox.Sandbox) {
	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)
}

// stoppedFor is box made and then stopped for idling by Daytona, and taken by the router to
// have stopped too, so its next run would start it again.
func (s *MeteredSuite) stoppedFor(box sandbox.Sandbox, server *daytonatest.Server) {
	s.ran(box)
	ran := time.Now()
	server.Stop("sandbox-1")
	s.Require().Eventually(func() bool { return time.Since(ran) > idle }, time.Second, 10*time.Millisecond)
}

// refusedEverything checks that money refused, with kind, both starting the stopped box again
// and a new sandbox of owner's.
func (s *MeteredSuite) refusedEverything(box sandbox.Sandbox, sandboxes *Sandboxes, owner routing.Owner,
	server *daytonatest.Server, kind error,
) {
	_, err := box.Run(s.ctx, "print(2)", nil)
	s.Require().ErrorIs(err, kind, "the refusal is what the model is told")
	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/start", "not started again")
	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/stop", "nor stopped again")

	_, err = s.opened(sandboxes, owner).Run(s.ctx, "print(3)", nil)
	s.Require().ErrorIs(err, kind)
	s.Equal([]string{"sandbox-1"}, server.Created(), "no other is made")
	s.Equal([]string{"sandbox-1: print(1)"}, server.Ran())
}

func (s *MeteredSuite) TestASuspendedOrganizationNeitherMakesNorStartsAgainASandbox() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := s.admitting(money, server, time.Hour, nil)
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.stoppedFor(box, server)

	money.Usage.Suspend(s.T(), owner.OrganizationID, "fraud")
	s.refusing(money, owner, policy.ErrSuspended)

	s.refusedEverything(box, sandboxes, owner, server, policy.ErrSuspended)
}

func (s *MeteredSuite) TestAnOrganizationWithNoCreditNeitherMakesNorStartsAgainASandbox() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := s.admitting(money, server, time.Hour, nil)
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.stoppedFor(box, server)

	money.Usage.Credit(s.T(), owner.OrganizationID, -plenty)
	s.refusing(money, owner, policy.ErrNoCredits)

	s.refusedEverything(box, sandboxes, owner, server, policy.ErrNoCredits)
	_, err := box.Run(s.ctx, "print(4)", nil)
	s.ErrorContains(err, "no credit left")
}

func (s *MeteredSuite) TestAnOrganizationOverItsSpendHardLimitNeitherMakesNorStartsAgainASandbox() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := s.admitting(money, server, time.Hour, nil)
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.stoppedFor(box, server)

	daily := int64(1_000)
	money.Usage.SetLimits(s.T(), owner.OrganizationID, usage.Limits{SpendHardLimit: &usage.SpendHardLimit{DailyMicros: &daily}})
	money.Usage.Spend(s.T(), owner.OrganizationID, daily)
	s.refusing(money, owner, policy.ErrSystemLimit)

	s.refusedEverything(box, sandboxes, owner, server, policy.ErrSystemLimit)
}

func (s *MeteredSuite) TestASandboxInTheOwnersOwnDaytonaOrganizationIsRefusedWithNoCredit() {
	money, server := s.money(), daytonatest.Start(s.T())
	owner := s.payer(money, 0)
	sandboxes := s.admitting(money, server, time.Hour, keptKeys{owner.CustomerID + "/daytona/us": customerDaytonaKey})
	s.refusing(money, owner, policy.ErrNoCredits)

	_, err := s.opened(sandboxes, owner).Run(s.ctx, "print(1)", nil)

	s.Require().ErrorIs(err, policy.ErrNoCredits, "its BYOK fee is billed, as a BYOK model's is")
	s.Empty(server.Created())
}

func (s *MeteredSuite) TestASandboxThePricesMakeFreeIsMadeWithNoCredit() {
	money, server := s.money(), daytonatest.Start(s.T())
	free, err := pricing.NewList("free", []pricing.Entry{{Modality: string(routing.Sandbox), Provider: daytona.Provider,
		Model: SandboxRunning, Region: pricing.RegionUS, Free: true}}, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	recorder := routing.NewRecorder(routing.Sandbox, s.store, nil, free, slog.New(slog.DiscardHandler))
	s.T().Cleanup(recorder.Close)
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: idle},
		APIKey: "key-1", APIURL: server.APIURL(), Every: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, 0)
	s.refusing(money, owner, policy.ErrNoCredits)

	s.ran(s.opened(sandboxes, owner))

	s.Equal([]string{"sandbox-1"}, server.Created(), "work that costs nothing runs on, as a free model answers")
}

func (s *MeteredSuite) TestASandboxRecordedNowhereIsRefusedWithNoCredit() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := NewSandboxes(SandboxesOptions{Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: idle},
		APIKey: "key-1", APIURL: server.APIURL(), Every: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, 0)
	s.refusing(money, owner, policy.ErrNoCredits)

	_, err := s.opened(sandboxes, owner).Run(s.ctx, "print(1)", nil)

	s.Require().ErrorIs(err, policy.ErrNoCredits, "Daytona bills it whether or not it is recorded")
	s.Empty(server.Created())
}

func (s *MeteredSuite) TestASandboxInTheOwnersOwnDaytonaOrganizationIsRefusedWhileTheyAreSuspended() {
	money, server := s.money(), daytonatest.Start(s.T())
	owner := s.payer(money, plenty)
	sandboxes := s.admitting(money, server, time.Hour, keptKeys{owner.CustomerID + "/daytona/us": customerDaytonaKey})
	money.Usage.Suspend(s.T(), owner.OrganizationID, "fraud")
	s.refusing(money, owner, policy.ErrSuspended)

	_, err := s.opened(sandboxes, owner).Run(s.ctx, "print(1)", nil)

	s.Require().ErrorIs(err, policy.ErrSuspended)
	s.Empty(server.Created())
}

func (s *MeteredSuite) TestARunRefusedForMoneyStopsTheRunningSandboxAtOnceUntilItsOrganizationMayPayAgain() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: s.recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute},
		APIKey: "key-1", APIURL: server.APIURL(), Every: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.ran(box)
	money.Usage.Suspend(s.T(), owner.OrganizationID, "fraud")
	s.refusing(money, owner, policy.ErrSuspended)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, policy.ErrSuspended)
	held, _ := server.Sandbox("sandbox-1")
	s.Equal(daytonatest.Stopped, held.State, "stopped rather than left to bill until it idles")

	money.Usage.Restore(s.T(), owner.OrganizationID)
	s.Require().Eventually(func() bool {
		admission, err := money.Enforcer.Admit(s.ctx, owner)
		return err == nil && admission.Spend == nil
	}, 5*time.Second, 10*time.Millisecond, "the restored standing is read")
	s.ran(box)

	s.Contains(server.Calls(), "POST /api/sandbox/sandbox-1/start", "the same sandbox, started again")
	s.Require().NoError(box.Close())
	s.Equal([]string{SandboxRunning, SandboxStopped, SandboxRunning}, models(s.rows()), "billed as stopped while it was")
}

func (s *MeteredSuite) TestARunningSandboxIsStoppedOnceItsOrganizationRunsOutOfCredit() {
	money, server := s.money(), daytonatest.Start(s.T())
	every := 50 * time.Millisecond
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: s.recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute},
		APIKey: "key-1", APIURL: server.APIURL(), Every: every, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.ran(box)

	money.Usage.Credit(s.T(), owner.OrganizationID, -plenty)

	s.Require().Eventually(func() bool {
		held, _ := server.Sandbox("sandbox-1")
		return held.State == daytonatest.Stopped
	}, 5*time.Second, 10*time.Millisecond, "stopped with no more code run in it")
	_, err := box.Run(s.ctx, "print(2)", nil)
	s.Require().ErrorIs(err, policy.ErrNoCredits)
	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/start")
	s.Require().NoError(box.Close())
	billed := models(s.rows())
	stopped := slices.Index(billed, SandboxStopped)
	s.Require().Positive(stopped, "running, then stopped: %v", billed)
	s.NotContains(billed[stopped:], SandboxRunning, "billed as stopped from the stop on: %v", billed)
}

func (s *MeteredSuite) TestARunningSandboxRunsOnWhileOtherWorkHoldsMoreThanItsOrganizationsCredit() {
	money, server := s.money(), daytonatest.Start(s.T())
	// Recorded through the enforcer, as the router records sandboxes.
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: money.Recorder(routing.Sandbox, s.prices), Gate: money.Enforcer,
		Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute}, APIKey: "key-1", APIURL: server.APIURL(), Every: 50 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, 1_000_000)
	box := s.opened(sandboxes, owner)
	s.ran(box)
	other := uuid.Must(uuid.NewV7())
	s.Require().NoError(money.Enforcer.Reserve(owner, other, 1_500_000, time.Minute))
	s.T().Cleanup(func() { money.Enforcer.Release(other) })
	s.refusing(money, owner, policy.ErrNoCredits)

	s.ran(box)

	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/stop", "$1.00 of credit is left, only held by other work")
	_, err := s.opened(sandboxes, owner).Run(s.ctx, "print(2)", nil)
	s.Require().ErrorIs(err, policy.ErrNoCredits, "new work is still refused for what is held")
	s.Equal([]string{"sandbox-1"}, server.Created())
}

func (s *MeteredSuite) TestInShadowASandboxOfAnOrganizationWithNoCreditIsMadeAndCountedAsWouldRefuse() {
	var mu sync.Mutex
	var hits []policy.LimitHit
	money := s.money(func(settings *policy.Settings) {
		settings.Mode = config.LimitsShadow
		settings.LimitHit = func(hit policy.LimitHit) {
			mu.Lock()
			defer mu.Unlock()
			hits = append(hits, hit)
		}
	})
	heard := func() []policy.LimitHit {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(hits)
	}
	server := daytonatest.Start(s.T())
	sandboxes := s.admitting(money, server, time.Hour, nil)
	stamp := time.Now().UnixNano()
	owner := routing.Owner{CustomerID: fmt.Sprintf("app-%d", stamp), Attribution: routing.Attribution{OrganizationID: fmt.Sprintf("org-%d", stamp)}}
	owner.SessionID = "session-" + uuid.NewString()
	s.owner = owner
	s.Require().NoError(money.Store.AppOrganizations.Upsert(s.ctx, &store.AppOrganization{AppID: owner.CustomerID, OrganizationID: owner.OrganizationID}))
	money.Enforcer.Join(s.ctx, owner.CustomerID, owner.OrganizationID, usage.DefaultTier)
	s.Require().Eventually(func() bool {
		_, err := money.Enforcer.Admit(s.ctx, owner)
		return err == nil && len(heard()) > 0
	}, 5*time.Second, 10*time.Millisecond, "the organization's standing is read")
	before := len(heard())

	s.ran(s.opened(sandboxes, owner))

	s.Equal([]string{"sandbox-1"}, server.Created(), "shadow refuses nothing")
	after := heard()[before:]
	s.Require().NotEmpty(after, "the sandbox's admission is told as one that would have been refused")
	s.Equal(store.LimitNoCredit, after[0].Kind)
	s.False(after[0].Enforced)
}

func (s *MeteredSuite) TestWhileMoneyCannotBeCheckedARunningSandboxRunsOnAndNoOtherIsMade() {
	redis, sharing := policytest.SharedHolds(s.T())
	money := s.money(sharing, func(settings *policy.Settings) { settings.FlushMaxLag = 300 * time.Millisecond })
	server := daytonatest.Start(s.T())
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: s.recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute},
		APIKey: "key-1", APIURL: server.APIURL(), Every: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, plenty)
	box := s.opened(sandboxes, owner)
	s.ran(box)
	redis.Stop()
	// A hold no other router can be told of leaves spend unchecked once FlushMaxLag has passed.
	s.Require().NoError(money.Enforcer.Reserve(owner, uuid.Must(uuid.NewV7()), 1, time.Minute))
	s.refusing(money, owner, policy.ErrSpendCheckUnavailable)

	s.ran(box)

	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/stop", "it passes again on its own, so what runs waits for it")
	_, err := s.opened(sandboxes, owner).Run(s.ctx, "print(2)", nil)
	s.Require().ErrorIs(err, policy.ErrSpendCheckUnavailable)
	s.True(policy.Transient(err))
	s.Equal([]string{"sandbox-1"}, server.Created(), "nothing new bills until it can be checked")
}

func (s *MeteredSuite) TestASandboxWhoseSessionClosedIsStillDeletedOnceItIdlesThoughItsOrganizationIsRefused() {
	money, server := s.money(), daytonatest.Start(s.T())
	sandboxes := NewSandboxes(SandboxesOptions{Recorder: s.recorder, Gate: money.Enforcer, Lifecycle: sandbox.Lifecycle{IdleStop: time.Second},
		APIKey: "key-1", APIURL: server.APIURL(), Every: 50 * time.Millisecond, Logger: slog.New(slog.DiscardHandler)})
	owner := s.payer(money, plenty)
	box, err := sandboxes.Open(s.ctx, owner, SandboxSpec{})
	s.Require().NoError(err)
	s.ran(box)
	server.RefuseDeletes(true)
	s.Require().Error(box.Close(), "Daytona would not delete it")
	money.Usage.Suspend(s.T(), owner.OrganizationID, "fraud")
	s.refusing(money, owner, policy.ErrSuspended)
	server.RefuseDeletes(false)

	s.Require().Eventually(func() bool { return slices.Contains(server.Deleted(), "sandbox-1") }, 5*time.Second, 10*time.Millisecond,
		"deleted once it idles, as any sandbox whose session could not delete it")
	s.NotContains(server.Calls(), "POST /api/sandbox/sandbox-1/stop", "rather than stopped and left for Daytona's timer")
}
