//go:build integration

package metered

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/turbopuffer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/turbopuffer/turbopuffertest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// KnowledgeGateSuite reads knowledge under the real enforcer, from a turbopuffer answered in
// process.
type KnowledgeGateSuite struct {
	suite.Suite
	ctx   context.Context
	money *policytest.Money
	owner routing.Owner
	// entries are the prices the test's work is billed at.
	entries []pricing.Entry
	// asked counts the requests turbopuffer was sent, held the money the owner's work had
	// reserved as each arrived, and down has it answer none.
	asked atomic.Int64
	held  atomic.Int64
	down  atomic.Bool
	owned OwnedKnowledge
}

func TestKnowledgeGateSuite(t *testing.T) { suite.Run(t, new(KnowledgeGateSuite)) }

func (s *KnowledgeGateSuite) SetupTest() {
	s.entries = prices.Vendor().Entries
	s.start()
	s.own(1_000_000)
}

func (s *KnowledgeGateSuite) start(change ...func(*policy.Settings)) {
	s.ctx = context.Background()
	s.money = policytest.Start(s.T(), "knowledge_gate", change...)
	s.down.Store(false)
}

// own makes the test's owner one with credit, whose knowledge is read as base reads it.
func (s *KnowledgeGateSuite) own(credit int64) {
	s.owner = s.money.Owner(s.T(), credit)
	s.owned = s.base(s.money.Enforcer, nil).For(s.owner)
}

// base is a knowledge base gated by gate, in the deployment's account unless keys holds the
// owner's.
func (s *KnowledgeGateSuite) base(gate routing.Gate, keys routing.CustomerKeys) *Knowledge {
	s.asked.Store(0)
	vendor := turbopuffertest.Start(s.T())
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.asked.Add(1)
		s.held.Store(s.money.Enforcer.Held(s.owner.OrganizationID).BalanceMicros)
		if s.down.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		vendor.Config.Handler.ServeHTTP(w, r)
	}))
	s.T().Cleanup(front.Close)
	list, err := pricing.NewList("vendor", s.entries, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	backend, err := turbopuffer.New(turbopuffer.Options{APIKey: "unused", BaseURL: front.URL})
	s.Require().NoError(err)
	base := NewRegionalKnowledge(map[string]knowledge.Backend{pricing.RegionUS: backend},
		s.money.Recorder(routing.Knowledge, list), gate, s.money.Store.KnowledgeNamespaces)
	if keys != nil {
		base.WithCustomerAccounts(keys, func(_, key string) (knowledge.Backend, error) {
			return turbopuffer.New(turbopuffer.Options{APIKey: key, BaseURL: front.URL})
		})
	}
	return base
}

func (s *KnowledgeGateSuite) write() {
	s.Require().NoError(s.owned.Upsert(s.ctx, "docs", []knowledge.Document{{ID: "a#0", Text: "A call costs a penny.", Source: "a"}}))
	s.asked.Store(0)
}

func (s *KnowledgeGateSuite) search() error {
	_, err := s.owned.Search(s.ctx, knowledge.Query{Namespace: "docs", Text: "penny"})
	return err
}

func (s *KnowledgeGateSuite) fetch(passages int) error {
	ids := make([]string, passages)
	for i := range ids {
		ids[i] = fmt.Sprintf("a#%d", i)
	}
	_, err := s.owned.Fetch(s.ctx, "docs", ids)
	return err
}

func (s *KnowledgeGateSuite) suspend() {
	s.money.Usage.Suspend(s.T(), s.owner.OrganizationID, "review")
	s.Require().Eventually(func() bool {
		_, err := s.money.Enforcer.Admit(s.ctx, s.owner)
		return errors.Is(err, policy.ErrSuspended)
	}, 5*time.Second, 5*time.Millisecond, "the suspension is read")
}

// lookups drains the writer and returns the owner's lookup rows, which are want of them.
func (s *KnowledgeGateSuite) lookups(want int) []store.Request {
	s.money.Writer.Close(s.ctx)
	var rows []store.Request
	s.Require().NoError(s.money.Store.ShardDB().NewSelect().Model(&rows).
		Where("customer_id = ? AND model = 'lookup'", s.owner.CustomerID).Scan(s.ctx))
	s.Require().Len(rows, want)
	return rows
}

func (s *KnowledgeGateSuite) TestWithNoCreditNothingIsReadAndNoVendorIsAsked() {
	s.own(0)

	s.ErrorIs(s.search(), policy.ErrNoCredits)
	s.ErrorIs(s.fetch(1), policy.ErrNoCredits)
	s.Zero(s.asked.Load())
	s.NoError(s.fetch(0), "reading back nothing asks for nothing")
	s.lookups(0)
}

func (s *KnowledgeGateSuite) TestASuspendedOrganizationReadsNothing() {
	s.write()
	s.suspend()

	s.ErrorIs(s.search(), policy.ErrSuspended)
	s.ErrorIs(s.fetch(1), policy.ErrSuspended)
	s.Zero(s.asked.Load())
	s.NoError(s.owned.Delete(s.ctx, "docs", []string{"a#0"}), "what is kept can always be removed")
}

// reserved is what an owner of its own had set aside as read asked turbopuffer.
func (s *KnowledgeGateSuite) reserved(read func() error) int64 {
	s.own(1_000_000)
	s.Require().NoError(read())
	return s.held.Load()
}

func (s *KnowledgeGateSuite) TestALookupReservesTheLeastItsQueriesAreBilled() {
	s.EqualValues(1, s.reserved(s.search), "1.28 GB at 1.08 times $1 a PB")
	s.EqualValues(1, s.reserved(func() error { return s.fetch(1) }))
	s.EqualValues(1, s.reserved(func() error { return s.fetch(256) }), "one request")
	s.EqualValues(3, s.reserved(func() error { return s.fetch(257) }), "two requests, 2.76 micros")
	s.EqualValues(6, s.reserved(func() error { return s.fetch(1_024) }), "four requests, 5.53 micros")
}

func (s *KnowledgeGateSuite) TestALookupThatFailsLetsGoOfWhatItReserved() {
	s.down.Store(true)

	s.Require().Error(s.search())

	s.EqualValues(1, s.held.Load())
	s.Zero(s.money.Enforcer.Held(s.owner.OrganizationID).BalanceMicros)
}

func (s *KnowledgeGateSuite) TestALookupInsideAPaidTurnIsAdmittedAndChargedOnce() {
	s.own(1_000)
	s.write()
	list, err := pricing.NewList("vendor", pricing.AllAtCost([]pricing.Entry{{Modality: "llm", Provider: "paid", Model: "model", Region: pricing.RegionUS,
		Rates: map[pricing.Meter]pricing.Rate{pricing.InputTokens: {USD: 1, Per: 1_000_000}}}}), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	model := routing.ProviderConfig{Provider: "paid", Model: "model"}
	recorder := s.money.Recorder(routing.LLM, list)
	// The turn's response reserves all the credit, settles at what it used, and calls the tool.
	response := uuid.Must(uuid.NewV7())
	s.Require().NoError(recorder.Reserve(model, s.owner, response, pricing.Quantities{pricing.InputTokens: 5_000}, time.Minute))
	stat := routing.Stat{Owner: s.owner, UsageID: response, StartedAt: time.Now(), Success: true}
	stat.Use(pricing.Quantities{pricing.InputTokens: 10})
	recorder.Record(model, stat)

	s.Require().NoError(s.search())

	s.EqualValues(1, s.asked.Load())
	rows := s.lookups(1)
	s.EqualValues(1, rows[0].CostMicros)
	s.True(rows[0].Billable)
}

func (s *KnowledgeGateSuite) TestAnUnpricedLookupIsServedToAnOwnerWhoMayPay() {
	s.entries = nil
	s.own(1_000_000)

	s.NoError(s.search())
	s.NoError(s.fetch(1))
	s.EqualValues(2, s.asked.Load())
}

func (s *KnowledgeGateSuite) TestALookupThatCostsNothingNeedsNoCredit() {
	s.entries = []pricing.Entry{{Modality: "knowledge", Provider: "turbopuffer", Model: "lookup", Region: pricing.RegionUS, Free: true}}
	s.own(0)

	s.NoError(s.search())
	s.NoError(s.fetch(1))
}

func (s *KnowledgeGateSuite) TestABaseInTheCustomersOwnAccountIsReadWithNoCredit() {
	s.own(1_000_000)
	s.owned = s.base(s.money.Enforcer, keptKeys{s.owner.CustomerID + "/turbopuffer/us": customerTurbopufferKey}).For(s.owner)
	s.Require().NoError(s.owned.Keep(s.ctx, "docs"), "its place is kept, and nothing is billed")
	s.money.Usage.Credit(s.T(), s.owner.OrganizationID, -1_000_000)
	s.Require().Eventually(func() bool { return errors.Is(s.money.Spend(s.T(), s.owner), policy.ErrNoCredits) },
		5*time.Second, 5*time.Millisecond, "the credit is read")

	s.NoError(s.search())
	s.Zero(s.held.Load(), "the fee on a query is under a micro")
	s.NoError(s.fetch(1))

	s.suspend()
	s.ErrorIs(s.search(), policy.ErrSuspended)
}

func (s *KnowledgeGateSuite) TestARouterThatHasReadNothingRefusesASuspendedOrganization() {
	s.suspend()
	client, err := usage.NewClient(usage.ClientOptions{URL: s.money.Usage.URL, Token: s.money.Usage.Token})
	s.Require().NoError(err)
	cold, err := policy.New(s.money.Store, policy.Settings{Usage: client, FirstRead: 5 * time.Second}, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	defer cold.Close()
	s.owned = s.base(cold, nil).For(s.owner)

	s.ErrorIs(s.fetch(1), policy.ErrSuspended)
	s.Zero(s.asked.Load())
}

func (s *KnowledgeGateSuite) TestInShadowMoneyRefusesNoLookup() {
	s.start(func(settings *policy.Settings) { settings.Mode = config.LimitsShadow })
	s.unenforced()
	s.Require().Eventually(func() bool {
		_, read := s.money.Enforcer.Balance(s.owner.CustomerID, s.owner.OrganizationID)
		return read
	}, 5*time.Second, 5*time.Millisecond, "the suspension is read")

	s.NoError(s.search())
	s.NoError(s.fetch(1))
}

func (s *KnowledgeGateSuite) TestWithLimitsOffMoneyRefusesNoLookup() {
	s.start(func(settings *policy.Settings) { settings.Mode = config.LimitsOff })
	s.unenforced()

	s.NoError(s.search())
	s.NoError(s.fetch(1))
}

// unenforced makes the owner a suspended one, whose suspension no request is refused for.
func (s *KnowledgeGateSuite) unenforced() {
	app := uuid.NewString()
	s.owner = routing.Owner{CustomerID: app, Attribution: routing.Attribution{OrganizationID: "org-" + app}}
	s.Require().NoError(s.money.Store.AppOrganizations.Upsert(s.ctx,
		&store.AppOrganization{AppID: app, OrganizationID: s.owner.OrganizationID}))
	s.money.Usage.Suspend(s.T(), s.owner.OrganizationID, "review")
	s.money.Enforcer.Join(s.ctx, app, s.owner.OrganizationID, usage.DefaultTier)
	s.owned = s.base(s.money.Enforcer, nil).For(s.owner)
}
