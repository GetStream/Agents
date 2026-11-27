//go:build integration

package searchrouter

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing/pricingtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing/failovertest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/search"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// BillingSuite searches through a router over Postgres, priced by the vendor list, and reads
// back the rows each search left.
type BillingSuite struct {
	suite.Suite
	ctx      context.Context
	store    *store.Store
	prices   *pricing.List
	customer string
}

func TestBillingSuite(t *testing.T) {
	suite.Run(t, new(BillingSuite))
}

func (s *BillingSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "searchrouter_billing"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.prices, err = pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
}

func (s *BillingSuite) SetupTest() {
	s.customer = "search-" + uuid.NewString()
}

// routes is a router over two of the vendor list's search models, built by factories.
func (s *BillingSuite) routes(factories map[string]search.Provider) *Router {
	registry := NewRegistry()
	for name, provider := range factories {
		registry.Register(name, func(routing.Spec) (search.Provider, error) { return provider, nil })
	}
	router, err := New(Options{
		Config: routing.ModalityConfig{
			Providers: []routing.ProviderConfig{
				{Provider: "exa", Model: "fast", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency},
				{Provider: "tavily", Model: "basic", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency},
			},
			Aliases: map[string]routing.Alias{
				"en-low-latency": {Languages: []string{"en"}, RequireRealtime: true, Tier: routing.LowLatency},
			},
		},
		Registry: registry,
		Store:    s.store,
		Pricer:   s.prices,
		Logger:   slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	return router
}

// rows closes the router, which writes what it still holds, and returns the test customer's
// rows, oldest first.
func (s *BillingSuite) rows(router *Router) []store.Request {
	router.Close()
	var rows []store.Request
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", s.customer).
		Order("started_at", "id").Scan(s.ctx))
	return rows
}

// price is one search of the model at the vendor list's rate.
func (s *BillingSuite) price(provider, model string) int64 {
	item := pricing.Item{Modality: string(routing.Search), Provider: provider, Model: model, Region: pricing.DefaultRegion}
	return pricingtest.Micros(s.T(), s.prices, item, pricing.Quantities{pricing.Requests: 1})
}

func (s *BillingSuite) TestEachSearchWritesOneBilledRowAtItsModelsPrice() {
	exa := &stubSearch{model: "fast", found: search.Result{Answer: "I-70 is clear."}}
	router := s.routes(map[string]search.Provider{"exa": exa})
	session, err := router.Start(s.ctx, Request{CustomerID: s.customer, AgentID: "agent-1", CallID: "call-1",
		Target: "exa/fast", Tags: routing.Tags{"project": "roads"}})
	s.Require().NoError(err)

	for _, question := range []string{"traffic on I-70", "and on US-36"} {
		_, err := session.Search(s.ctx, search.Query{Text: question})
		s.Require().NoError(err)
	}

	rows := s.rows(router)
	s.Require().Len(rows, 2, "a row a question")
	s.NotZero(s.price("exa", "fast"))
	for _, row := range rows {
		s.Equal(string(routing.Search), row.Modality)
		s.Equal([]string{"exa", "fast"}, []string{row.Provider, row.Model})
		s.Equal(int64(1), row.Meters[pricing.Requests])
		s.Equal(s.price("exa", "fast"), row.CostMicros)
		s.Equal("search-exa-fast-us", row.SKU)
		s.True(row.Billable)
		s.True(row.Success)
		s.Equal([]string{"agent-1", "call-1"}, []string{row.AgentID, row.CallID})
		s.Equal(map[string]string{"project": "roads"}, row.Tags)
	}
	s.NotEqual(rows[0].UsageID, rows[1].UsageID, "each question is its own work")
}

func (s *BillingSuite) TestAFailedSearchIsRecordedAndNotBilled() {
	exa := &stubSearch{model: "fast", err: errors.New("rate limited")}
	router := s.routes(map[string]search.Provider{"exa": exa})
	session, err := router.Start(s.ctx, Request{CustomerID: s.customer, Target: "exa/fast"})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic on I-70"})
	s.Require().Error(err)

	rows := s.rows(router)
	s.Require().Len(rows, 1)
	s.False(rows[0].Success)
	s.Equal("search_failed", rows[0].ErrorCode)
	s.Zero(rows[0].CostMicros, "nothing was answered, so nothing is charged")
	s.Zero(rows[0].Meters[pricing.Requests])
}

func (s *BillingSuite) TestASearchAnsweredByTheNextEntryIsBilledAtThatEntrysPrice() {
	exa := &stubSearch{model: "fast", err: errors.New("rate limited")}
	tavily := &stubSearch{model: "basic", found: search.Result{Answer: "I-70 is clear."}}
	router := s.routes(map[string]search.Provider{"exa": exa, "tavily": tavily})
	session, err := router.Start(s.ctx, Request{CustomerID: s.customer,
		Options: options.Search{Providers: []string{"exa/fast", "tavily/basic"}}})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic on I-70"})
	s.Require().NoError(err)

	rows := s.rows(router)
	s.Require().Len(rows, 2, "the failure and the answer")
	s.Equal([]string{"exa", "tavily"}, []string{rows[0].Provider, rows[1].Provider})
	s.False(rows[0].Success)
	s.Zero(rows[0].CostMicros)
	s.True(rows[1].Success)
	s.Equal(s.price("tavily", "basic"), rows[1].CostMicros)
	s.Equal("search-tavily-basic-us", rows[1].SKU)
}

// refusingGate admits every customer and refuses to set money aside for work of one price.
type refusingGate struct{ refused int64 }

func (g refusingGate) Admit(context.Context, routing.Owner) (routing.Admission, error) {
	return routing.Admission{}, nil
}

func (g refusingGate) Reserve(_ routing.Owner, _ uuid.UUID, micros int64, _ time.Duration) error {
	if micros == g.refused {
		return errors.New("policy: the customer's limit on this model is spent")
	}
	return nil
}

func (g refusingGate) Release(uuid.UUID) {}

// A fallback whose money was refused before its vendor was asked is not that vendor failing.
func (s *BillingSuite) TestAFallbackRefusedForItsMoneyIsNoVendorFailover() {
	registry := NewRegistry()
	stubs := map[string]*stubSearch{
		"exa":        {model: "fast", err: errors.New("exa: 503")},
		"tavily":     {model: "basic", found: search.Result{Answer: "never asked"}},
		"perplexity": {model: "search", found: search.Result{Answer: "I-70 is clear."}},
	}
	for name, stub := range stubs {
		registry.Register(name, func(routing.Spec) (search.Provider, error) { return stub, nil })
	}
	router, err := New(Options{
		Config: routing.ModalityConfig{Providers: []routing.ProviderConfig{
			{Provider: "exa", Model: "fast", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency},
			{Provider: "tavily", Model: "basic", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency},
			{Provider: "perplexity", Model: "search", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency},
		}},
		Registry: registry,
		Store:    s.store,
		Pricer:   s.prices,
		Gate:     refusingGate{refused: s.price("tavily", "basic")},
		Logger:   slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	before := failovertest.Of(s.T(), "tavily")["search use vendor"]
	session, err := router.Start(s.ctx, Request{CustomerID: s.customer, Options: options.Search{Providers: []string{"exa/fast", "tavily/basic", "perplexity/search"}}})
	s.Require().NoError(err)

	found, err := session.Search(s.ctx, search.Query{Text: "traffic on I-70"})

	s.Require().NoError(err)
	s.Equal("I-70 is clear.", found.Answer)
	s.Empty(stubs["tavily"].asked, "refused before tavily was asked")
	s.Equal(before, failovertest.Of(s.T(), "tavily")["search use vendor"], "a refused reservation is not tavily failing")
	router.Close()
}
