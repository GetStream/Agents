package searchrouter

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing/failovertest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/search"
)

// stubSearch stands in for a real provider so routing can be driven without credentials.
type stubSearch struct {
	model string
	found search.Result
	err   error

	// together, when set, holds each search until as many as it counts have been asked.
	together *sync.WaitGroup

	mu     sync.Mutex
	asked  []search.Query
	closed bool
}

func (s *stubSearch) Search(_ context.Context, query search.Query) (search.Result, error) {
	if s.together != nil {
		s.together.Done()
		s.together.Wait()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, query)
	if s.err != nil {
		return search.Result{}, s.err
	}
	return s.found, nil
}

func (s *stubSearch) Start(context.Context) error { return nil }
func (s *stubSearch) Close() error                { s.closed = true; return nil }
func (s *stubSearch) Provider() string            { return "stub" }
func (s *stubSearch) Model() string               { return s.model }

type SearchRouterSuite struct {
	suite.Suite
	ctx context.Context
}

func TestSearchRouterSuite(t *testing.T) {
	suite.Run(t, new(SearchRouterSuite))
}

func (s *SearchRouterSuite) SetupTest() {
	s.ctx = context.Background()
}

// config is two providers at the same tier, which is what failover needs something to fall
// back to.
func (s *SearchRouterSuite) config() routing.ModalityConfig {
	return routing.ModalityConfig{
		Providers: []routing.ProviderConfig{
			{
				Provider: "quick", Model: "fast", Languages: []string{"en"},
				Realtime: true, Tier: routing.LowLatency,
				Price: routing.Price{PerThousandRequests: 5},
			},
			{
				Provider: "spare", Model: "fast", Languages: []string{"en"},
				Realtime: true, Tier: routing.LowLatency,
			},
		},
		Aliases: map[string]routing.Alias{
			"en-low-latency": {
				Languages: []string{"en"}, RequireRealtime: true, Tier: routing.LowLatency,
			},
		},
	}
}

func (s *SearchRouterSuite) newRouter(factories map[string]routing.Factory[search.Provider]) *Router {
	registry := NewRegistry()
	for name, factory := range factories {
		registry.Register(name, factory)
	}

	router, err := New(Options{
		Config:   s.config(),
		Registry: registry,
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	return router
}

func (s *SearchRouterSuite) TestATargetResolvesAndSearches() {
	found := search.Result{
		Answer:    "I-70 is clear.",
		Documents: []search.Document{{Title: "COtrip", URL: "https://cotrip.org"}},
	}
	provider := &stubSearch{model: "fast", found: found}
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) { return provider, nil },
	})

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "en-low-latency"})
	s.Require().NoError(err)

	answered, err := session.Search(s.ctx, search.Query{Text: "traffic on I-70"})
	s.Require().NoError(err)

	s.Equal("quick", session.Provider())
	s.Equal("fast", session.Model())
	s.Equal("I-70 is clear.", answered.Answer)
	s.Require().Len(provider.asked, 1)
	s.Equal("traffic on I-70", provider.asked[0].Text)
}

func (s *SearchRouterSuite) TestAProviderWithoutAKeyDropsToTheNextCandidate() {
	// A provider is built when the session opens, so a deployment holding a key for one
	// of three still searches rather than offering the tool and then failing on it.
	spare := &stubSearch{model: "fast"}
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) {
			return nil, errors.New("QUICK_API_KEY is required")
		},
		"spare": func(routing.Spec) (search.Provider, error) { return spare, nil },
	})

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "en-low-latency"})
	s.Require().NoError(err)

	s.Equal("spare", session.Provider())
}

func (s *SearchRouterSuite) TestNoProviderAtAllSaysWhatEachOneComplainedAbout() {
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) {
			return nil, errors.New("QUICK_API_KEY is required")
		},
		"spare": func(routing.Spec) (search.Provider, error) {
			return nil, errors.New("SPARE_API_KEY is required")
		},
	})

	_, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "en-low-latency"})

	s.Require().Error(err)
	s.ErrorContains(err, "QUICK_API_KEY")
	s.ErrorContains(err, "SPARE_API_KEY")
}

func (s *SearchRouterSuite) TestASearchThatFailedIsReportedRatherThanRetriedElsewhere() {
	// A single target fails over only when a provider cannot start: the caller is
	// mid-sentence, and a second provider's latency on top of the first one's failure is
	// a longer silence than saying it could not check. A priority list is the caller
	// choosing that wait, which the next test covers.
	provider := &stubSearch{model: "fast", err: errors.New("rate limited")}
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) { return provider, nil },
	})

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "en-low-latency"})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic"})

	s.ErrorContains(err, "rate limited")
	s.Equal("quick", session.Provider(), "the session stays on the provider it chose")
}

func (s *SearchRouterSuite) TestAPriorityListAnswersASearchItsFirstEntryFailed() {
	// Written against config order, so the list is what decides who is asked first.
	spare := &stubSearch{model: "fast", err: errors.New("rate limited")}
	quick := &stubSearch{model: "fast", found: search.Result{Answer: "I-70 is clear."}}
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) { return quick, nil },
		"spare": func(routing.Spec) (search.Provider, error) { return spare, nil },
	})

	session, err := router.Start(s.ctx, Request{
		CustomerID: "acme",
		Options:    options.Search{Providers: []string{"spare", "quick"}},
	})
	s.Require().NoError(err)
	s.Equal("spare", session.Provider())

	answered, err := session.Search(s.ctx, search.Query{Text: "traffic on I-70"})
	s.Require().NoError(err)
	s.Equal("I-70 is clear.", answered.Answer)
	s.Equal("quick", session.Provider(), "the session reports who answered")

	_, err = session.Search(s.ctx, search.Query{Text: "and on US-36"})
	s.Require().NoError(err)
	s.Len(spare.asked, 1, "a provider that failed is not asked again once the session has moved on")
	s.Len(quick.asked, 2)
	s.True(spare.closed)
}

func (s *SearchRouterSuite) TestAPriorityListThatFailsThroughSaysWhatEachEntrySaid() {
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) {
			return &stubSearch{model: "fast", err: errors.New("quick is rate limited")}, nil
		},
		"spare": func(routing.Spec) (search.Provider, error) {
			return &stubSearch{model: "fast", err: errors.New("spare timed out")}, nil
		},
	})

	session, err := router.Start(s.ctx, Request{
		CustomerID: "acme",
		Options:    options.Search{Providers: []string{"quick", "spare"}},
	})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic"})

	s.ErrorContains(err, "quick is rate limited")
	s.ErrorContains(err, "spare timed out")
	s.Equal("quick", session.Provider(), "nothing answered, so the session stays where it was")
}

// listRouter routes search to one model of each vendor, named by no other test so the
// process's failover counter can be read by vendor, each answering with its stub.
func (s *SearchRouterSuite) listRouter(stubs map[string]*stubSearch) *Router {
	config := routing.ModalityConfig{}
	registry := NewRegistry()
	for vendor, stub := range stubs {
		config.Providers = append(config.Providers, routing.ProviderConfig{
			Provider: vendor, Model: "fast", Languages: []string{"en"}, Realtime: true, Tier: routing.LowLatency,
		})
		registry.Register(vendor, func(routing.Spec) (search.Provider, error) { return stub, nil })
	}
	router, err := New(Options{Config: config, Registry: registry, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	return router
}

// vendors are n vendor names no other test uses.
func vendors(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = "v" + strings.ToLower(rand.Text())
	}
	return names
}

func (s *SearchRouterSuite) TestEachEntryAPriorityListMovedPastIsAFailoverAtUse() {
	names := vendors(3)
	router := s.listRouter(map[string]*stubSearch{
		names[0]: {model: "fast", err: errors.New("rate limited")},
		names[1]: {model: "fast", err: errors.New("timed out")},
		names[2]: {model: "fast", found: search.Result{Answer: "I-70 is clear."}},
	})
	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Options: options.Search{Providers: names}})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic on I-70"})

	s.Require().NoError(err)
	s.Equal(names[2], session.Provider())
	s.Equal(map[string]float64{"search use vendor": 1}, failovertest.Of(s.T(), names[0]), "the session's own provider")
	s.Equal(map[string]float64{"search use vendor": 1}, failovertest.Of(s.T(), names[1]), "a fallback that failed too")
	s.Empty(failovertest.Of(s.T(), names[2]), "it answered")
}

func (s *SearchRouterSuite) TestTheLastEntryOfAPriorityListFailingIsNoFailover() {
	names := vendors(2)
	router := s.listRouter(map[string]*stubSearch{
		names[0]: {model: "fast", err: errors.New("rate limited")},
		names[1]: {model: "fast", err: errors.New("timed out")},
	})
	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Options: options.Search{Providers: names}})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic"})

	s.Require().Error(err)
	s.Equal(map[string]float64{"search use vendor": 1}, failovertest.Of(s.T(), names[0]))
	s.Empty(failovertest.Of(s.T(), names[1]), "nothing was started in its place")
}

// Two searches the session's provider failed at once each walk the fallbacks on their own,
// so each counts its own move: run under -race, a walk shared between them is a race.
func (s *SearchRouterSuite) TestTwoSearchesFailingAtOnceEachCountTheirOwnFailover() {
	names := vendors(2)
	// Both searches reach the failing provider before either moves the session off it.
	var together sync.WaitGroup
	together.Add(2)
	router := s.listRouter(map[string]*stubSearch{
		names[0]: {model: "fast", err: errors.New("rate limited"), together: &together},
		names[1]: {model: "fast", found: search.Result{Answer: "I-70 is clear."}},
	})
	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Options: options.Search{Providers: names}})
	s.Require().NoError(err)

	var searches sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		searches.Go(func() { _, errs[i] = session.Search(s.ctx, search.Query{Text: "traffic"}) })
	}
	searches.Wait()

	s.Require().NoError(errors.Join(errs...))
	s.Equal(map[string]float64{"search use vendor": 2}, failovertest.Of(s.T(), names[0]))
}

func (s *SearchRouterSuite) TestASearchWithoutAPriorityListIsNeverAFailover() {
	names := vendors(2)
	router := s.listRouter(map[string]*stubSearch{
		names[0]: {model: "fast", err: errors.New("rate limited")},
		names[1]: {model: "fast", found: search.Result{Answer: "I-70 is clear."}},
	})
	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: names[0] + "/fast"})
	s.Require().NoError(err)

	_, err = session.Search(s.ctx, search.Query{Text: "traffic"})

	s.Require().Error(err)
	s.Empty(failovertest.Of(s.T(), names[0]), "a single target is not retried elsewhere")
}

func (s *SearchRouterSuite) TestClosingASessionClosesTheProvider() {
	provider := &stubSearch{model: "fast"}
	router := s.newRouter(map[string]routing.Factory[search.Provider]{
		"quick": func(routing.Spec) (search.Provider, error) { return provider, nil },
	})

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "en-low-latency"})
	s.Require().NoError(err)

	s.Require().NoError(session.Close())
	s.Require().NoError(session.Close())
	s.True(provider.closed)
}

func (s *SearchRouterSuite) TestTheDefaultRegistryHasEveryProviderTheConfigDeclares() {
	// A provider declared in router.yaml with no factory behind it is a candidate that
	// can never be chosen, and the router only says so once a call has already started.
	config, err := routing.DefaultConfig()
	s.Require().NoError(err)

	section, ok := config[routing.Search]
	s.Require().True(ok, "search is a routed modality")

	registry := DefaultRegistry()
	for _, provider := range section.Providers {
		s.Truef(registry.Has(provider.Provider),
			"%s is declared but nothing can build it", provider.Name())
	}
}
