//go:build integration

package channels

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

type LinkReleaseSuite struct {
	suite.Suite
	money *policytest.Money
}

func TestLinkReleaseSuite(t *testing.T) { suite.Run(t, new(LinkReleaseSuite)) }

func (s *LinkReleaseSuite) SetupSuite() { s.money = policytest.Start(s.T(), "channels_link_release") }

// cancelOnQuery cancels a context when a query containing value starts, as a router shutting
// down does while a code is being tried.
type cancelOnQuery struct {
	value  string
	cancel context.CancelFunc
}

func (c *cancelOnQuery) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if strings.Contains(event.Query, c.value) {
		c.cancel()
	}
	return ctx
}

func (*cancelOnQuery) AfterQuery(context.Context, *bun.QueryEvent) {}

// A code counted and then not tried, because the router stopped, is given back: the number
// does not lose a try of the five to a code that matched nothing.
func (s *LinkReleaseSuite) TestACodeCountedThenCutOffByAShutdownIsGivenBack() {
	list, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	service := &Service{store: s.money.Store, recorder: s.money.Recorder(routing.Phone, list), logger: slog.New(slog.DiscardHandler)}
	telnyx := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer telnyx.Close()
	provider := &telnyxProvider{client: telnyx.Client(), baseURL: telnyx.URL}
	owner := s.money.Owner(s.T(), 0)
	config := store.AgentConfig{ID: s.T().Name() + "-config", CustomerID: owner.CustomerID,
		Channels: store.AgentAnswersIn{Lines: store.AgentChannels{Identity: store.ChannelIdentityLink}}}
	account := store.ChannelAccount{ID: store.NewID(), CustomerID: owner.CustomerID, Kind: string(SMS), E164: "+12187021098"}
	line := Account{Kind: SMS, E164: account.E164}
	message := Message{From: "+13479018418", Text: "123456"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hook := &cancelOnQuery{value: `"channel_links"`, cancel: cancel}
	s.money.Store.GlobalDB().AddQueryHook(hook)
	s.T().Cleanup(func() { hook.cancel = func() {} })

	_, ok := service.identify(ctx, config, account, line, provider, message)

	s.False(ok)
	s.Require().Error(ctx.Err(), "the shutdown came while the code was tried")
	window := time.Now().UTC().Truncate(LinkValidFor)
	counted, err := s.money.Store.ChannelLinkFailures.Reserve(context.Background(), account.ID, message.From,
		window, window.Add(-LinkValidFor), 1, maxLineLinkFailures)
	s.Require().NoError(err)
	s.True(counted, "the code was given back, so the number has its try")
}
