//go:build integration

package channels

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

type ReserveSuite struct {
	suite.Suite
	money *policytest.Money
}

func TestReserveSuite(t *testing.T) { suite.Run(t, new(ReserveSuite)) }

func (s *ReserveSuite) SetupSuite() { s.money = policytest.Start(s.T(), "channels_reserve") }

// A reply the organization cannot afford is refused at its reservation: Telnyx is not asked and no row is written.
func (s *ReserveSuite) TestAReplyTheOrganizationCannotAffordIsNotSent() {
	list, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	service := &Service{store: s.money.Store, recorder: s.money.Recorder(routing.Phone, list), logger: slog.New(slog.DiscardHandler)}
	owner := s.money.Owner(s.T(), 0)
	var asked atomic.Int32
	telnyx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { asked.Add(1) }))
	defer telnyx.Close()
	provider := &telnyxProvider{client: telnyx.Client(), baseURL: telnyx.URL}
	line := Account{Kind: SMS, E164: "+12187021098", Token: "telnyx-key"}

	err = service.deliver(context.Background(), line, provider, "+13479018418", "+13479018418", Reply{Text: "On my way."}, owner)

	s.ErrorIs(err, policy.ErrNoCredits)
	s.True(routing.ReservationRefused(err))
	s.Zero(asked.Load(), "Telnyx was not asked")
	s.Never(func() bool {
		var rows []store.Request
		err := s.money.Store.ShardDB().NewSelect().Model(&rows).Where("customer_id = ?", owner.CustomerID).Scan(context.Background())
		return err != nil || len(rows) > 0
	}, 300*time.Millisecond, 50*time.Millisecond, "a refused reply writes no row")
}

// A reply with a file costs a text and an MMS, and all of it is set aside while Telnyx is asked.
func (s *ReserveSuite) TestAReplyWithAFileReservesItsMMS() {
	list, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	item := pricing.Item{Modality: string(routing.Phone), Provider: "telnyx", Model: "sms", Region: pricing.DefaultRegion,
		Attributes: map[string]string{"country": "US", "direction": "outbound"}}
	both := list.Quote(item, pricing.Quantities{pricing.Messages: 2, pricing.MessageSegments: 1, pricing.MediaMessages: 1}).CostMicros
	words := list.Quote(item, pricing.Quantities{pricing.Messages: 1, pricing.MessageSegments: 1}).CostMicros
	s.Require().Greater(both, words, "the MMS costs something")
	service := &Service{store: s.money.Store, recorder: s.money.Recorder(routing.Phone, list), logger: slog.New(slog.DiscardHandler)}
	owner := s.money.Owner(s.T(), both)
	var during atomic.Value
	telnyx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		during.CompareAndSwap(nil, errBox{s.money.Spend(s.T(), owner)})
	}))
	defer telnyx.Close()
	provider := &telnyxProvider{client: telnyx.Client(), baseURL: telnyx.URL}
	line := Account{Kind: SMS, E164: "+12187021098", Token: "telnyx-key"}

	err = service.deliver(context.Background(), line, provider, "+13479018418", "+13479018418",
		Reply{Text: "Here it is.", Files: []File{{URL: "https://files/render.png"}}}, owner)

	s.Require().NoError(err)
	box, ok := during.Load().(errBox)
	s.Require().True(ok, "Telnyx was asked")
	s.ErrorIs(box.err, policy.ErrNoCredits, "the credit is held whole, the MMS with the words")
}

// errBox lets an error, nil included, be stored in an atomic.Value.
type errBox struct{ err error }

// What is held for a reply is what Send bills for it, whatever it carries.
func (s *ReserveSuite) TestWhatIsHeldForAReplyIsWhatItBills() {
	telnyx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer telnyx.Close()
	provider := &telnyxProvider{client: telnyx.Client(), baseURL: telnyx.URL}
	line := Account{Kind: SMS, E164: "+12187021098", Token: "telnyx-key"}
	files := func(n int) []File {
		out := make([]File, n)
		for i := range out {
			out[i] = File{URL: "https://files/render.png"}
		}
		return out
	}
	long := strings.Repeat("a", 150)
	replies := map[string]Reply{
		"words":              {Text: "On my way."},
		"words with a link":  {Text: long, Link: &Link{Text: "Open", URL: "https://example.com/" + strings.Repeat("b", 40)}},
		"a link alone":       {Link: &Link{Text: "Open", URL: "https://example.com/x"}},
		"files alone":        {Files: files(3)},
		"words and a file":   {Text: "Here it is.", Files: files(1)},
		"eleven files":       {Text: "Here they are.", Files: files(11)},
		"a file with no url": {Text: "Hi", Files: []File{{}}},
		"unicode words":      {Text: "Привет, как дела?"},
	}
	for name, reply := range replies {
		sent, err := provider.Send(context.Background(), line, "+13479018418", reply)
		s.Require().NoError(err, name)

		s.Equal(sent, provider.Estimate(reply), name)
	}
	sent, err := provider.Send(context.Background(), line, "+13479018418", replies["words with a link"])
	s.Require().NoError(err)
	s.Equal(int64(2), sent.Segments, "the link is billed with the words, which fit one segment without it")
}
