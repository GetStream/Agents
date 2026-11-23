//go:build integration

package channels

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
