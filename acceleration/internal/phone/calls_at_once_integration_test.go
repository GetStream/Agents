//go:build integration

package phone

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// tierMoney admits every customer into one organization whose tier allows atOnce calls up at
// once, as the usage service's admission does.
type tierMoney struct {
	organization string
	atOnce       int
}

func (m tierMoney) Admit(context.Context, routing.Owner) (routing.Admission, error) {
	return routing.Admission{OrganizationID: m.organization, LimitsTier: "starter", Limits: &usage.Limits{
		Channels: map[string]usage.ChannelLimits{dlc.Voice: {Concurrent: &m.atOnce}},
	}}, nil
}

func (m tierMoney) Attribute(context.Context, string) (routing.Admission, error) {
	return routing.Admission{OrganizationID: m.organization}, nil
}

// countingVendor is the stub vendor counting the dials it is asked for, from any goroutine.
type countingVendor struct {
	*stub
	dials atomic.Int64
}

func (v *countingVendor) Dial(_ context.Context, _ Outbound) (Dialed, error) {
	v.dials.Add(1)
	return Dialed{VendorCallID: v.vendor + "-call-" + uuid.NewString(), Status: "dialing"}, nil
}

func (s *OutcomeSuite) TestFiftyCallsPlacedAtOnceRingNoMoreThanTheTierAllows() {
	const atOnce = 5
	organization := "org-" + uuid.NewString()
	vendor := &countingVendor{stub: &stub{vendor: "telnyx"}}
	gate := dlc.NewGate(s.store, s.redis, dlc.Sandbox{}, nil).
		Charging(tierMoney{organization: organization, atOnce: atOnce}).Limiting(config.LimitsEnforce)
	s.buildGated(vendor, "https://router.example", gate)

	var refused atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, err := s.service.Call(s.ctx, CallRequest{
				Owner: routing.Owner{CustomerID: s.customer, OrganizationID: organization}, From: s.e164, To: "+15125550123",
			})
			if err == nil {
				return
			}
			s.Equal(dlc.TierLimit(err), "concurrent", "%v", err)
			refused.Add(1)
		})
	}
	wg.Wait()

	s.EqualValues(atOnce, vendor.dials.Load(), "the vendor is asked for as many calls as the tier allows at once")
	s.EqualValues(50-atOnce, refused.Load())
}
