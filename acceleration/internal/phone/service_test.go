package phone

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

type ServiceSuite struct {
	suite.Suite
}

func TestService(t *testing.T) { suite.Run(t, new(ServiceSuite)) }

func (s *ServiceSuite) TestEachCallResultIsAnOutcome() {
	for result, want := range map[CallResult]outcome.Outcome{
		CallCompleted: outcome.Completed,
		CallBusy:      outcome.Busy,
		CallNoAnswer:  outcome.NoAnswer,
		CallFailed:    outcome.Failed,
		CallCanceled:  outcome.Canceled,
	} {
		got, err := outcomeOf(result)
		s.Require().NoError(err)
		s.Equal(want, got, string(result))
	}
}

func (s *ServiceSuite) TestANonFinalResultIsNoOutcome() {
	got, err := outcomeOf("")
	s.Require().NoError(err)

	s.Empty(got)
}

func (s *ServiceSuite) TestAnUnknownResultIsRefused() {
	_, err := outcomeOf(CallResult("voicemail"))

	s.ErrorContains(err, "voicemail")
}

// TestEveryVendorOfTheServiceHasItsCallSeriesAtZeroFromTheStart catches a vendor the service
// counts calls under whose first call makes a series that starts at 1, which increase() reads as
// no change: the registry's vendors and sip_trunk, with no call placed.
func (s *ServiceSuite) TestEveryVendorOfTheServiceHasItsCallSeriesAtZeroFromTheStart() {
	config, err := DefaultConfig()
	s.Require().NoError(err)
	_, err = NewService(ServiceOptions{Registry: NewRegistry(config), Store: &store.Store{}})
	s.Require().NoError(err)

	families, err := prometheus.DefaultGatherer.Gather()
	s.Require().NoError(err)
	has := map[string]bool{}
	for _, family := range families {
		if family.GetName() != "router_phone_call_outcomes_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["direction"] == "outbound" && labels["outcome"] == "completed" {
				has[labels["vendor"]] = true
			}
		}
	}
	vendors := []string{SIPTrunkVendor, "other"}
	for _, vendor := range config.Vendors {
		vendors = append(vendors, vendor.Vendor)
	}
	for _, vendor := range vendors {
		s.True(has[vendor], vendor)
	}
}

func (s *ServiceSuite) TestAPlacingTheDatabaseFailedIsAPlacingFailure() {
	err := notPlaced(context.Background(), errors.New("lock timeout"))

	s.ErrorIs(err, ErrNotPlaced)
	s.ErrorIs(err, ErrPlacingFailed)
}

func (s *ServiceSuite) TestAPlacingCutOffByItsContextIsNoPlacingFailure() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := notPlaced(ctx, ctx.Err())

	s.ErrorIs(err, ErrNotPlaced)
	s.NotErrorIs(err, ErrPlacingFailed, "a shutdown rings the contact again at once")
}

// A failure a second try would meet again is lasting; a read that failed is not. Which of them
// a campaign ends the contact on is TestOnlyAFailureEveryLaterTryMeetsIsMarkedCannotDial.
func (s *ServiceSuite) TestOnlyAFailureASecondTryMeetsIsLasting() {
	for _, err := range []error{ErrInvalid, store.ErrNoNumber, store.ErrNoSIPTrunk, ErrNoStream, ErrNoSealerToCall, ErrNotConfigured,
		ErrSIPTrunksDisabled, streamapp.ErrNoIdentity, streamapp.ErrStreamAppMoved, streamapp.ErrStreamAppDisconnected,
		streamapp.ErrReadOnly, streamapp.ErrDeploymentAppMismatch, lastingError{errors.New("telnyx: no api key")}} {
		s.True(lasting(stack.Wrap(err)), err.Error())
	}
	s.False(lasting(errors.New("store: number: read tcp: i/o timeout")))
}

// A call that failed before it was sent is marked cannot-dial only for a failure every later
// try meets, so a campaign ends the contact; anything else is marked never dialed, so it is
// tried again. Catches ErrNoIdentity marked cannot-dial: in app mode a gateway mint or a
// database blip answers it too, and one blip would end every contact a campaign dialed.
func (s *ServiceSuite) TestOnlyAFailureEveryLaterTryMeetsIsMarkedCannotDial() {
	cases := []struct {
		err        error
		cannotDial bool
	}{
		{ErrInvalid, true},
		{store.ErrNoNumber, true},
		{store.ErrNoSIPTrunk, true},
		{store.ErrNumberHeldElsewhere, true},
		{store.ErrCallTaken, true},
		{ErrNoStream, true},
		{ErrNoSealerToCall, true},
		{ErrNotConfigured, true},
		{ErrSIPTrunksDisabled, true},
		{streamapp.ErrStreamAppMoved, true},
		{streamapp.ErrStreamAppDisconnected, true},
		{streamapp.ErrReadOnly, true},
		{streamapp.ErrDeploymentAppMismatch, true},
		{lastingError{errors.New("telnyx: no api key")}, true},
		{streamapp.ErrNoIdentity, false},
		{fmt.Errorf("%w: %w", streamapp.ErrNoIdentity, errors.New("gateway: mint: 503")), false},
		{errors.New("store: number: read tcp: i/o timeout"), false},
	}
	for _, c := range cases {
		marked := unsent(stack.Wrap(c.err))
		s.ErrorIs(marked, c.err, "the failure keeps its own kind")
		if c.cannotDial {
			s.ErrorIs(marked, ErrCannotDial, c.err.Error())
			s.NotErrorIs(marked, ErrNotDialed, c.err.Error())
		} else {
			s.ErrorIs(marked, ErrNotDialed, c.err.Error())
			s.NotErrorIs(marked, ErrCannotDial, c.err.Error())
		}
	}
}
