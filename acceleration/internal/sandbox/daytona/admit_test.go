//go:build integration

package daytona

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
)

func (s *LifecycleSuite) TestASandboxStoppedForItsOwnersMoneyGivesBackItsSlot() {
	slots := s.slotsFor(1)
	unpaid := errors.New("the organization has no credit left")
	var refused atomic.Bool
	box, err := New(Options{APIKey: "key-1", APIURL: s.daytona.APIURL(), Lifecycle: sandbox.Lifecycle{IdleStop: time.Minute},
		Meter: s.meter, Slots: slots, Logger: slog.New(slog.NewTextHandler(s.logs, nil)),
		Admit: func(context.Context, bool) error {
			if refused.Load() {
				return unpaid
			}
			return nil
		}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	s.run(box)
	s.Require().Equal(1, s.held(slots))
	refused.Store(true)

	_, err = box.Run(s.ctx, "print(2)", nil)

	s.Require().ErrorIs(err, unpaid)
	s.told("created sandbox-1 1 vCPU 1 GiB 3 GiB container", "stopped")
	s.Zero(s.held(slots), "given back once stopped, as at an idle stop, so another of the organization's may run")
}
