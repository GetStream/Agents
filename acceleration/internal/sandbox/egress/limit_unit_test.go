package egress

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
)

type SandboxTunnelsSuite struct {
	suite.Suite
}

func TestSandboxTunnelsSuite(t *testing.T) {
	suite.Run(t, new(SandboxTunnelsSuite))
}

// TestASandboxWithNoTunnelOpenLeavesTheMap: a router serves sandboxes for as long as it runs, so a
// count of none is no entry, and the map holds the sandboxes with a tunnel open alone.
func (s *SandboxTunnelsSuite) TestASandboxWithNoTunnelOpenLeavesTheMap() {
	tunnels := sandboxTunnels{max: 2, open: map[string]int{}}
	s.Require().True(tunnels.hold("first"))
	s.Require().True(tunnels.hold("first"))
	s.Require().True(tunnels.hold("second"))

	tunnels.release("first")
	s.Equal(map[string]int{"first": 1, "second": 1}, tunnels.open)
	tunnels.release("first")
	s.Equal(map[string]int{"second": 1}, tunnels.open)
	tunnels.release("second")

	s.Empty(tunnels.open)
}

// TestConcurrentHoldsOfOneSandboxTakeItsLimitAlone: CONNECTs of one sandbox arrive at once; run
// with -race.
func (s *SandboxTunnelsSuite) TestConcurrentHoldsOfOneSandboxTakeItsLimitAlone() {
	tunnels := sandboxTunnels{max: 10, open: map[string]int{}}
	var held atomic.Int32
	var group sync.WaitGroup
	for range 50 {
		group.Go(func() {
			if tunnels.hold("sandbox") {
				held.Add(1)
			}
		})
	}
	group.Wait()

	s.Equal(int32(10), held.Load())
	s.Equal(map[string]int{"sandbox": 10}, tunnels.open)
}
