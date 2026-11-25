//go:build integration

package api

import (
	"net"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/suite"
)

// SuiteMemorySuite covers what the suites of this package leave behind: they run in one process,
// and what each one kept to the end took more memory than CI's runner has (AI-3939).
type SuiteMemorySuite struct {
	suite.Suite
}

func TestSuiteMemorySuite(t *testing.T) { suite.Run(t, new(SuiteMemorySuite)) }

// closedOver is a suite its own cleanups close over, as RouterSuite's do: with runSuite's, they
// fill the 256 that maxSuiteCleanups writes over.
type closedOver struct {
	suite.Suite
}

func (s *closedOver) SetupSuite() {
	for range 255 {
		s.T().Cleanup(func() { runtime.KeepAlive(s) })
	}
}

func (s *closedOver) TestNothing() {}

func (s *SuiteMemorySuite) TestAFinishedSuiteIsLetGoWhileItsTestIsKept() {
	var kept *testing.T
	var finished weak.Pointer[closedOver]
	s.T().Run("group", func(t *testing.T) {
		t.Run("suite", func(t *testing.T) {
			ran := new(closedOver)
			kept, finished = t, weak.Make(ran)
			runSuite(t, ran)
		})
	})

	s.Eventually(func() bool {
		runtime.GC()
		return finished.Value() == nil
	}, settleFor, 10*time.Millisecond, "the finished suite is still reached from its test")
	runtime.KeepAlive(kept)
}

func (s *SuiteMemorySuite) TestAConnectionTakenFromANodeIsClosedWithTheTestThatMadeIt() {
	var caller net.Conn
	s.T().Run("node", func(t *testing.T) {
		nodes := new(RouterSuite)
		nodes.SetT(t)
		listener := nodes.unstartedNode()
		taken := make(chan error, 1)
		listener.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _, err := http.NewResponseController(w).Hijack()
			taken <- err
		})
		listener.Start()
		nodes.T().Cleanup(listener.Close)

		var err error
		caller, err = net.Dial("tcp", listener.Listener.Addr().String())
		nodes.Require().NoError(err)
		_, err = caller.Write([]byte("GET / HTTP/1.1\r\nHost: node\r\n\r\n"))
		nodes.Require().NoError(err)
		nodes.Require().NoError(<-taken)
	})
	defer caller.Close() //nolint:errcheck // it is closed by the node

	s.Require().NoError(caller.SetReadDeadline(time.Now().Add(settleFor)))
	_, err := caller.Read(make([]byte, 1))
	s.Error(err)
	s.NotErrorIs(err, os.ErrDeadlineExceeded, "the node still holds the connection")
}
