package egress

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type TimedBodySuite struct {
	suite.Suite
}

func TestTimedBodySuite(t *testing.T) {
	suite.Run(t, new(TimedBodySuite))
}

// tunnelDeadline is a tunnel's read deadline, as a body sets it.
type tunnelDeadline struct {
	at time.Time
}

func (d *tunnelDeadline) set(at time.Time) error {
	d.at = at
	return nil
}

func (s *TimedBodySuite) body(deadline *tunnelDeadline) *timedBody {
	return &timedBody{ReadCloser: io.NopCloser(strings.NewReader("body")), timeout: time.Minute, deadline: deadline.set}
}

// TestABodyReadToItsEndLeavesTheTunnelsDeadlineToTheServer: a read of the body puts the tunnel under its deadline,
// and once the body has ended no read does: the server's background read of the tunnel, which it starts at the
// body's end with no deadline (net/http, connReader.startBackgroundRead), lasts as long as the host's answer.
func (s *TimedBodySuite) TestABodyReadToItsEndLeavesTheTunnelsDeadlineToTheServer() {
	deadline := &tunnelDeadline{}
	body := s.body(deadline)

	_, err := io.ReadAll(body)
	s.Require().NoError(err)
	s.False(deadline.at.IsZero(), "a read of the body sets the tunnel's deadline")
	deadline.at = time.Time{}
	_, err = body.Read(make([]byte, 1))

	s.ErrorIs(err, io.EOF)
	s.True(deadline.at.IsZero(), "a read after the end set the tunnel's deadline: %v", deadline.at)
}

// TestTheRestOfABodyGetsOneTimeoutOnceItsHandlerReturns: a body not read to its end gets one timeout for the server's
// drain of its rest when the handler returns, and a read of it the transport still makes sets no other deadline.
func (s *TimedBodySuite) TestTheRestOfABodyGetsOneTimeoutOnceItsHandlerReturns() {
	deadline := &tunnelDeadline{}
	body := s.body(deadline)
	before := time.Now()

	body.stop()
	drain := deadline.at
	n, err := body.Read(make([]byte, 2))

	s.Require().NoError(err)
	s.Equal(2, n)
	s.WithinRange(drain, before.Add(time.Minute), time.Now().Add(time.Minute))
	s.Equal(drain, deadline.at, "a read after the handler returned set the tunnel's deadline")
}

// TestABodyReadToItsEndSetsNoDeadlineWhenItsHandlerReturns: the server's background read of the tunnel, which it
// starts at the body's end, is not put under a deadline when the handler returns.
func (s *TimedBodySuite) TestABodyReadToItsEndSetsNoDeadlineWhenItsHandlerReturns() {
	deadline := &tunnelDeadline{}
	body := s.body(deadline)
	_, err := io.ReadAll(body)
	s.Require().NoError(err)
	deadline.at = time.Time{}

	body.stop()

	s.True(deadline.at.IsZero(), "the handler's return set the tunnel's deadline: %v", deadline.at)
}

// TestACutOffBodyKeepsItsPassedDeadlineWhenItsHandlerReturns: a body cut off leaves the tunnel's deadline passed, so
// the server's drain of its rest fails at once and the tunnel closes.
func (s *TimedBodySuite) TestACutOffBodyKeepsItsPassedDeadlineWhenItsHandlerReturns() {
	deadline := &tunnelDeadline{}
	body := &timedBody{ReadCloser: io.NopCloser(stalledReader{}), timeout: time.Minute, deadline: deadline.set}
	_, err := body.Read(make([]byte, 1))
	s.Require().ErrorIs(err, errBodyStalled)
	passed := time.Unix(1, 0)
	deadline.at = passed

	body.stop()

	s.True(body.cut())
	s.Equal(passed, deadline.at)
}

// stalledReader is a tunnel whose read deadline has passed.
type stalledReader struct{}

func (stalledReader) Read([]byte) (int, error) { return 0, os.ErrDeadlineExceeded }
