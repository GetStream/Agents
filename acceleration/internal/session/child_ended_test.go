package session

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// ChildEndedSuite is the error a child agent given up on returns: the model reads it, and the
// tool call's line classes it by what ended the child.
type ChildEndedSuite struct {
	suite.Suite
}

func TestChildEndedSuite(t *testing.T) {
	suite.Run(t, new(ChildEndedSuite))
}

func (s *ChildEndedSuite) TestAChildOutOfTimeIsATimeoutTheModelHearsAbout() {
	err := childEnded("ask_c", context.DeadlineExceeded)

	s.ErrorIs(err, context.DeadlineExceeded)
	s.NotErrorIs(err, context.Canceled)
	s.EqualError(err, "ask_c did not finish in the time it had")
}

func (s *ChildEndedSuite) TestAChildStoppedIsACancellationTheModelHearsAbout() {
	err := childEnded("ask_c", context.Canceled)

	s.ErrorIs(err, context.Canceled)
	s.NotErrorIs(err, context.DeadlineExceeded)
	s.EqualError(err, "ask_c was stopped before it finished")
}
