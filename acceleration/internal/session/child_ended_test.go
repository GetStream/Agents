package session

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// ChildEndedSuite is the error a child agent given up on returns: the tool call's line says it
// and classes it by what ended the child. The model is told only that the tool failed.
type ChildEndedSuite struct {
	suite.Suite
}

func TestChildEndedSuite(t *testing.T) {
	suite.Run(t, new(ChildEndedSuite))
}

func (s *ChildEndedSuite) TestAChildOutOfTimeIsATimeout() {
	err := childEnded("ask_c", context.DeadlineExceeded)

	s.ErrorIs(err, context.DeadlineExceeded)
	s.NotErrorIs(err, context.Canceled)
	s.EqualError(err, "ask_c did not finish in the time it had")
}

func (s *ChildEndedSuite) TestAChildStoppedIsACancellation() {
	err := childEnded("ask_c", context.Canceled)

	s.ErrorIs(err, context.Canceled)
	s.NotErrorIs(err, context.DeadlineExceeded)
	s.EqualError(err, "ask_c was stopped before it finished")
}
