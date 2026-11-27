package api

import (
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/stretchr/testify/suite"
)

type SessionStateSuite struct {
	suite.Suite
}

func TestSessionStateSuite(t *testing.T) {
	suite.Run(t, new(SessionStateSuite))
}

func (s *SessionStateSuite) TestSessionStateOfMapsLifecycleStatesToWireEnum() {
	s.Equal(SessionState("live"), sessionStateOf(session.Starting))
	s.Equal(SessionState("live"), sessionStateOf(session.Live))
	s.Equal(SessionState("live"), sessionStateOf(session.Retiring))
	s.Equal(SessionState("live"), sessionStateOf(session.Closing))
	s.Equal(SessionState("ended"), sessionStateOf(session.Ended))
}
