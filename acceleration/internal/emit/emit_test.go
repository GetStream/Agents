package emit

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type EmitterSuite struct {
	suite.Suite
}

func TestEmitterSuite(t *testing.T) {
	suite.Run(t, new(EmitterSuite))
}

func (s *EmitterSuite) TestTrySendQueuesWhenBufferHasRoom() {
	emitter := New[string](2)
	defer emitter.Close()

	s.True(emitter.TrySend("first"))
	s.True(emitter.TrySend("second"))

	s.Equal("first", <-emitter.Events())
	s.Equal("second", <-emitter.Events())
}

func (s *EmitterSuite) TestTrySendReturnsFalseWhenBufferIsFull() {
	emitter := New[string](1)
	defer emitter.Close()

	s.True(emitter.TrySend("only"))
	s.False(emitter.TrySend("overflow"))

	s.Equal("only", <-emitter.Events())
}

func (s *EmitterSuite) TestTrySendAfterCloseReturnsFalseWithoutPanicking() {
	emitter := New[string](2)
	emitter.Close()

	s.NotPanics(func() {
		s.False(emitter.TrySend("after-close"))
	})
}
