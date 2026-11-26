package emit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type EmitterSuite struct {
	suite.Suite
}

func TestEmitterSuite(t *testing.T) {
	suite.Run(t, new(EmitterSuite))
}

func (s *EmitterSuite) TestSendWithinQueuesWhenBufferHasRoom() {
	emitter := New[string](2)
	defer emitter.Close()

	s.True(emitter.SendWithin("first", time.Second))
	s.True(emitter.SendWithin("second", time.Second))

	s.Equal("first", <-emitter.Events())
	s.Equal("second", <-emitter.Events())
}

func (s *EmitterSuite) TestSendWithinTimesOutWhenBufferStaysFull() {
	emitter := New[string](1)
	defer emitter.Close()

	s.True(emitter.SendWithin("only", time.Second))
	s.False(emitter.SendWithin("overflow", 20*time.Millisecond))

	s.Equal("only", <-emitter.Events())
}

func (s *EmitterSuite) TestSendWithinDeliversWhenSlowReaderCatchesUp() {
	emitter := New[string](1)
	defer emitter.Close()

	s.True(emitter.SendWithin("first", time.Second))

	sent := make(chan bool, 1)
	go func() {
		sent <- emitter.SendWithin("second", time.Second)
	}()

	time.Sleep(25 * time.Millisecond)
	s.Equal("first", <-emitter.Events())

	select {
	case ok := <-sent:
		s.True(ok)
	case <-time.After(time.Second):
		s.FailNow("SendWithin did not return after reader drained")
	}
	s.Equal("second", <-emitter.Events())
}

func (s *EmitterSuite) TestSendWithinAfterCloseReturnsFalseWithoutPanicking() {
	emitter := New[string](2)
	emitter.Close()

	s.NotPanics(func() {
		s.False(emitter.SendWithin("after-close", time.Second))
	})
}
