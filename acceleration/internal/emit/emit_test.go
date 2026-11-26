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

func (s *EmitterSuite) TestSendWithinDeliversWhenBufferHasRoom() {
	emitter := New[int](1)
	defer emitter.Close()

	s.True(emitter.SendWithin(42, time.Second))
	s.Equal(42, <-emitter.Events())
}

func (s *EmitterSuite) TestSendWithinReturnsFalseWhenBufferStaysFull() {
	emitter := New[int](1)
	defer emitter.Close()

	emitter.Send(1)
	start := time.Now()
	s.False(emitter.SendWithin(2, 30*time.Millisecond))
	s.GreaterOrEqual(time.Since(start), 25*time.Millisecond)
	s.Equal(1, <-emitter.Events())
}

func (s *EmitterSuite) TestSendWithinReturnsFalseWhenClosed() {
	emitter := New[int](1)
	emitter.Close()

	s.False(emitter.SendWithin(1, time.Second))
}

func (s *EmitterSuite) TestSendWithinDeliversWhenConsumerReadsBeforeTimeout() {
	emitter := New[int](1)
	defer emitter.Close()

	emitter.Send(1)
	done := make(chan bool, 1)
	go func() {
		done <- emitter.SendWithin(2, time.Second)
	}()

	time.Sleep(20 * time.Millisecond)
	s.Equal(1, <-emitter.Events())
	s.True(<-done)
	s.Equal(2, <-emitter.Events())
}
