package goroutine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/suite"
)

type GoroutineSuite struct {
	suite.Suite
}

func TestGoroutineSuite(t *testing.T) {
	suite.Run(t, new(GoroutineSuite))
}

func (s *GoroutineSuite) TestAPanicIsLoggedWithItsStackAndStopsItsCall() {
	var logged bytes.Buffer
	ended := false
	synctest.Test(s.T(), func(*testing.T) {
		ctx := WithEnd(context.Background(), slog.New(slog.NewJSONHandler(&logged, nil)), func() { ended = true })
		Go(ctx, func() { panic("a frame the provider could not read") })
		synctest.Wait()
	})

	s.True(ended, "the call of the goroutine did not stop")
	var line struct {
		Panic string `json:"panic"`
		Stack string `json:"stack"`
	}
	s.Require().NoError(json.Unmarshal(logged.Bytes(), &line))
	s.Equal("a frame the provider could not read", line.Panic)
	s.Contains(line.Stack, "TestAPanicIsLoggedWithItsStackAndStopsItsCall", "the log does not show the stack of the panic")
}

// If Go does not recover the panic, the panic stops the process, and this test fails.
func (s *GoroutineSuite) TestAPanicUnderAContextWithNoCallStopsOnlyItsGoroutine() {
	synctest.Test(s.T(), func(*testing.T) {
		Go(context.Background(), func() { panic("a frame the provider could not read") })
		synctest.Wait()
	})
}
