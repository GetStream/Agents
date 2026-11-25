package api

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type LogHandlerSuite struct {
	suite.Suite

	out    *bytes.Buffer
	logger *slog.Logger
}

func TestLogHandlerSuite(t *testing.T) {
	suite.Run(t, new(LogHandlerSuite))
}

func (s *LogHandlerSuite) SetupTest() {
	s.out = &bytes.Buffer{}
	s.logger = slog.New(NewLogHandler(slog.NewTextHandler(s.out, nil)))
}

func (s *LogHandlerSuite) TestALineLoggedWithTheRequestsContextNamesItsAppAndKey() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.InfoContext(ctx, "probe", "x", 1)

	s.Contains(s.out.String(), " x=1 app_id=1732479 api_key=pubkey123\n")
}

func (s *LogHandlerSuite) TestALineLoggedWithoutTheContextNamesNeither() {
	s.logger.Info("probe")
	s.logger.InfoContext(context.Background(), "probe")

	s.NotContains(s.out.String(), "app_id")
	s.NotContains(s.out.String(), "api_key")
}

func (s *LogHandlerSuite) TestAKeyNobodyNamedIsNotLoggedEmpty() {
	s.logger.InfoContext(WithLoggedApp(context.Background(), "1732479", ""), "probe")

	s.Contains(s.out.String(), "app_id=1732479")
	s.NotContains(s.out.String(), "api_key")
}

func (s *LogHandlerSuite) TestARequestWithNoAppNamesNothing() {
	s.logger.InfoContext(WithLoggedApp(context.Background(), "", ""), "probe")

	s.NotContains(s.out.String(), "app_id")
	s.NotContains(s.out.String(), "api_key")
}

func (s *LogHandlerSuite) TestALineThatNamesTheAppItselfIsNotGivenASecondOne() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.InfoContext(ctx, "probe", "app_id", "other")
	s.logger.InfoContext(ctx, "probe", "api_key", "other")

	lines := strings.Split(strings.TrimSpace(s.out.String()), "\n")
	s.Require().Len(lines, 2)
	s.Equal(1, strings.Count(lines[0], "app_id="))
	s.Contains(lines[0], "app_id=other")
	s.Contains(lines[0], "api_key=pubkey123")
	s.Equal(1, strings.Count(lines[1], "api_key="))
	s.Contains(lines[1], "api_key=other")
	s.Contains(lines[1], "app_id=1732479")
}

func (s *LogHandlerSuite) TestAFieldNamedAfterOthersIsFoundToo() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.InfoContext(ctx, "probe", "x", 1, "y", 2, "app_id", "other", "api_key", "other")

	s.Equal(1, strings.Count(s.out.String(), "app_id="))
	s.Equal(1, strings.Count(s.out.String(), "api_key="))
}

func (s *LogHandlerSuite) TestALoggerThatAlreadyNamesTheAppIsNotGivenASecondOne() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.With("app_id", "other").InfoContext(ctx, "probe")
	s.logger.With("api_key", "other").With("x", 1).InfoContext(ctx, "probe")

	lines := strings.Split(strings.TrimSpace(s.out.String()), "\n")
	s.Require().Len(lines, 2)
	s.Equal(1, strings.Count(lines[0], "app_id="))
	s.Contains(lines[0], "api_key=pubkey123")
	s.Equal(1, strings.Count(lines[1], "api_key="))
	s.Contains(lines[1], "app_id=1732479")
}

func (s *LogHandlerSuite) TestALoggerWithAFieldDoesNotHideTheAppFromItsSiblings() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")
	named := s.logger.With("app_id", "other")

	s.logger.InfoContext(ctx, "probe")
	named.InfoContext(ctx, "probe")
	s.logger.With("x", 1).InfoContext(ctx, "probe")

	lines := strings.Split(strings.TrimSpace(s.out.String()), "\n")
	s.Require().Len(lines, 3)
	s.Contains(lines[0], "app_id=1732479")
	s.Equal(1, strings.Count(lines[1], "app_id="))
	s.Contains(lines[2], "app_id=1732479")
}

func (s *LogHandlerSuite) TestALineOfAGroupedLoggerNamesTheAppInsideTheGroup() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.With("app_id", "other").WithGroup("g").InfoContext(ctx, "probe", "x", 1)
	s.logger.WithGroup("g").With("app_id", "other").InfoContext(ctx, "probe")

	lines := strings.Split(strings.TrimSpace(s.out.String()), "\n")
	s.Require().Len(lines, 2)
	s.Contains(lines[0], " app_id=other g.x=1 g.app_id=1732479 g.api_key=pubkey123")
	s.Equal(1, strings.Count(lines[1], "g.app_id="))
	s.Contains(lines[1], " g.app_id=other g.api_key=pubkey123")
}

func (s *LogHandlerSuite) TestALoggerThatAlreadyNamesTheKeyIsNotGivenASecondOne() {
	ctx := WithLoggedApp(context.Background(), "1732479", "pubkey123")

	s.logger.With("api_key", "other").InfoContext(ctx, "probe")

	s.Equal(1, strings.Count(s.out.String(), "api_key="))
	s.Contains(s.out.String(), "app_id=1732479")
}

// lockedLog is what the router logs, written from any goroutine and read by the test.
type lockedLog struct {
	mu      sync.Mutex
	written bytes.Buffer
	// onWrite, when set, runs on the writing goroutine after each line is written, so a test can
	// hold the router at a line until it has changed something.
	onWrite func(line string)
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	n, err := l.written.Write(p)
	onWrite := l.onWrite
	l.mu.Unlock()
	if onWrite != nil {
		onWrite(string(p))
	}
	return n, err
}

// whenWritten sets the function Write runs after each line; nil clears it.
func (l *lockedLog) whenWritten(onWrite func(line string)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onWrite = onWrite
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written.String()
}
