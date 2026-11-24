// Package goroutine starts the goroutines of a call. A panic in one of these goroutines
// stops only that call. The router and its other calls continue.
package goroutine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/getsentry/sentry-go"
)

// endWithin is how long the end of a call may take after a panic before it is logged as
// stuck. The end drains the turn, decision and memory writers of the call for up to 40 s.
const endWithin = time.Minute

type endKey struct{}

// ending tells what to do when a goroutine of a call panics. logger writes the panic. end
// stops the call.
type ending struct {
	logger *slog.Logger
	end    func()
}

// WithEnd returns a copy of ctx that holds the logger and the end function of a call. Go
// uses them when a goroutine that it started under this ctx panics.
func WithEnd(ctx context.Context, logger *slog.Logger, end func()) context.Context {
	return context.WithValue(ctx, endKey{}, ending{logger: logger, end: end})
}

// Go runs fn in a new goroutine. If fn panics, Go writes the panic and its stack to the log,
// sends the panic to Sentry, and stops the call that ctx holds (WithEnd). A call that has not
// ended a minute later is logged and sent to Sentry as stuck. If ctx holds no
// call, only this goroutine stops. An example is a speech socket that has no agent.
func Go(ctx context.Context, fn func()) {
	go func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			call, ok := ctx.Value(endKey{}).(ending)
			logger := slog.Default()
			if ok {
				logger = call.logger
			}
			logger.Error("a goroutine panicked", "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
			sentry.CurrentHub().Recover(recovered)
			if ok {
				// A panic that left a lock of the call held makes the end wait forever.
				stuck := time.AfterFunc(endWithin, func() {
					call.logger.Error("a call did not end after a goroutine panicked", "panic", fmt.Sprint(recovered))
					sentry.CurrentHub().CaptureMessage("a call did not end after a goroutine panicked")
				})
				// The end function waits for all goroutines of the call, and this goroutine
				// is one of them.
				go func() {
					call.end()
					stuck.Stop()
				}()
			}
		}()
		fn()
	}()
}
