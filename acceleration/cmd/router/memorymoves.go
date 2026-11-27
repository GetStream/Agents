package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// memoryMoveEvery is how often a router looks for claimed guests whose memories are due to move.
const memoryMoveEvery = 5 * time.Second

// startMemoryMover runs the passes of claimed guests' memory moves as they come due, until ctx
// is done. A router without a memory store or a database has none to run.
func startMemoryMover(ctx context.Context, sessions *session.Manager, logger *slog.Logger) {
	goroutine.Go(ctx, func() {
		ticker := time.NewTicker(memoryMoveEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := sessions.MoveGuestMemories(ctx); err != nil && ctx.Err() == nil {
					logger.Error("could not claim the guest memory moves due", "error", err)
				}
			}
		}
	})
}
