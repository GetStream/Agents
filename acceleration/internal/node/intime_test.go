package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// The bubble fails a test that leaves a goroutine blocked in it.
func TestARequestNotWaitedForEndsWhenItIsAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		answered := make(chan struct{})

		_, err := InTime(ctx, func() (int, error) {
			<-answered
			return 1, nil
		})

		require.ErrorIs(t, err, context.DeadlineExceeded)
		close(answered)
	})
}
