package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/redisscript"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ShardsKey is where a deployment's routers keep the shard list they agree on.
const ShardsKey = "node:shards"

// ErrShardsDisagree is a router that knows other shards than the routers running. Its calls
// would be written to a shard their readers never look in, or read from fewer shards than
// they are written to.
var ErrShardsDisagree = errors.New("node: shards differs from the running routers'")

// agreeScript takes the list for holdFor when no router holds one, renews it when it is the
// one held, and answers the list in force.
var agreeScript = redisscript.New(`
local held = redis.call('GET', KEYS[1])
if held == false or held == ARGV[1] then
	redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
	return ARGV[1]
end
return held`)

// AgreeOnShards says which shards this router knows, refuses with ErrShardsDisagree when the
// routers running know others, and goes on saying so until ctx ends. The list lapses holdFor
// after the last router holding it stops, so a deliberate change rolls out once the routers
// with the old list are gone.
func AgreeOnShards(ctx context.Context, redis rueidis.Client, key string, shards []string, logger *slog.Logger) error {
	known := strings.Join(slices.Sorted(slices.Values(shards)), ",")
	held, err := agree(ctx, redis, key, known)
	if err != nil {
		return err
	}
	if held != known {
		return stack.Wrap(fmt.Errorf("%w: this router knows %q, the running ones %q", ErrShardsDisagree, known, held))
	}
	go func() {
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			held, err := agree(ctx, redis, key, known)
			switch {
			case err != nil:
				logger.Warn("could not renew the shard list this router agreed on", "error", err)
			case held != known:
				// Only after the list lapsed, as in a Redis outage: a router with
				// another list took it, and this one cannot hold it again.
				logger.Error(ErrShardsDisagree.Error(), "shards", known, "held", held)
				return
			}
		}
	}()
	return nil
}

func agree(ctx context.Context, redis rueidis.Client, key, known string) (string, error) {
	held, err := agreeScript.Exec(ctx, redis, []string{key}, []string{known, strconv.FormatInt(holdFor.Milliseconds(), 10)}).ToString()
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("node: agree on the shard list: %w", err))
	}
	return held, nil
}
