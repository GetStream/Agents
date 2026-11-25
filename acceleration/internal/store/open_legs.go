package store

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/redisscript"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// OpenLegs are how many legs of each organization's calls each shard has up or ringing, in the
// shared Redis, so a shard places a call within the organization's calls at once without
// reading another shard's legs. A shard writes its own count when it places a call
// (CallLegs.PlaceWithin) and every few seconds (Heal), so a leg that ended drops out, and a
// count no shard has written for openLegsStale is no longer counted.
//
// A store with no Redis, or of a deployment of one unnamed shard, counts its own legs alone.
type OpenLegs struct {
	store *Store
}

// openLegsStale is how long a shard's count stands unwritten before it is ignored: a shard
// that is gone, or stopped healing, no longer holds its organizations' calls.
const openLegsStale = time.Minute

// HealOpenLegsEvery is how often each router writes its shard's counts again.
const HealOpenLegsEvery = 5 * time.Second

// placeOpenLeg writes this shard's count (ARGV[2]) of the organization's open legs, and adds
// the leg being placed to it when the counts of every shard not older than ARGV[5] leave room
// under ARGV[3], answering 1, or 0 when they do not. KEYS[2] lists the organizations this
// shard holds a count of, for Heal.
var placeOpenLeg = redisscript.New(`
local now = tonumber(ARGV[4])
local own = tonumber(ARGV[2])
local total = own
local fields = redis.call('HGETALL', KEYS[1])
for i = 1, #fields, 2 do
  if fields[i] ~= ARGV[1] then
    local count, at = string.match(fields[i + 1], '^(%d+) (%d+)$')
    if count and tonumber(at) >= tonumber(ARGV[5]) then total = total + tonumber(count) end
  end
end
if total >= tonumber(ARGV[3]) then
  redis.call('HSET', KEYS[1], ARGV[1], own .. ' ' .. now)
  redis.call('SADD', KEYS[2], ARGV[6])
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], (own + 1) .. ' ' .. now)
redis.call('SADD', KEYS[2], ARGV[6])
return 1
`)

// shared reports whether the counts are kept in Redis for other shards to read.
func (r *OpenLegs) shared() bool {
	return r.store.cache != nil && r.store.homed
}

// elsewhere is how many open legs of organization the other shards last counted.
func (r *OpenLegs) elsewhere(ctx context.Context, organization string) (int, error) {
	if !r.shared() {
		return 0, nil
	}
	client := r.store.cache.Client()
	fields, err := client.Do(ctx, client.B().Hgetall().Key(openLegsKey(organization)).Build()).AsStrMap()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: open legs of other shards: %w", err))
	}
	since := time.Now().Add(-openLegsStale).Unix()
	total := 0
	for shard, value := range fields {
		if shard == r.store.ShardName() {
			continue
		}
		count, at, ok := strings.Cut(value, " ")
		written, err := strconv.ParseInt(at, 10, 64)
		if !ok || err != nil || written < since {
			continue
		}
		n, err := strconv.Atoi(count)
		if err == nil {
			total += n
		}
	}
	return total, nil
}

// place reports whether the organization's open legs, own of them this shard's, leave room
// for another under limit, and counts it in this shard's when they do.
func (r *OpenLegs) place(ctx context.Context, organization string, own, limit int) (bool, error) {
	if !r.shared() {
		return own < limit, nil
	}
	now := time.Now()
	placed, err := placeOpenLeg.Exec(ctx, r.store.cache.Client(),
		[]string{openLegsKey(organization), openLegsShardKey(r.store.ShardName())},
		[]string{r.store.ShardName(), strconv.Itoa(own), strconv.Itoa(limit), strconv.FormatInt(now.Unix(), 10),
			strconv.FormatInt(now.Add(-openLegsStale).Unix(), 10), organization}).AsInt64()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: place an open leg: %w", err))
	}
	return placed == 1, nil
}

// write sets this shard's count of the organization's open legs.
func (r *OpenLegs) write(ctx context.Context, organization string, own int) error {
	client := r.store.cache.Client()
	key, held := openLegsKey(organization), openLegsShardKey(r.store.ShardName())
	var err error
	if own == 0 {
		for _, answer := range client.DoMulti(ctx,
			client.B().Hdel().Key(key).Field(r.store.ShardName()).Build(),
			client.B().Srem().Key(held).Member(organization).Build()) {
			err = answer.Error()
		}
	} else {
		value := strconv.Itoa(own) + " " + strconv.FormatInt(time.Now().Unix(), 10)
		for _, answer := range client.DoMulti(ctx,
			client.B().Hset().Key(key).FieldValue().FieldValue(r.store.ShardName(), value).Build(),
			client.B().Sadd().Key(held).Member(organization).Build()) {
			err = answer.Error()
		}
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: write open legs: %w", err))
	}
	return nil
}

// Heal writes this shard's count of every organization it holds a count of or has open legs
// of, each under the lock PlaceWithin places under, so a leg that ended is no longer counted.
// A leg rung or placed since ringingSince counts as ringing.
func (r *OpenLegs) Heal(ctx context.Context, ringingSince time.Time) error {
	if !r.shared() {
		return nil
	}
	client := r.store.cache.Client()
	held, err := client.Do(ctx, client.B().Smembers().Key(openLegsShardKey(r.store.ShardName())).Build()).AsStrSlice()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: organizations with open legs: %w", err))
	}
	var open []string
	err = r.store.shard.NewSelect().Model((*CallLeg)(nil)).ColumnExpr("DISTINCT organization_id").
		Where("organization_id <> ''").Where("ended_at IS NULL").
		Scan(ctx, &open)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: organizations with open legs: %w", err))
	}
	organizations := append(held, open...)
	slices.Sort(organizations)
	for _, organization := range slices.Compact(organizations) {
		err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if err := lockUntilCommit(ctx, tx, organization, callsAtOnceLockSeed); err != nil {
				return err
			}
			count, err := countOpen(ctx, tx, organization, ringingSince, "", "")
			if err != nil {
				return err
			}
			return r.write(ctx, organization, count.Total)
		})
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: heal open legs: %w", err))
		}
	}
	return nil
}

// openLegsKey holds each shard's count of an organization's open legs.
func openLegsKey(organization string) string {
	return "dlc:open:" + organization
}

// openLegsShardKey lists the organizations a shard holds a count of.
func openLegsShardKey(shard string) string {
	return "dlc:open:shard:" + shard
}
