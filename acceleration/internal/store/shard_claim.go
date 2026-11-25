package store

import (
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
)

// claimOverdue is how long work meant for another shard may wait to be claimed there before
// any shard claims it, so a shard that is down holds up no reminder and no dial.
const claimOverdue = 5 * time.Minute

// ShardClaim is whose work a router claims: the work Routing means for Shard, and work meant
// for no shard the deployment has. The zero ShardClaim, or one whose Routing is not Enabled,
// claims every shard's work.
type ShardClaim struct {
	// Shard is the claiming router's own.
	Shard string
	// Routing is the deployment's shard routing, which names the shard a piece of work is
	// meant for.
	Routing shardmap.Map
	// Shards are the deployment's shards besides the ones Routing names, such as
	// the shards setting lists. A user last served by one of them has their work claimed there.
	Shards []string
}

// filtered reports whether the claim leaves another shard's work alone.
func (c ShardClaim) filtered() bool {
	return c.Shard != "" && c.Routing.Enabled()
}

// others are the deployment's shards other than the claim's own: work meant for one of them is
// theirs, and work meant for any other name is anyone's.
func (c ShardClaim) others() []string {
	others := slices.DeleteFunc(append(c.Routing.Shards(), c.Shards...), func(shard string) bool {
		return shard == c.Shard || shard == ""
	})
	slices.Sort(others)
	return slices.Compact(others)
}

// pairs is mapped as the two arrays an unnest of (key, value) reads.
func pairs(mapped map[string]string) (any, any) {
	keys := slices.Sorted(maps.Keys(mapped))
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = mapped[key]
	}
	return pgdialect.Array(keys), pgdialect.Array(values)
}

// scheduledTaskShard is the condition that the scheduled task st is the claim's
// (shardmap.Map.ForUser): its agent's region's shard when Routing maps that region, or else
// the shard that last served its user, is not another shard of the deployment.
const scheduledTaskShard = `COALESCE(
    (SELECT r.shard FROM agent_configs AS ac
        JOIN unnest(?::text[], ?::text[]) AS r (region, shard) ON r.region = ac.region
        WHERE ac.id = st.config_id),
    (SELECT u.shard FROM users AS u WHERE u.customer_id = st.customer_id AND u.id = st.user_id),
    '') <> ALL(?::text[])`

// scheduledTaskHomedHere is the condition that the scheduled task st is about no session that
// lives in another shard than the claim's. Such a session's rows are there alone, so its task
// is claimed there however overdue.
const scheduledTaskHomedHere = `NOT EXISTS (SELECT 1 FROM session_homes AS sh
    WHERE sh.customer_id = st.customer_id AND sh.session_id = st.session_id AND sh.shard <> ?)`

// scheduledTaskClaimable is the claim's condition on the scheduled task st: it is the claim's,
// or it was claimable for longer than claimOverdue, its due time or the lease or hold on it
// having ended that long ago.
func (c ShardClaim) scheduledTaskClaimable() (string, []any) {
	regions, shards := pairs(c.Routing.Regions)
	return `(GREATEST(st.next_run_at, st.lease_until) <= now() - make_interval(secs => ?) OR ` + scheduledTaskShard + `)`,
		[]any{claimOverdue.Seconds(), regions, shards, pgdialect.Array(c.others())}
}

// campaignShard is the condition that the campaign cp is the claim's (shardmap.Map.ForCountry):
// the shard of the country of the number it rings from, or Routing's default for a country
// Routing does not list, is not another shard of the deployment.
const campaignShard = `COALESCE(
    (SELECT c.shard FROM phone_numbers AS pn
        JOIN unnest(?::text[], ?::text[]) AS c (country, shard) ON c.country = upper(pn.country)
        WHERE pn.customer_id = cp.customer_id AND pn.e164 = cp.from_number
        LIMIT 1),
    ?) <> ALL(?::text[])`

// campaignClaimable is campaignShard's query and arguments.
func (c ShardClaim) campaignClaimable() (string, []any) {
	countries, shards := pairs(c.Routing.Countries)
	return campaignShard, []any{countries, shards, c.Routing.Default, pgdialect.Array(c.others())}
}
