package store

import (
	"fmt"
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

// scheduledTaskShard is the shard today's rules mean the scheduled task st for
// (shardmap.Map.ForUser): its agent's region's shard when Routing maps that region, or else
// the shard that last served its user, or empty for no shard.
const scheduledTaskShard = `COALESCE(
    (SELECT r.shard FROM agent_configs AS ac
        JOIN unnest(?::text[], ?::text[]) AS r (region, shard) ON r.region = ac.region
        WHERE ac.id = st.config_id),
    (SELECT u.shard FROM users AS u WHERE u.customer_id = st.customer_id AND u.id = st.user_id),
    '')`

// scheduledTaskHomedHere is the condition that the scheduled task st is about no session that
// lives in another shard than the claim's. Such a session's rows are there alone, so its task
// is claimed there however overdue.
const scheduledTaskHomedHere = `NOT EXISTS (SELECT 1 FROM session_homes AS sh
    WHERE sh.customer_id = st.customer_id AND sh.session_id = st.session_id AND sh.shard <> ?)`

// scheduledTaskClaimable is the claim's condition on the scheduled task st (claimable), overdue
// when it was claimable for longer than claimOverdue, its due time or the lease or hold on it
// having ended that long ago.
func (c ShardClaim) scheduledTaskClaimable() (string, []any) {
	regions, shards := pairs(c.Routing.Regions)
	return c.claimable("st", scheduledTaskShard, []any{regions, shards},
		`GREATEST(st.next_run_at, st.lease_until) <= now() - make_interval(secs => ?)`, []any{claimOverdue.Seconds()})
}

// campaignShard is the shard today's rules mean the campaign cp for (shardmap.Map.ForCountry):
// the shard of the country of the number it rings from, or Routing's default for a country
// Routing does not list.
const campaignShard = `COALESCE(
    (SELECT c.shard FROM phone_numbers AS pn
        JOIN unnest(?::text[], ?::text[]) AS c (country, shard) ON c.country = upper(pn.country)
        WHERE pn.customer_id = cp.customer_id AND pn.e164 = cp.from_number
        LIMIT 1),
    ?)`

// campaignClaimable is the claim's condition on the campaign cp (claimable), with overdue and
// its arguments saying when its work waited past claimOverdue.
func (c ShardClaim) campaignClaimable(overdue string, overdueArgs ...any) (string, []any) {
	countries, shards := pairs(c.Routing.Countries)
	return c.claimable("cp", campaignShard, []any{countries, shards, c.Routing.Default}, overdue, overdueArgs)
}

// workPolicy is the routing policy of the work %[1]s, a scheduled task or a campaign, read as
// shardmap.Effective does: its agent's, else its app's, else empty for shardmap.PolicyClosest.
const workPolicy = `COALESCE(NULLIF((SELECT ac.routing_policy FROM agent_configs AS ac WHERE ac.id = %[1]s.config_id), ''),
    (SELECT ap.settings->>'routing_policy' FROM apps AS ap WHERE ap.id = %[1]s.customer_id), '')`

// claimedHere is the condition that work under policy, which today's rules mean for the shard
// closest, is the claim's: the shard shardmap.Map.Place gives it is not another shard of the
// deployment, or the work is overdue. A residency policy's work is claimed by its geography's
// shards alone, however overdue, and by none when the geography has none.
// OFFSET 0 keeps the planner from inlining w, which would read its subqueries again at each use.
const claimedHere = `EXISTS (SELECT 1 FROM (SELECT %s AS policy, %s AS closest OFFSET 0) AS w,
    LATERAL (SELECT array_agg(g.shard ORDER BY g.n) AS shards
        FROM unnest(?::text[], ?::text[]) WITH ORDINALITY AS g (policy, shard, n)
        WHERE g.policy = w.policy) AS allowed
    WHERE (w.policy <> ALL(?::text[]) OR ? = ANY(allowed.shards))
    AND (%s OR CASE
        WHEN w.policy = ANY(?::text[]) THEN
            CASE WHEN w.closest = ANY(allowed.shards) THEN w.closest ELSE allowed.shards[1] END
        WHEN w.policy = ? THEN COALESCE(NULLIF(?, ''), w.closest)
        ELSE w.closest END <> ALL(?::text[])))`

// claimable is claimedHere for the work alias, whose closest shard and overdue condition are
// SQL with their arguments.
func (c ShardClaim) claimable(alias, closest string, closestArgs []any, overdue string, overdueArgs []any) (string, []any) {
	var policies, allowed, strict []string
	for _, policy := range shardmap.Policies {
		if !policy.Strict() {
			continue
		}
		strict = append(strict, string(policy))
		for _, shard := range c.Routing.Allowed(policy) {
			policies = append(policies, string(policy))
			allowed = append(allowed, shard)
		}
	}
	args := slices.Concat(closestArgs,
		[]any{pgdialect.Array(policies), pgdialect.Array(allowed), pgdialect.Array(strict), c.Shard},
		overdueArgs,
		[]any{pgdialect.Array(strict), string(shardmap.PolicyUSEast), c.Routing.Home, pgdialect.Array(c.others())})
	return fmt.Sprintf(claimedHere, fmt.Sprintf(workPolicy, alias), closest, overdue), args
}
