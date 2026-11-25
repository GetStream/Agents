// Package shardmap says which shard of a deployment does a piece of work, from where its
// person is: the country of a phone number, the region an agent is kept in, or the shard that
// last served a user's own client.
//
// Every router of a deployment holds the same Map, so whichever one a hook lands on sends the
// work to the same shard.
package shardmap

import (
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
)

// Map is where work goes, by shard name. The zero Map sends nothing anywhere: every piece of
// work stays on the router that received it.
type Map struct {
	// Default is the shard of a country Countries does not list. Empty leaves such a call on
	// the router it landed on.
	Default string
	// Countries maps an ISO 3166-1 alpha-2 country code, such as DE, to a shard.
	Countries map[string]string
	// Regions maps an agent region other than pricing.DefaultRegion, such as eu, to the shard
	// its work stays in.
	Regions map[string]string
}

// Enabled reports whether the map sends any work anywhere.
func (m Map) Enabled() bool {
	return m.Default != "" || len(m.Countries) > 0 || len(m.Regions) > 0
}

// ForCountry is the shard a call to or from a number in country is answered in, empty for
// the router that received it.
func (m Map) ForCountry(country string) string {
	if shard, listed := m.Countries[strings.ToUpper(country)]; listed {
		return shard
	}
	return m.Default
}

// ForUser is the shard a user's work under an agent of agentRegion is done in: the region's
// own shard when the agent is kept in one other than the default, so residency wins, and
// otherwise userShard, the shard that last served the user's own client. Empty is the router
// that received it, which is every answer of a map that is not Enabled.
func (m Map) ForUser(agentRegion, userShard string) string {
	if !m.Enabled() {
		return ""
	}
	if agentRegion != "" && agentRegion != pricing.DefaultRegion {
		if shard, mapped := m.Regions[agentRegion]; mapped {
			return shard
		}
	}
	return userShard
}
