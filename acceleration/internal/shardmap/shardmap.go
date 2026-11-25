// Package shardmap says which shard of a deployment does a piece of work, from where its
// person is: the country of a phone number, the region an agent is kept in, or the shard that
// last served a user's own client.
//
// Every router of a deployment holds the same Map, so whichever one a hook lands on sends the
// work to the same shard.
package shardmap

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
)

// Policy is where an app's or an agent's work may be placed.
type Policy string

const (
	// PolicyClosest does work in the shard today's rules pick: where the request landed, the
	// number's country, the agent's region or the user's last shard.
	PolicyClosest Policy = "closest"
	// PolicyUSEast does all work in the deployment's home shard, where configs live.
	PolicyUSEast Policy = "us_east"
	// PolicyUSOnly, PolicyEUOnly and PolicyCanadaOnly keep all work in the shards of one
	// geography, and fail it rather than place it anywhere else.
	PolicyUSOnly     Policy = "us_only"
	PolicyEUOnly     Policy = "eu_only"
	PolicyCanadaOnly Policy = "canada_only"
)

// Policies is every policy, in the order the API lists them.
var Policies = []Policy{PolicyClosest, PolicyUSEast, PolicyUSOnly, PolicyEUOnly, PolicyCanadaOnly}

// Geographies are the keys of Map.Residency, one per residency policy.
const (
	GeographyUS     = "us"
	GeographyEU     = "eu"
	GeographyCanada = "ca"
)

// ErrNoShardInResidency is work under a residency policy whose geography has no shard: it is
// done nowhere rather than outside the geography.
var ErrNoShardInResidency = errors.New("shardmap: no shard in the policy's geography")

// Effective is the policy work is placed by: the agent's when it sets one, else the app's, else
// PolicyClosest.
func Effective(appPolicy, agentPolicy string) Policy {
	if agentPolicy != "" {
		return Policy(agentPolicy)
	}
	if appPolicy != "" {
		return Policy(appPolicy)
	}
	return PolicyClosest
}

// Valid reports whether p is one of Policies.
func (p Policy) Valid() bool {
	return slices.Contains(Policies, p)
}

// Residency is the geography a residency policy keeps work in, a key of Map.Residency, or
// empty for a policy that keeps it nowhere in particular.
func (p Policy) Residency() string {
	switch p {
	case PolicyUSOnly:
		return GeographyUS
	case PolicyEUOnly:
		return GeographyEU
	case PolicyCanadaOnly:
		return GeographyCanada
	}
	return ""
}

// Strict reports whether work under p fails rather than leave its geography.
func (p Policy) Strict() bool {
	return p.Residency() != ""
}

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
	// Home is the shard PolicyUSEast does work in, the one holding the configs. Empty is an
	// unsharded deployment, where PolicyUSEast is PolicyClosest.
	Home string
	// Residency maps a geography, GeographyUS, GeographyEU or GeographyCanada, to its shards in
	// order of preference: the first is where work that landed outside it goes.
	Residency map[string][]string
}

// Enabled reports whether the map sends any work anywhere.
func (m Map) Enabled() bool {
	return m.Default != "" || len(m.Countries) > 0 || len(m.Regions) > 0 || m.Home != "" || len(m.Residency) > 0
}

// Allowed is the shards work under p may be done in; nil means any shard. A residency policy
// whose geography lists none is empty and not nil: no shard. A map that is not Enabled is an
// unsharded deployment, whose one router is wherever its operator put it, so it allows any.
func (m Map) Allowed(p Policy) []string {
	geography := p.Residency()
	if geography == "" || !m.Enabled() {
		return nil
	}
	if shards := m.Residency[geography]; len(shards) > 0 {
		return shards
	}
	return []string{}
}

// Place is the shard work under p is done in, given closest, the shard today's rules pick for
// it. closest must be a shard name: a caller whose rules leave the work on the router that
// received it passes that router's own shard. Empty is in no geography, so a residency policy
// moves it to the geography's first shard, and the other policies keep it empty, as today.
// ErrNoShardInResidency is a residency policy whose geography has no shard in a map that is
// Enabled; one that is not keeps every policy's work where it landed (Allowed).
func (m Map) Place(p Policy, closest string) (string, error) {
	if p == PolicyUSEast {
		return cmp.Or(m.Home, closest), nil
	}
	allowed := m.Allowed(p)
	switch {
	case allowed == nil:
		return closest, nil
	case len(allowed) == 0:
		return "", ErrNoShardInResidency
	case slices.Contains(allowed, closest):
		return closest, nil
	}
	return allowed[0], nil
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

// Shards is every shard the map sends work to, sorted, each once.
func (m Map) Shards() []string {
	var shards []string
	if m.Default != "" {
		shards = append(shards, m.Default)
	}
	shards = append(shards, slices.Collect(maps.Values(m.Countries))...)
	shards = append(shards, slices.Collect(maps.Values(m.Regions))...)
	if m.Home != "" {
		shards = append(shards, m.Home)
	}
	for _, geography := range m.Residency {
		shards = append(shards, geography...)
	}
	slices.Sort(shards)
	return slices.Compact(shards)
}
