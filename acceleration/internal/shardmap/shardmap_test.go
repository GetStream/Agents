package shardmap

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// MapSuite covers which shard a piece of work is sent to.
type MapSuite struct {
	suite.Suite
	shards Map
}

func TestMapSuite(t *testing.T) {
	suite.Run(t, new(MapSuite))
}

func (s *MapSuite) SetupTest() {
	s.shards = Map{
		Default:   "us-east4",
		Countries: map[string]string{"DE": "europe-west4", "NL": "europe-west4"},
		Regions:   map[string]string{"eu": "europe-west4"},
	}
}

func (s *MapSuite) TestACallToAListedCountryGoesToItsShardWhateverTheCase() {
	s.Equal("europe-west4", s.shards.ForCountry("DE"))
	s.Equal("europe-west4", s.shards.ForCountry("nl"))
}

func (s *MapSuite) TestACallToACountryNotListedGoesToTheDefaultShard() {
	s.Equal("us-east4", s.shards.ForCountry("BR"))
	s.Equal("us-east4", s.shards.ForCountry(""))
}

func (s *MapSuite) TestAnAgentKeptInAnotherRegionStaysInItsShardWhereverItsUserIs() {
	s.Equal("europe-west4", s.shards.ForUser("eu", "us-west1"))
}

func (s *MapSuite) TestAnAgentInTheDefaultRegionFollowsItsUser() {
	s.Equal("us-west1", s.shards.ForUser("us", "us-west1"))
	s.Equal("us-west1", s.shards.ForUser("", "us-west1"))
}

func (s *MapSuite) TestAnAgentInARegionNoShardIsMappedToFollowsItsUser() {
	s.Equal("us-west1", s.shards.ForUser("asia", "us-west1"))
}

func (s *MapSuite) TestAMapOfNothingSendsAUserNowhere() {
	s.Empty(Map{}.ForUser("us", "us-west1"))
	s.Empty(Map{}.ForCountry("DE"))
}

func (s *MapSuite) TestShardsNamesEveryShardTheMapSendsWorkToOnce() {
	shards := Map{Default: "us-east4", Countries: map[string]string{"DE": "europe-west4", "NL": "europe-west4"},
		Regions: map[string]string{"asia": "asia-east1"}}

	s.Equal([]string{"asia-east1", "europe-west4", "us-east4"}, shards.Shards())
	s.Empty(Map{}.Shards())
}

func (s *MapSuite) TestShardsNamesTheHomeAndEveryResidencyShard() {
	shards := Map{Home: "us-east4", Residency: map[string][]string{"us": {"us-east4", "us-west1"},
		"ca": {"northamerica-northeast1"}}}

	s.Equal([]string{"northamerica-northeast1", "us-east4", "us-west1"}, shards.Shards())
}

func (s *MapSuite) TestAMapWithAHomeOrAResidencyAloneIsEnabled() {
	s.True(Map{Home: "us-east4"}.Enabled())
	s.True(Map{Residency: map[string][]string{"eu": {"europe-west4"}}}.Enabled())
	s.False(Map{}.Enabled())
}

// policies is a deployment of a home, a US geography of two shards, an EU one and a Canadian
// geography with no shard.
func policies() Map {
	return Map{Home: "us-east4", Residency: map[string][]string{
		"us": {"us-east4", "us-west1"},
		"eu": {"europe-west4"},
	}}
}

func (s *MapSuite) TestTheAgentsPolicyWinsOverTheAppsAndClosestIsWhatNeitherSets() {
	s.Equal(PolicyEUOnly, Effective("us_east", "eu_only"))
	s.Equal(PolicyUSEast, Effective("us_east", ""))
	s.Equal(PolicyClosest, Effective("", ""))
}

func (s *MapSuite) TestOnlyTheFivePoliciesAreValid() {
	for _, policy := range []Policy{PolicyClosest, PolicyUSEast, PolicyUSOnly, PolicyEUOnly, PolicyCanadaOnly} {
		s.True(policy.Valid(), policy)
	}
	s.False(Policy("").Valid())
	s.False(Policy("eu").Valid())
	s.False(Policy("US_ONLY").Valid())
}

func (s *MapSuite) TestEachResidencyPolicyKeepsWorkInItsGeographyAndIsStrict() {
	s.Equal("us", PolicyUSOnly.Residency())
	s.Equal("eu", PolicyEUOnly.Residency())
	s.Equal("ca", PolicyCanadaOnly.Residency())
	for _, policy := range []Policy{PolicyUSOnly, PolicyEUOnly, PolicyCanadaOnly} {
		s.True(policy.Strict(), policy)
	}
	for _, policy := range []Policy{PolicyClosest, PolicyUSEast} {
		s.Empty(policy.Residency(), policy)
		s.False(policy.Strict(), policy)
	}
}

func (s *MapSuite) TestClosestAndUSEastMayBeDoneInAnyShard() {
	s.Nil(policies().Allowed(PolicyClosest))
	s.Nil(policies().Allowed(PolicyUSEast))
}

func (s *MapSuite) TestAResidencyPolicyIsAllowedItsGeographysShardsAlone() {
	s.Equal([]string{"us-east4", "us-west1"}, policies().Allowed(PolicyUSOnly))
	s.Equal([]string{"europe-west4"}, policies().Allowed(PolicyEUOnly))
}

func (s *MapSuite) TestAGeographyWithNoShardAllowsNoShardRatherThanAny() {
	allowed := policies().Allowed(PolicyCanadaOnly)

	s.NotNil(allowed)
	s.Empty(allowed)
}

func (s *MapSuite) TestClosestKeepsTheShardTodaysRulesPick() {
	for _, closest := range []string{"europe-west4", "asia-east1", ""} {
		shard, err := policies().Place(PolicyClosest, closest)
		s.Require().NoError(err)
		s.Equal(closest, shard)
	}
}

func (s *MapSuite) TestUSEastSendsWorkHomeFromAnywhere() {
	for _, closest := range []string{"europe-west4", "us-west1", ""} {
		shard, err := policies().Place(PolicyUSEast, closest)
		s.Require().NoError(err)
		s.Equal("us-east4", shard)
	}
}

func (s *MapSuite) TestUSEastInADeploymentWithNoHomeIsClosest() {
	shard, err := Map{}.Place(PolicyUSEast, "europe-west4")
	s.Require().NoError(err)
	s.Equal("europe-west4", shard)
}

func (s *MapSuite) TestAResidencyPolicyKeepsWorkInTheClosestShardWhenItIsInTheGeography() {
	shard, err := policies().Place(PolicyUSOnly, "us-west1")
	s.Require().NoError(err)
	s.Equal("us-west1", shard)
}

func (s *MapSuite) TestAResidencyPolicyMovesWorkClosestOutsideTheGeographyToItsFirstShard() {
	shard, err := policies().Place(PolicyUSOnly, "europe-west4")
	s.Require().NoError(err)
	s.Equal("us-east4", shard)

	shard, err = policies().Place(PolicyEUOnly, "us-east4")
	s.Require().NoError(err)
	s.Equal("europe-west4", shard)
}

func (s *MapSuite) TestAResidencyPolicyGivenNoShardPlacesWorkInTheGeography() {
	shard, err := policies().Place(PolicyEUOnly, "")
	s.Require().NoError(err)
	s.Equal("europe-west4", shard)
}

func (s *MapSuite) TestAResidencyPolicyWhoseGeographyHasNoShardPlacesWorkNowhere() {
	shard, err := policies().Place(PolicyCanadaOnly, "us-east4")
	s.ErrorIs(err, ErrNoShardInResidency)
	s.Empty(shard)

	_, err = Map{Home: "us-east4"}.Place(PolicyEUOnly, "us-east4")
	s.ErrorIs(err, ErrNoShardInResidency, "a sharded deployment that lists no geography")
}

func (s *MapSuite) TestAnUnshardedDeploymentDoesResidentWorkWhereItLands() {
	s.Nil(Map{}.Allowed(PolicyEUOnly))
	shard, err := Map{}.Place(PolicyEUOnly, "")
	s.Require().NoError(err)
	s.Empty(shard)
}
