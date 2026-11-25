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
