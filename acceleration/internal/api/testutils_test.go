//go:build integration

package api

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// TestUtilsSuite covers the values the suites take from testUtils.
type TestUtilsSuite struct {
	suite.Suite
}

func TestTestUtilsSuite(t *testing.T) {
	suite.Run(t, new(TestUtilsSuite))
}

// A number two tests hold at once is refused by phone_numbers_held_idx, so no call may repeat an
// earlier one. Twenty thousand calls from ten million numbers, drawn at random, repeat one about
// twenty times.
func (s *TestUtilsSuite) TestNoNumberIsGivenOutTwice() {
	var utils testUtils
	given := make(map[string]bool)
	for range 20_000 {
		number := utils.number()
		s.Require().False(given[number], "%s was given out twice", number)
		given[number] = true
	}
}
