package store

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type LimitOwnerUnitSuite struct {
	suite.Suite
}

func TestLimitOwnerUnitSuite(t *testing.T) {
	suite.Run(t, new(LimitOwnerUnitSuite))
}

func (s *LimitOwnerUnitSuite) ids(limits []Limit) []int64 {
	ids := []int64{}
	for _, limit := range limits {
		ids = append(ids, limit.ID)
	}
	return ids
}

func (s *LimitOwnerUnitSuite) TestAnOwnerHoldsItsRowsAndIsHeldToThoseOfNobodyKnownToo() {
	rows := []Limit{
		{ID: 1, OrganizationID: "x", Billed: new(true)}, {ID: 2, OrganizationID: "x", Billed: new(false)}, {ID: 3, OrganizationID: "x"},
		{ID: 4, OrganizationID: "y", Billed: new(true)}, {ID: 5, OrganizationID: "y", Billed: new(false)}, {ID: 6, OrganizationID: "y"},
	}

	s.Equal([]int64{1}, s.ids(OwnedBy("x", rows)))
	s.Equal([]int64{2}, s.ids(OwnedBy(AppOwner("x"), rows)))
	s.Equal([]int64{1, 3}, s.ids(HeldTo("x", rows)))
	s.Equal([]int64{2, 3}, s.ids(HeldTo(AppOwner("x"), rows)))
}

func (s *LimitOwnerUnitSuite) TestARowOfNobodyKnownHoldsBothOwners() {
	s.Equal([]string{"x", "\x00app\x00x"}, Holders(Limit{OrganizationID: "x"}))
	s.Equal([]string{"x"}, Holders(Limit{OrganizationID: "x", Billed: new(true)}))
	s.Equal([]string{"\x00app\x00x"}, Holders(LimitGrant{OrganizationID: "x", Billed: new(false)}))
}
