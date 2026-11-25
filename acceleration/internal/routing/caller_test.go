package routing

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CallerSuite struct {
	suite.Suite
}

func TestCallerSuite(t *testing.T) {
	suite.Run(t, new(CallerSuite))
}

func (s *CallerSuite) TestAnIPv4AddressIsCountedAsItself() {
	s.Equal("192.0.2.1", Caller{IP: "192.0.2.1"}.CountedAddress())
}

func (s *CallerSuite) TestAnIPv6AddressIsCountedAsItsNetwork() {
	s.Equal("2001:db8:0:1::/64", Caller{IP: "2001:db8:0:1:ffff:ffff:ffff:ffff"}.CountedAddress())
	s.Equal("2001:db8:0:1::/64", Caller{IP: "2001:db8:0:1::"}.CountedAddress())
	s.Equal("2001:db8:0:2::/64", Caller{IP: "2001:db8:0:2::1"}.CountedAddress())
}

func (s *CallerSuite) TestAnIPv4MappedAddressIsCountedAsTheIPv4Address() {
	s.Equal("192.0.2.1", Caller{IP: "::ffff:192.0.2.1"}.CountedAddress())
	s.Equal("192.0.2.2", Caller{IP: "::ffff:c000:202"}.CountedAddress())
	s.Equal("192.0.2.1", Caller{IP: "::ffff:192.0.2.1%eth0"}.CountedAddress())
}

func (s *CallerSuite) TestTheZoneOfAnAddressIsNotCounted() {
	s.Equal("fe80::/64", Caller{IP: "fe80::1%eth0"}.CountedAddress())
	s.Equal("fe80::/64", Caller{IP: "fe80::1%eth1"}.CountedAddress())
}

func (s *CallerSuite) TestAnAddressThatCannotBeReadIsCountedAsItIsWritten() {
	s.Equal("unreadable", Caller{IP: "unreadable"}.CountedAddress())
	s.Empty(Caller{}.CountedAddress())
}
