package core

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

// CustomConnectorSuite holds CustomManifest to the limits the API's request schema documents,
// each at the limit and one past it. A length is counted in characters, so a name of é, two
// bytes each, is at the limit with as many é as the limit.
type CustomConnectorSuite struct {
	suite.Suite
}

func TestCustomConnectorSuite(t *testing.T) {
	suite.Run(t, new(CustomConnectorSuite))
}

// manifest is CustomManifest's error for a bearer connector custom_acme changed by edit.
func (s *CustomConnectorSuite) manifest(edit func(*CustomConnector)) error {
	connector := CustomConnector{ID: "custom_acme", Name: "Acme", Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"bearer"}}
	edit(&connector)
	_, err := CustomManifest(connector)
	return err
}

func (s *CustomConnectorSuite) TestAnIDIsCustomThenALetterThenUpTo56More() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.ID = "custom_a" + strings.Repeat("b", 56) }), "64 in all")
	s.NoError(s.manifest(func(c *CustomConnector) { c.ID = "custom_a_9" }))
	for _, id := range []string{"custom_a" + strings.Repeat("b", 57), "custom_9abc", "custom_", "custom_Abc", "custom_a-b", "acme", "xcustom_acme"} {
		s.ErrorContains(s.manifest(func(c *CustomConnector) { c.ID = id }), "is not custom_ then a lowercase letter", id)
	}
}

func (s *CustomConnectorSuite) TestANameIsAtMost120Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Name = strings.Repeat("é", 120) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Name = strings.Repeat("é", 121) }), "name is longer than 120 characters")
}

func (s *CustomConnectorSuite) TestACategoryIsAtMost80Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Category = strings.Repeat("é", 80) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Category = strings.Repeat("é", 81) }), "category is longer than 80 characters")
}

func (s *CustomConnectorSuite) TestADescriptionIsAtMost1000Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Description = strings.Repeat("é", 1000) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Description = strings.Repeat("é", 1001) }),
		"description is longer than 1000 characters")
}

func (s *CustomConnectorSuite) TestAnEndpointIsAtMost2048Characters() {
	base := "https://mcp.acme.example/"
	s.NoError(s.manifest(func(c *CustomConnector) { c.Endpoint = base + strings.Repeat("a", 2048-len(base)) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Endpoint = base + strings.Repeat("a", 2049-len(base)) }),
		"endpoint is longer than 2048 characters")
}

func (s *CustomConnectorSuite) TestAConnectorAsksForAtMost100Scopes() {
	scopes := func(n int) []string {
		list := make([]string, n)
		for i := range list {
			list[i] = fmt.Sprintf("scope%d", i)
		}
		return list
	}
	s.NoError(s.manifest(func(c *CustomConnector) { c.Scopes = scopes(100) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = scopes(101) }), "scopes holds more than 100")
}

// RFC 6749 section 3.3: scope-token = 1*( %x21 / %x23-5B / %x5D-7E ).
func (s *CustomConnectorSuite) TestAScopeIsAnRFC6749ScopeToken() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Scopes = []string{"!", "#", "[", "]", "~", "repo:read", "https://acme.example/read"} }))
	for _, scope := range []string{"", "a b", `a"b`, `a\b`, "é", "a\x00", "a\tb"} {
		s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = []string{"read", scope} }), "scopes[1] is not an RFC 6749 scope token",
			"%q", scope)
	}
}

func (s *CustomConnectorSuite) TestAScopeListedTwiceIsRefused() {
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = []string{"read", "write", "read"} }), `scopes[2] "read" is listed twice`)
}

// An imported manifest reaches the limits through AsCustom.
func (s *CustomConnectorSuite) TestAManifestPastALimitIsNoCustomOne() {
	manifest, err := CustomManifest(CustomConnector{ID: "custom_acme", Name: "Acme", Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"bearer"}})
	s.Require().NoError(err)
	_, err = manifest.AsCustom("custom_acme")
	s.Require().NoError(err)

	manifest.Name = strings.Repeat("a", 121)
	_, err = manifest.AsCustom("custom_acme")

	s.ErrorContains(err, "name is longer than 120 characters")
}
