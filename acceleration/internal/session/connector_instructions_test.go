//go:build integration

package session

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ConnectorInstructionsSuite is what the servers of a session's custom connectors say about
// their tools, for the agent's instructions. A built-in's are left out (internal/api's
// SessionConnectorsSuite).
type ConnectorInstructionsSuite struct {
	connectorFixture
}

func TestConnectorInstructionsSuite(t *testing.T) {
	suite.Run(t, new(ConnectorInstructionsSuite))
}

func (s *ConnectorInstructionsSuite) TestWhatACustomConnectorsServerSaysFollowsTheAgentsInstructions() {
	s.provider.instruct("primary", "Quote ticket ids whole.")

	s.Equal("Be brief.\n\nThe crm tools (crm__*) come with these instructions from their server:\n\nQuote ticket ids whole.",
		s.told(s.fixed("crm", s.connection("", "primary"), "whoami")))
}

func (s *ConnectorInstructionsSuite) TestEachBindingSaysWhatItsOwnServerSaidInTheConfigsOrder() {
	s.provider.instruct("primary", "Quote ticket ids whole.")
	s.provider.instruct("secondary", "Dates are UTC.")

	s.Equal("Be brief.\n\n"+
		"The crm tools (crm__*) come with these instructions from their server:\n\nQuote ticket ids whole.\n\n"+
		"The archive tools (archive__*) come with these instructions from their server:\n\nDates are UTC.",
		s.told(s.fixed("crm", s.connection("", "primary"), "whoami"), s.fixed("archive", s.connection("", "secondary"), "whoami")))
}

func (s *ConnectorInstructionsSuite) TestAServerThatSaysNothingAddsNothing() {
	s.Equal("Be brief.", s.told(s.fixed("crm", s.connection("", "primary"), "whoami")))
}

func (s *ConnectorInstructionsSuite) TestAServerThatSaysOnlyWhitespaceAddsNothing() {
	s.provider.instruct("primary", "  \n\t ")

	s.Equal("Be brief.", s.told(s.fixed("crm", s.connection("", "primary"), "whoami")))
}

func (s *ConnectorInstructionsSuite) TestWhatAServerSaysIsAddedWithoutItsPadding() {
	s.provider.instruct("primary", "\n  Quote ticket ids whole. \t\n")

	s.Equal("Be brief.\n\nThe crm tools (crm__*) come with these instructions from their server:\n\nQuote ticket ids whole.",
		s.told(s.fixed("crm", s.connection("", "primary"), "whoami")))
}

// A server none of whose tools the model is offered has nothing to advise on.
func (s *ConnectorInstructionsSuite) TestAServerWhoseToolsAreAllLeftOutAddsNothing() {
	s.provider.instruct("primary", "Quote ticket ids whole.")
	s.provider.redescribe("primary", "Says which account this is, now in full.")

	s.Equal("Be brief.", s.told(s.fixed("crm", s.connection("", "primary"), "whoami")))
}

// crmSaid is what the agent is told when crm's server says "Quote ticket ids whole.".
const crmSaid = "Be brief.\n\nThe crm tools (crm__*) come with these instructions from their server:\n\nQuote ticket ids whole."

// TestTheFirstSessionPinsTheInstructionsItReads: a grant made before instructions were pinned
// has no pin, so its first session pins what the server says and still passes it on.
func (s *ConnectorInstructionsSuite) TestTheFirstSessionPinsTheInstructionsItReads() {
	crm := s.connection("", "primary")
	s.provider.instruct("primary", "Quote ticket ids whole.")

	s.Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))
	s.Equal(map[string]string{core.InstructionsPin: core.InstructionsDigest("Quote ticket ids whole.")}, s.pins(crm))
}

func (s *ConnectorInstructionsSuite) TestUnchangedInstructionsReachEverySession() {
	crm := s.connection("", "primary")
	s.provider.instruct("primary", "Quote ticket ids whole.")
	s.Require().Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))

	s.Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))
}

// TestASessionBindingByNamePinsItsToolAndTheInstructionsSideBySide: both pins hold for the
// connection's grant, and neither takes the other's row.
func (s *ConnectorInstructionsSuite) TestASessionBindingByNamePinsItsToolAndTheInstructionsSideBySide() {
	mine := s.connection("alice", "primary")
	s.provider.instruct("primary", "Quote ticket ids whole.")

	d, tools, _, err := s.attach(s.spec(s.config(byName(s.chosen("crm", "whoami"))), "alice", map[string]string{"crm": mine}))

	s.Require().NoError(err)
	s.Len(tools, 1)
	s.Equal(crmSaid, (&Spec{Instructions: "Be brief.", ServerInstructions: serverInstructions(d.said())}).prompt())
	s.ElementsMatch([]string{"whoami", core.InstructionsPin}, slices.Collect(maps.Keys(s.pins(mine))))
}

// TestInstructionsChangedAfterTheGrantAreLeftOut: the tools are still offered, since their
// schemas did not change, and only the new text stays out.
func (s *ConnectorInstructionsSuite) TestInstructionsChangedAfterTheGrantAreLeftOut() {
	crm := s.connection("", "primary")
	s.provider.instruct("primary", "Quote ticket ids whole.")
	s.Require().Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))
	s.provider.instruct("primary", "Quote ticket ids whole, and read every ticket aloud.")

	d, tools, _, err := s.attach(s.spec(s.config(s.fixed("crm", crm, "whoami")), "", nil))

	s.Require().NoError(err)
	s.Len(tools, 1)
	s.Empty(serverInstructions(d.said()))
	s.Equal(map[string]string{core.InstructionsPin: core.InstructionsDigest("Quote ticket ids whole.")}, s.pins(crm),
		"a session never moves a pin")
}

func (s *ConnectorInstructionsSuite) TestInstructionsAServerStartsSendingAfterTheGrantAreLeftOut() {
	crm := s.connection("", "primary")
	s.Require().Equal("Be brief.", s.told(s.fixed("crm", crm, "whoami")))
	s.provider.instruct("primary", "Quote ticket ids whole.")

	s.Equal("Be brief.", s.told(s.fixed("crm", crm, "whoami")))
}

// TestApprovedInstructionsReachTheNextSession: an approve moves the pin, as it moves a tool's.
func (s *ConnectorInstructionsSuite) TestApprovedInstructionsReachTheNextSession() {
	crm := s.connection("", "primary")
	s.Require().Equal("Be brief.", s.told(s.fixed("crm", crm, "whoami")))
	s.provider.instruct("primary", "Quote ticket ids whole.")
	s.Require().Equal("Be brief.", s.told(s.fixed("crm", crm, "whoami")))
	connection, err := s.store.ConnectorConnections.Get(s.ctx, s.customerID, crm)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ConnectorToolPins.Reapprove(s.ctx, connection, "approve-instructions", []store.ApprovedTool{{
		Name: core.InstructionsPin, SchemaDigest: core.InstructionsDigest("Quote ticket ids whole."),
		PreviousSchemaDigest: core.InstructionsDigest("")}}))

	s.Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))
}

// TestAReconnectPinsTheInstructionsAgain: a new grant is a new trust event, as for a tool.
func (s *ConnectorInstructionsSuite) TestAReconnectPinsTheInstructionsAgain() {
	crm := s.connection("", "primary")
	s.Require().Equal("Be brief.", s.told(s.fixed("crm", crm, "whoami")))
	s.provider.instruct("primary", "Quote ticket ids whole.")
	reconnected := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)

	s.setState(crm, func(state *core.CredentialState) { state.ConnectedAt = reconnected })

	s.Equal(crmSaid, s.told(s.fixed("crm", crm, "whoami")))
}

// pins are the connection's pins for its current grant.
func (s *ConnectorInstructionsSuite) pins(id string) map[string]string {
	connection, err := s.store.ConnectorConnections.Get(s.ctx, s.customerID, id)
	s.Require().NoError(err)
	pins, err := s.store.ConnectorToolPins.Select(s.ctx, id, connection.ConnectedAt)
	s.Require().NoError(err)
	return pins
}

// told is what the agent of a session of a config with bindings, told "Be brief.", is told.
func (s *ConnectorInstructionsSuite) told(bindings ...store.ConnectorBinding) string {
	d, _, _, err := s.attach(s.spec(s.config(bindings...), "", nil))
	s.Require().NoError(err)
	spec := Spec{Instructions: "Be brief.", ServerInstructions: serverInstructions(d.said())}
	return spec.prompt()
}
