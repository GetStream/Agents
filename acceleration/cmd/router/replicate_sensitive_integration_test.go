//go:build integration

package main

import (
	"encoding/json"
	"os"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/providers"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// A page of changes is weighed against the sensitive-connector rule as it leaves the agents,
// in its own transaction: one that breaks it is refused as a whole, as a page naming another
// app's row is, whichever of the parent and the child changed first.
func (s *ReplicateRepeatSuite) TestAPageWhereTheParentCallsASenderIsRefusedAsAWhole() {
	s.refusesThePage(true)
}

func (s *ReplicateRepeatSuite) TestAPageWhereACalledChildStartsSendingIsRefusedAsAWhole() {
	s.refusesThePage(false)
}

// refusesThePage follows a page in which a parent binding nessy starts calling a child and the
// child starts sending data out, the parent's change first when parentFirst says.
func (s *ReplicateRepeatSuite) refusesThePage(parentFirst bool) {
	parent, child := s.copiedParentAndChild(nil)
	calls := func() { s.changed(parent, func(config *store.AgentConfig) { config.Agents = calling(child) }) }
	sends := func() {
		s.changed(child, func(config *store.AgentConfig) { config.Connectors = []store.ConnectorBinding{writer()} })
	}
	if parentFirst {
		calls()
		sends()
	} else {
		sends()
		calls()
	}

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `connector binding "customers" binds "nessy", whose tools read private data`)
	s.Empty(s.copied(parent).Agents, "nothing of the page is written")
	s.Empty(s.copied(child).Connectors, "nothing of the page is written")
}

// The page is weighed once it is all written, so one that leaves the agents within the rule is
// applied though a change in it broke the rule until the next.
func (s *ReplicateRepeatSuite) TestAPageThatEndsWithinTheSensitiveRuleIsApplied() {
	parent, child := s.copiedParentAndChild(func(parent, child *store.AgentConfig) { parent.Agents = calling(*child) })
	s.changed(child, func(config *store.AgentConfig) { config.Connectors = []store.ConnectorBinding{writer()} })
	s.changed(parent, func(config *store.AgentConfig) { config.Agents = nil })

	s.Require().NoError(s.once("acme", false))

	s.Empty(s.copied(parent).Agents)
	s.Len(s.copied(child).Connectors, 1)
}

// A connector defined earlier in the page is weighed as the page defines it.
func (s *ReplicateRepeatSuite) TestAPageIsWeighedWithTheConnectorsItDefines() {
	parent, child := s.copiedParentAndChild(nil)
	s.definedCRM()
	// A config the page changes within the rule comes first, so each config the page changes is weighed.
	s.changed(child, func(config *store.AgentConfig) { config.Instructions = "be briefer" })
	s.changed(parent, func(config *store.AgentConfig) {
		config.Connectors = append(config.Connectors, crm())
	})

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `and connector binding "crm" can send data out`)
	s.Len(s.copied(parent).Connectors, 1, "nothing of the page is written")
}

// A first copy writes the agents before the connectors they bind (the export's order), and a
// connector it writes is weighed with the agents binding it.
func (s *ReplicateRepeatSuite) TestAFirstCopyIsWeighedWithTheConnectorsItDefinesAfterTheAgents() {
	parent, _ := s.parentAndChild(func(parent, _ *store.AgentConfig) {
		s.definedCRM()
		parent.Connectors = append(parent.Connectors, crm())
	})

	err := s.once("", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `store: import connector_definitions:{"id":"custom_crm","revision":1}: the agent "parent" binds it: `+
		`connector binding "customers" binds "nessy", whose tools read private data, and connector binding "crm" can send data out`)
	s.notDefined("custom_crm")
	s.Len(s.copied(parent).Connectors, 2, "the agent was written before the connector was refused")
}

// A page that defines a connector an agent copied by an earlier page binds is weighed with it.
func (s *ReplicateRepeatSuite) TestAPageDefiningAConnectorAnEarlierPageBoundIsRefused() {
	parent, _ := s.copiedParentAndChild(nil)
	s.boundUndefinedCRM(parent)
	s.Require().NoError(s.once("acme", false))
	s.Require().Len(s.copied(parent).Connectors, 2)
	s.definedCRM()

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `store: import connector_definitions:{"id":"custom_crm","revision":1}: the agent "parent" binds it: `+
		`connector binding "customers" binds "nessy", whose tools read private data, and connector binding "crm" can send data out`)
	s.notDefined("custom_crm")
}

// A page that binds a connector and then defines it is weighed as it leaves them.
func (s *ReplicateRepeatSuite) TestAPageBindingAConnectorBeforeDefiningItIsRefused() {
	parent, _ := s.copiedParentAndChild(nil)
	s.boundUndefinedCRM(parent)
	s.definedCRM()

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `connector binding "customers" binds "nessy", whose tools read private data, and connector binding "crm" can send data out`)
	s.Len(s.copied(parent).Connectors, 1, "nothing of the page is written")
	s.notDefined("custom_crm")
}

// definedCRM defines acme's custom connector custom_crm at the source.
func (s *ReplicateRepeatSuite) definedCRM() {
	manifest, err := core.ParseManifest([]byte(`
id: custom_crm
revision: 1
name: CRM
endpoints:
  mcp: https://crm.example/mcp
schemes: [bearer]
sources:
  - kind: mcp
    endpoint: mcp
`))
	s.Require().NoError(err)
	_, err = s.source.ConnectorDefinitions.Revise(s.ctx, "acme", manifest)
	s.Require().NoError(err)
}

// boundUndefinedCRM binds custom_crm to the parent at the source while acme has not defined it,
// as a source's SQL can and the store's writes refuse (store.refuseUndefined).
func (s *ReplicateRepeatSuite) boundUndefinedCRM(parent store.AgentConfig) {
	binding, err := json.Marshal([]store.ConnectorBinding{crm()})
	s.Require().NoError(err)
	_, err = s.source.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET connectors = connectors || ?::jsonb WHERE id = ?",
		string(binding), parent.ID)
	s.Require().NoError(err)
}

// notDefined says the target holds no definition of acme's connector id.
func (s *ReplicateRepeatSuite) notDefined(id string) {
	_, err := s.target.ConnectorDefinitions.Latest(s.ctx, "acme", id)
	s.ErrorIs(err, store.ErrNoConnectorDefinition)
}

// copiedParentAndChild makes the target a deployment opened as the command opens one
// (openStore), where acme sees nessy, and copies to it two text agents of acme's from the source:
// a parent binding nessy and a child binding nothing, as edit leaves them.
func (s *ReplicateRepeatSuite) copiedParentAndChild(edit func(parent, child *store.AgentConfig)) (store.AgentConfig, store.AgentConfig) {
	parent, child := s.parentAndChild(edit)
	s.Require().NoError(s.once("", false))
	return parent, child
}

// parentAndChild is copiedParentAndChild before the copy.
func (s *ReplicateRepeatSuite) parentAndChild(edit func(parent, child *store.AgentConfig)) (store.AgentConfig, store.AgentConfig) {
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(os.Getenv(dsnEnvVar), "repeat_target")
	target, err := openStore(s.ctx, settings)
	s.Require().NoError(err)
	s.T().Cleanup(func() { target.Close() })
	s.target = target
	organization := store.Organization{Name: "acme"}
	s.Require().NoError(target.Organizations.Insert(s.ctx, &organization))
	s.Require().NoError(target.Apps.Insert(s.ctx, &store.App{ID: "acme", OrganizationID: organization.ID, Name: "acme",
		Settings: store.AppSettings{InternalConnectors: new(true)}}))
	s.Require().NoError(s.source.ConnectorDefinitions.Seed(s.ctx, providers.FS))

	parent := store.AgentConfig{CustomerID: "acme", Name: "parent", Mode: store.AgentModeText, Instructions: "be brief",
		Connectors: []store.ConnectorBinding{{Name: "customers", ConnectorID: "nessy", Connection: store.ConnectionBinding{Type: "session"},
			Tools: []store.ToolGrant{{Name: "org_lookup"}}}}}
	child := store.AgentConfig{CustomerID: "acme", Name: "child", Mode: store.AgentModeText, Instructions: "be brief"}
	if edit != nil {
		edit(&parent, &child)
	}
	s.Require().NoError(s.source.AgentConfigs.Insert(s.ctx, &child))
	s.Require().NoError(s.source.AgentConfigs.Insert(s.ctx, &parent))
	return parent, child
}

// changed edits an agent config of acme's at the source.
func (s *ReplicateRepeatSuite) changed(config store.AgentConfig, edit func(*store.AgentConfig)) {
	stored, err := s.source.AgentConfigs.Get(s.ctx, "acme", config.ID)
	s.Require().NoError(err)
	edit(&stored)
	s.Require().NoError(s.source.AgentConfigs.Update(s.ctx, &stored))
}

// copied is an agent config of acme's as the target holds it.
func (s *ReplicateRepeatSuite) copied(config store.AgentConfig) store.AgentConfig {
	held, err := s.target.AgentConfigs.Get(s.ctx, "acme", config.ID)
	s.Require().NoError(err)
	return held
}

// calling is the agents of a parent that hands work to child.
func calling(child store.AgentConfig) []store.ChildAgent {
	return []store.ChildAgent{{Config: child.Name, Tool: "ask_child"}}
}

// writer is a binding granting a GitHub tool that may send data out.
func writer() store.ConnectorBinding {
	return store.ConnectorBinding{Name: "code", ConnectorID: "github", Connection: store.ConnectionBinding{Type: "session"},
		Tools: []store.ToolGrant{{Name: "create_issue"}}}
}

// crm is a binding granting a custom_crm tool, which a custom connector's source does not say
// only reads.
func crm() store.ConnectorBinding {
	return store.ConnectorBinding{Name: "crm", ConnectorID: "custom_crm", Connection: store.ConnectionBinding{Type: "session"},
		Tools: []store.ToolGrant{{Name: "create_contact"}}}
}
