//go:build integration

package store

import (
	"errors"
	"strings"
	"time"
)

// boundConfig stores a config of the customer's carrying bindings.
func (s *StoreSuite) boundConfig(customerID string, bindings ...ConnectorBinding) AgentConfig {
	config := &AgentConfig{CustomerID: customerID, Name: "agent-" + newID(), Connectors: bindings}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	return *config
}

func fixed(connectionID string) ConnectorBinding {
	return ConnectorBinding{
		Name: "crm", ConnectorID: "acme",
		Connection: ConnectionBinding{Type: "fixed", ConnectionID: connectionID},
		Tools:      []ToolGrant{{Name: "search", SchemaDigest: strings.Repeat("a", 64)}},
		Required:   true, TimeoutMs: 5000,
	}
}

func (s *StoreSuite) TestAConfigsBindingsAreReadBackAsTheyWereStored() {
	created := s.boundConfig("acme-app", fixed(s.connection("acme-app", nil).ID),
		ConnectorBinding{Name: "inbox", ConnectorID: "acme", Connection: ConnectionBinding{Type: "session"}, Tools: []ToolGrant{}})

	read, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", created.ID)
	s.Require().NoError(err)
	s.Equal(created.Connectors, read.Connectors)
}

func (s *StoreSuite) TestAConfigCannotBindAConnectionThatIsNotLive() {
	deleted := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", deleted.ID))
	others := s.connection("other-app", nil)

	for _, id := range []string{deleted.ID, others.ID, newID()} {
		err := s.store.AgentConfigs.Insert(s.ctx, &AgentConfig{CustomerID: "acme-app", Name: "bound",
			Connectors: []ConnectorBinding{fixed(id)}})
		s.ErrorIs(err, ErrNoConnectorConnection, "deleted, another customer's, never made")
	}
	configs, err := s.store.AgentConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Empty(configs)
}

func (s *StoreSuite) TestAnUpdateCannotNewlyBindADeletedConnection() {
	deleted := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", deleted.ID))
	config := s.boundConfig("acme-app")

	config.Connectors = []ConnectorBinding{fixed(deleted.ID)}
	s.ErrorIs(s.store.AgentConfigs.Update(s.ctx, &config), ErrNoConnectorConnection)

	read, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	s.Empty(read.Connectors)
}

// A forced delete leaves the binding behind on purpose, and saving the config for anything
// else keeps it rather than failing until it is removed.
func (s *StoreSuite) TestAnUpdateKeepsABindingAForcedDeleteLeftBehind() {
	connection := s.connection("acme-app", nil)
	config := s.boundConfig("acme-app", fixed(connection.ID))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	config.Instructions = "be brief"
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, &config))

	read, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	s.Equal("be brief", read.Instructions)
	s.Equal([]ConnectorBinding{fixed(connection.ID)}, read.Connectors)
}

func (s *StoreSuite) TestAConfigWithoutBindingsStoresAnEmptyList() {
	created := s.boundConfig("acme-app")

	var stored string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT connectors::text FROM agent_configs WHERE id = ?", created.ID).Scan(&stored))
	s.Equal("[]", stored)
}

func (s *StoreSuite) TestUpdatingAConfigReplacesItsBindings() {
	config := s.boundConfig("acme-app", fixed(s.connection("acme-app", nil).ID))
	replacement := s.connection("acme-app", nil)

	config.Connectors = []ConnectorBinding{fixed(replacement.ID)}
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, &config))

	read, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	s.Require().Len(read.Connectors, 1)
	s.Equal(replacement.ID, read.Connectors[0].Connection.ConnectionID)
}

// The containment query in ConnectorConnectionReferenced reads the JSON this model writes,
// so a change to its tags shows here rather than as a connection deleted out from under an
// agent.
func (s *StoreSuite) TestAFixedBindingAConfigStoresReferencesItsConnection() {
	connection := s.connection("acme-app", nil)
	s.boundConfig("acme-app", fixed(connection.ID))

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.True(referenced)
}

// secondNode is another router's store on the same database and Redis, for what one node
// writes and the other has cached.
func (s *StoreSuite) secondNode() *Store {
	if s.store.cache == nil {
		s.T().Skip("needs redis")
	}
	other, err := OpenShards(Options{DSN: s.dsn, Redis: s.redis()})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(other.Close()) })
	return other
}

func (s *StoreSuite) TestAnAgentConfigIsOnlyItsOwnCustomers() {
	mine := s.boundConfig("acme-app")
	s.boundConfig("other-app")

	_, err := s.store.AgentConfigs.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoAgentConfig)
	_, err = s.store.AgentConfigs.GetByName(s.ctx, "other-app", mine.Name)
	s.ErrorIs(err, ErrNoAgentConfig)
	listed, err := s.store.AgentConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)

	stolen := mine
	stolen.CustomerID = "other-app"
	s.ErrorIs(s.store.AgentConfigs.Update(s.ctx, &stolen), ErrNoAgentConfig)
	s.ErrorIs(s.store.AgentConfigs.Delete(s.ctx, "other-app", mine.ID), ErrNoAgentConfig)
	_, err = s.store.AgentConfigs.Get(s.ctx, "acme-app", mine.ID)
	s.NoError(err)
}

func (s *StoreSuite) TestAnInvalidAgentConfigIsNotWritten() {
	err := s.store.AgentConfigs.Insert(s.ctx, &AgentConfig{CustomerID: "acme-app", Name: "support", Mode: "video"})
	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("mode", invalid.Field)

	listed, err := s.store.AgentConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Empty(listed)

	stored := s.boundConfig("acme-app")
	stored.Region = "Not A Region"
	s.Require().ErrorAs(s.store.AgentConfigs.Update(s.ctx, &stored), &invalid)
	s.Equal("region", invalid.Field)
	read, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", stored.ID)
	s.Require().NoError(err)
	s.NotEqual("Not A Region", read.Region)
}

func (s *StoreSuite) TestARenamedAgentConfigIsNoLongerFoundUnderItsOldNameOnAnotherNode() {
	other := s.secondNode()
	config := AgentConfig{CustomerID: "acme-app", Name: "support", Mode: AgentModeText}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &config))
	_, err := other.AgentConfigs.GetByName(s.ctx, "acme-app", "sales")
	s.ErrorIs(err, ErrNoAgentConfig, "a name nobody has yet is cached as absent")

	found, err := other.AgentConfigs.GetByName(s.ctx, "acme-app", "support")
	s.Require().NoError(err)
	s.Equal(config.ID, found.ID)

	config.Name = "sales"
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, &config))

	s.Eventually(func() bool {
		_, err := other.AgentConfigs.GetByName(s.ctx, "acme-app", "support")
		return errors.Is(err, ErrNoAgentConfig)
	}, 5*time.Second, 10*time.Millisecond, "the other node still answers to the old name")
	renamed, err := other.AgentConfigs.GetByName(s.ctx, "acme-app", "sales")
	s.Require().NoError(err)
	s.Equal(config.ID, renamed.ID)
	byID, err := other.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	s.Equal("sales", byID.Name)
}

func (s *StoreSuite) TestADeletedAgentConfigIsForgottenByIdAndName() {
	config := s.boundConfig("acme-app")
	_, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	_, err = s.store.AgentConfigs.GetByName(s.ctx, "acme-app", config.Name)
	s.Require().NoError(err)

	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", config.ID))

	_, err = s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.ErrorIs(err, ErrNoAgentConfig)
	_, err = s.store.AgentConfigs.GetByName(s.ctx, "acme-app", config.Name)
	s.ErrorIs(err, ErrNoAgentConfig)
}

// Every name asked for is a key of its own, and no write deletes one nobody creates.
func (s *StoreSuite) TestANameNoConfigHoldsIsCachedForAMinuteOnly() {
	config := s.boundConfig("acme-app")
	_, err := s.store.AgentConfigs.GetByName(s.ctx, "acme-app", config.Name)
	s.Require().NoError(err)
	missing := "nobody-" + newID()
	_, err = s.store.AgentConfigs.GetByName(s.ctx, "acme-app", missing)
	s.Require().ErrorIs(err, ErrNoAgentConfig)

	s.Positive(s.expiryOf(agentConfigNameKey("acme-app", missing)), "the miss is cached")
	s.LessOrEqual(s.expiryOf(agentConfigNameKey("acme-app", missing)), time.Minute)
	s.Greater(s.expiryOf(agentConfigNameKey("acme-app", config.Name)), 59*time.Minute, "a config found is still cached for the hour")
}

// expiryOf is how long a key has left in Redis.
func (s *StoreSuite) expiryOf(key string) time.Duration {
	redis := s.store.sharedRedis()
	ttl, err := redis.Do(s.ctx, redis.B().Pttl().Key(key).Build()).AsInt64()
	s.Require().NoError(err)
	return time.Duration(ttl) * time.Millisecond
}

func (s *StoreSuite) TestAgentConfigQueryBudgets() {
	for range 20 {
		s.boundConfig("acme-app")
	}
	config := AgentConfig{CustomerID: "acme-app", Name: "moderator", Role: RoleAgentModerator,
		Channels: AgentAnswersIn{Chat: []ChatChannel{{Type: "callouts"}}}, Agents: []ChildAgent{{Config: "child"}}}

	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &config)) }),
		"begin, the insert and commit: a config that binds nothing locks no connection")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.AgentConfigs.Select(s.ctx, "acme-app")
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.AgentConfigs.GetByRole(s.ctx, "acme-app", RoleAgentModerator)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.AgentConfigs.GetByChatChannel(s.ctx, "acme-app", "callouts")
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		parent, err := s.store.AgentConfigs.NameCalling(s.ctx, "acme-app", "child", "")
		s.Require().NoError(err)
		s.Equal("moderator", parent)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.AgentConfigs.GetUnscoped(s.ctx, config.ID)
		s.Require().NoError(err)
	}))
	config.Instructions = "be brief"
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, &config)) }),
		"begin, the update and commit: a config that binds nothing locks no connection")
	if s.store.cache != nil {
		for range 2 {
			_, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
			s.Require().NoError(err)
		}
		s.Equal(0, s.countQueries(func() {
			s.Equal(0, s.countRedis(func() {
				_, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
				s.Require().NoError(err)
			}), "the second read is answered from the local cache")
		}))
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, &config)) }),
			"forgetting the id and both names is one round trip")
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", config.ID)) }))
		return
	}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", config.ID)) }))
}
