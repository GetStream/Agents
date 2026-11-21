//go:build integration

package store

import "errors"

func (s *StoreSuite) TestTheAgentOnANumberIsTheOneThatNamedIt() {
	customerID := newID()
	e164 := "+1555" + newID()[:7]
	config := &AgentConfig{
		CustomerID: customerID, Name: "channelled-" + newID(),
		Channels: AgentAnswersIn{Lines: AgentChannels{WhatsApp: &ChannelLine{Number: e164}, Identity: ChannelIdentityPhone}},
	}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))

	found, ok, err := s.store.ConfigOnChannel(s.ctx, customerID, "whatsapp", e164)

	s.Require().NoError(err)
	s.Require().True(ok)
	s.Equal(config.ID, found.ID)
}

func (s *StoreSuite) TestNoAgentIsFoundOnANumberNobodyNamed() {
	_, ok, err := s.store.ConfigOnChannel(s.ctx, newID(), "whatsapp", "+15550000000")

	s.Require().NoError(err)
	s.False(ok)
}

// account gives a connection the account id a consent would.
func (s *StoreSuite) account(id, accountID string) {
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET account_id = ? WHERE id = ?", accountID, id)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestTheAppConnectionOfAnAccountIsFound() {
	bot := s.connection("acme-app", nil)
	s.account(bot.ID, "T0000TEAM")
	user := s.connection("acme-app", userOwned("alice"))
	s.account(user.ID, "T0000TEAM")

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(bot.ID, found.ID, "a user's connection is never the app's")
}

func (s *StoreSuite) TestAnotherCustomersAppConnectionOfTheSameAccountIsNotFound() {
	bot := s.connection("other-app", nil)
	s.account(bot.ID, "T0000TEAM")

	_, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestADeletedAppConnectionIsNotFound() {
	bot := s.connection("acme-app", nil)
	s.account(bot.ID, "T0000TEAM")
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", bot.ID))

	_, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.True(errors.Is(err, ErrNoConnectorConnection))
}

func (s *StoreSuite) TestTheConfigsThatBindAConnectionAreFound() {
	bot := s.connection("acme-app", nil)
	first := s.bind("acme-app", fixedBinding(bot.ID))
	second := s.bind("acme-app", fixedBinding(bot.ID))
	s.bind("acme-app", fixedBinding("another-connection"))
	s.bind("other-app", fixedBinding(bot.ID))

	configs, err := s.store.AgentConfigsBindingConnection(s.ctx, "acme-app", bot.ID)

	s.Require().NoError(err)
	s.Require().Len(configs, 2)
	s.Equal(first, configs[0].ID)
	s.Equal(second, configs[1].ID)
}

// AI-1049, AI-1048: a test copy (DraftOfTag) answers no channel message and subscribes to no
// event, so it is not among the configs that bind a connection.
func (s *StoreSuite) TestATestCopyIsNotAmongTheConfigsThatBindAConnection() {
	bot := s.connection("acme-app", nil)
	live := s.bind("acme-app", fixedBinding(bot.ID))
	copied := s.bind("acme-app", fixedBinding(bot.ID))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET tags = ?::jsonb WHERE id = ?", `{"draft_of": "`+live+`"}`, copied)
	s.Require().NoError(err)

	configs, err := s.store.AgentConfigsBindingConnection(s.ctx, "acme-app", bot.ID)

	s.Require().NoError(err)
	s.Require().Len(configs, 1)
	s.Equal(live, configs[0].ID)
}
