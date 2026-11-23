//go:build integration

package store

import (
	"errors"
	"sync"
	"time"
)

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

// accountConnection is an app connection of the workspace T0000TEAM in a status, made at
// the base time plus age, so the order of two is not left to the clock.
func (s *StoreSuite) accountConnection(customerID, status string, age time.Duration) ConnectorConnection {
	connection := s.connection(customerID, nil)
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE connector_connections SET account_id = 'T0000TEAM', status = ?, created_at = ? WHERE id = ?",
		status, s.base.Add(age), connection.ID)
	s.Require().NoError(err)
	return connection
}

// AI-3503: a second connection to a workspace that no agent binds took every message from
// the bound first one.
func (s *StoreSuite) TestTheBoundAppConnectionOfAnAccountIsFoundBeforeANewerUnboundOne() {
	older := s.accountConnection("acme-app", ConnectionConnected, 0)
	s.bind("acme-app", fixedBinding(older.ID))
	s.accountConnection("acme-app", ConnectionConnected, time.Hour)

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(older.ID, found.ID)
}

// The brief's second case: the newer bound connection is also the connected one.
func (s *StoreSuite) TestTheConnectedOfTwoBoundAppConnectionsIsFoundWhenItIsTheNewer() {
	older := s.accountConnection("acme-app", ConnectionNeedsReauthorization, 0)
	s.bind("acme-app", fixedBinding(older.ID))
	newer := s.accountConnection("acme-app", ConnectionConnected, time.Hour)
	s.bind("acme-app", fixedBinding(newer.ID))

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(newer.ID, found.ID)
}

func (s *StoreSuite) TestTheConnectedOfTwoBoundAppConnectionsIsFoundWhenItIsTheOlder() {
	older := s.accountConnection("acme-app", ConnectionConnected, 0)
	s.bind("acme-app", fixedBinding(older.ID))
	newer := s.accountConnection("acme-app", ConnectionNeedsReauthorization, time.Hour)
	s.bind("acme-app", fixedBinding(newer.ID))

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(older.ID, found.ID)
}

// A bound connection that needs reauthorization still comes before a connected one nobody
// binds: only a bound connection is answered, so the unbound one would drop the message.
func (s *StoreSuite) TestABoundAppConnectionThatNeedsReauthorizationIsFoundBeforeAnUnboundConnectedOne() {
	older := s.accountConnection("acme-app", ConnectionNeedsReauthorization, 0)
	s.bind("acme-app", fixedBinding(older.ID))
	s.accountConnection("acme-app", ConnectionConnected, time.Hour)

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(older.ID, found.ID)
}

// With no binding at all the newest is found, as on base, and the caller drops the message.
func (s *StoreSuite) TestTheNewestOfTwoUnboundAppConnectionsIsFound() {
	s.accountConnection("acme-app", ConnectionConnected, 0)
	newer := s.accountConnection("acme-app", ConnectionConnected, time.Hour)

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(newer.ID, found.ID)
}

// Only what AgentConfigsBindingConnection counts as a binding makes a connection bound: a
// test copy, a deleted config and another customer's config do not.
func (s *StoreSuite) TestOnlyALiveConfigOfTheCustomerMakesAnAppConnectionBound() {
	for name, bindNewer := range map[string]func(connectionID string){
		"test copy": func(connectionID string) {
			copied := s.bind("acme-app", fixedBinding(connectionID))
			_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET tags = ?::jsonb WHERE id = ?", `{"draft_of": "x"}`, copied)
			s.Require().NoError(err)
		},
		"deleted config": func(connectionID string) {
			deleted := s.bind("acme-app", fixedBinding(connectionID))
			_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET deleted_at = now() WHERE id = ?", deleted)
			s.Require().NoError(err)
		},
		"another customer's config": func(connectionID string) {
			s.bind("other-app", fixedBinding(connectionID))
		},
	} {
		s.Run(name, func() {
			s.SetupTest()
			older := s.accountConnection("acme-app", ConnectionConnected, 0)
			s.bind("acme-app", fixedBinding(older.ID))
			newer := s.accountConnection("acme-app", ConnectionConnected, time.Hour)
			bindNewer(newer.ID)

			found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

			s.Require().NoError(err)
			s.Equal(older.ID, found.ID)
		})
	}
}

// Another customer's bound connection of the same workspace is never found for this one.
func (s *StoreSuite) TestAnotherCustomersBoundAppConnectionOfTheSameAccountIsNotFound() {
	own := s.accountConnection("acme-app", ConnectionConnected, 0)
	other := s.accountConnection("other-app", ConnectionConnected, time.Hour)
	s.bind("other-app", fixedBinding(other.ID))

	found, err := s.store.AppConnectionByAccount(s.ctx, "acme-app", "acme", "T0000TEAM")

	s.Require().NoError(err)
	s.Equal(own.ID, found.ID)
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

// smsAgent is a config of customerID answering on the SMS number e164.
func smsAgent(customerID, e164 string) *AgentConfig {
	return &AgentConfig{
		CustomerID: customerID, Name: "texted-" + newID(),
		Channels: AgentAnswersIn{Lines: AgentChannels{SMS: &ChannelLine{Number: e164}, Identity: ChannelIdentityPhone}},
	}
}

func (s *StoreSuite) TestASecondConfigNamingAnSMSNumberIsRefusedAndTheFirstStillSaves() {
	customerID, e164 := newID(), "+1555"+newID()[:7]
	first := smsAgent(customerID, e164)
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, first))

	err := s.store.AgentConfigs.Insert(s.ctx, smsAgent(customerID, e164))
	taken, ok := errors.AsType[*ChannelLineTakenError](err)
	s.Require().True(ok, "%v", err)
	s.Equal(first.ID, taken.OwnerID)
	s.Equal("sms", taken.Kind)

	first.Instructions = "Answer texts."
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, first), "the config on the line keeps it")
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, smsAgent(newID(), e164)), "another customer's number is theirs")
}

// Two configs saved at once each pass the API's check before the write: one of them is the
// line's agent, and the other is refused.
func (s *StoreSuite) TestTwoConfigsSavedAtOnceNamingOneSMSNumberLeaveOneOnIt() {
	customerID, e164 := newID(), "+1555"+newID()[:7]
	saved := make([]error, 2)
	var wg sync.WaitGroup
	for i := range saved {
		wg.Go(func() { saved[i] = s.store.AgentConfigs.Insert(s.ctx, smsAgent(customerID, e164)) })
	}
	wg.Wait()

	refused := 0
	for _, err := range saved {
		if _, ok := errors.AsType[*ChannelLineTakenError](err); ok {
			refused++
		} else {
			s.Require().NoError(err)
		}
	}
	s.Equal(1, refused)
	on, err := s.store.GlobalDB().NewSelect().Model((*AgentConfig)(nil)).
		Where("customer_id = ?", customerID).Where("deleted_at IS NULL").Count(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, on)
}
