//go:build integration

package store

import (
	"time"
)

// answering is a live config of the customer naming the channel types given, made at.
func (s *StoreSuite) answering(customerID string, at time.Time, types ...string) string {
	config := &AgentConfig{CustomerID: customerID, Name: "agent-" + newID()}
	for _, named := range types {
		config.Channels.Chat = append(config.Channels.Chat, ChatChannel{Type: named})
	}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET created_at = ? WHERE id = ?", at, config.ID)
	s.Require().NoError(err)
	return config.ID
}

// The oldest of the customer's live configs naming the type answers in it: not a newer one,
// another type's, another customer's, a deleted one or a test copy.
func (s *StoreSuite) TestTheOldestConfigNamingAChannelTypeAnswersInIt() {
	customer, other := "acme-"+newID(), "other-"+newID()
	now := time.Now().UTC()
	deleted := s.answering(customer, now.Add(-4*time.Hour), "callouts")
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, customer, deleted))
	copied := s.answering(customer, now.Add(-3*time.Hour), "callouts")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET tags = ?::jsonb WHERE id = ?", `{"draft_of": "x"}`, copied)
	s.Require().NoError(err)
	s.answering(other, now.Add(-2*time.Hour), "callouts")
	s.answering(customer, now.Add(-90*time.Minute), "shifts")
	oldest := s.answering(customer, now.Add(-time.Hour), "shifts", "callouts")
	s.answering(customer, now, "callouts")

	config, err := s.store.AgentConfigs.GetByChatChannel(s.ctx, customer, "callouts")

	s.Require().NoError(err)
	s.Equal(oldest, config.ID)
	s.Equal([]ChatChannel{{Type: "shifts"}, {Type: "callouts"}}, config.Channels.Chat)
}

// A type is matched whole: one no config names, or that only begins or ends like one, has none.
func (s *StoreSuite) TestAChannelTypeNoConfigNamesHasNone() {
	customer := "acme-" + newID()
	s.answering(customer, time.Now().UTC(), "callouts")
	s.answering(customer, time.Now().UTC())

	for _, asked := range []string{"call", "callouts2", "Callouts", "agent"} {
		_, err := s.store.AgentConfigs.GetByChatChannel(s.ctx, customer, asked)
		s.ErrorIs(err, ErrNoAgentConfig, asked)
	}
}
