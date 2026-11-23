//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// heldNumber is a number customerID holds, attached to a trunk, with no agent yet.
func (s *StoreSuite) heldNumber(customerID string) string {
	e164 := fmt.Sprintf("+1512%07d", rand.IntN(10_000_000))
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: e164, Vendor: "twilio", Country: "US", CustomerID: customerID, PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, customerID, e164, NumberAttachment{TrunkID: "trunk-1", CallType: "default"}))
	return e164
}

// liveAgent is a config of customerID.
func (s *StoreSuite) liveAgent(customerID string) *AgentConfig {
	config := &AgentConfig{CustomerID: customerID, Name: "answering-" + newID()}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	return config
}

func (s *StoreSuite) TestAConfigANumberAnswersWithIsNotDeleted() {
	customerID := newID()
	config := s.liveAgent(customerID)
	e164 := s.heldNumber(customerID)
	s.Require().NoError(s.store.PhoneNumbers.SetAgent(s.ctx, customerID, e164, config.ID))

	err := s.store.AgentConfigs.Delete(s.ctx, customerID, config.ID)
	answering, ok := errors.AsType[*ConfigAnswersNumbersError](err)
	s.Require().True(ok, "%v", err)
	s.Equal([]string{e164}, answering.Numbers)
	_, err = s.store.AgentConfigs.Get(s.ctx, customerID, config.ID)
	s.NoError(err, "the config is still there")
}

func (s *StoreSuite) TestANumberIsNotGivenAConfigDeletedSinceItWasRead() {
	customerID := newID()
	config := s.liveAgent(customerID)
	e164 := s.heldNumber(customerID)
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, customerID, config.ID))

	err := s.store.PhoneNumbers.SetAgent(s.ctx, customerID, e164, config.ID)
	s.ErrorIs(err, ErrNoAgentConfig)
	number, err := s.store.PhoneNumbers.GetByE164(s.ctx, customerID, e164)
	s.Require().NoError(err)
	s.Empty(number.ConfigID)
}

// Attaching again keeps the agent a number had, unless its config was deleted meanwhile.
func (s *StoreSuite) TestANumberAttachedAgainDropsAnAgentDeletedMeanwhile() {
	customerID := newID()
	config := s.liveAgent(customerID)
	e164 := s.heldNumber(customerID)
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, customerID, config.ID))

	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, customerID, e164,
		NumberAttachment{TrunkID: "trunk-2", CallType: "default", ConfigID: config.ID}))
	number, err := s.store.PhoneNumbers.GetByE164(s.ctx, customerID, e164)
	s.Require().NoError(err)
	s.Empty(number.ConfigID)

	kept := s.liveAgent(customerID)
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, customerID, e164,
		NumberAttachment{TrunkID: "trunk-3", CallType: "default", ConfigID: kept.ID}))
	number, err = s.store.PhoneNumbers.GetByE164(s.ctx, customerID, e164)
	s.Require().NoError(err)
	s.Equal(kept.ID, number.ConfigID, "a live agent is kept")
}

// A delete that starts while a number is being given the config waits for it, and then sees
// the number rather than leaving it answering with a deleted agent.
func (s *StoreSuite) TestADeleteWaitsForANumberBeingGivenTheConfig() {
	customerID := newID()
	config := s.liveAgent(customerID)
	e164 := s.heldNumber(customerID)

	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	var live []string
	s.Require().NoError(liveConfig(tx, customerID, config.ID).Scan(s.ctx, &live))
	s.Require().Len(live, 1)
	_, err = tx.NewUpdate().Model((*PhoneNumber)(nil)).Set("config_id = ?", config.ID).
		Where("customer_id = ?", customerID).Where("e164 = ?", e164).Exec(s.ctx)
	s.Require().NoError(err)

	deleted := make(chan error, 1)
	go func() { deleted <- s.store.AgentConfigs.Delete(context.WithoutCancel(s.ctx), customerID, config.ID) }()
	select {
	case err := <-deleted:
		s.Require().NoError(tx.Rollback())
		s.FailNowf("the delete did not wait for the number", "it returned %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	s.Require().NoError(tx.Commit())

	err = <-deleted
	_, ok := errors.AsType[*ConfigAnswersNumbersError](err)
	s.True(ok, "%v", err)
}
