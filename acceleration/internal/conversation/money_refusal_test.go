package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
)

// MoneyRefusalSuite covers a reply's refusal for money read back by a conversation opened again
// (moneyRefusalField, commandsIn).
type MoneyRefusalSuite struct {
	suite.Suite
	chat   *chatStore
	client *getstream.Stream
}

func TestMoneyRefusalSuite(t *testing.T) { suite.Run(t, new(MoneyRefusalSuite)) }

// failedThenReopened has begin start a reply in a conversation, fails it on failure, and returns
// the conversation another process opens once the first stopped with nothing kept in memory,
// and the reply's message id.
func (s *MoneyRefusalSuite) failedThenReopened(begin func(c *Conversation) error, failure error) (*Conversation, string) {
	db, client := newChat(s.T())
	s.chat, s.client = db, client
	first := NewForChat(client)
	c, _, _, err := first.Open(context.Background(), "customer", "support-agent", "")
	s.Require().NoError(err)
	s.Require().NoError(begin(c))
	c.Observe(agent.Error{Err: failure})
	saved(s.T(), c)
	id := current(c).ID
	first.Close()

	return s.reopened(c.CID()), id
}

// reopened is the conversation cid as another process opens it, with nothing kept in memory.
func (s *MoneyRefusalSuite) reopened(cid string) *Conversation {
	restarted := NewForChat(s.client)
	s.T().Cleanup(restarted.Close)
	reopened, _, _, err := restarted.Open(context.Background(), "customer", "support-agent", cid)
	s.Require().NoError(err)
	return reopened
}

// forged writes into the channel, as author, a copy of the stored reply id under a new message
// id, as a member with a token of their own could: its state set to state and, when refusal is
// not nil, its refusal field set to refusal, the field being left out otherwise.
func (s *MoneyRefusalSuite) forged(id, author, state string, refusal any) {
	s.chat.mu.Lock()
	defer s.chat.mu.Unlock()
	var copied map[string]any
	raw, err := json.Marshal(s.chat.messages[id])
	s.Require().NoError(err)
	s.Require().NoError(json.Unmarshal(raw, &copied))
	copied["id"] = "forged-" + id
	copied["user"] = map[string]any{"id": author}
	custom, _ := copied["custom"].(map[string]any)
	custom["support_runtime"].(map[string]any)["state"] = state
	custom["support_message"].(map[string]any)["state"] = state
	delete(custom, moneyRefusalField)
	if refusal != nil {
		custom[moneyRefusalField] = refusal
	}
	s.chat.messages[copied["id"].(string)] = copied
	s.chat.order = append(s.chat.order, copied["id"].(string))
}

// followUp begins the follow-up a scheduled delivery puts to a conversation.
func followUp(c *Conversation) error {
	_, err := c.BeginFollowUp(ReadBackPrefix+"run-a", "Remind me to stretch", "")
	return err
}

// A follow-up's reply refused for money reads back after a reopen as a refusal of the same kind,
// checked or not, as the attempt that ran it was told: a scheduled delivery retried after a
// drain, a restart or the unwatched grace ends under the money, final or retried as it would
// have been, rather than as a reply that failed or one delivered.
func (s *MoneyRefusalSuite) TestARefusalForMoneyIsReadBackAfterAReopen() {
	for _, tc := range []struct {
		name      string
		failure   error
		kind      error
		code      string
		transient bool
	}{
		{"suspended", fmt.Errorf("the turn was refused: %w", policy.ErrSuspended), policy.ErrSuspended, "account_suspended", false},
		{"no credit", fmt.Errorf("the turn was refused: %w", policy.ErrNoCredits), policy.ErrNoCredits, "insufficient_credits", false},
		{"balance unread", policy.Refusal("insufficient_credits", true), policy.ErrNoCredits, "insufficient_credits", true},
		{"top-up", policy.ErrTopUpRequired, policy.ErrTopUpRequired, "top_up_required", false},
		{"hard limit", policy.ErrSystemLimit, policy.ErrSystemLimit, "system_limit_reached", false},
		{"spend limit", policy.ErrBudgetSpent, policy.ErrBudgetSpent, "spend_limit_reached", false},
		{"user limit", policy.ErrUserLimit, policy.ErrUserLimit, "user_limit_reached", false},
		{"spend unshared", policy.Refusal("spend_check_unavailable", true), policy.ErrSpendCheckUnavailable, "spend_check_unavailable", true},
	} {
		s.Run(tc.name, func() {
			reopened, _ := s.failedThenReopened(followUp, tc.failure)

			state, _, known := reopened.FollowUpReply(ReadBackPrefix + "run-a")
			s.Require().True(known)
			s.Equal("failed", state)
			got := reopened.FollowUpFailure(ReadBackPrefix + "run-a")
			s.Require().ErrorIs(got, tc.kind)
			s.Equal(tc.code, policy.Code(got))
			s.Equal(tc.transient, policy.Transient(got), "retried only when the money could not be checked")
		})
	}
}

// A person's command whose reply was refused for money reads the refusal back after a reopen too,
// as the latest reply under its id says.
func (s *MoneyRefusalSuite) TestACommandsRefusalForMoneyIsReadBackAfterAReopen() {
	reopened, _ := s.failedThenReopened(func(c *Conversation) error {
		_, err := c.BeginCommand("command-a", "How long until my shift?", "")
		return err
	}, fmt.Errorf("the turn was refused: %w", policy.ErrBudgetSpent))

	got := reopened.FollowUpFailure("command-a")
	s.Require().ErrorIs(got, policy.ErrBudgetSpent)
	s.False(policy.Transient(got))
}

// A reply that failed for a reason that is not money keeps nothing on its message, and reads back
// with no failure after a reopen, as before this was kept: only a refusal for money decides how a
// delivery ends.
func (s *MoneyRefusalSuite) TestAFailureNotAboutMoneyIsNotKept() {
	reopened, id := s.failedThenReopened(followUp, errors.New("the model is unavailable"))

	state, _, _ := reopened.FollowUpReply(ReadBackPrefix + "run-a")
	s.Equal("failed", state)
	s.NoError(reopened.FollowUpFailure(ReadBackPrefix + "run-a"))
	s.chat.mu.Lock()
	defer s.chat.mu.Unlock()
	s.NotContains(s.chat.messages[id]["custom"], moneyRefusalField)
}

// A shape the router never writes reads as no refusal, so a reply never fails a delivery under a
// code no refusal gave.
func (s *MoneyRefusalSuite) TestAShapeTheRouterNeverWritesReadsAsNoRefusal() {
	for name, kept := range map[string]any{
		"none":               nil,
		"empty":              map[string]any{},
		"a bare code":        "insufficient_credits",
		"no flag":            map[string]any{"code": "insufficient_credits"},
		"a flag of text":     map[string]any{"code": "insufficient_credits", "unchecked": "true"},
		"a code of a number": map[string]any{"code": 7, "unchecked": false},
		"an unknown code":    map[string]any{"code": "prompt_injection", "unchecked": false},
		"an empty code":      map[string]any{"code": "", "unchecked": false},
	} {
		s.Run(name, func() {
			s.NoError(moneyRefusalOf(kept))
		})
	}
}

// What the router writes is read back as the refusal it was, checked or not.
func (s *MoneyRefusalSuite) TestAShapeTheRouterWritesReadsAsItsRefusal() {
	for _, tc := range []struct {
		name      string
		kept      any
		kind      error
		transient bool
	}{
		{"checked", map[string]any{"code": "user_limit_reached", "unchecked": false}, policy.ErrUserLimit, false},
		{"unchecked", map[string]any{"code": "insufficient_credits", "unchecked": true}, policy.ErrNoCredits, true},
	} {
		s.Run(tc.name, func() {
			got := moneyRefusalOf(tc.kept)

			s.Require().ErrorIs(got, tc.kind)
			s.Equal(tc.transient, policy.Transient(got))
		})
	}
}

// A member of the channel who writes a copy of a failed reply with the refusal field set does not
// make it a refusal: only a reply the agent wrote is read (readsAsWritten), so a reply that failed
// for another reason is not failed under a code its own run never had.
func (s *MoneyRefusalSuite) TestAMembersForgedRefusalIsNotRead() {
	first, id := s.failedThenReopened(followUp, errors.New("the model is unavailable"))
	s.forged(id, "mallory", "failed", map[string]any{"code": "insufficient_credits", "unchecked": false})

	reopened := s.reopened(first.CID())

	state, _, known := reopened.FollowUpReply(ReadBackPrefix + "run-a")
	s.Require().True(known)
	s.Equal("failed", state)
	s.NoError(reopened.FollowUpFailure(ReadBackPrefix + "run-a"))
}

// A member who writes a copy of a refused reply as completed, with no refusal field, does not
// erase the refusal or complete the reply: the run still ends under the money.
func (s *MoneyRefusalSuite) TestAMembersCopyOfARefusedReplyDoesNotEraseTheRefusal() {
	first, id := s.failedThenReopened(followUp, fmt.Errorf("the turn was refused: %w", policy.ErrNoCredits))
	s.forged(id, "mallory", "completed", nil)

	reopened := s.reopened(first.CID())

	state, _, known := reopened.FollowUpReply(ReadBackPrefix + "run-a")
	s.Require().True(known)
	s.Equal("failed", state)
	s.ErrorIs(reopened.FollowUpFailure(ReadBackPrefix+"run-a"), policy.ErrNoCredits)
}
