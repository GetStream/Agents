package conversation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
)

// MoneyRefusalSuite covers a reply's refusal for money read back by a conversation opened again
// (moneyRefusalField, commandsIn).
type MoneyRefusalSuite struct {
	suite.Suite
	chat *chatStore
}

func TestMoneyRefusalSuite(t *testing.T) { suite.Run(t, new(MoneyRefusalSuite)) }

// failedThenReopened has begin start a reply in a conversation, fails it on failure, and returns
// the conversation another process opens once the first stopped with nothing kept in memory,
// and the reply's message id.
func (s *MoneyRefusalSuite) failedThenReopened(begin func(c *Conversation) error, failure error) (*Conversation, string) {
	db, client := newChat(s.T())
	s.chat = db
	first := NewForChat(client)
	c, _, _, err := first.Open(context.Background(), "customer", "support-agent", "")
	s.Require().NoError(err)
	s.Require().NoError(begin(c))
	c.Observe(agent.Error{Err: failure})
	saved(s.T(), c)
	id := current(c).ID
	first.Close()

	restarted := NewForChat(client)
	s.T().Cleanup(restarted.Close)
	reopened, _, _, err := restarted.Open(context.Background(), "customer", "support-agent", c.CID())
	s.Require().NoError(err)
	return reopened, id
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

// What the field holds is read back as a refusal only when it is one the router writes: any other
// shape, type or code reads as no refusal, so a reply never fails a delivery under a code no
// refusal gave.
func (s *MoneyRefusalSuite) TestOnlyAKeptRefusalReadsAsOne() {
	for _, tc := range []struct {
		name      string
		kept      any
		kind      error
		transient bool
	}{
		{"none", nil, nil, false},
		{"empty", map[string]any{}, nil, false},
		{"a bare code", "insufficient_credits", nil, false},
		{"no flag", map[string]any{"code": "insufficient_credits"}, nil, false},
		{"a flag of text", map[string]any{"code": "insufficient_credits", "unchecked": "true"}, nil, false},
		{"a code of a number", map[string]any{"code": 7, "unchecked": false}, nil, false},
		{"an unknown code", map[string]any{"code": "prompt_injection", "unchecked": false}, nil, false},
		{"an empty code", map[string]any{"code": "", "unchecked": false}, nil, false},
		{"checked", map[string]any{"code": "user_limit_reached", "unchecked": false}, policy.ErrUserLimit, false},
		{"unchecked", map[string]any{"code": "insufficient_credits", "unchecked": true}, policy.ErrNoCredits, true},
	} {
		s.Run(tc.name, func() {
			got := moneyRefusalOf(tc.kept)

			if tc.kind == nil {
				s.NoError(got)
				return
			}
			s.Require().ErrorIs(got, tc.kind)
			s.Equal(tc.transient, policy.Transient(got))
		})
	}
}
