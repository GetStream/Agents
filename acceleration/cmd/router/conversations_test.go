package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// BackfillOwnerKindsCommandSuite is what backfill-owner-kinds takes and prints, which needs no
// database.
type BackfillOwnerKindsCommandSuite struct {
	suite.Suite
}

func TestBackfillOwnerKindsCommandSuite(t *testing.T) {
	suite.Run(t, new(BackfillOwnerKindsCommandSuite))
}

func (s *BackfillOwnerKindsCommandSuite) TestTheFlagsSayWhereToStartAndWhetherToWrite() {
	for _, row := range []struct {
		name string
		args []string
		want backfillOwnerKindsOptions
	}{
		{"nothing", nil, backfillOwnerKindsOptions{}},
		{"a customer", []string{"--customer", "acme", "--write"},
			backfillOwnerKindsOptions{customer: "acme", after: store.HeldConversation{CustomerID: "acme"}, write: true}},
		{"after a conversation of the customer", []string{"--customer", "acme", "--after-conversation", "agent:a"},
			backfillOwnerKindsOptions{customer: "acme", after: store.HeldConversation{CustomerID: "acme", ConversationID: "agent:a"}}},
		{"after a conversation of another customer", []string{"--customer", "acme", "--after-customer", "globex", "--after-conversation", "agent:a"},
			backfillOwnerKindsOptions{customer: "acme", after: store.HeldConversation{CustomerID: "globex", ConversationID: "agent:a"}}},
		{"after one with no customer named", []string{"--after-customer", "globex", "--after-conversation", "agent:a"},
			backfillOwnerKindsOptions{after: store.HeldConversation{CustomerID: "globex", ConversationID: "agent:a"}}},
	} {
		got, err := parseBackfillOwnerKinds(row.args)

		s.Require().NoError(err, row.name)
		s.Equal(row.want, got, row.name)
	}
}

func (s *BackfillOwnerKindsCommandSuite) TestAFlagItDoesNotKnowIsRefused() {
	_, err := parseBackfillOwnerKinds([]string{"--everyone"})

	s.Error(err)
}

func (s *BackfillOwnerKindsCommandSuite) TestWritingNeedsACustomerAndADryRunDoesNot() {
	for _, args := range [][]string{
		{"--write"},
		{"--write", "--after-customer", "globex", "--after-conversation", "agent:a"},
	} {
		_, err := parseBackfillOwnerKinds(args)

		s.ErrorContains(err, "--customer", args)
	}
	_, err := parseBackfillOwnerKinds(nil)
	s.NoError(err)
}

func (s *BackfillOwnerKindsCommandSuite) TestWritingWithNoCustomerStopsBeforeAnyStoreIsOpened() {
	err := runBackfillOwnerKinds(context.Background(), []string{"--write"}, config.Config{}, slog.New(slog.DiscardHandler), io.Discard)

	s.ErrorContains(err, "--customer")
}

func (s *BackfillOwnerKindsCommandSuite) TestAConversationToStartAfterNeedsACustomer() {
	_, err := parseBackfillOwnerKinds([]string{"--after-conversation", "agent:a"})

	s.ErrorContains(err, "--after-customer")
}

func (s *BackfillOwnerKindsCommandSuite) TestEachChannelIsALineOfCustomerConversationKindAndAction() {
	var out bytes.Buffer

	writeBackfillLine(&out, conversation.OwnerKindLine{CustomerID: "acme", ConversationID: "agent:a", Kind: auth.KindGuest,
		Outcome: conversation.OwnerKindStamped})
	writeBackfillLine(&out, conversation.OwnerKindLine{CustomerID: "acme", ConversationID: "agent:b",
		Outcome: conversation.OwnerKindFailed, Err: errors.New("boom api_key=SECRET")})

	s.Equal("acme\tagent:a\tguest\twould stamp\nacme\tagent:b\t-\tfailed:error\n", out.String())
}

func (s *BackfillOwnerKindsCommandSuite) TestTheSummaryIsTheCountsThenWhereToResumeWhenTheWalkStopped() {
	done := store.HeldConversation{CustomerID: "acme", ConversationID: "agent:a"}
	counted := map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 2, conversation.OwnerKindFailed: 1,
		conversation.OwnerKindLeftAlone: 3, conversation.OwnerKindNoOwner: 4}
	for _, row := range []struct {
		name   string
		result conversation.OwnerKindResult
		write  bool
		want   string
	}{
		{"a dry run that finished", conversation.OwnerKindResult{Counted: counted, Complete: true}, false,
			"failed: 1\nleft alone: 3\nno owner: 4\nstamped: 2\nnothing was written: --write records the kinds\n"},
		{"a write that finished", conversation.OwnerKindResult{Counted: counted, Complete: true}, true,
			"failed: 1\nleft alone: 3\nno owner: 4\nstamped: 2\n"},
		{"a write that stopped", conversation.OwnerKindResult{Counted: counted, Done: done}, true,
			"failed: 1\nleft alone: 3\nno owner: 4\nstamped: 2\nstopped: resume with --after-customer \"acme\" --after-conversation \"agent:a\"\n"},
	} {
		var out bytes.Buffer

		writeBackfillSummary(&out, row.result, row.write)

		s.Equal(row.want, out.String(), row.name)
	}
}
