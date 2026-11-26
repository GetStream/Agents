package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

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

		backfillOwnerKindsOptions{write: row.write}.summarize(&out, row.result)

		s.Equal(row.want, out.String(), row.name)
	}
}

func (s *BackfillOwnerKindsCommandSuite) TestTheWalkFollowsTheFlags() {
	for _, row := range []struct {
		name string
		args []string
		want conversation.OwnerKindRun
	}{
		{"a dry run", nil, conversation.OwnerKindRun{Mode: auth.APIKey}},
		{"a write", []string{"--customer", "acme", "--write"}, conversation.OwnerKindRun{Mode: auth.APIKey, Customer: "acme", Write: true,
			After: store.HeldConversation{CustomerID: "acme"}}},
		{"a customer to start after, in another mode", []string{"--customer", "acme", "--after-customer", "globex", "--after-conversation", "agent:a"},
			conversation.OwnerKindRun{Mode: auth.Proxy, Customer: "acme", After: store.HeldConversation{CustomerID: "globex", ConversationID: "agent:a"}}},
	} {
		options, err := parseBackfillOwnerKinds(row.args)
		s.Require().NoError(err, row.name)
		var out bytes.Buffer

		got := options.run(row.want.Mode, &out)

		s.Require().NotNil(got.Line, row.name)
		got.Line(conversation.OwnerKindLine{CustomerID: "acme", ConversationID: "agent:a", Kind: auth.KindGuest, Outcome: conversation.OwnerKindStamped, Written: got.Write})
		wantLine := "acme\tagent:a\tguest\twould stamp\n"
		if row.want.Write {
			wantLine = "acme\tagent:a\tguest\tstamped\n"
		}
		s.Equal(wantLine, out.String(), row.name)
		got.Line = nil
		s.Equal(row.want, got, row.name)
	}
}

// TestSignalContextHelper is the child TestASecondSignalEndsTheCommand runs: it takes a signal,
// sees its context done, and keeps signalling itself until a second one kills it.
func TestSignalContextHelper(t *testing.T) {
	if os.Getenv("SIGNAL_CONTEXT_HELPER") == "" {
		t.Skip("run by TestASecondSignalEndsTheCommand")
	}
	ctx, stop := signalContext(syscall.SIGTERM)
	defer stop()
	self := syscall.Getpid()
	if err := syscall.Kill(self, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if err := syscall.Kill(self, syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
	}
	os.Stdout.WriteString("survived\n")
}

func (s *BackfillOwnerKindsCommandSuite) TestASecondSignalEndsTheCommand() {
	command := exec.Command(os.Args[0], "-test.run=^TestSignalContextHelper$")
	command.Env = append(os.Environ(), "SIGNAL_CONTEXT_HELPER=1")

	out, err := command.CombinedOutput()

	var exited *exec.ExitError
	s.Require().ErrorAs(err, &exited, string(out))
	status, ok := exited.Sys().(syscall.WaitStatus)
	s.Require().True(ok)
	s.True(status.Signaled(), string(out))
	s.Equal(syscall.SIGTERM, status.Signal())
}

func (s *BackfillOwnerKindsCommandSuite) TestTheHelpSaysWhereToResume() {
	s.Contains(conversationsUsage, "as the resume line names it")
	s.NotContains(conversationsUsage, "last line printed or")
}
