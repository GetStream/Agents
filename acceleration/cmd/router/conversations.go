package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

const conversationsUsage = `usage: router conversations <command> [flags]

  backfill-owner-kinds  record the owner's kind on the channels of conversations this shard's
                        sessions held that were made before channels recorded it, where every
                        session of the owner's id on it was of one verified kind, and it is the
                        kind that id's next token says (a guest's is left alone otherwise). Says
                        what it would do unless --write. Run it a day after every router
                        records the kind, once no guest holds a token an older router refreshed
                        to no role. Prints one line per channel, dry run included: the customer,
                        the conversation, the kind and what was done (would stamp, stamped,
                        skipped:<why> or failed:<why>), tab separated; then the counts, and where
                        to resume when it stops early. Waits out Stream's rate limit.
                        Leave out a customer moved from a proxy deployment: every end user was
                        recorded authenticated there, so a guest whose app signs the role would be
                        stamped authenticated and refused its conversation. Without --customer it
                        walks every customer, those included: name the customers, one run each.
    --customer          only this customer's conversations
    --after-customer    start after this customer's conversation (see --after-conversation)
    --after-conversation  start after this conversation, as the last line printed or the resume
                        line names it; --customer stands for --after-customer when it is given
    --write             record the kinds
`

// runConversations looks after the conversations kept in Stream Chat.
func runConversations(args []string, settings config.Config, logger *slog.Logger) error {
	if len(args) == 0 {
		return errors.New(conversationsUsage)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "backfill-owner-kinds":
		return runBackfillOwnerKinds(ctx, args[1:], settings, logger, os.Stdout)
	}
	return fmt.Errorf("unknown conversations command %q\n\n%s", args[0], conversationsUsage)
}

func runBackfillOwnerKinds(ctx context.Context, args []string, settings config.Config, logger *slog.Logger, out io.Writer) error {
	options, err := parseBackfillOwnerKinds(args)
	if err != nil {
		return err
	}
	mode, err := auth.ParseMode(settings.Auth.Mode)
	if err != nil {
		return err
	}
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	defer pgStore.Close()
	secrets, err := newSecretSealer(ctx, settings)
	if err != nil {
		return err
	}
	clients, err := newStreamClients(settings, pgStore, secrets, logger)
	if err != nil {
		return err
	}
	// A conversation with no pin is kept in the deployment's own app, which app mode has to
	// know before it can tell that app's from a customer's. A deployment with no app of its own
	// keeps none there, as replicate takes it.
	if settings.Stream.Tenancy == config.TenancyApp {
		learning, cancel := context.WithTimeout(ctx, learnTimeout)
		defer cancel()
		if _, err := clients.LearnDeploymentApp(learning); err != nil && !errors.Is(err, streamapp.ErrNoIdentity) {
			return fmt.Errorf("the deployment's own Stream app id could not be read: %w", err)
		}
	}
	service := conversation.NewForChats(conversation.StreamApps(clients))
	defer service.Close()
	service.SetPins(pgStore.Sessions.ConversationPin)

	result, err := service.BackfillOwnerKinds(ctx, pgStore, conversation.OwnerKindRun{
		Mode: mode, Customer: options.customer, After: options.after, Write: options.write,
		Line: func(line conversation.OwnerKindLine) { writeBackfillLine(out, line) },
	})
	writeBackfillSummary(out, result, options.write)
	return err
}

// backfillOwnerKindsOptions is what backfill-owner-kinds was asked to do.
type backfillOwnerKindsOptions struct {
	customer string
	after    store.HeldConversation
	write    bool
}

func parseBackfillOwnerKinds(args []string) (backfillOwnerKindsOptions, error) {
	flags := flag.NewFlagSet("conversations backfill-owner-kinds", flag.ContinueOnError)
	customer := flags.String("customer", "", "only this customer's conversations")
	afterCustomer := flags.String("after-customer", "", "start after this customer's conversation")
	afterConversation := flags.String("after-conversation", "", "start after this conversation")
	write := flags.Bool("write", false, "record the kinds")
	if err := flags.Parse(args); err != nil {
		return backfillOwnerKindsOptions{}, err
	}
	after := store.HeldConversation{CustomerID: cmp.Or(*afterCustomer, *customer), ConversationID: *afterConversation}
	if after.ConversationID != "" && after.CustomerID == "" {
		return backfillOwnerKindsOptions{}, errors.New("--after-conversation needs --after-customer or --customer")
	}
	return backfillOwnerKindsOptions{customer: *customer, after: after, write: *write}, nil
}

// writeBackfillLine prints a channel as tab separated customer, conversation, kind and action.
func writeBackfillLine(out io.Writer, line conversation.OwnerKindLine) {
	fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", line.CustomerID, line.ConversationID, cmp.Or(string(line.Kind), "-"), line.Action())
}

// writeBackfillSummary prints the counts, and where to resume when the walk stopped early.
func writeBackfillSummary(out io.Writer, result conversation.OwnerKindResult, write bool) {
	for _, outcome := range slices.Sorted(maps.Keys(result.Counted)) {
		fmt.Fprintf(out, "%s: %d\n", outcome, result.Counted[outcome])
	}
	if !write {
		fmt.Fprintln(out, "nothing was written: --write records the kinds")
	}
	if !result.Complete {
		fmt.Fprintf(out, "stopped: resume with --after-customer %q --after-conversation %q\n",
			result.Done.CustomerID, result.Done.ConversationID)
	}
}
