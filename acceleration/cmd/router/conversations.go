package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

const conversationsUsage = `usage: router conversations <command> [flags]

  backfill-owner-kinds  record the owner's kind on the channels of conversations this shard's
                        sessions held that were made before channels recorded it, where every
                        session of the owner's id on it was of one verified kind, and it is the
                        kind that id's next token says (a guest's is left alone otherwise). Says
                        what it would do unless --write. Run it once every router records the kind.
    --customer          only this customer's conversations
    --write             record the kinds
`

// runConversations looks after the conversations kept in Stream Chat.
func runConversations(args []string, settings config.Config, logger *slog.Logger) error {
	if len(args) == 0 {
		return errors.New(conversationsUsage)
	}
	ctx := context.Background()
	switch args[0] {
	case "backfill-owner-kinds":
		return runBackfillOwnerKinds(ctx, args[1:], settings, logger, os.Stdout)
	}
	return fmt.Errorf("unknown conversations command %q\n\n%s", args[0], conversationsUsage)
}

func runBackfillOwnerKinds(ctx context.Context, args []string, settings config.Config, logger *slog.Logger, out io.Writer) error {
	flags := flag.NewFlagSet("conversations backfill-owner-kinds", flag.ContinueOnError)
	customer := flags.String("customer", "", "only this customer's conversations")
	write := flags.Bool("write", false, "record the kinds")
	if err := flags.Parse(args); err != nil {
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

	counted, err := service.BackfillOwnerKinds(ctx, pgStore, mode, *customer, *write)
	for _, outcome := range slices.Sorted(maps.Keys(counted)) {
		fmt.Fprintf(out, "%s: %d\n", outcome, counted[outcome])
	}
	if !*write {
		fmt.Fprintln(out, "nothing was written: --write records the kinds")
	}
	return err
}
