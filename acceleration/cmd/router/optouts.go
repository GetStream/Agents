package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const optOutsUsage = `usage: router opt-outs <command>

  normalize   rewrite every opt-out whose recipient is a phone number not in E.164 into E.164,
              so it matches the number dialled or texted: 12025550123 becomes +12025550123.
              A number is rewritten only when exactly one reading of it is a valid number,
              as written with a + or as an international number without one; a national
              number such as (202) 555-0123 names no country, so it is kept as written and
              logged by id. An opt-out left with a live one of the same recipient and channel
              already in E.164, or with one the recipient lifted after it was recorded, is
              revoked, its row kept. A recipient that is not a number, such as an iMessage
              email handle, is kept and counted. Run it once every router writes E.164, and
              again after a data move or replicate brings in opt-outs; running it again
              changes nothing. Writes the shared database
`

// optOutsBatch is how many opt-outs one transaction of normalize rewrites.
const optOutsBatch = 1000

// runOptOuts looks after the opt-outs the shared database keeps.
func runOptOuts(args []string, settings config.Config, logger *slog.Logger) error {
	if len(args) == 0 {
		return errors.New(optOutsUsage)
	}
	switch args[0] {
	case "normalize":
		return runNormalizeOptOuts(context.Background(), settings, logger, os.Stdout, optOutsBatch)
	}
	return fmt.Errorf("unknown opt-outs command %q\n\n%s", args[0], optOutsUsage)
}

func runNormalizeOptOuts(ctx context.Context, settings config.Config, logger *slog.Logger, out io.Writer, batch int) error {
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	defer pgStore.Close()
	done, err := pgStore.OptOuts.Normalize(ctx, batch, func(unread []store.OptOut) {
		for _, optOut := range unread {
			logger.Warn("an opt-out's recipient looks like a phone number but has no one valid reading, and was kept as it is",
				"opt_out", optOut.ID, "customer", optOut.CustomerID, "channel", optOut.Channel)
		}
	})
	fmt.Fprintf(out, "%d opt-outs rewritten into E.164, %d revoked; %d name a handle, not a number, and were kept\n",
		done.Rewritten, done.Revoked, done.Handles)
	return err
}
