package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

const phoneUsage = `usage: router phone <command> [flags]

  move-twilio   point the Twilio numbers attached before the router answered their calls itself at
                its call webhook (ROUTER_PUBLIC_URL + /v1/phone/hooks/twilio), as attaching one does
                now. Says what it would do, a line a number, unless --apply. A number already there
                is left alone, and so is one whose trunk credentials were not kept, which needs a
                re-attach. Refuses before asking Twilio anything without TWILIO_WEBHOOK_AUTH_TOKEN,
                an https ROUTER_PUBLIC_URL, the Twilio account or the keyring.
    --apply     point the numbers at the webhook
    --one       only this number, in E.164: move one first and call it
    --app       only the numbers made in this Stream app
`

// runPhone looks after the numbers the shared database holds.
func runPhone(args []string, settings config.Config, logger *slog.Logger) error {
	if len(args) == 0 {
		return errors.New(phoneUsage)
	}
	ctx := context.Background()
	switch args[0] {
	case "move-twilio":
		service, pgStore, err := numberMover(ctx, settings, logger)
		if err != nil {
			return err
		}
		defer pgStore.Close()
		return moveTwilioNumbers(ctx, args[1:], service, os.Stdout)
	}
	return fmt.Errorf("unknown phone command %q\n\n%s", args[0], phoneUsage)
}

// numberMover is the phone service with what moving a number needs: the database, the keyring
// the webhook opens trunk passwords with, the vendors and the public URL.
// The store is the caller's to close.
func numberMover(ctx context.Context, settings config.Config, logger *slog.Logger) (*phone.Service, *store.Store, error) {
	vendorConfig, err := phone.LoadConfig(settings.PhoneConfig)
	if err != nil {
		return nil, nil, err
	}
	secrets, err := newSecretSealer(ctx, settings)
	if err != nil {
		return nil, nil, err
	}
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return nil, nil, err
	}
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: vendors.Registry(vendorConfig), Store: pgStore, Sealer: secrets,
		PublicURL: settings.PublicURL, Logger: logger,
	})
	if err != nil {
		pgStore.Close()
		return nil, nil, err
	}
	return service, pgStore, nil
}

// moveTwilioNumbers runs router phone move-twilio on service, writing a line a number to out.
// A line names the number and its app only.
func moveTwilioNumbers(ctx context.Context, args []string, service *phone.Service, out io.Writer) error {
	flags := flag.NewFlagSet("phone move-twilio", flag.ContinueOnError)
	apply := flags.Bool("apply", false, "point the numbers at the webhook")
	one := flags.String("one", "", "only this number, in E.164")
	app := flags.String("app", "", "only the numbers made in this Stream app")
	if err := flags.Parse(args); err != nil {
		return err
	}
	move := phone.CallHookMove{Vendor: "twilio", E164: *one, Apply: *apply}
	if *one != "" && !store.IsE164(*one) {
		return fmt.Errorf("--one is a number in E.164, such as +15125551234, not %q", *one)
	}
	if *app != "" {
		pin, ok := streamapp.ParseAppID(*app)
		if !ok {
			return fmt.Errorf("--app is a Stream app id, not %q", *app)
		}
		move.App = &pin
	}

	counted := map[phone.Move]int{}
	err := service.MoveToCallHook(ctx, move, func(number phone.MovedNumber) {
		counted[number.Move]++
		app := "deployment"
		if number.App != 0 {
			app = strconv.FormatInt(number.App, 10)
		}
		fmt.Fprintf(out, "%s\tapp %s\t%s\n", number.E164, app, number.Move)
	})
	fmt.Fprintf(out, "%d moved, %d to move, %d already moved, %d with no stored bridge\n",
		counted[phone.Moved], counted[phone.WouldMove], counted[phone.AlreadyMoved], counted[phone.NoStoredBridge])
	if !*apply {
		fmt.Fprintln(out, "nothing was written at Twilio: --apply points the numbers at the webhook")
	}
	return err
}
