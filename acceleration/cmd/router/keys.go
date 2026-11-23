package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const keysUsage = `usage: router keys create [flags]

  --app-id      the id to give the app, which is what every row's customer_id holds.
                Pass the id the deployment you are moving from used, so the rows you
                import still belong to something. Empty mints a new one
  --app-name    what to call the app, for a human reading the list
  --org         the organization to put a new app in
  --key-name    what to call the key, so an operator knows which to revoke
  --env         live or test

       router keys revoke --app <id> --key <id>

  --app         the app the key belongs to
  --key         the key to stop. Every router stops taking it at once when this command
                has the routers' Redis (redis.addr), and within a minute when it has none
                or that Redis does not take the change
`

// keysKeyring is the keyring `router keys create` seals a new key's secret with. A test
// swaps it for one whose current version is not 1, which loadKeyring reaches only with a
// KMS.
var keysKeyring = loadKeyring

// runKeys mints the first credential a self-hosted router has.
//
// It exists because api_key mode has no way to bootstrap itself over HTTP: every endpoint
// wants a key, and there is nobody to issue the first one. A command run beside the
// database can, and it is the same command that moves an app id across from a deployment
// somebody is leaving.
func runKeys(args []string, settings config.Config, logger *slog.Logger) error {
	if len(args) == 0 {
		return errors.New(keysUsage)
	}
	switch args[0] {
	case "create":
	case "revoke":
		return revokeKey(args[1:], settings, logger, os.Stdout)
	default:
		return fmt.Errorf("unknown keys command %q\n\n%s", args[0], keysUsage)
	}

	flags := flag.NewFlagSet("keys create", flag.ContinueOnError)
	appID := flags.String("app-id", "", "the app id to use, or empty for a new one")
	appName := flags.String("app-name", "", "what to call the app")
	orgName := flags.String("org", "self-hosted", "the organization a new app belongs to")
	keyName := flags.String("key-name", "default", "what to call the key")
	environment := flags.String("env", string(auth.Live), "live or test")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	ctx := context.Background()
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	defer pgStore.Close()

	sealer, err := keysKeyring(ctx, settings, "minting a key needs")
	if err != nil {
		return err
	}

	app, err := resolveApp(ctx, pgStore, *appID, *appName, *orgName)
	if err != nil {
		return err
	}

	key, secret, err := auth.NewCredential(auth.Environment(*environment))
	if err != nil {
		return err
	}
	sealed, err := sealer.Seal(secret)
	if err != nil {
		return err
	}
	if err := pgStore.APIKeys.Insert(ctx, &store.APIKey{
		ID:         key,
		AppID:      app.ID,
		Name:       *keyName,
		Env:        *environment,
		Sealed:     sealed,
		KEKVersion: sealer.CurrentVersion(),
		Last4:      auth.Last4(secret),
		CreatedBy:  "router keys create",
	}); err != nil {
		return err
	}
	logger.Info("created an api key", "app", app.ID, "key", key)

	// Printed rather than logged: the secret is never shown again, and a log is the one
	// place it must not be. Standard output is what the operator is looking at.
	fmt.Printf("organization  %s\napp           %s\nkey           %s\nsecret        %s\n\n"+
		"The secret is shown once. Set it where your SDK reads it and keep it out of the router's own config.\n",
		app.OrganizationID, app.ID, key, secret)
	return nil
}

// revokeKey stops one of an app's keys. The app is asked for as well as the key, so a key
// id pasted under the wrong app revokes nothing.
//
// Routers hold a key's owner in the cache for a minute. A command with the routers' Redis
// drops it from every one at once; without Redis, or when the drop fails, it cannot tell
// them, so it says the key works for up to a minute longer.
func revokeKey(args []string, settings config.Config, logger *slog.Logger, out io.Writer) error {
	flags := flag.NewFlagSet("keys revoke", flag.ContinueOnError)
	appID := flags.String("app", "", "the app the key belongs to")
	keyID := flags.String("key", "", "the key to revoke")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *appID == "" || *keyID == "" {
		return errors.New("revoking a key needs both --app and --key")
	}

	ctx := context.Background()
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	defer pgStore.Close()

	told, err := pgStore.APIKeys.Revoke(ctx, *appID, *keyID, "router keys revoke")
	if errors.Is(err, store.ErrNoAPIKey) {
		return fmt.Errorf("app %s has no live key %s", *appID, *keyID)
	}
	if err != nil {
		return err
	}
	logger.Info("revoked an api key", "app", *appID, "key", *keyID)
	fmt.Fprintf(out, "revoked key %s of app %s\n", *keyID, *appID)
	switch {
	case settings.Redis.Addr == "":
		fmt.Fprintln(out, "redis.addr is not set here, so the routers are not told: they drop it within a minute.")
	case !told:
		fmt.Fprintln(out, "the routers' Redis did not take the change, so the routers are not told: they drop it within a minute.")
	}
	return nil
}

// resolveApp finds the app the key is for, creating it and its organization when it is
// not there yet. An id that already exists is used as it is, so running this twice adds a
// second key rather than refusing.
func resolveApp(ctx context.Context, pgStore *store.Store, appID, appName, orgName string) (store.App, error) {
	if appID != "" {
		app, err := pgStore.Apps.Get(ctx, appID)
		if err == nil {
			return app, nil
		}
		if !errors.Is(err, store.ErrNoApp) {
			return store.App{}, err
		}
	}

	if orgName == "" {
		return store.App{}, errors.New("a new app needs an organization: pass --org")
	}
	org := store.Organization{Name: orgName}
	if err := pgStore.Organizations.Insert(ctx, &org); err != nil {
		return store.App{}, err
	}

	name := appName
	if name == "" {
		name = orgName
	}
	app := store.App{ID: appID, OrganizationID: org.ID, Name: name}
	if err := pgStore.Apps.Insert(ctx, &app); err != nil {
		return store.App{}, err
	}
	return app, nil
}
