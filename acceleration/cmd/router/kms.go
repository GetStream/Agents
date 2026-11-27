package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// kekAADFormat is the additional authenticated data a key of version n is wrapped with,
// router-kek-v<n>. It binds the ciphertext to its version, so one put under another
// version's variable does not unwrap. The string is this router's choice: Cloud KMS takes
// any AAD up to 64 KiB and decrypts only with the one it encrypted with
// (https://docs.cloud.google.com/kms/docs/additional-authenticated-data). The operator
// wrapping a key passes the same string.
const kekAADFormat = "router-kek-v%d"

// kekUnwrapper decrypts a key encryption key a KMS wrapped, so the key itself is in no
// variable or secret the deployment holds.
type kekUnwrapper interface {
	// Unwrap returns the plaintext of ciphertext, wrapped with aad as its additional
	// authenticated data.
	Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error)
}

// kmsClientOptions are added to the options of the KMS client the router opens: none in the
// binary, which reaches Cloud KMS with Application Default Credentials. A test points them at
// a fake server.
var kmsClientOptions []option.ClientOption

// cloudKMS unwraps with Decrypt under one Cloud KMS key. The client's default Decrypt call
// gives up after 60 s and retries only Unavailable and DeadlineExceeded
// (defaultKeyManagementCallOptions in cloud.google.com/go/kms/apiv1), so a KMS that does not
// answer fails startup rather than holding it.
type cloudKMS struct {
	client *kms.KeyManagementClient
	// key is the CryptoKey's resource name, projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>:
	// Decrypt takes the key, never a version, and the server finds the version
	// (kmspb.DecryptRequest.Name, cloud.google.com/go/kms v1.26.0).
	key string
}

// newKEKUnwrapper opens a Cloud KMS client for key, the CryptoKey the setting names. The
// caller closes it.
func newKEKUnwrapper(ctx context.Context, setting, key string) (*cloudKMS, error) {
	// The client's own logger is silenced: with GOOGLE_SDK_GO_LOGGING_LEVEL=debug it logs the
	// Decrypt request and response, which carry the ciphertext and the plaintext key
	// (executeRPC in cloud.google.com/go/kms v1.26.0, apiv1/helpers.go).
	options := append([]option.ClientOption{option.WithLogger(slog.New(slog.DiscardHandler))}, kmsClientOptions...)
	client, err := kms.NewKeyManagementClient(ctx, options...)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("%s: open a Cloud KMS client: %w", setting, err))
	}
	return &cloudKMS{client: client, key: key}, nil
}

func (c *cloudKMS) Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error) {
	response, err := c.client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:                        c.key,
		Ciphertext:                  ciphertext,
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("cloud kms: decrypt with %s: %w", c.key, err))
	}
	return response.GetPlaintext(), nil
}

// Close closes the client's connection.
func (c *cloudKMS) Close() error {
	return c.client.Close()
}

// unwrapKEK unwraps the base64 ciphertext of the key of version n into the key itself, the
// same string the plain key it took the place of held, so the rows it sealed still open.
func unwrapKEK(ctx context.Context, unwrapper kekUnwrapper, version int, encoded string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// The value is left out: a plain key pasted into the wrong variable would otherwise
		// reach the logs and Sentry with this error.
		return "", errors.New("not standard base64")
	}
	plaintext, err := unwrapper.Unwrap(ctx, ciphertext, fmt.Appendf(nil, kekAADFormat, version))
	if err != nil {
		return "", fmt.Errorf("unwrap: %w", err)
	}
	if len(plaintext) == 0 {
		return "", errors.New("unwrapped to an empty key")
	}
	return string(plaintext), nil
}

// newAppKeys opens the Cloud KMS client that unwraps the apps' data keys, and the appkeys.Keys
// that use it. The client stays open for the life of the router, since every open of a secret
// under a data key calls it. Nothing is opened when auth.app_keys_kms_key is unset, connectors
// are off or there is no database: no secret under a data key is read then.
func newAppKeys(ctx context.Context, settings config.Config, db *store.Store) (*appkeys.Keys, func(), error) {
	if settings.Auth.AppKeysKMSKey == "" || !settings.Connectors.Enabled || db == nil {
		return nil, func() {}, nil
	}
	client, err := newKEKUnwrapper(ctx, "auth.app_keys_kms_key", settings.Auth.AppKeysKMSKey)
	if err != nil {
		return nil, nil, err
	}
	keys, err := appkeys.New(db, client, appkeys.Options{TTL: settings.Auth.AppKeysTTL,
		StaleLimit: settings.Auth.AppKeysStaleLimit, KMSTimeout: settings.Auth.AppKeysKMSTimeout})
	if err != nil {
		// The start already fails with err, so a failed close adds nothing.
		_ = client.Close()
		return nil, nil, err
	}
	// The router is stopping, so a failed close changes nothing.
	return keys, func() { _ = client.Close() }, nil
}
