package streamapp

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// tokenPurpose names a sealed server token, so it opens as nothing else the keyring
	// seals, and nothing else opens as one.
	tokenPurpose = "accelerate:stream-app-token:v1"
	// refreshBefore is how long before a held token expires the gateway is asked for
	// another, so nothing is started with a token about to stop working.
	refreshBefore = time.Hour
	// tokenRecordKind is the record label of forwarded tokens in the appkeys counters.
	tokenRecordKind = "stream_app_token"
)

// The counters start here so a dashboard or alert on the stream_app_token series reads 0 from the
// router's start, not "no data" until the first fallback or open under the router's key.
func init() { appkeys.StartCounting(tokenRecordKind) }

// errTokenDoesNotOpen means the router holds a token for an app and cannot open it. The cause may
// name the KMS key or a setting, so it goes to the log and not into this error.
var errTokenDoesNotOpen = errors.New("streamapp: cannot open the token held for this app, see the router log for the cause")

// ForwardedOptions configures Forwarded. Store and Sealer are required.
type ForwardedOptions struct {
	Store  *store.Store
	Sealer *auth.Sealer
	// Keys seals new tokens under each app's data key when sealing is on, and opens the tokens
	// already under one. It is nil when no KMS key wraps data keys.
	Keys *appkeys.Keys
	// Mint is the gateway's mint endpoint, asked for a token when the one held runs out.
	// Without it an app is acted in only while a token it forwarded lasts.
	Mint *Mint
	// BaseURL is the Stream API every app is reached at. Empty is Stream's default.
	BaseURL string
	Logger  *slog.Logger
	// Now is the clock, for tests.
	Now func() time.Time
}

// Forwarded acts in each customer's own Stream app with the server token the gateway in
// front minted for it, rather than with secrets the customer registered. The gateway
// already holds every app's secret to check its callers, and forwards a token with each
// request it vouches for; the newest is kept sealed in Postgres, so every replica and a
// restart act with it.
type Forwarded struct {
	store   *store.Store
	sealer  *auth.Sealer
	keys    *appkeys.Keys
	mint    *Mint
	baseURL string
	logger  *slog.Logger
	now     func() time.Time

	mu   sync.Mutex
	held map[int64]heldToken
}

// heldToken is a token opened from the store, or kept from a request.
type heldToken struct {
	customer string
	apiKey   string
	token    Secret
	expires  time.Time
}

// NewForwarded returns the source of forwarded tokens.
func NewForwarded(options ForwardedOptions) (*Forwarded, error) {
	if options.Store == nil || options.Sealer == nil {
		return nil, errors.New("streamapp: forwarded tokens need the store and the keyring to keep them")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Forwarded{
		store: options.Store, sealer: options.Sealer, keys: options.Keys, mint: options.Mint, baseURL: options.BaseURL,
		logger: cmp.Or(options.Logger, slog.Default()), now: now,
		held: map[int64]heldToken{},
	}, nil
}

// Mint is the gateway's mint endpoint, nil when there is none.
func (f *Forwarded) Mint() *Mint { return f.mint }

// Keep holds a token the gateway forwarded for a customer's own app, unless the one held
// already outlasts it, and reports whether it was kept. A customer that is not an app's
// own has no token to keep.
func (f *Forwarded) Keep(ctx context.Context, customer, apiKey, token string) (bool, error) {
	app, ok := ParseAppID(customer)
	if !ok || apiKey == "" || token == "" {
		return false, nil
	}
	f.mu.Lock()
	held, ok := f.held[app]
	f.mu.Unlock()
	if ok && held.token.Reveal() == token {
		return false, nil
	}
	expires, err := expiryOf(token)
	if err != nil {
		return false, err
	}
	if ok && !held.expires.Before(expires) {
		return false, nil
	}
	return f.keep(ctx, app, heldToken{customer: customer, apiKey: apiKey, token: NewSecret(token), expires: expires})
}

// keep stores a token unless the store already holds one that expires later, and reports whether
// it stored it. It seals the token under the app's data key when sealing is on. Otherwise (sealing
// off, no data keys, or KMS unavailable) it seals under the router's key, as before data keys.
func (f *Forwarded) keep(ctx context.Context, app int64, token heldToken) (bool, error) {
	aad := boundTo(tokenPurpose, token.customer, app, token.apiKey)
	sealed, err := f.keys.SealRecord(ctx, tokenRecord(token.customer, app), []byte(token.token.Reveal()), aad,
		func() ([]byte, int, error) {
			blob, err := f.sealer.SealWithAAD(token.token.Reveal(), aad)
			return blob, f.sealer.CurrentVersion(), err
		})
	if err != nil {
		return false, err
	}
	kept, err := f.store.StreamAppTokens.Keep(ctx, &store.StreamAppToken{
		StreamAppPK: app, CustomerID: token.customer, APIKey: token.apiKey,
		Sealed: sealed.Blob, KEKVersion: sealed.KEKVersion, DataKeyVersion: sealed.DataKeyVersion, ExpiresAt: token.expires,
	})
	if err != nil || !kept {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if held, ok := f.held[app]; !ok || held.expires.Before(token.expires) {
		f.held[app] = token
	}
	return true, nil
}

// identity is the customer's own app acted in with the token held for it, refreshed
// through the gateway when it is about to run out. ErrNoIdentity is an app with no token
// held and none to be had. Without refresh the gateway is not asked, so a token that has run
// out is none to be had, as it is when the gateway will not mint another.
func (f *Forwarded) identity(ctx context.Context, customer string, app int64, refresh bool) (Identity, error) {
	if customer != CustomerOf(app) {
		return Identity{}, ErrNoIdentity
	}
	token, err := f.read(ctx, app)
	if err != nil {
		return Identity{}, err
	}
	if refresh && token.expires.Sub(f.now()) < refreshBefore && f.mint != nil {
		minted, err := f.mint.ServerToken(ctx, app, token.apiKey)
		switch {
		case err == nil:
			token = heldToken{customer: customer, apiKey: token.apiKey, token: minted.Token, expires: minted.ExpiresAt}
			if _, err := f.keep(ctx, app, token); err != nil {
				f.logger.Warn("stream: could not keep a token the gateway minted", "stream_app", app, "error", err)
			}
		case token.expires.After(f.now()):
			f.logger.Warn("stream: could not refresh an app's token, acting with the one held",
				"stream_app", app, "expires_at", token.expires, "error", err)
		default:
			return Identity{}, fmt.Errorf("%w: %w", ErrNoIdentity, err)
		}
	}
	if !token.expires.After(f.now()) {
		return Identity{}, ErrNoIdentity
	}
	return Identity{
		CustomerID: customer, StreamApp: app, APIKey: token.apiKey, Token: token.token, TokenExpires: token.expires,
		BaseURL: f.baseURL, Registered: true,
	}, nil
}

// read is the token held for an app, from memory or else from the store.
func (f *Forwarded) read(ctx context.Context, app int64) (heldToken, error) {
	f.mu.Lock()
	held, ok := f.held[app]
	f.mu.Unlock()
	if ok && held.expires.Sub(f.now()) >= refreshBefore {
		return held, nil
	}
	row, err := f.store.StreamAppTokens.Get(ctx, app)
	if errors.Is(err, store.ErrNoStreamAppToken) {
		return heldToken{}, ErrNoIdentity
	}
	if err != nil {
		return heldToken{}, err
	}
	if !row.ExpiresAt.After(f.now()) {
		// Nothing to open: an expired row, or a placeholder seeded for an app that had
		// registered its keys, is only the key the gateway is asked to mint for.
		return heldToken{customer: row.CustomerID, apiKey: row.APIKey, expires: row.ExpiresAt}, nil
	}
	aad := boundTo(tokenPurpose, row.CustomerID, app, row.APIKey)
	dataKeyVersion := tokenDataKeyVersion(row)
	opened, err := f.keys.OpenRecord(ctx, tokenRecord(row.CustomerID, app), dataKeyVersion, row.Sealed, aad,
		func() ([]byte, error) {
			token, err := f.sealer.OpenWithAADVersion(row.Sealed, aad, row.KEKVersion)
			return []byte(token), err
		})
	if err != nil {
		f.logger.ErrorContext(ctx, "stream: could not open the token held for an app", "stream_app", app, "error", err)
		return heldToken{}, stack.Wrap(errTokenDoesNotOpen)
	}
	if dataKeyVersion == nil {
		f.reseal(ctx, row, opened)
	}
	read := heldToken{customer: row.CustomerID, apiKey: row.APIKey, token: NewSecret(string(opened)), expires: row.ExpiresAt}
	f.mu.Lock()
	defer f.mu.Unlock()
	if held, ok := f.held[app]; !ok || held.expires.Before(read.expires) {
		f.held[app] = read
	}
	return f.held[app], nil
}

// reseal moves a token that opened under the router's key to the app's data key when sealing is
// on. A failure is logged and leaves the row as it was, since the token opened and can still be
// used.
func (f *Forwarded) reseal(ctx context.Context, row store.StreamAppToken, token []byte) {
	sealed, err := f.keys.ResealRecord(ctx, tokenRecord(row.CustomerID, row.StreamAppPK), token,
		boundTo(tokenPurpose, row.CustomerID, row.StreamAppPK, row.APIKey))
	// ResealRecord seals nothing and returns no version with sealing off or no data keys.
	if err == nil && sealed.DataKeyVersion != nil {
		_, err = f.store.StreamAppTokens.Reseal(ctx, row, sealed.Blob, *sealed.DataKeyVersion)
	}
	if err != nil {
		f.logger.WarnContext(ctx, "stream: left an app's token under the router's key, since it could not be sealed "+
			"again under the app's data key", "stream_app", row.StreamAppPK, "error", err)
	}
}

// tokenDataKeyVersion returns the version of the app's data key that sealed the row's token, or nil
// when the router's key sealed it. It ignores DataKeyVersion when KEKVersion is set, since a build
// from before data keys rewrites the token and leaves that column as it was.
func tokenDataKeyVersion(row store.StreamAppToken) *int {
	if row.KEKVersion != 0 {
		return nil
	}
	return row.DataKeyVersion
}

// tokenRecord is an app's token as the appkeys counters and log lines name it.
func tokenRecord(customer string, app int64) appkeys.Record {
	return appkeys.Record{Kind: tokenRecordKind, CustomerID: customer, Attrs: []slog.Attr{slog.Int64("stream_app", app)}}
}

// boundTo is what something sealed for an app's key is bound to: what it is, whose it is,
// which app and which key. A sealed value moved to any other row does not open there. Each
// part is prefixed with its length, so no two different rows can run together into the
// same bytes.
func boundTo(purpose, customer string, app int64, apiKey string) []byte {
	var aad []byte
	for _, part := range []string{purpose, customer, strconv.FormatInt(app, 10), apiKey} {
		aad = binary.BigEndian.AppendUint32(aad, uint32(len(part)))
		aad = append(aad, part...)
	}
	return aad
}

// expiryOf is when a token the gateway minted expires. Its signature is not checked: the
// router holds no secret to check it with, and believes the gateway as it believes the
// app it names. A token that never expires is refused, since it could never be replaced.
func expiryOf(token string) (time.Time, error) {
	claims := jwt.RegisteredClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(token, &claims); err != nil {
		return time.Time{}, fmt.Errorf("streamapp: reading a forwarded token: %w", err)
	}
	if claims.ExpiresAt == nil {
		return time.Time{}, errors.New("streamapp: a forwarded token names no expiry")
	}
	return claims.ExpiresAt.Time, nil
}
