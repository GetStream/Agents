package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// StreamAppToken is the newest server token the gateway minted for a Stream app. It is sealed
// under the router's key or the app's data key, and opens only for the customer, app and API key
// in this row.
type StreamAppToken struct {
	bun.BaseModel `bun:"table:stream_app_tokens,alias:sat"`

	StreamAppPK int64  `bun:"stream_app_pk,pk"`
	CustomerID  string `bun:"customer_id,notnull"`
	APIKey      string `bun:"api_key,notnull"`
	Sealed      []byte `bun:"token_sealed,notnull"`
	// KEKVersion names which key encryption key sealed this row, and is 0 when the app's data
	// key sealed it.
	KEKVersion int `bun:"kek_version,notnull"`
	// DataKeyVersion is the version of the app's data key that sealed Sealed, or nil when the
	// router's key sealed it. Ignore it when KEKVersion is not 0: a build from before data keys
	// rewrites Sealed and KEKVersion and leaves this column as it was.
	DataKeyVersion *int      `bun:"token_data_key_version"`
	ExpiresAt      time.Time `bun:"expires_at,notnull"`
	UpdatedAt      time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a token without its app, its customer, its key or a sealed token.
func (t *StreamAppToken) Validate() error {
	return validate("stream app token").
		check(t.StreamAppPK != 0, "stream_app_pk", "is required").
		required("customer_id", t.CustomerID).
		required("api_key", t.APIKey).
		check(t.Sealed != nil, "token_sealed", "is required").
		check(!t.ExpiresAt.IsZero(), "expires_at", "is required").
		done()
}

// ErrNoStreamAppToken is an app the gateway has minted no token for here.
var ErrNoStreamAppToken = errors.New("store: no token is held for this stream app")

// StreamAppTokens are the server tokens held for Stream apps, in the shared database.
type StreamAppTokens struct {
	store *Store
}

// Keep holds a token for its app, unless the one already held outlasts it, and reports
// whether it was kept.
func (r *StreamAppTokens) Keep(ctx context.Context, token *StreamAppToken) (bool, error) {
	if err := token.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	token.ExpiresAt = token.ExpiresAt.UTC()
	token.UpdatedAt = time.Now().UTC()
	result, err := r.store.global.NewInsert().Model(token).
		On("CONFLICT (stream_app_pk) DO UPDATE").
		Set("customer_id = EXCLUDED.customer_id").
		Set("api_key = EXCLUDED.api_key").
		Set("token_sealed = EXCLUDED.token_sealed").
		Set("kek_version = EXCLUDED.kek_version").
		Set("token_data_key_version = EXCLUDED.token_data_key_version").
		Set("expires_at = EXCLUDED.expires_at").
		Set("updated_at = EXCLUDED.updated_at").
		Where("sat.expires_at < EXCLUDED.expires_at").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: keep stream app token: %w", err))
	}
	written, _ := result.RowsAffected()
	return written > 0, nil
}

// Reseal replaces the token in the row it was opened from with the same token sealed under version
// dataKeyVersion of the app's data key. It returns false and writes nothing when the row changed
// after it was read, since a token kept in the meantime is newer.
func (r *StreamAppTokens) Reseal(ctx context.Context, opened StreamAppToken, sealed []byte, dataKeyVersion int) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*StreamAppToken)(nil)).
		Set("token_sealed = ?", sealed).
		Set("kek_version = 0").
		Set("token_data_key_version = ?", dataKeyVersion).
		Where("stream_app_pk = ?", opened.StreamAppPK).
		Where("customer_id = ?", opened.CustomerID).
		// Every seal makes a new blob, so an equal blob means no keep or reseal came between.
		Where("token_sealed = ?", opened.Sealed).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal stream app token: %w", err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal stream app token: %w", err))
	}
	return written == 1, nil
}

// Get returns the token held for an app, or ErrNoStreamAppToken.
func (r *StreamAppTokens) Get(ctx context.Context, app int64) (StreamAppToken, error) {
	var token StreamAppToken
	err := r.store.global.NewSelect().Model(&token).Where("stream_app_pk = ?", app).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return StreamAppToken{}, stack.Wrap(ErrNoStreamAppToken)
	}
	if err != nil {
		return StreamAppToken{}, stack.Wrap(fmt.Errorf("store: read stream app token: %w", err))
	}
	return token, nil
}
