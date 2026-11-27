// Package pgsealed is the core.CredentialStore that keeps a connection's credential state in
// Postgres: the connection row in connector_connections, its StoredCredentials sealed with
// auth.Sealer, under a session-level advisory lock held across replicas
// (store.ConnectorConnections.WithLocked).
package pgsealed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// unreadableError is what LastError says when the stored credentials do not open: the key
// that sealed them is gone, or the blob is not this row's at this revision. Either way only a
// reconnect gives the connection credentials again (core.CredentialState.LastError says what
// to do).
const unreadableError = "The stored credential could not be opened; reconnect the account"

// errRevisionIsTheStores says fn moved CredentialState.Revision. The credential store
// advances it, by one, whenever the StoredCredentials change, so the revision a blob is
// sealed for is never chosen by fn.
var errRevisionIsTheStores = errors.New("pgsealed: the credential store advances the revision; fn must not change it")

// errNoDataKeys says the credentials are sealed under the app's data key, and the credential
// store was built without appkeys. The setting is missing, not the connection's credentials.
var errNoDataKeys = errors.New("pgsealed: the credentials are sealed under the app's data key, " +
	"and auth.app_keys_kms_key is not set")

// CredentialStore implements core.CredentialStore over a store and a sealer.
type CredentialStore struct {
	store  *store.Store
	sealer *auth.Sealer
	// dataKeys opens credentials sealed under the app's data key. It is nil when no KMS key
	// wraps data keys.
	dataKeys *appkeys.Keys
}

var _ core.CredentialStore = (*CredentialStore)(nil)

// Option changes how New builds a credential store.
type Option func(*CredentialStore)

// WithDataKeys opens credentials sealed under the app's data key with keys. A nil keys leaves
// the store as it would be without the option.
func WithDataKeys(keys *appkeys.Keys) Option {
	return func(b *CredentialStore) { b.dataKeys = keys }
}

// New is a credential store over db, sealing with sealer's current key version.
func New(db *store.Store, sealer *auth.Sealer, options ...Option) (*CredentialStore, error) {
	if db == nil || sealer == nil {
		return nil, errors.New("pgsealed: a store and a sealer are required")
	}
	b := &CredentialStore{store: db, sealer: sealer}
	for _, option := range options {
		option(b)
	}
	return b, nil
}

// Update loads the credential state under the connection's lock, opens its
// StoredCredentials and runs fn.
//
// The credential store owns CredentialState.Revision: a commit whose StoredCredentials differ
// from the stored ones seals them for the next revision; one that keeps them keeps the
// revision. StoredCredentials under an older key are sealed again under the current one by
// the first use that returns no error, even one that changed nothing; when fn returned
// changed false, the rewrap writes what was stored, not fn's edits. StoredCredentials that do
// not authenticate for this row and revision are handed to fn empty, with the state at
// needs_reauthorization, committed before fn runs: no use can get an access credential from
// them, and a reconnect can still replace them. Their blob stays as it is until then.
// StoredCredentials under a key version the keyring does not hold are a configuration fault,
// not the connection's:
// Update returns auth.ErrKeyVersionUnavailable without running fn or writing anything, so
// restoring the key restores the connection. In the same way, StoredCredentials under the
// app's data key fail with appkeys.ErrUnavailable when KMS does not unwrap the key.
func (b *CredentialStore) Update(ctx context.Context, ref core.ConnectionRef,
	fn func(state *core.CredentialState, checkpoint func() error) (changed bool, err error)) error {
	return b.store.ConnectorConnections.WithLocked(ctx, ref.CustomerID, ref.ConnectionID,
		func(connection *store.ConnectorConnection, save func() error) (bool, error) {
			stored := &locked{connection: connection}
			var err error
			stored.credentials, stored.opened, err = b.open(ctx, ref, connection)
			if err != nil {
				return false, err
			}
			state := stored.state()
			if !stored.opened && connection.Status != store.ConnectionNeedsReauthorization {
				state.Status = store.ConnectionNeedsReauthorization
				state.LastError = unreadableError
				if err := b.commit(ref, stored, &state, save); err != nil {
					return false, err
				}
			}
			changed, err := fn(&state, func() error { return b.commit(ref, stored, &state, save) })
			if err != nil {
				return false, err
			}
			if changed {
				return false, b.commit(ref, stored, &state, save)
			}
			if b.stale(stored) {
				rewrap := stored.state()
				return false, b.commit(ref, stored, &rewrap, save)
			}
			return false, nil
		})

}

// locked is the connection as last committed, and the StoredCredentials it holds when they
// opened.
type locked struct {
	connection  *store.ConnectorConnection
	credentials core.StoredCredentials
	opened      bool
}

// state is what fn sees of the connection.
func (l *locked) state() core.CredentialState {
	state := core.CredentialState{
		Revision:    l.connection.Revision,
		Status:      l.connection.Status,
		Credentials: cloneCredentials(l.credentials),
		LastError:   l.connection.LastError,
		AccountID:   l.connection.AccountID,
		Metadata:    maps.Clone(l.connection.Metadata),
		Scopes:      slices.Clone(l.connection.GrantedScopes),
		// The revision is read under the lock, since a consent moves it with the grant.
		DefinitionRevision: l.connection.DefinitionRevision,
	}
	if l.connection.ExpiresAt != nil {
		state.ExpiresAt = *l.connection.ExpiresAt
	}
	if l.connection.ConnectedAt != nil {
		state.ConnectedAt = *l.connection.ConnectedAt
	}
	return state
}

// stale reports StoredCredentials that opened under a key version other than the current one.
// Credentials under the app's data key are not stale, since sealing them again under the
// router's key would undo the data key.
func (b *CredentialStore) stale(stored *locked) bool {
	return stored.opened && len(stored.connection.CredentialsSealed) > 0 &&
		stored.connection.CredentialsDataKeyVersion == nil &&
		stored.connection.CredentialsKEKVersion != b.sealer.CurrentVersion()
}

// commit writes state onto the connection and saves it at the revision last committed. New
// StoredCredentials are sealed for the next revision; stale ones are sealed again under the
// current key at the same revision, since the AAD binds the revision, not the key. A save that fails
// leaves the connection as it was.
func (b *CredentialStore) commit(ref core.ConnectionRef, stored *locked, state *core.CredentialState, save func() error) error {
	connection := stored.connection
	if state.Revision != connection.Revision {
		return errRevisionIsTheStores
	}
	previous := *connection
	revision := connection.Revision
	if !sameCredentials(state.Credentials, stored.credentials) {
		revision++
	}
	if revision != connection.Revision || b.stale(stored) {
		sealed, version, err := b.seal(ref, revision, state.Credentials)
		if err != nil {
			return err
		}
		connection.Revision, connection.CredentialsSealed, connection.CredentialsKEKVersion = revision, sealed, version
		connection.CredentialsDataKeyVersion = nil
	}
	connection.Status = state.Status
	connection.LastError = state.LastError
	connection.AccountID = state.AccountID
	connection.Metadata = maps.Clone(state.Metadata)
	connection.GrantedScopes = slices.Clone(state.Scopes)
	connection.DefinitionRevision = state.DefinitionRevision
	connection.ExpiresAt = nil
	if !state.ExpiresAt.IsZero() {
		expires := state.ExpiresAt.UTC().Truncate(time.Microsecond)
		connection.ExpiresAt = &expires
	}
	// The time is fn's: whatever writes a new grant, a consent or a credentials write, names
	// when it began, and a renewal keeps it, even one whose save is retried in an Update of its
	// own after its checkpoint (the resolver's saveAgain), which loads needs_reauthorization.
	connection.ConnectedAt = nil
	if !state.ConnectedAt.IsZero() {
		connected := state.ConnectedAt.UTC().Truncate(time.Microsecond)
		connection.ConnectedAt = &connected
	}
	if err := save(); err != nil {
		*connection = previous
		return err
	}
	if revision != previous.Revision {
		stored.credentials, stored.opened = cloneCredentials(state.Credentials), true
	}
	state.Revision = connection.Revision
	return nil
}

// seal is StoredCredentials as stored: empty, at key version 0 (what the migration stores
// for none), when there are none, else their JSON sealed under the current key for this row
// and revision.
func (b *CredentialStore) seal(ref core.ConnectionRef, revision int, c core.StoredCredentials) ([]byte, int, error) {
	if isEmpty(c) {
		return []byte{}, 0, nil
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, 0, fmt.Errorf("pgsealed: encode credentials: %w", err)
	}
	sealed, err := b.sealer.SealWithAAD(string(raw), credentialsAAD(ref, revision))
	if err != nil {
		return nil, 0, fmt.Errorf("pgsealed: seal credentials: %w", err)
	}
	return sealed, b.sealer.CurrentVersion(), nil
}

// open is the connection's StoredCredentials, and false when there is a blob that does not open for
// this row at this revision. A key version the keyring does not hold is an error instead, and so
// is a data key that KMS did not unwrap.
func (b *CredentialStore) open(ctx context.Context, ref core.ConnectionRef,
	connection *store.ConnectorConnection) (core.StoredCredentials, bool, error) {
	if len(connection.CredentialsSealed) == 0 {
		return core.StoredCredentials{}, true, nil
	}
	plain, err := b.openSealed(ctx, ref, connection)
	if errors.Is(err, errDoesNotOpen) {
		return core.StoredCredentials{}, false, nil
	}
	if err != nil {
		return core.StoredCredentials{}, false, fmt.Errorf("pgsealed: open credentials: %w", err)
	}
	var credentials core.StoredCredentials
	if err := json.Unmarshal(plain, &credentials); err != nil {
		return core.StoredCredentials{}, false, nil
	}
	return credentials, true, nil
}

// errDoesNotOpen marks a blob that is not this row's at this revision, under the key the row
// names. Any other error from openSealed is a fault of the keys, not of the connection.
var errDoesNotOpen = errors.New("pgsealed: the credentials do not open for this row")

// openSealed opens the connection's blob under the router's key at the row's key version, or
// under the app's data key when the row names a data key version.
func (b *CredentialStore) openSealed(ctx context.Context, ref core.ConnectionRef,
	connection *store.ConnectorConnection) ([]byte, error) {
	aad := credentialsAAD(ref, connection.Revision)
	if connection.CredentialsDataKeyVersion == nil {
		plain, err := b.sealer.OpenWithAADVersion(connection.CredentialsSealed, aad, connection.CredentialsKEKVersion)
		if err != nil && !errors.Is(err, auth.ErrKeyVersionUnavailable) {
			return nil, fmt.Errorf("%w: %w", errDoesNotOpen, err)
		}
		return []byte(plain), err
	}
	if b.dataKeys == nil {
		return nil, stack.Wrap(errNoDataKeys)
	}
	plain, err := b.dataKeys.Open(ctx, ref.CustomerID, *connection.CredentialsDataKeyVersion, connection.CredentialsSealed, aad)
	if errors.Is(err, appkeys.ErrDoesNotOpen) {
		return nil, fmt.Errorf("%w: %w", errDoesNotOpen, err)
	}
	return plain, err
}

// credentialsAAD binds sealed StoredCredentials to their tenant, connection and revision, so a
// blob copied to another row, or written back at a later revision, does not open. Each string
// is length-prefixed, so no customer and id split the same bytes two ways. The label is not
// the prototype's ("accelerate:connector-credentials:v1", connectors/secrets.go:113 at
// cf62af0d): what is sealed is a core.StoredCredentials, not the prototype's credentials, so
// neither opens as the other. The label keeps its old word "material" on purpose: blobs
// already sealed under it must still open. v1 changes with the layout.
func credentialsAAD(ref core.ConnectionRef, revision int) []byte {
	return fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d",
		len(ref.CustomerID), ref.CustomerID, len(ref.ConnectionID), ref.ConnectionID, revision)
}

func isEmpty(c core.StoredCredentials) bool {
	return c.Scheme == "" && c.Version == 0 && len(c.Payload) == 0
}

func sameCredentials(a, b core.StoredCredentials) bool {
	return a.Scheme == b.Scheme && a.Version == b.Version && bytes.Equal(a.Payload, b.Payload)
}

// cloneCredentials copies Payload, so fn changing it in place is a change commit sees.
func cloneCredentials(c core.StoredCredentials) core.StoredCredentials {
	c.Payload = bytes.Clone(c.Payload)
	return c
}
