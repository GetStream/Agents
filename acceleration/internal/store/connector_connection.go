package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// The two owners a connection can have (architecture doc, one-way door 8). An app-owned
// connection has no owner id; a user-owned one names the user. The migration checks it too.
const (
	OwnerApp  = "app"
	OwnerUser = "user"
)

// Connection statuses, the prototype's (internal/store/connectors.go on codex/connector-support
// at cf62af0d) less failed, which nothing in the design moves a connection to:
// core.CredentialState names connected, needs_reauthorization and disconnected, and pending
// is a connection no credentials were saved onto yet.
const (
	ConnectionPending              = "pending"
	ConnectionConnected            = "connected"
	ConnectionNeedsReauthorization = "needs_reauthorization"
	ConnectionDisconnected         = "disconnected"
)

// How many connections are handed back at once: the session list's sizes (sessions.go),
// since both are lists a dashboard shows a page of. Neither is measured.
const (
	defaultConnectionLimit = 25
	maxConnectionLimit     = 200
)

// ErrNoConnectorConnection says there is no live connection with that id for this customer:
// none was made, it is another customer's, or it was deleted. A sentinel, because each of
// those is an ordinary 404.
var ErrNoConnectorConnection = errors.New("store: no such connector connection")

// ErrConnectorConnectionBound says an unforced delete left a live connection alone because
// an agent config of the customer's binds it as its fixed connection.
var ErrConnectorConnectionBound = errors.New("store: an agent config binds this connector connection")

// ErrUnregisteredScheme says a connection names a scheme no adapter is registered for.
var ErrUnregisteredScheme = errors.New("store: no such scheme is registered")

// ErrSchemeNotAllowed says a connection names a registered scheme its connector's manifest
// does not list. The manifest's schemes are the ones a connection may use
// (core.Manifest.Schemes), so a registered scheme alone is not enough.
var ErrSchemeNotAllowed = errors.New("store: the connector does not allow this scheme")

// Get is one account at one connector. Stored credentials are sealed by the
// caller before it reaches the store, and saved by a revisioned write (T8), never by Create.
type ConnectorConnection struct {
	bun.BaseModel `bun:"table:connector_connections,alias:cc"`

	ID          string `bun:"id,pk"`
	CustomerID  string `bun:"customer_id,notnull"`
	ConnectorID string `bun:"connector_id,notnull"`
	// DefinitionRevision is the connector_definitions revision the connection was made from.
	DefinitionRevision int    `bun:"definition_revision,notnull"`
	OwnerType          string `bun:"owner_type,notnull"`
	OwnerID            string `bun:"owner_id,notnull"`
	// AuthScheme is a name in the scheme registry.
	AuthScheme string `bun:"auth_scheme,notnull"`
	// Inputs are what the connection was created with; Metadata what consent captured.
	Inputs        map[string]string `bun:"inputs,type:jsonb,notnull"`
	Metadata      map[string]string `bun:"metadata,type:jsonb,notnull"`
	Label         string            `bun:"label,notnull"`
	AccountID     string            `bun:"account_id,notnull"`
	Status        string            `bun:"status,notnull"`
	GrantedScopes []string          `bun:"granted_scopes,type:jsonb,notnull"`
	// Revision advances with every new stored credentials, which are sealed against it.
	Revision int `bun:"revision,notnull"`
	// CredentialsSealed is core.StoredCredentials sealed under CredentialsKEKVersion, with
	// customer, id and revision as AAD. Empty, with version 0, until credentials are saved.
	CredentialsSealed     []byte          `bun:"credentials_sealed,notnull"`
	CredentialsKEKVersion int             `bun:"credentials_kek_version,notnull"`
	ExpiresAt             *time.Time      `bun:"expires_at"`
	CachedTools           []ConnectorTool `bun:"cached_tools,type:jsonb,notnull"`
	ToolsDigest           string          `bun:"tools_digest,notnull"`
	ToolsCheckedAt        *time.Time      `bun:"tools_checked_at"`
	LastError             string          `bun:"last_error,notnull"`
	CreatedAt             time.Time       `bun:"created_at,notnull"`
	UpdatedAt             time.Time       `bun:"updated_at,notnull"`
	DeletedAt             *time.Time      `bun:"deleted_at"`

	// ConnectedAt is when a consent last stored credentials (core.CredentialState.ConnectedAt).
	// Nil until one did, and for a connection connected before the column existed.
	ConnectedAt *time.Time `bun:"connected_at"`
	// Client is the OAuth client the connection's grant was issued to, zero when none is
	// recorded.
	Client ConnectorConnectionClient `bun:"embed:client_"`
	// Validation is the connection's last validate, zero before the first.
	Validation ConnectorConnectionValidation `bun:"embed:validation_"`
}

// Validate refuses a connection the table refuses (connector_connections_owner and the
// revision checks), or one in a status no write moves a connection to.
func (c *ConnectorConnection) Validate() error {
	return validate("connector connection").
		required("customer_id", c.CustomerID).
		required("connector_id", c.ConnectorID).
		check(c.DefinitionRevision >= 1, "definition_revision", "is below 1").
		oneOf("owner_type", c.OwnerType, OwnerApp, OwnerUser).
		check(validOwner(c.OwnerType, c.OwnerID), "owner_id", "breaks the owner rule: an app owner has no id and a user owner has one").
		required("auth_scheme", c.AuthScheme).
		oneOf("status", c.Status, ConnectionPending, ConnectionConnected, ConnectionNeedsReauthorization, ConnectionDisconnected).
		check(c.Revision >= 1, "revision", "is below 1").
		done()
}

// ConnectorTool is one tool a connection offered when it was last checked, with the digest
// of the schema a ToolGrant pins.
type ConnectorTool struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"input_schema"`
	SchemaDigest string         `json:"schema_digest"`
	// NeedsScopes are the scopes a call of the tool needs, as the manifest says
	// (core.ToolSpec.NeedsScopes). Absent from rows a validate wrote before it existed, which
	// read as none.
	NeedsScopes []string `json:"needs_scopes,omitempty"`
}

// ConnectionFilter picks one owner's live connections, optionally of one connector, a page
// at a time.
type ConnectionFilter struct {
	OwnerType string
	OwnerID   string
	// EveryUser, with OwnerType user and no OwnerID, picks every user's instead of one's. It is
	// asked for by name so a user id left empty by mistake is still refused.
	EveryUser   bool
	ConnectorID string
	// Status keeps only connections in that status (Connection*). Empty keeps every status.
	Status string
	Limit  int
	// After is the last connection of the previous page.
	After *ConnectionPosition
}

// ConnectionPosition is where a page of connections ended, newest first.
type ConnectionPosition struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"id"`
}

// ConnectionLimit is the page size a connection list uses for the limit asked for.
// Select returns one row more than this, so a caller can tell the page
// is not the last without counting.
func ConnectionLimit(asked int) int {
	return ClampLimit(asked, defaultConnectionLimit, maxConnectionLimit)
}

// ConnectorConnections are the accounts customers connected at connectors, in the shared
// database.
type ConnectorConnections struct {
	store *Store
}

// Insert records a new connection, pending until credentials are saved
// onto it. Its scheme must be in registry and listed by the definition revision it pins, which
// must be one the customer can see. The registry is passed in rather than held by the store, so which
// schemes exist is decided by whoever built it, and a test can register its own.
func (r *ConnectorConnections) Insert(ctx context.Context, registry core.Registry, connection *ConnectorConnection) error {
	connection.Status = ConnectionPending
	connection.Revision = 1
	if err := connection.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if _, found := registry.Schemes[connection.AuthScheme]; !found {
		return stack.Wrap(fmt.Errorf("%w: auth scheme %q", ErrUnregisteredScheme, connection.AuthScheme))
	}
	// The stored credentials' AAD binds the connection id, which does not exist until this returns.
	if len(connection.CredentialsSealed) > 0 {
		return stack.Wrap(errors.New("store: credentials are saved onto a connection after it exists, not with it"))
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// A custom definition is held until the connection is stored, so a delete of it waits
		// and then finds the connection (ConnectorDefinitions.Delete). A built-in is never
		// deleted, so a revision found here stays.
		if _, err := lockCustomDefinitions(ctx, tx, connection.CustomerID, []string{connection.ConnectorID}); err != nil {
			return err
		}
		definition, err := connectorDefinition(ctx, tx, connection.CustomerID, connection.ConnectorID, connection.DefinitionRevision)
		if err != nil {
			return err
		}
		if !slices.Contains(definition.Manifest.Schemes, connection.AuthScheme) {
			return fmt.Errorf("%w: %s revision %d does not list auth scheme %q", ErrSchemeNotAllowed,
				connection.ConnectorID, connection.DefinitionRevision, connection.AuthScheme)
		}
		// Truncated to what Postgres keeps, so the row handed back is the row a read returns.
		now := time.Now().UTC().Truncate(time.Microsecond)
		connection.ID = newID()
		connection.CredentialsSealed = []byte{}
		connection.CredentialsKEKVersion = 0
		connection.CreatedAt = now
		connection.UpdatedAt = now
		connection.DeletedAt = nil
		connection.Inputs = orEmptyMap(connection.Inputs)
		connection.Metadata = orEmptyMap(connection.Metadata)
		connection.GrantedScopes = orEmpty(connection.GrantedScopes)
		connection.CachedTools = orEmpty(connection.CachedTools)
		_, err = tx.NewInsert().Model(connection).Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: create connector connection: %w", err)
		}
		return nil
	}))
}

// Get returns one live connection of the customer's.
func (r *ConnectorConnections) Get(ctx context.Context, customerID, id string) (ConnectorConnection, error) {
	if customerID == "" || id == "" {
		return ConnectorConnection{}, stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	var connection ConnectorConnection
	err := r.store.global.NewSelect().Model(&connection).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("deleted_at IS NULL").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorConnection{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorConnection, id))
	}
	if err != nil {
		return ConnectorConnection{}, stack.Wrap(fmt.Errorf("store: connector connection: %w", err))
	}
	return connection, nil
}

// SetTools stores the tools a validate found on one live connection of the
// customer's, with the digest of the list and when it was checked, replacing what it held. It
// writes only those three columns, as a credentials write leaves them out
// (credentialColumns), so neither puts back a stale copy of the other.
func (r *ConnectorConnections) SetTools(ctx context.Context, customerID, id string, tools []ConnectorTool, digest string, checkedAt time.Time) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	tools = orEmpty(tools)
	raw, err := json.Marshal(tools)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: encode connector connection tools: %w", err))
	}
	checked := checkedAt.UTC().Truncate(time.Microsecond)
	result, err := r.store.global.NewUpdate().Model((*ConnectorConnection)(nil)).
		Set("cached_tools = ?::jsonb", string(raw)).
		Set("tools_digest = ?", digest).
		Set("tools_checked_at = ?", checked).
		Set("updated_at = ?", time.Now().UTC()).
		Where("cc.customer_id = ?", customerID).
		Where("cc.id = ?", id).
		Where("cc.deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set connector connection tools: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set connector connection tools: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorConnection, id))
	}
	return nil
}

// Select lists one owner's live connections, or with filter.EveryUser
// every user's, newest first, one more than ConnectionLimit(filter.Limit).
func (r *ConnectorConnections) Select(ctx context.Context, customerID string, filter ConnectionFilter) ([]ConnectorConnection, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: a customer id is required"))
	}
	if filter.EveryUser && (filter.OwnerType != OwnerUser || filter.OwnerID != "") {
		return nil, stack.Wrap(fmt.Errorf("store: every user's connections are owner %q with no id, not %q with id %q", OwnerUser, filter.OwnerType, filter.OwnerID))
	}
	if !filter.EveryUser && !validOwner(filter.OwnerType, filter.OwnerID) {
		return nil, stack.Wrap(fmt.Errorf("store: owner %q with id %q: an app owner has no id and a user owner has one", filter.OwnerType, filter.OwnerID))
	}
	connections := []ConnectorConnection{}
	// connector_connections_owner_idx leads with (customer_id, owner_type), so every user's
	// are read from it too, and sorted after.
	query := r.store.global.NewSelect().Model(&connections).
		Where("customer_id = ?", customerID).
		Where("owner_type = ?", filter.OwnerType).
		Where("deleted_at IS NULL")
	if !filter.EveryUser {
		query = query.Where("owner_id = ?", filter.OwnerID)
	}
	if filter.ConnectorID != "" {
		query = query.Where("connector_id = ?", filter.ConnectorID)
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	if after := filter.After; after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.CreatedAt, after.ID)
	}
	err := query.
		Order("created_at DESC", "id DESC").
		Limit(ConnectionLimit(filter.Limit) + 1).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list connector connections: %w", err))
	}
	return connections, nil
}

// Delete soft deletes a live connection and drops its credentials at once,
// so nothing can use it from here on, whether or not a config still binds it. It is the
// forced delete; DeleteUnbound is the one that refuses a bound connection.
func (r *ConnectorConnections) Delete(ctx context.Context, customerID, id string) error {
	affected, err := softDeleteConnections(ctx, r.store.global, customerID, false, id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorConnection, id))
	}
	return nil
}

// DeleteUnbound soft deletes a live connection as Delete does, unless a live agent config of
// the customer's binds it as its fixed connection.
//
// It locks the connection FOR UPDATE first, and checks for a binding in the UPDATE after.
// A config write locks each connection it binds before it writes the binding
// (lockBoundConnections), so a bind in progress holds the delete at the lock until it
// commits. Under READ COMMITTED each statement sees the rows committed before it began
// (https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED), so the
// UPDATE, begun after the wait, sees a binding that committed during it. Checked in the
// locking statement itself, it would not: a statement that waits for a row lock re-checks
// only that row.
func (r *ConnectorConnections) DeleteUnbound(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var locked string
		err := tx.NewSelect().Model((*ConnectorConnection)(nil)).Column("cc.id").
			Where("cc.customer_id = ?", customerID).
			Where("cc.id = ?", id).
			Where("cc.deleted_at IS NULL").
			For("UPDATE").
			Scan(ctx, &locked)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrNoConnectorConnection, id)
		}
		if err != nil {
			return fmt.Errorf("store: lock connector connection: %w", err)
		}
		affected, err := softDeleteConnections(ctx, tx, customerID, true, id)
		if err != nil {
			return err
		}
		// The row is live and locked by this transaction, so only a binding stops the UPDATE.
		if affected == 0 {
			return fmt.Errorf("%w: %s", ErrConnectorConnectionBound, id)
		}
		return nil
	}))
}

// boundByConfig is true for a connection cc that a live agent config of the same customer
// binds as its fixed connection: the connections DeleteUnbound refuses.
//
// A binding is matched by containment on the shape the prototype stored
// ({"connection": {"type": "fixed", "connection_id": ...}}, ConnectorBinding in
// internal/store/models.go on codex/connector-support at cf62af0d), which T20 keeps. A
// customer has few configs and agent_configs_customer_idx finds them, so the column has no
// index of its own.
const boundByConfig = `EXISTS (
    SELECT 1 FROM agent_configs AS ac
    WHERE ac.customer_id = cc.customer_id
      AND ac.deleted_at IS NULL
      AND ac.connectors @> jsonb_build_array(jsonb_build_object(
          'connection', jsonb_build_object('type', 'fixed', 'connection_id', cc.id))))`

// softDeleteConnections marks live connections deleted and drops their credentials and their
// tool pins in one statement, and when unbound is set only those no config binds. It returns
// the rows it changed.
func softDeleteConnections(ctx context.Context, db bun.IDB, customerID string, unbound bool, ids ...string) (int64, error) {
	if customerID == "" || len(ids) == 0 || slices.Contains(ids, "") {
		return 0, stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	now := time.Now().UTC()
	query := db.NewUpdate().Model((*ConnectorConnection)(nil)).
		Set("status = ?", ConnectionDisconnected).
		Set("credentials_sealed = ?", []byte{}).
		Set("credentials_kek_version = 0").
		Set("expires_at = NULL").
		Set("deleted_at = ?", now).
		Set("updated_at = ?", now).
		Where("cc.customer_id = ?", customerID).
		Where("cc.id IN (?)", bun.In(ids)).
		Where("cc.deleted_at IS NULL")
	if unbound {
		query = query.Where("NOT " + boundByConfig)
	}
	// The connection's tool pins go in the same statement, and only with a connection it
	// deleted (ConnectorToolPin).
	var affected int64
	err := db.NewSelect().
		With("deleted", query.Returning("cc.id")).
		With("dropped", db.NewDelete().Model((*ConnectorToolPin)(nil)).Where("ctp.connection_id IN (SELECT id FROM deleted)")).
		TableExpr("deleted").ColumnExpr("count(*)").
		Scan(ctx, &affected)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete connector connection: %w", err))
	}
	return affected, nil
}

// DeletedConnection is one connection ConnectorConnections.DeleteUser or
// ConnectorDefinitions.Delete removed.
type DeletedConnection struct {
	ID          string
	ConnectorID string
	OwnerType   string
	// HadGrant says it still held stored credentials, which the delete revoked.
	HadGrant bool
}

// SelectGrantedOfUser is every live connection of one user of the customer's
// that holds credentials, oldest first: the grants DeleteUser drops.
func (r *ConnectorConnections) SelectGrantedOfUser(ctx context.Context, customerID, userID string) ([]ConnectorConnection, error) {
	var connections []ConnectorConnection
	err := r.store.global.NewSelect().Model(&connections).
		Where("cc.customer_id = ?", customerID).
		Where("cc.owner_type = ?", OwnerUser).
		Where("cc.owner_id = ?", userID).
		Where("cc.deleted_at IS NULL").
		Where("cc.credentials_sealed <> ''::bytea").
		Order("cc.created_at", "cc.id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list a user's granted connector connections: %w", err))
	}
	return connections, nil
}

// DeleteUser hard deletes every connection of one user of the
// customer's, live or already soft deleted, with their attempts, invocations and MCP event subscriptions, so
// the user's id and their accounts' ids are gone (architecture doc, «Add» item 9: «hard
// delete of owner_id and account_id on request»). The audit rows of those connections stay,
// with their session, request and attempt ids emptied. A user with none is not an error.
//
// The rows are locked FOR UPDATE before they go, so a credentials write in flight commits
// first or finds the row gone (ErrConnectorConnectionChanged). A connection the user makes
// after the delete began is not one it removes.
func (r *ConnectorConnections) DeleteUser(ctx context.Context, customerID, userID string) ([]DeletedConnection, error) {
	if customerID == "" || userID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a user id are required"))
	}
	var deleted []DeletedConnection
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var rows []struct {
			ID          string `bun:"id"`
			ConnectorID string `bun:"connector_id"`
			OwnerType   string `bun:"owner_type"`
			HadGrant    bool   `bun:"had_grant"`
		}
		err := tx.NewSelect().Model((*ConnectorConnection)(nil)).
			Column("cc.id", "cc.connector_id", "cc.owner_type").
			ColumnExpr("cc.credentials_sealed <> ''::bytea AS had_grant").
			Where("cc.customer_id = ?", customerID).
			Where("cc.owner_type = ?", OwnerUser).
			Where("cc.owner_id = ?", userID).
			For("UPDATE").
			Scan(ctx, &rows)
		if err != nil {
			return fmt.Errorf("store: lock a user's connector connections: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
			deleted = append(deleted, DeletedConnection{ID: row.ID, ConnectorID: row.ConnectorID, OwnerType: row.OwnerType, HadGrant: row.HadGrant})
		}
		// Attempts reference their connection with no cascade, so they go first. Invocations
		// cascade.
		if _, err := tx.NewDelete().Model((*ConnectorAuthorizationAttempt)(nil)).
			Where("caa.connection_id IN (?)", bun.In(ids)).Exec(ctx); err != nil {
			return fmt.Errorf("store: delete a user's authorization attempts: %w", err)
		}
		// Event subscriptions name their connection with no foreign key, so nothing cascades: their
		// sealed secrets and callback tokens go with the connections, in this transaction (the
		// single delete does it through ConnectionEventSubscriptions.DeleteConnection).
		if _, err := tx.NewDelete().Model((*ConnectionEventSubscription)(nil)).
			Where("ces.customer_id = ?", customerID).
			Where("ces.connection_id IN (?)", bun.In(ids)).Exec(ctx); err != nil {
			return fmt.Errorf("store: delete a user's connection event subscriptions: %w", err)
		}
		if _, err := tx.NewDelete().Model((*ConnectorConnection)(nil)).
			Where("cc.customer_id = ?", customerID).
			Where("cc.id IN (?)", bun.In(ids)).Exec(ctx); err != nil {
			return fmt.Errorf("store: delete a user's connector connections: %w", err)
		}
		// The audit keeps the connections' rows, which name no user, but its correlation ids
		// lead back to one: a session id joins agent_sessions.user_id, and a request id and an
		// attempt id are found in the access log beside the user's requests.
		if _, err := tx.NewUpdate().Model((*ConnectorAuditEvent)(nil)).
			Set("session_id = ''").
			Set("request_id = ''").
			Set("attempt_id = ''").
			Where("ca.customer_id = ?", customerID).
			Where("ca.connection_id IN (?)", bun.In(ids)).Exec(ctx); err != nil {
			return fmt.Errorf("store: unlink a user's connector audit: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}
	return deleted, nil
}

// ConnectionUse is one binding of a live agent config that names a connection as its fixed
// connection: what deleting the connection would break.
type ConnectionUse struct {
	ConfigID   string `bun:"config_id"`
	ConfigName string `bun:"config_name"`
	// Binding is the alias the config binds it under.
	Binding      string `bun:"binding"`
	ConnectionID string `bun:"connection_id"`
}

// Uses are the bindings of the customer's live agent configs that name
// each of ids as their fixed connection, by connection id, each list by config name and
// alias. A binding a session fills names no connection, so it is never one of them. It reads
// the shape boundByConfig matches.
func (r *ConnectorConnections) Uses(ctx context.Context, customerID string, ids []string) (map[string][]ConnectionUse, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: a customer id is required"))
	}
	uses := map[string][]ConnectionUse{}
	if len(ids) == 0 {
		return uses, nil
	}
	var rows []ConnectionUse
	// A row whose connectors is not an array binds nothing, rather than failing the read: the
	// column is jsonb, and a writer that skips the store can put anything there.
	err := r.store.global.NewSelect().
		TableExpr("agent_configs AS ac").
		Join("CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(ac.connectors) = 'array' THEN ac.connectors ELSE '[]'::jsonb END) AS binding").
		ColumnExpr("ac.id AS config_id, ac.name AS config_name").
		ColumnExpr("binding ->> 'name' AS binding").
		ColumnExpr("binding -> 'connection' ->> 'connection_id' AS connection_id").
		Where("ac.customer_id = ?", customerID).
		Where("ac.deleted_at IS NULL").
		Where("binding -> 'connection' ->> 'type' = 'fixed'").
		Where("binding -> 'connection' ->> 'connection_id' IN (?)", bun.In(ids)).
		OrderExpr("ac.name, ac.id, binding ->> 'name'").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: connector connection uses: %w", err))
	}
	for _, row := range rows {
		uses[row.ConnectionID] = append(uses[row.ConnectionID], row)
	}
	return uses, nil
}

// validOwner is the owner rule the migration's connector_connections_owner CHECK holds.
func validOwner(ownerType, ownerID string) bool {
	return ownerType == OwnerApp && ownerID == "" || ownerType == OwnerUser && ownerID != ""
}

// ConnectorConnectionClient is the OAuth client a connection's grant was issued to (AI-990
// F16), in columns of its own so it is read without unsealing the credentials. A client_id is
// not a secret (RFC 6749 section 2.2).
type ConnectorConnectionClient struct {
	Registration core.ClientRegistrationMethod `bun:"registration,notnull"`
	ClientID     string                        `bun:"id,notnull"`
	UpdatedAt    time.Time                     `bun:"updated_at,nullzero"`
}

// PutClient records the client a consent of the connection used, replacing
// the one an earlier consent recorded.
func (r *ConnectorConnections) PutClient(ctx context.Context, customerID, connectionID string, client ConnectorConnectionClient) error {
	if customerID == "" || connectionID == "" || client.ClientID == "" {
		return stack.Wrap(errors.New("store: a connection client needs a customer, a connection and a client id"))
	}
	_, err := r.store.global.NewUpdate().Model((*ConnectorConnection)(nil)).
		Set("client_registration = ?", client.Registration).
		Set("client_id = ?", client.ClientID).
		Set("client_updated_at = ?", time.Now().UTC().Truncate(time.Microsecond)).
		Where("id = ?", connectionID).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: put connection client: %w", err))
	}
	return nil
}

// SelectGrantedOfClient is every live connection of the customer's to the
// connector that holds credentials its consent got from the client of registration with
// clientID (ConnectorConnectionClient), oldest first: the grants that client's secret revokes.
func (r *ConnectorConnections) SelectGrantedOfClient(ctx context.Context, customerID, connectorID string,
	registration core.ClientRegistrationMethod, clientID string) ([]ConnectorConnection, error) {
	var connections []ConnectorConnection
	err := r.store.global.NewSelect().Model(&connections).
		Where("cc.customer_id = ?", customerID).
		Where("cc.connector_id = ?", connectorID).
		Where("cc.deleted_at IS NULL").
		Where("cc.credentials_sealed <> ''::bytea").
		Where("cc.client_registration = ?", registration).
		Where("cc.client_id = ?", clientID).
		Order("cc.created_at", "cc.id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list a client's granted connector connections: %w", err))
	}
	return connections, nil
}

// ConnectorConnectionValidation is the last validate of a connection (AI-1052): its status,
// its code (a stable reason, or the provider's HTTP status), its error and when it ran, and
// the revision of the credentials it checked. The error is the validate's with the
// credential's values cut out, and capped (api.storedError).
type ConnectorConnectionValidation struct {
	// Revision is the connection's revision (ConnectorConnection.Revision) whose credentials
	// the validate checked. A connection read at a later revision has newer credentials than
	// the validate saw.
	Revision  int       `bun:"revision,notnull"`
	Status    string    `bun:"status,notnull"`
	Code      string    `bun:"code,notnull"`
	Error     string    `bun:"error,notnull"`
	CheckedAt time.Time `bun:"checked_at,nullzero"`
}

// PutValidation records a validate of the connection, replacing the one an
// earlier validate recorded. Of two validates at once, the one of the later revision stays,
// and of two of the same revision the one that ran later, whichever writes last: a validate
// of credentials replaced while it ran never covers one of the new credentials.
func (r *ConnectorConnections) PutValidation(ctx context.Context, customerID, connectionID string, validation ConnectorConnectionValidation) error {
	if customerID == "" || connectionID == "" || validation.Status == "" || validation.CheckedAt.IsZero() {
		return stack.Wrap(errors.New("store: a connection validation needs a customer, a connection, a status and a time"))
	}
	_, err := r.store.global.NewUpdate().Model((*ConnectorConnection)(nil)).
		Set("validation_revision = ?", validation.Revision).
		Set("validation_status = ?", validation.Status).
		Set("validation_code = ?", validation.Code).
		Set("validation_error = ?", validation.Error).
		Set("validation_checked_at = ?", validation.CheckedAt).
		Where("id = ?", connectionID).
		Where("customer_id = ?", customerID).
		Where("validation_checked_at IS NULL OR (validation_revision, validation_checked_at) <= (?, ?)",
			validation.Revision, validation.CheckedAt).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: put connection validation: %w", err))
	}
	return nil
}

// ErrConnectorConnectionChanged says a credentials write found the connection no longer at the
// revision it expected, or deleted: someone else saved first, or the connection is gone.
// The writer reloads under the lock rather than overwrite what it did not see.
var ErrConnectorConnectionChanged = errors.New("store: connector connection changed while saving")

// credentialColumns are what a credentials write sets: the credential state itself
// (core.CredentialState: status, revision, stored credentials, expiry, last error) and what a
// consent captures beside it (account, scopes,
// metadata, when it connected, the definition revision it ran on). The prototype's list (SaveConnectorConnectionAtRevision at cf62af0d) with its
// credential columns renamed to credentials ones and metadata added, less cached_tools and
// tools_*: those have a writer of their own, and a credentials write would put back a stale copy.
var credentialColumns = []string{
	"status", "revision", "credentials_sealed", "credentials_kek_version", "expires_at", "last_error",
	"account_id", "granted_scopes", "metadata", "connected_at", "definition_revision", "updated_at",
}

// SaveAtRevision saves new stored credentials, already sealed for
// expectedRevision + 1, onto a connection that is live and still at expectedRevision, under
// the same lock WithLocked holds, so it never lands in the middle of a
// refresh. It always advances the revision by one. A locked callback can commit a status
// without advancing it (needs_reauthorization before a refresh), so a save at the same
// revision from a snapshot read before that would pass the compare-and-swap and put back the
// credentials the callback retired; a save of new credentials replaces them instead.
func (r *ConnectorConnections) SaveAtRevision(ctx context.Context, connection *ConnectorConnection, expectedRevision int) error {
	if connection.CustomerID == "" || connection.ID == "" {
		return errors.New("store: a customer and a connection id are required")
	}
	if expectedRevision < 1 {
		return errors.New("store: the expected revision is 1 or more")
	}
	if connection.Revision != expectedRevision+1 {
		return fmt.Errorf("store: a save is new credentials at revision %d, not %d", expectedRevision+1, connection.Revision)
	}
	return r.store.withCredentialLock(ctx, connection.CustomerID, connection.ID, func(conn bun.Conn) error {
		return r.saveAtRevision(ctx, conn, connection, expectedRevision)
	})
}

// WithLocked loads one live connection under the session-level advisory
// lock on its credential state and runs fn. fn may call checkpoint to commit the connection as it is
// now, before a side effect it cannot take back, and keeps the lock while it does; returning
// changed commits the final state the same way. Each commit is a compare-and-swap on the
// revision the previous one left. fn must not lock the same connection again: that is a
// second pool connection waiting on the first.
func (r *ConnectorConnections) WithLocked(
	ctx context.Context,
	customerID, id string,
	fn func(connection *ConnectorConnection, checkpoint func() error) (changed bool, err error),
) error {
	if customerID == "" || id == "" {
		return errors.New("store: a customer and a connection id are required")
	}
	return r.store.withCredentialLock(ctx, customerID, id, func(conn bun.Conn) error {
		var connection ConnectorConnection
		err := r.store.global.NewSelect().Conn(conn).Model(&connection).
			Where("customer_id = ?", customerID).
			Where("id = ?", id).
			Where("deleted_at IS NULL").
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrNoConnectorConnection, id)
		}
		if err != nil {
			return fmt.Errorf("store: load locked connector connection: %w", err)
		}
		expected := connection.Revision
		checkpoint := func() error {
			if err := r.saveAtRevision(ctx, conn, &connection, expected); err != nil {
				return err
			}
			expected = connection.Revision
			return nil
		}
		changed, err := fn(&connection, checkpoint)
		if err != nil || !changed {
			return err
		}
		return checkpoint()
	})
}

// saveAtRevision is the compare-and-swap every credentials write is: it matches only the live row
// still at expected, so of two writers that read the same revision one wins and the other
// gets ErrConnectorConnectionChanged. A write never moves the revision back, since that would
// make a blob sealed for an earlier revision open again. It runs detached from ctx, within
// credentialDetachedTimeout: the driver turns a passed deadline into a passed socket deadline
// (Conn.deadline in github.com/uptrace/bun/driver/pgdriver v1.2.18), which would drop a
// rotation the provider already made.
func (r *ConnectorConnections) saveAtRevision(ctx context.Context, conn bun.IConn, connection *ConnectorConnection, expected int) error {
	if connection.Revision < expected {
		return fmt.Errorf("store: revision %d is behind the stored %d", connection.Revision, expected)
	}
	if err := connection.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), credentialDetachedTimeout)
	defer cancel()
	connection.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	connection.CredentialsSealed = orEmpty(connection.CredentialsSealed)
	connection.Metadata = orEmptyMap(connection.Metadata)
	connection.GrantedScopes = orEmpty(connection.GrantedScopes)
	result, err := r.store.global.NewUpdate().Conn(conn).Model(connection).
		Column(credentialColumns...).
		Where("id = ?", connection.ID).
		Where("customer_id = ?", connection.CustomerID).
		Where("revision = ?", expected).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: save connector credentials: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: save connector credentials: %w", err)
	}
	if affected == 0 {
		return ErrConnectorConnectionChanged
	}
	return nil
}

// SelectByIdentity is every live connection of a connector, of any customer, whose
// inputs hold every pair of inputs and whose metadata hold every pair of metadata: the
// connections of the account a provider event names (core.Signal.Identity), split by whether
// each identity part is an input or a captured value. An event names fewer parts for every
// account that has them, such as a workspace uninstall that names the team and no user.
//
// It reads across customers because an event on a connector's own route is signed with the
// operator's secret, which every customer's connections to that connector share; a custom
// connector's id is its customer's alone (CustomPrefix), so no other customer's connection
// can match. The parts are compared one by one as stored (JSONB containment, @>), never by
// splitting the joined account id, whose parts may themselves hold the ":" it is joined with.
func (r *ConnectorConnections) SelectByIdentity(ctx context.Context, connectorID string, inputs, metadata map[string]string) ([]core.ConnectionRef, error) {
	if connectorID == "" || len(inputs)+len(metadata) == 0 {
		return nil, stack.Wrap(errors.New("store: a connector id and at least one identity part are required"))
	}
	inputsJSON, err := json.Marshal(nonNil(inputs))
	if err != nil {
		return nil, stack.Wrap(err)
	}
	metadataJSON, err := json.Marshal(nonNil(metadata))
	if err != nil {
		return nil, stack.Wrap(err)
	}
	var connections []ConnectorConnection
	err = r.store.global.NewSelect().Model(&connections).
		Column("customer_id", "id").
		Where("connector_id = ?", connectorID).
		Where("inputs @> ?::jsonb", string(inputsJSON)).
		Where("metadata @> ?::jsonb", string(metadataJSON)).
		Where("deleted_at IS NULL").
		Order("customer_id", "id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: connections by identity: %w", err))
	}
	refs := make([]core.ConnectionRef, 0, len(connections))
	for _, connection := range connections {
		refs = append(refs, core.ConnectionRef{CustomerID: connection.CustomerID, ConnectionID: connection.ID})
	}
	return refs, nil
}

// nonNil is pairs, or no pairs when it is nil, so it marshals as an object.
func nonNil(pairs map[string]string) map[string]string {
	if pairs == nil {
		return map[string]string{}
	}
	return pairs
}
