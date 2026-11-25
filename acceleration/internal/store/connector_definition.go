package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// BuiltinCustomer is the customer the built-in connector definitions are stored under. It is
// empty because no caller can be it: every auth mode refuses an empty app id
// (internal/auth/auth.go) and api.CustomerFrom reports no customer for one, so no tenant can
// write a built-in or pass for its owner.
const BuiltinCustomer = ""

// CustomPrefix starts the id of every custom definition and of no built-in, so a customer's
// definition can never shadow a built-in and a lookup by id is never ambiguous. The prefix is
// the one the architecture's plan reserves for custom definitions
// (acceleration/docs/connectors/subtasks.md, T6 acceptance, on connectors/planning), which the
// core's custom_crm and custom_mtls_service fixtures already use. The migration checks it too.
const CustomPrefix = core.CustomPrefix

// ConnectorLabel is a connector id as a metric label: a built-in's own id, one of the
// catalog the router ships, or "custom" for every custom definition, whose id its customer
// chose, so the label has one value per built-in and one more.
func ConnectorLabel(id string) string {
	if strings.HasPrefix(id, CustomPrefix) {
		return "custom"
	}
	return id
}

// definitionLockSeed is the second argument to hashtextextended for the lock a revision is
// written under. Any value other than the 731 AgentLogs.Insert uses would do, so a customer's
// log writes never wait on its definitions; 833 is this table's issue, AI-833.
const definitionLockSeed = 833

// ErrNoConnectorDefinition says there is no such definition for this customer, built-in or
// its own. A sentinel, because asking for an id nobody defined is an ordinary 404, and a
// caller has to tell it apart from the database being down.
var ErrNoConnectorDefinition = errors.New("store: no such connector definition")

// Get is one revision of a connector's manifest. Built-ins are stored under
// BuiltinCustomer; a custom definition under the customer that made it.
type ConnectorDefinition struct {
	bun.BaseModel `bun:"table:connector_definitions,alias:cd"`

	CustomerID  string        `bun:"customer_id,pk"`
	ID          string        `bun:"id,pk"`
	Revision    int           `bun:"revision,pk"`
	Name        string        `bun:"name,notnull"`
	Category    string        `bun:"category,notnull"`
	Description string        `bun:"description,notnull"`
	Manifest    core.Manifest `bun:"manifest,type:jsonb,notnull"`
	// SyncHash is a fingerprint of the file the CLI last synced onto this revision
	// (20270201001200_connector_router_sync_hash.sql). Empty for a built-in, and for a revision
	// that was never synced or was written since by anything that is not a sync.
	SyncHash  string    `bun:"sync_hash,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// The limits on a sync hash, which the API's request schema documents (api.CustomConnectorRequest)
// and an import checks (importableDefinition). Up to 128 characters is a SHA-512 hex digest, well
// over the 32 of the MD5 hex digest the CLI computes for an agent (Agentrc.Hash in GetStream/cli
// internal/agentrc/agentrc.go), and printable ASCII without a space holds no NUL, which a Postgres
// text column refuses.
const (
	SyncHashMaxLength   = 128
	SyncHashPattern     = `^[!-~]*$`
	SyncHashDescription = "printable ASCII without a space"
)

var syncHash = regexp.MustCompile(SyncHashPattern)

// Validate refuses a revision the table's checks refuse: a custom id is a customer's and only a
// customer's (connector_definitions_custom_prefix), and revisions count from 1.
func (d *ConnectorDefinition) Validate() error {
	return validate("connector definition").
		required("id", d.ID).
		check((d.CustomerID == BuiltinCustomer) != strings.HasPrefix(d.ID, CustomPrefix), "id",
			"starts with "+CustomPrefix+" when a customer owns it and only then, so no custom definition shadows a built-in").
		check(d.Revision >= 1, "revision", "is below 1").
		required("name", d.Name).
		done()
}

// Resolve is the definition's manifest resolved for one connection (core.Manifest.Resolve),
// marked built-in when the definition is stored under BuiltinCustomer.
func (d ConnectorDefinition) Resolve(scheme string, inputs, metadata map[string]string) (core.ResolvedManifest, error) {
	resolved, err := d.Manifest.Resolve(scheme, inputs, metadata)
	if err != nil {
		return core.ResolvedManifest{}, err
	}
	resolved.Builtin = d.CustomerID == BuiltinCustomer
	return resolved, nil
}

// ConnectorDefinitions are the connector manifests, built-in and custom, by revision, in the
// shared database.
type ConnectorDefinitions struct {
	store *Store
}

// Seed stores every built-in manifest in fsys, one <id>.yaml per
// connector, as a built-in definition at the revision the file names. The file's author
// numbers built-in revisions, the way a migration is numbered, because two router builds can
// share a database at once: old and new pods in a rolling deploy, a rollback, or a branch
// build beside an accelerate build on staging. Each start of either build then finds its own
// revision already stored and changes nothing, rather than storing its manifest as the next
// revision and flipping latest back and forth. Every file is parsed before any is written, so
// one invalid manifest stores none of them.
func (r *ConnectorDefinitions) Seed(ctx context.Context, fsys fs.FS) error {
	manifests, err := builtinManifests(fsys)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		if err := r.seedBuiltin(ctx, manifest); err != nil {
			return err
		}
	}
	return nil
}

// Revise stores a customer's own definition as the next revision of its
// id, or returns the latest revision when that already says the same. The manifest's own
// revision is replaced: the store numbers revisions. It is no sync, so the revision it returns
// carries no sync hash.
func (r *ConnectorDefinitions) Revise(ctx context.Context, customerID string, manifest core.Manifest) (ConnectorDefinition, error) {
	return r.ReviseSynced(ctx, customerID, manifest, "")
}

// ReviseSynced is Revise for a write that carries the fingerprint of the file it came from,
// which the revision it returns then carries: empty for a write that is not a sync.
func (r *ConnectorDefinitions) ReviseSynced(ctx context.Context, customerID string, manifest core.Manifest, syncHash string) (ConnectorDefinition, error) {
	if customerID == BuiltinCustomer {
		return ConnectorDefinition{}, stack.Wrap(errCustomerRequired)
	}
	probe := ConnectorDefinition{CustomerID: customerID, ID: manifest.ID, Revision: 1, Name: manifest.Name}
	if err := probe.Validate(); err != nil {
		return ConnectorDefinition{}, stack.Wrap(err)
	}
	return r.saveRevision(ctx, customerID, manifest, syncHash)
}

// Get returns one revision of a definition the customer can see: a built-in
// or its own.
func (r *ConnectorDefinitions) Get(ctx context.Context, customerID, id string, revision int) (ConnectorDefinition, error) {
	if customerID == "" || id == "" {
		return ConnectorDefinition{}, stack.Wrap(errors.New("store: a customer and a connector id are required"))
	}

	return connectorDefinition(ctx, r.store.global, customerID, id, revision)
}

// connectorDefinition is one revision of a definition the customer can see, read on db.
func connectorDefinition(ctx context.Context, db bun.IDB, customerID, id string, revision int) (ConnectorDefinition, error) {
	var definition ConnectorDefinition
	err := db.NewSelect().Model(&definition).
		Apply(visibleTo(customerID)).
		Where("id = ?", id).
		Where("revision = ?", revision).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("%w: %s revision %d", ErrNoConnectorDefinition, id, revision))
	}
	if err != nil {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("store: connector definition: %w", err))
	}
	return definition, nil
}

// Latest returns the newest revision of a definition the customer can see.
func (r *ConnectorDefinitions) Latest(ctx context.Context, customerID, id string) (ConnectorDefinition, error) {
	return latestVisibleDefinition(ctx, r.store.global, customerID, id)
}

// Definitions reads the connector definitions a customer can see: ConnectorDefinitions.
type Definitions interface {
	Latest(ctx context.Context, customerID, id string) (ConnectorDefinition, error)
}

// latestVisibleDefinition is ConnectorDefinitions.Latest read on db.
func latestVisibleDefinition(ctx context.Context, db bun.IDB, customerID, id string) (ConnectorDefinition, error) {
	if customerID == "" || id == "" {
		return ConnectorDefinition{}, stack.Wrap(errors.New("store: a customer and a connector id are required"))
	}
	definition, err := latestDefinition(ctx, db, visibleTo(customerID), id)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorDefinition, id))
	}
	if err != nil {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("store: latest connector definition: %w", err))
	}
	return definition, nil
}

// LatestBuiltin returns the newest revision of a built-in definition, for
// a caller that is no customer: a provider delivering an event to a connector's route.
func (r *ConnectorDefinitions) LatestBuiltin(ctx context.Context, id string) (ConnectorDefinition, error) {
	if id == "" {
		return ConnectorDefinition{}, stack.Wrap(errors.New("store: a connector id is required"))
	}
	definition, err := latestDefinition(ctx, r.store.global, ownedBy(BuiltinCustomer), id)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorDefinition, id))
	}
	if err != nil {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("store: latest built-in connector definition: %w", err))
	}
	return definition, nil
}

// How many connector definitions are handed back at once: the session list's numbers
// (sessions.go), since a catalog is read a page at a time in a picker the same way a
// sidebar is. Not measured for a catalog.
const (
	defaultConnectorDefinitionLimit = 25
	maxConnectorDefinitionLimit     = 200
)

// ConnectorDefinitionLimit is the page size a definition list uses for the limit asked for.
// Select returns one row more than this.
func ConnectorDefinitionLimit(asked int) int {
	return ClampLimit(asked, defaultConnectorDefinitionLimit, maxConnectorDefinitionLimit)
}

// ConnectorDefinitionFilter narrows a definition list and says where its page starts.
type ConnectorDefinitionFilter struct {
	// Text keeps the definitions whose id, name, category or description holds it, ignoring
	// case, as the prototype's catalog search did (internal/api/connectors.go:72 on
	// codex/connector-support at cf62af0d). It is matched on the newest revision only.
	Text   string
	Cursor *ConnectorDefinitionPosition
	Limit  int
}

// ConnectorDefinitionPosition is the last definition of a page. Custom stands in for the
// customer id, so a cursor holds nothing of whose it was and the order is the same for
// every customer: built-ins, then the customer's own.
type ConnectorDefinitionPosition struct {
	Custom bool   `json:"c"`
	ID     string `json:"id"`
}

// Select returns the newest revision of every definition the customer can
// see that matches the filter: the built-ins first, then its own, each by id. It returns up
// to one more than ConnectorDefinitionLimit, so a caller can tell the page is not the last.
func (r *ConnectorDefinitions) Select(ctx context.Context, customerID string, filter ConnectorDefinitionFilter) ([]ConnectorDefinition, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}

	// The newest revision first, then the text matched on it, so an older revision that
	// matched cannot stand in for a newer one that does not.
	latest := r.store.global.NewSelect().Model((*ConnectorDefinition)(nil)).
		DistinctOn("customer_id, id").
		Apply(visibleTo(customerID)).
		Order("customer_id", "id", "revision DESC")
	if after := filter.Cursor; after != nil {
		// customer_id <> '' is false for a built-in, so it sorts first, as customer_id does.
		latest = latest.Where("(customer_id <> ?, id) > (?, ?)", BuiltinCustomer, after.Custom, after.ID)
	}
	var definitions []ConnectorDefinition
	query := r.store.global.NewSelect().Model(&definitions).ModelTableExpr("(?) AS cd", latest)
	if filter.Text != "" {
		escaped := likeEscaper.Replace(filter.Text)
		// Each field on its own, so a query cannot match across the boundary of two.
		query = query.Where("(cd.id ILIKE ?0 OR cd.name ILIKE ?0 OR cd.category ILIKE ?0 OR cd.description ILIKE ?0)", "%"+escaped+"%")
	}
	err := query.
		OrderExpr("cd.customer_id, cd.id").
		Limit(ConnectorDefinitionLimit(filter.Limit) + 1).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list connector definitions: %w", err))
	}
	return definitions, nil
}

// saveRevision stores a custom manifest as the next revision of its id under the customer,
// unless the latest revision already says the same, in which case that one is returned with
// syncHash recorded on it. A custom definition has one writer, the customer through the API,
// so the store numbers it.
//
// It runs under a transaction-scoped advisory lock on the customer and id, so two routers
// starting at once, or two requests creating the same custom id, take turns: the second
// finds the first's revision and either matches it or follows it. The primary key would
// refuse a duplicate revision anyway; the lock is what turns that into a wait, not an error.
func (r *ConnectorDefinitions) saveRevision(ctx context.Context, customerID string, manifest core.Manifest, syncHash string) (ConnectorDefinition, error) {
	var saved ConnectorDefinition
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, customerID+"/"+manifest.ID, definitionLockSeed); err != nil {
			return err
		}

		latest, err := latestDefinition(ctx, tx, ownedBy(customerID), manifest.ID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			manifest.Revision = 1
		case err != nil:
			return err
		default:
			same, err := sameManifest(latest.Manifest, manifest)
			if err != nil {
				return err
			}
			if same {
				saved = latest
				if latest.SyncHash == syncHash {
					return nil
				}
				// The same manifest, from a file of another fingerprint or from a write that is no
				// sync: no new revision, but the hash is this write's, so the CLI's next sync of
				// that file finds it unchanged, and one after an edit elsewhere does not.
				saved.SyncHash = syncHash
				_, err := tx.NewUpdate().Model(&saved).Column("sync_hash").WherePK().Exec(ctx)
				return err
			}
			manifest.Revision = latest.Revision + 1
			// Another manifest under the latest's hash is no sync of the file that hash names,
			// such as a read sent back edited, so the new revision carries none.
			if syncHash == latest.SyncHash {
				syncHash = ""
			}
		}

		// Validated as it is stored, with the revision it is stored under.
		if err := manifest.Validate(); err != nil {
			return err
		}
		saved = ConnectorDefinition{
			CustomerID:  customerID,
			ID:          manifest.ID,
			Revision:    manifest.Revision,
			Name:        manifest.Name,
			Category:    manifest.Category,
			Description: manifest.Description,
			Manifest:    manifest,
			SyncHash:    syncHash,
			CreatedAt:   time.Now().UTC(),
		}
		if err := saved.Validate(); err != nil {
			return err
		}
		// Read back what Postgres stored, which keeps microseconds, so the answer to a create
		// is the row a later read returns.
		_, err = tx.NewInsert().Model(&saved).Returning("created_at").Exec(ctx)
		return err
	})
	if err != nil {
		return ConnectorDefinition{}, stack.Wrap(fmt.Errorf("store: save connector definition %s: %w", manifest.ID, err))
	}
	return saved, nil
}

// seedBuiltin stores a built-in manifest at the revision its file names, under the same lock
// saveRevision takes, so routers starting at once take turns:
//   - that revision is stored and says the same: nothing to do, the usual restart;
//   - that revision is stored and says something else: the file was edited without a new
//     revision, which is refused so a connection pinned to it never reads two manifests;
//   - it is not stored: it is stored now.
//
// The latest is the highest revision, so an older build starting last, even on a database
// that has never seen its revision, cannot make its manifest latest again. Reverting a
// manifest is a new revision whose content is the old one.
func (r *ConnectorDefinitions) seedBuiltin(ctx context.Context, manifest core.Manifest) error {
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, BuiltinCustomer+"/"+manifest.ID, definitionLockSeed); err != nil {
			return err
		}

		var stored ConnectorDefinition
		err := tx.NewSelect().Model(&stored).
			Where("customer_id = ?", BuiltinCustomer).
			Where("id = ?", manifest.ID).
			Where("revision = ?", manifest.Revision).
			Scan(ctx)
		switch {
		case err == nil:
			same, err := sameManifest(stored.Manifest, manifest)
			if err != nil {
				return err
			}
			if !same {
				return fmt.Errorf("%s.yaml says revision %d, which is already stored with other content: give the change a new revision", manifest.ID, manifest.Revision)
			}
			return markBroken(ctx, tx, manifest)
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		seeded := ConnectorDefinition{
			CustomerID:  BuiltinCustomer,
			ID:          manifest.ID,
			Revision:    manifest.Revision,
			Name:        manifest.Name,
			Category:    manifest.Category,
			Description: manifest.Description,
			Manifest:    manifest,
			CreatedAt:   time.Now().UTC(),
		}
		if err := seeded.Validate(); err != nil {
			return err
		}
		_, err = tx.NewInsert().Model(&seeded).Exec(ctx)
		if err != nil {
			return err
		}
		return markBroken(ctx, tx, manifest)
	})
	if err != nil {
		return fmt.Errorf("store: seed connector definition %s: %w", manifest.ID, err)
	}
	return nil
}

// How a connection's pinned revision compares with its connector's (DefinitionStatus.Status).
const (
	// DefinitionCurrent: the connection reads its connector's latest revision, or one that
	// differs from it only in what no connection reads (core.Manifest.ReadsAsConnection).
	DefinitionCurrent = "current"
	// DefinitionOutdated: a later revision changes what the connection reads, and the pinned
	// one is not marked broken.
	DefinitionOutdated = "outdated"
	// DefinitionBroken: a later revision marked the pinned one broken.
	DefinitionBroken = "broken"
)

// DefinitionStatus is how one connection's pinned revision compares with its connector's.
type DefinitionStatus struct {
	Status string
	// Reason is the mark's, when Status is DefinitionBroken.
	Reason string
}

// Statuses is the DefinitionStatus of each of the customer's connections, by
// connection id, in at most three reads whatever their number: the latest revision of each
// connector, the revisions the connections behind it pin, and the marks on them.
func (r *ConnectorDefinitions) Statuses(ctx context.Context, customerID string, connections []ConnectorConnection) (map[string]DefinitionStatus, error) {
	statuses := make(map[string]DefinitionStatus, len(connections))
	if len(connections) == 0 {
		return statuses, nil
	}
	ids := make([]string, 0, len(connections))
	for _, connection := range connections {
		if !slices.Contains(ids, connection.ConnectorID) {
			ids = append(ids, connection.ConnectorID)
		}
	}
	var latest []ConnectorDefinition
	err := r.store.global.NewSelect().Model(&latest).
		DistinctOn("cd.id").Column("id", "revision", "manifest").
		Apply(visibleTo(customerID)).
		Where("cd.id IN (?)", bun.In(ids)).
		OrderExpr("cd.id, cd.revision DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: latest connector revisions: %w", err))
	}
	newest := make(map[string]ConnectorDefinition, len(latest))
	for _, definition := range latest {
		newest[definition.ID] = definition
	}
	pinned, err := r.pinnedBehind(ctx, customerID, connections, newest)
	if err != nil {
		return nil, err
	}
	var marks []ConnectorBrokenRevision
	err = r.store.global.NewSelect().Model(&marks).Where("connector_id IN (?)", bun.In(ids)).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: broken connector revisions: %w", err))
	}
	for _, connection := range connections {
		status := DefinitionStatus{Status: DefinitionCurrent}
		if connection.DefinitionRevision < newest[connection.ConnectorID].Revision {
			// A pinned revision the customer cannot read is the zero manifest here, which reads
			// as no stored one does.
			at := pinned[pinnedRevision{connection.ConnectorID, connection.DefinitionRevision}]
			if !at.ReadsAsConnection(newest[connection.ConnectorID].Manifest) {
				status.Status = DefinitionOutdated
			}
		}
		for _, mark := range marks {
			if mark.ConnectorID == connection.ConnectorID && mark.Revision == connection.DefinitionRevision {
				status = DefinitionStatus{Status: DefinitionBroken, Reason: mark.Reason}
			}
		}
		statuses[connection.ID] = status
	}
	return statuses, nil
}

// pinnedRevision is one revision of one connector.
type pinnedRevision struct {
	connectorID string
	revision    int
}

// pinnedBehind is the manifest of each revision a connection pins that is older than its
// connector's newest, in one read, or none when no connection is behind.
func (r *ConnectorDefinitions) pinnedBehind(ctx context.Context, customerID string, connections []ConnectorConnection, newest map[string]ConnectorDefinition) (map[pinnedRevision]core.Manifest, error) {
	var ids []string
	var revisions []int
	for _, connection := range connections {
		if connection.DefinitionRevision >= newest[connection.ConnectorID].Revision {
			continue
		}
		if !slices.Contains(ids, connection.ConnectorID) {
			ids = append(ids, connection.ConnectorID)
		}
		if !slices.Contains(revisions, connection.DefinitionRevision) {
			revisions = append(revisions, connection.DefinitionRevision)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var definitions []ConnectorDefinition
	err := r.store.global.NewSelect().Model(&definitions).
		Column("id", "revision", "manifest").
		Apply(visibleTo(customerID)).
		Where("cd.id IN (?)", bun.In(ids)).
		Where("cd.revision IN (?)", bun.In(revisions)).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: pinned connector revisions: %w", err))
	}
	pinned := make(map[pinnedRevision]core.Manifest, len(definitions))
	for _, definition := range definitions {
		pinned[pinnedRevision{definition.ID, definition.Revision}] = definition.Manifest
	}
	return pinned, nil
}

// ErrConnectorDefinitionInUse says an unforced delete found a live connection made from the
// custom definition, or a live agent config binding it. DeletedConnector.Uses names them.
var ErrConnectorDefinitionInUse = errors.New("store: the connector is in use")

// ConnectorUses are what names one custom definition: what deleting it would break.
type ConnectorUses struct {
	// Connections are the ids of its live connections, app-owned and users', oldest first.
	Connections []string
	// Bindings are the bindings of the customer's live agent configs that name it, fixed or
	// chosen per session, by config name and alias.
	Bindings []ConnectorBindingUse
}

// ConnectorBindingUse is one binding of a live agent config that names a connector.
type ConnectorBindingUse struct {
	ConfigID   string `bun:"config_id"`
	ConfigName string `bun:"config_name"`
	// Binding is the alias the config binds it under.
	Binding string `bun:"binding"`
}

// DeletedConnector is what ConnectorDefinitions.Delete found and removed.
type DeletedConnector struct {
	// Uses is what named the definition when the delete looked, set on ErrConnectorDefinitionInUse too.
	Uses ConnectorUses
	// Connections are the live connections a forced delete removed.
	Connections []DeletedConnection
}

// Delete removes every revision of one of the customer's own definitions.
// Unforced, a live connection made from it or a live agent config binding it refuses the
// delete with ErrConnectorDefinitionInUse, and nothing changes. Forced, its live connections
// are deleted as a forced connection delete deletes one (softDeleteConnections: credentials and
// tool pins dropped), and the bindings are left in place, naming a connector that no longer
// exists, as a forced connection delete leaves its bindings. Either way the rows that hold the
// customer's sealed secrets for the connector go in the same transaction: its OAuth client and
// provider app (connector_oauth_clients), its configuration token and its event destinations
// with their pending deliveries, so no secret outlives the connector it was for. Only the
// customer's own rows are read, so a built-in, stored under BuiltinCustomer, is never deleted:
// its id is ErrNoConnectorDefinition, as one nobody defined is.
//
// Every revision is locked FOR UPDATE first, under the lock saveRevision takes. A writer of a
// row that names the definition, a connection, an OAuth client or a config's binding, locks
// it FOR KEY SHARE before its write (lockCustomDefinitions), so of a delete and that write one
// always waits for the other: the write that came first is seen by the check that follows the
// lock, and one that came second finds no definition. The connections are locked FOR NO KEY
// UPDATE, which a config save's FOR KEY SHARE on the connections it binds does not wait for
// (https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS, table 13.3),
// so a save holding a connection while it waits for the definition cannot deadlock with this.
func (r *ConnectorDefinitions) Delete(ctx context.Context, customerID, id string, force bool) (DeletedConnector, error) {
	if customerID == BuiltinCustomer || id == "" {
		return DeletedConnector{}, stack.Wrap(errors.New("store: a customer and a connector id are required"))
	}
	var deleted DeletedConnector
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, customerID+"/"+id, definitionLockSeed); err != nil {
			return err
		}
		var revisions []int
		err := tx.NewSelect().Model((*ConnectorDefinition)(nil)).Column("revision").
			Where("customer_id = ?", customerID).
			Where("id = ?", id).
			For("UPDATE").
			Scan(ctx, &revisions)
		if err != nil {
			return fmt.Errorf("store: lock connector definition: %w", err)
		}
		if len(revisions) == 0 {
			return fmt.Errorf("%w: %s", ErrNoConnectorDefinition, id)
		}
		var connections []struct {
			ID        string `bun:"id"`
			OwnerType string `bun:"owner_type"`
			HadGrant  bool   `bun:"had_grant"`
		}
		err = tx.NewSelect().Model((*ConnectorConnection)(nil)).
			Column("cc.id", "cc.owner_type").
			ColumnExpr("cc.credentials_sealed <> ''::bytea AS had_grant").
			Where("cc.customer_id = ?", customerID).
			Where("cc.connector_id = ?", id).
			Where("cc.deleted_at IS NULL").
			Order("cc.created_at", "cc.id").
			For("NO KEY UPDATE").
			Scan(ctx, &connections)
		if err != nil {
			return fmt.Errorf("store: lock the connector's connections: %w", err)
		}
		for _, connection := range connections {
			deleted.Uses.Connections = append(deleted.Uses.Connections, connection.ID)
		}
		// A row whose connectors is not an array binds nothing, as in ConnectorConnections.Uses.
		err = tx.NewSelect().
			TableExpr("agent_configs AS ac").
			Join("CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(ac.connectors) = 'array' THEN ac.connectors ELSE '[]'::jsonb END) AS binding").
			ColumnExpr("ac.id AS config_id, ac.name AS config_name").
			ColumnExpr("binding ->> 'name' AS binding").
			Where("ac.customer_id = ?", customerID).
			Where("ac.deleted_at IS NULL").
			Where("binding ->> 'connector_id' = ?", id).
			OrderExpr("ac.name, ac.id, binding ->> 'name'").
			Scan(ctx, &deleted.Uses.Bindings)
		if err != nil {
			return fmt.Errorf("store: connector binding uses: %w", err)
		}
		if !force && (len(deleted.Uses.Connections) > 0 || len(deleted.Uses.Bindings) > 0) {
			return fmt.Errorf("%w: %s", ErrConnectorDefinitionInUse, id)
		}
		if len(connections) > 0 {
			if _, err := softDeleteConnections(ctx, tx, customerID, false, deleted.Uses.Connections...); err != nil {
				return err
			}
		}
		for _, connection := range connections {
			deleted.Connections = append(deleted.Connections, DeletedConnection{
				ID: connection.ID, ConnectorID: id, OwnerType: connection.OwnerType, HadGrant: connection.HadGrant,
			})
		}
		// The deliveries go with their destination (ON DELETE CASCADE).
		for _, model := range []any{(*ConnectorOAuthClient)(nil), (*ConnectorConfigToken)(nil), (*EventDestination)(nil)} {
			q := tx.NewDelete().Model(model).Where("customer_id = ? AND connector_id = ?", customerID, id)
			if _, err := q.Exec(ctx); err != nil {
				return fmt.Errorf("store: delete the connector's %s: %w", q.GetTableName(), err)
			}
		}
		_, err = tx.NewDelete().Model((*ConnectorDefinition)(nil)).
			Where("customer_id = ?", customerID).
			Where("id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: delete connector definition: %w", err)
		}
		return nil
	})
	if err != nil {
		return deleted, stack.Wrap(err)
	}
	return deleted, nil
}

// lockCustomDefinitions locks every revision of the customer's own definitions among ids FOR
// KEY SHARE until tx ends, and returns the custom ids among them that have none. A writer of
// a row naming a custom definition calls it before its write, so ConnectorDefinitions.Delete
// and the write take turns. A built-in is never deleted, so it is neither locked nor reported.
// FOR KEY SHARE, not FOR SHARE, as in lockBoundConnections: definitions are never updated,
// and nothing but the delete needs to wait.
func lockCustomDefinitions(ctx context.Context, tx bun.Tx, customerID string, ids []string) ([]string, error) {
	var custom []string
	for _, id := range ids {
		if strings.HasPrefix(id, CustomPrefix) && !slices.Contains(custom, id) {
			custom = append(custom, id)
		}
	}
	if len(custom) == 0 {
		return nil, nil
	}
	var live []string
	err := tx.NewSelect().Model((*ConnectorDefinition)(nil)).Column("id").
		Where("customer_id = ?", customerID).
		Where("id IN (?)", bun.In(custom)).
		For("KEY SHARE").
		Scan(ctx, &live)
	if err != nil {
		return nil, fmt.Errorf("store: lock connector definitions: %w", err)
	}
	return slices.DeleteFunc(custom, func(id string) bool { return slices.Contains(live, id) }), nil
}

// latestDefinition is the newest revision of id among the definitions scope keeps.
func latestDefinition(ctx context.Context, db bun.IDB, scope func(*bun.SelectQuery) *bun.SelectQuery, id string) (ConnectorDefinition, error) {
	var definition ConnectorDefinition
	err := db.NewSelect().Model(&definition).
		Apply(scope).
		Where("id = ?", id).
		Order("revision DESC").
		Limit(1).
		Scan(ctx)
	return definition, stack.Wrap(err)
}

// visibleTo keeps the definitions an app sees: the built-ins and its own, less the internal
// ones unless the app's settings say internal_connectors. customer_id is the app's id, so the
// flag is read in the same query. The JSON is compared as jsonb, so a setting that is not a
// boolean hides the internal definitions rather than failing every read.
//
// A built-in is internal at every revision when its latest revision says so, so making a
// public built-in internal hides the revisions before it too, and making it public shows them.
// A custom definition is internal revision by revision, as it was before.
func visibleTo(customerID string) func(*bun.SelectQuery) *bun.SelectQuery {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where("cd.customer_id IN (?, ?)", BuiltinCustomer, customerID).
			Where("(CASE WHEN cd.customer_id = ? THEN ("+
				"SELECT lr.manifest->'internal' FROM connector_definitions AS lr "+
				"WHERE lr.customer_id = cd.customer_id AND lr.id = cd.id ORDER BY lr.revision DESC LIMIT 1"+
				") ELSE cd.manifest->'internal' END IS DISTINCT FROM 'true'::jsonb OR EXISTS ("+
				"SELECT 1 FROM apps AS ap WHERE ap.id = ? AND ap.settings->'internal_connectors' = 'true'::jsonb))",
				BuiltinCustomer, customerID)
	}
}

// ownedBy keeps one customer's definitions, or the built-ins for BuiltinCustomer.
func ownedBy(customerID string) func(*bun.SelectQuery) *bun.SelectQuery {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where("cd.customer_id = ?", customerID)
	}
}

// builtinManifests parses every *.yaml in fsys as a built-in manifest. Each must be valid,
// be named for its id, and not take the custom prefix; an error names the file, and the
// manifest's own error names the field.
func builtinManifests(fsys fs.FS) ([]core.Manifest, error) {
	paths, err := fs.Glob(fsys, "*.yaml")
	if err != nil {
		return nil, fmt.Errorf("store: built-in connectors: %w", err)
	}
	manifests := make([]core.Manifest, 0, len(paths))
	for _, path := range paths {
		raw, err := fs.ReadFile(fsys, path)
		if err != nil {
			return nil, fmt.Errorf("store: built-in connector %s: %w", path, err)
		}
		manifest, err := core.ParseManifest(raw)
		if err != nil {
			return nil, fmt.Errorf("store: built-in connector %s: %w", path, err)
		}
		// The file name is the id, so the file to edit for a connector is never in doubt.
		if name := strings.TrimSuffix(path, ".yaml"); manifest.ID != name {
			return nil, fmt.Errorf("store: built-in connector %s: id %q is not the file name %q", path, manifest.ID, name)
		}
		if strings.HasPrefix(manifest.ID, CustomPrefix) {
			return nil, fmt.Errorf("store: built-in connector %s: id %q takes the %s prefix kept for custom definitions", path, manifest.ID, CustomPrefix)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

// sameManifest is whether two manifests say the same thing, which is what decides whether a
// seeded manifest is a new revision. Both are compared as the JSON core.Manifest marshals to,
// with the revision left out: the YAML's comments, spacing, key order and quoting are gone by
// then, and Go writes struct fields in declaration order and map keys sorted, so equal
// manifests marshal to equal bytes. The stored side is decoded into the current
// core.Manifest first, so a key that model no longer has does not count as a change.
func sameManifest(stored, seeded core.Manifest) (bool, error) {
	stored.Revision, seeded.Revision = 0, 0
	a, err := json.Marshal(stored)
	if err != nil {
		return false, err
	}
	b, err := json.Marshal(seeded)
	if err != nil {
		return false, err
	}
	return bytes.Equal(a, b), nil
}
