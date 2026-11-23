package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ConnectorBrokenRevision marks one revision of a built-in connector as not working, as a later
// revision's manifest declared it (core.Manifest.BrokenRevisions). Table
// connector_broken_revisions; the seeder is its one writer.
type ConnectorBrokenRevision struct {
	bun.BaseModel `bun:"table:connector_broken_revisions,alias:cbr"`

	ConnectorID string `bun:"connector_id,pk"`
	Revision    int    `bun:"revision,pk"`
	Reason      string `bun:"reason,notnull"`
	// MarkedBy is the revision whose manifest declared it.
	MarkedBy  int       `bun:"marked_by,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// Validate refuses a mark the table refuses: on a built-in, with a reason, declared by a later
// revision.
func (m *ConnectorBrokenRevision) Validate() error {
	return validate("connector broken revision").
		required("connector_id", m.ConnectorID).
		check(!strings.HasPrefix(m.ConnectorID, CustomPrefix), "connector_id", "is a custom definition's, and only a built-in is marked").
		check(m.Revision >= 1, "revision", "is below 1").
		required("reason", m.Reason).
		check(m.MarkedBy > m.Revision, "marked_by", "is not a later revision than the one it marks").
		done()
}

// markBroken stores the marks manifest declares, in the seeder's transaction. A mark already
// stored is kept as it is, so marks only accumulate: a later revision that lists one no longer,
// or gives another reason, changes nothing.
func markBroken(ctx context.Context, tx bun.Tx, manifest core.Manifest) error {
	broken := manifest.Broken()
	if len(broken) == 0 {
		return nil
	}
	marks := make([]ConnectorBrokenRevision, 0, len(broken))
	for revision, reason := range broken {
		mark := ConnectorBrokenRevision{ConnectorID: manifest.ID, Revision: revision, Reason: reason,
			MarkedBy: manifest.Revision, CreatedAt: time.Now().UTC()}
		if err := mark.Validate(); err != nil {
			return err
		}
		marks = append(marks, mark)
	}
	_, err := tx.NewInsert().Model(&marks).On("CONFLICT (connector_id, revision) DO NOTHING").Exec(ctx)
	return err
}

// ConnectorBrokenRevisions are the marks the seeder stores on broken built-in revisions.
type ConnectorBrokenRevisions struct {
	store *Store
}

// Get is why revision of connector is marked broken, and whether it is.
func (r *ConnectorBrokenRevisions) Get(ctx context.Context, connectorID string, revision int) (string, bool, error) {
	var mark ConnectorBrokenRevision
	err := r.store.global.NewSelect().Model(&mark).
		Where("connector_id = ?", connectorID).
		Where("revision = ?", revision).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, stack.Wrap(fmt.Errorf("store: broken connector revision: %w", err))
	}
	return mark.Reason, true, nil
}
