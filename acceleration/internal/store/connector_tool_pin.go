package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ConnectorToolPin is the schema digest one tool of one connection had when a session first
// offered it, for a tool a session binding grants by name alone
// (20261011150000_connector_tool_pins.sql). It holds for the grant the connection had then,
// its ConnectedAt: a reconnect is a new trust event, and the next session pins again.
type ConnectorToolPin struct {
	bun.BaseModel `bun:"table:connector_tool_pins,alias:ctp"`

	ConnectionID string `bun:"connection_id,pk"`
	ToolName     string `bun:"tool_name,pk"`
	SchemaDigest string `bun:"schema_digest,notnull"`
	// ConnectedAt is the connection's ConnectedAt when the pin was taken; nil for a connection
	// connected before connected_at was kept.
	ConnectedAt *time.Time `bun:"connected_at"`
	PinnedAt    time.Time  `bun:"pinned_at,notnull"`
}

// Validate refuses a pin with no tool or no digest to compare a later listing with.
func (p *ConnectorToolPin) Validate() error {
	return validate("connector tool pin").
		required("connection_id", p.ConnectionID).
		required("tool_name", p.ToolName).
		required("schema_digest", p.SchemaDigest).
		done()
}

// ConnectorToolPins are the schema digests connections' tools are pinned at.
type ConnectorToolPins struct {
	store *Store
}

// Select is the digests a connection's tools are pinned at, by tool name, for the
// grant that began at connectedAt: the connection's ConnectedAt as the caller read it. A pin
// taken under another grant is not one.
func (r *ConnectorToolPins) Select(ctx context.Context, connectionID string, connectedAt *time.Time) (map[string]string, error) {
	if connectionID == "" {
		return nil, stack.Wrap(errors.New("store: a connection id is required"))
	}
	var pins []ConnectorToolPin
	err := r.store.global.NewSelect().Model(&pins).
		Where("ctp.connection_id = ?", connectionID).
		Where("ctp.connected_at IS NOT DISTINCT FROM ?", connectedAt).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: connector tool pins: %w", err))
	}
	digests := make(map[string]string, len(pins))
	for _, pin := range pins {
		digests[pin.ToolName] = pin.SchemaDigest
	}
	return digests, nil
}

// Pin pins each tool of digests, by name, at its digest for the grant that
// began at connectedAt, and returns the pins that hold for that grant afterwards, as
// Select does.
//
// A pin is written once per grant: a tool already pinned for this grant keeps its digest,
// so of two sessions that pin at once the first one's stands and both read it back. A pin of
// an older grant is replaced; one of a newer grant, written by a router that read the
// connection after a reconnect this caller has not seen, is kept.
func (r *ConnectorToolPins) Pin(ctx context.Context, connectionID string, connectedAt *time.Time, digests map[string]string) (map[string]string, error) {
	if connectionID == "" {
		return nil, stack.Wrap(errors.New("store: a connection id is required"))
	}
	if len(digests) > 0 {
		now := time.Now().UTC()
		pins := make([]ConnectorToolPin, 0, len(digests))
		// In one order, so two sessions pinning the same tools at once take the rows' locks in
		// the same order and neither waits on the other in a cycle.
		for _, name := range slices.Sorted(maps.Keys(digests)) {
			pin := ConnectorToolPin{ConnectionID: connectionID, ToolName: name,
				SchemaDigest: digests[name], ConnectedAt: connectedAt, PinnedAt: now}
			if err := pin.Validate(); err != nil {
				return nil, stack.Wrap(err)
			}
			pins = append(pins, pin)
		}
		_, err := r.store.global.NewInsert().Model(&pins).
			On("CONFLICT (connection_id, tool_name) DO UPDATE").
			Set("schema_digest = EXCLUDED.schema_digest").
			Set("connected_at = EXCLUDED.connected_at").
			Set("pinned_at = EXCLUDED.pinned_at").
			Where("EXCLUDED.connected_at > ctp.connected_at OR (ctp.connected_at IS NULL AND EXCLUDED.connected_at IS NOT NULL)").
			Exec(ctx)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: pin connector tools: %w", err))
		}
	}
	return r.store.ConnectorToolPins.Select(ctx, connectionID, connectedAt)
}

// ErrConnectorToolPinMoved says a re-approval found the connection's grant, or a tool's pin,
// other than its caller read: another approve, a session's first pin or a consent came first.
// Nothing was written; reading again and retrying is the remedy.
var ErrConnectorToolPinMoved = errors.New("store: a connector tool pin moved")

// ApprovedTool is one tool a re-approval pins at SchemaDigest, the digest the provider lists
// now, for the connection's current grant, in place of PreviousSchemaDigest: the digest it
// was pinned at for that grant, "" when it was not pinned for it.
type ApprovedTool struct {
	Name                 string `json:"name"`
	SchemaDigest         string `json:"schema_digest"`
	PreviousSchemaDigest string `json:"previous_schema_digest,omitempty"`
}

// Reapprove pins each of tools at its SchemaDigest for connection's grant, the
// one that began at its ConnectedAt as the caller read it, and records one tool_reapproved
// audit row naming them, in one transaction: all of it or none.
//
// Each pin is a compare-and-set. It is written only while the connection's connected_at is
// still connection.ConnectedAt and the tool is still pinned at its PreviousSchemaDigest for
// that grant, or still unpinned for it when that is "". Otherwise nothing is written and the
// error is ErrConnectorToolPinMoved, so an approve that raced another, a session's first pin
// or a consent never replaces what that one wrote, nor brings back a pin it replaced. A
// connection deleted since is ErrNoConnectorConnection.
func (r *ConnectorToolPins) Reapprove(ctx context.Context, connection ConnectorConnection, requestID string, tools []ApprovedTool) error {
	if connection.CustomerID == "" || connection.ID == "" || len(tools) == 0 {
		return stack.Wrap(errors.New("store: a re-approval needs a customer, a connection and a tool"))
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	// By name, so two approves of the same tools take the pins' locks in one order, as
	// PinConnectorTools does.
	ordered := slices.SortedFunc(slices.Values(tools), func(a, b ApprovedTool) int { return strings.Compare(a.Name, b.Name) })
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// FOR SHARE: a consent or a credentials write that begins a new grant updates this row,
		// so one that comes later waits for this transaction, and one that came first is seen.
		var sameGrant bool
		err := tx.NewSelect().Model((*ConnectorConnection)(nil)).
			ColumnExpr("cc.connected_at IS NOT DISTINCT FROM ?", connection.ConnectedAt).
			Where("cc.customer_id = ? AND cc.id = ? AND cc.deleted_at IS NULL", connection.CustomerID, connection.ID).
			For("SHARE").
			Scan(ctx, &sameGrant)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrNoConnectorConnection, connection.ID)
		}
		if err != nil {
			return err
		}
		if !sameGrant {
			return ErrConnectorToolPinMoved
		}
		for _, tool := range ordered {
			var result sql.Result
			if tool.PreviousSchemaDigest != "" {
				result, err = tx.NewUpdate().Model((*ConnectorToolPin)(nil)).
					Set("schema_digest = ?", tool.SchemaDigest).
					Set("pinned_at = ?", now).
					Where("connection_id = ?", connection.ID).
					Where("tool_name = ?", tool.Name).
					Where("schema_digest = ?", tool.PreviousSchemaDigest).
					Where("connected_at IS NOT DISTINCT FROM ?", connection.ConnectedAt).
					Exec(ctx)
			} else {
				// A pin of an earlier grant is replaced, as PinConnectorTools replaces it; one of
				// this grant was written since the caller read none.
				pin := &ConnectorToolPin{ConnectionID: connection.ID, ToolName: tool.Name,
					SchemaDigest: tool.SchemaDigest, ConnectedAt: connection.ConnectedAt, PinnedAt: now}
				if err := pin.Validate(); err != nil {
					return err
				}
				result, err = tx.NewInsert().Model(pin).
					On("CONFLICT (connection_id, tool_name) DO UPDATE").
					Set("schema_digest = EXCLUDED.schema_digest").
					Set("connected_at = EXCLUDED.connected_at").
					Set("pinned_at = EXCLUDED.pinned_at").
					Where("EXCLUDED.connected_at > ctp.connected_at OR (ctp.connected_at IS NULL AND EXCLUDED.connected_at IS NOT NULL)").
					Exec(ctx)
			}
			if err != nil {
				return err
			}
			if written, err := result.RowsAffected(); err != nil || written != 1 {
				return errors.Join(ErrConnectorToolPinMoved, err)
			}
		}
		event := ConnectorAuditEvent{ID: newID(), CustomerID: connection.CustomerID,
			ConnectionID: connection.ID, ConnectorID: connection.ConnectorID, OwnerType: connection.OwnerType,
			Action: AuditToolReapproved, Revision: connection.Revision, RequestID: requestID,
			Tools: ordered, CreatedAt: now}
		if err := event.Validate(); err != nil {
			return err
		}
		_, err = tx.NewInsert().Model(&event).Exec(ctx)
		return err
	})
	if errors.Is(err, ErrConnectorToolPinMoved) || errors.Is(err, ErrNoConnectorConnection) {
		return stack.Wrap(err)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: reapprove connector tools: %w", err))
	}
	return nil
}
