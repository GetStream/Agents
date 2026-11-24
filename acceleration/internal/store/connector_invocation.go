package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// How a connector tool call failed (ConnectorInvocation.ErrorType). The set is the subtask's
// (subtasks.md T29 on connectors/planning); the first four are the architecture doc's
// («Add» item 8), after ElevenLabs' tool executions (competitor-analysis.md, «Call history»).
const (
	// InvocationCustomerAuth: the provider refused the connection's credential, or the
	// connection had none to give. Whoever owns it has to reconnect.
	InvocationCustomerAuth = "customer_auth"
	// InvocationExternalServer: the provider answered with a failure, or could not be reached.
	InvocationExternalServer = "external_server"
	// InvocationClientTimeout: the connection's own client gave up on the request before the
	// provider answered, and before the binding's deadline.
	InvocationClientTimeout = "client_timeout"
	// InvocationOutcomeUnknown: the call was sent and cut off before it answered, by the
	// binding's deadline or an interrupted turn, so the provider may have done it.
	InvocationOutcomeUnknown = "outcome_unknown"
	// InvocationDenied: the router refused the call before anything was sent.
	InvocationDenied = "denied"
)

// How many invocations are handed back at once: the connection list's sizes, since both are
// lists a dashboard shows a page of. Neither is measured.
const (
	defaultInvocationLimit = 25
	maxInvocationLimit     = 200
)

// ConnectorInvocation is one connector tool call a session's dispatcher ran. It holds no
// argument and no result (20261007170000_connector_invocations_and_audit.sql).
type ConnectorInvocation struct {
	bun.BaseModel `bun:"table:connector_invocations,alias:ci"`

	ID           string `bun:"id,pk"`
	CustomerID   string `bun:"customer_id,notnull"`
	ConnectionID string `bun:"connection_id,notnull"`
	ConnectorID  string `bun:"connector_id,notnull"`
	ConfigID     string `bun:"config_id,notnull"`
	// Binding is the alias the config binds the connector under.
	Binding string `bun:"binding,notnull"`
	// Tool is the tool's name at the provider, without the alias.
	Tool string `bun:"tool,notnull"`
	// SessionID is empty for an incognito session.
	SessionID string    `bun:"session_id,notnull"`
	StartedAt time.Time `bun:"started_at,notnull"`
	LatencyMs int64     `bun:"latency_ms,notnull"`
	// ErrorType is one of the Invocation* values, empty for a call that answered.
	ErrorType string `bun:"error_type,notnull"`
	// ProviderStatus is the provider's HTTP status for the call's last answered request, nil
	// when none was answered (20261031130000_connector_invocation_failures.sql).
	ProviderStatus *int `bun:"provider_status"`
	// ErrorMessage is why the call failed, as its log line says it, redacted and at most 1 KiB
	// (session.dispatcher.record); empty for a call that answered.
	ErrorMessage string `bun:"error_message,notnull"`
	// Arguments is the shape of what the call was asked, never a value (AI-990 F40), nil when
	// its arguments were not a JSON object.
	Arguments []ArgumentShape `bun:"arguments,type:jsonb"`
}

// Validate refuses an invocation the table refuses (connector_invocations_error_type_check,
// connector_invocations_latency_ms_check), or one no list could tie to its connection.
func (i *ConnectorInvocation) Validate() error {
	return validate("connector invocation").
		required("customer_id", i.CustomerID).
		required("connection_id", i.ConnectionID).
		required("tool", i.Tool).
		oneOf("error_type", i.ErrorType, "", InvocationCustomerAuth, InvocationExternalServer, InvocationClientTimeout, InvocationOutcomeUnknown, InvocationDenied).
		check(i.LatencyMs >= 0, "latency_ms", "is negative").
		done()
}

// ArgumentShape is one top-level argument of a tool call without its value: its name, its JSON
// type (RFC 8259 section 3: object, array, string, number, boolean or null) and, for a string
// or an array, its length, so an empty one shows (thread_ts: string of length 0).
type ArgumentShape struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Length is a string's characters (Unicode code points) or an array's elements, nil for
	// any other type.
	Length *int `json:"length,omitempty"`
}

// InvocationFilter picks a page of one connection's calls.
type InvocationFilter struct {
	ConnectionID string
	Limit        int
	// After is the last call of the previous page.
	After *InvocationPosition
}

// InvocationPosition is where a page of invocations ended, newest first.
type InvocationPosition struct {
	StartedAt time.Time `json:"s"`
	ID        string    `json:"id"`
}

// InvocationLimit is the page size an invocation list uses for the limit asked for.
// Select returns one row more than this, so a caller can tell the page is not
// the last without counting.
func InvocationLimit(asked int) int {
	return clampLimit(asked, defaultInvocationLimit, maxInvocationLimit)
}

// ConnectorInvocations are the connector tool calls sessions ran.
type ConnectorInvocations struct {
	store *Store
}

// Insert stores one call.
func (r *ConnectorInvocations) Insert(ctx context.Context, invocation *ConnectorInvocation) error {
	invocations := []ConnectorInvocation{*invocation}
	err := r.InsertMany(ctx, invocations)
	*invocation = invocations[0]
	return err
}

// InsertMany stores calls in one statement, and none of them when one is refused.
func (r *ConnectorInvocations) InsertMany(ctx context.Context, invocations []ConnectorInvocation) error {
	for i := range invocations {
		invocation := &invocations[i]
		if err := invocation.Validate(); err != nil {
			return stack.Wrap(err)
		}
		// A caller that names the call before the row exists (the session's log line, AI-1242)
		// keeps its id.
		if invocation.ID == "" {
			invocation.ID = newID()
		}
		// Truncated to what Postgres keeps, so the row handed back is the row a read returns.
		invocation.StartedAt = invocation.StartedAt.UTC().Truncate(time.Microsecond)
	}
	if len(invocations) == 0 {
		return nil
	}
	_, err := r.store.global.NewInsert().Model(&invocations).Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record connector invocation: %w", err))
	}
	return nil
}

// Select lists one connection's calls, newest first, one more than
// InvocationLimit(limit).
func (r *ConnectorInvocations) Select(ctx context.Context, customerID string, filter InvocationFilter) ([]ConnectorInvocation, error) {
	if customerID == "" || filter.ConnectionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	invocations := []ConnectorInvocation{}
	query := r.store.global.NewSelect().Model(&invocations).
		Where("customer_id = ?", customerID).
		Where("connection_id = ?", filter.ConnectionID)
	if after := filter.After; after != nil {
		query = query.Where("(started_at, id) < (?, ?)", after.StartedAt, after.ID)
	}
	err := query.
		Order("started_at DESC", "id DESC").
		Limit(InvocationLimit(filter.Limit) + 1).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list connector invocations: %w", err))
	}
	return invocations, nil
}
