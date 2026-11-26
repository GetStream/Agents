package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Request is one recorded unit of work, stored so billing, cost and health can all be
// derived from the same rows.
type Request struct {
	bun.BaseModel `bun:"table:requests,alias:r"`

	ID int64 `bun:"id,pk,autoincrement"`
	// UsageID is the unit of work, chosen before the row is first written, so writing it
	// again is a no-op rather than a second charge. Rows from before it was kept have none.
	// InsertMany refuses a row without one; nullzero leaves a missing one to the column's
	// random default, which is what a router from before it gets.
	UsageID  uuid.UUID `bun:"usage_id,type:uuid,nullzero"`
	Modality string    `bun:"modality,notnull"`
	// CustomerID owns the request. Every statistic is keyed by it.
	CustomerID string `bun:"customer_id,notnull"`
	// OrganizationID is who is billed for it. The rest say who asked: OrganizationTier as
	// Stream sent it, APIKey the key or the origin of work no key started, UserID empty when
	// there is none, and UsageCategories what the customer counts the work under (nil
	// writes none). APIKey names a key, never holds its secret: a row the writer loses is
	// logged in full.
	OrganizationID   string   `bun:"organization_id,notnull"`
	OrganizationTier string   `bun:"organization_tier,notnull"`
	APIKey           string   `bun:"api_key,notnull"`
	UserID           string   `bun:"user_id,notnull"`
	UsageCategories  []string `bun:"usage_categories,array,nullzero"`
	// LimitGrants are the grants the work was admitted on past a spent limit, which its
	// billable cost is drawn from.
	LimitGrants []int64 `bun:"limit_grants,array,nullzero"`
	// AgentID is the agent the work was done for. Empty outside a conversation.
	AgentID string `bun:"agent_id,nullzero"`
	// SessionID is the agent session the work ran in, empty outside one.
	SessionID string `bun:"session_id,notnull"`
	// CallID is the call the work happened in. Empty outside a conversation.
	CallID string `bun:"call_id,nullzero"`
	// Tags are the customer's own cost labels, aggregated into the tag rollups so spend
	// can be broken down by whatever the customer labelled it with.
	Tags      map[string]string `bun:"tags,type:jsonb,nullzero"`
	Provider  string            `bun:"provider,notnull"`
	Model     string            `bun:"model,notnull"`
	StartedAt time.Time         `bun:"started_at,notnull"`
	// OperationID correlates a provider request with the turn or synthesis that caused it.
	OperationID string `bun:"operation_id,nullzero"`
	Purpose     string `bun:"purpose,nullzero"`
	TurnID      string `bun:"turn_id,nullzero"`
	// DurationMs is the full request; LatencyMs is the first useful output.
	DurationMs *float64 `bun:"duration_ms"`
	// AudioMs is billable audio, transcribed or produced.
	AudioMs int64 `bun:"audio_ms,notnull"`
	// Characters is billable text.
	Characters int64 `bun:"characters,notnull"`
	// InputTokens is the whole prompt an LLM read, cached part included.
	InputTokens int64 `bun:"input_tokens,notnull"`
	// CachedInputTokens is the part of the prompt served from the provider's cache.
	CachedInputTokens int64 `bun:"cached_input_tokens,notnull"`
	// OutputTokens is everything an LLM generated, reasoning included.
	OutputTokens int64 `bun:"output_tokens,notnull"`
	// InputParts is what the prompt was made of, estimated and scaled to InputTokens.
	InputParts InputParts `bun:"embed:input_"`
	// Images is how many pictures were drawn.
	Images int64 `bun:"images,notnull"`
	// Meters are the priced quantities with no column of their own: cache writes, speech to
	// speech by modality, requests, pixels.
	Meters pricing.Quantities `bun:"meters,type:jsonb,nullzero"`
	// Attributes chose the price (a service tier, a country) or explain the row.
	Attributes map[string]string `bun:"attributes,type:jsonb,nullzero"`
	// LatencyMs is nil for a request that failed before reaching the provider: it has none.
	LatencyMs *float64 `bun:"latency_ms"`
	// CostMicros is millionths of a dollar: what the work is charged at its entry's prices.
	CostMicros int64 `bun:"cost_micros,notnull"`
	// OutputCostMicros is the part of CostMicros the generated tokens were priced at.
	OutputCostMicros int64 `bun:"output_cost_micros,notnull"`
	// PriceVersion names what priced the row: the price change that wrote its entry, the
	// embedded or file list a router prices with before it reads the database, "quoted" for
	// an amount the vendor quoted outright, and empty for local or unpriced work.
	PriceVersion string `bun:"price_version,notnull"`
	// SKU is the priced item's, as its entry names it: empty for work no entry matched.
	SKU string `bun:"sku,notnull"`
	// Region and BYOK are the priced item's, whether or not an entry matched it: where the
	// work ran, and whether on the customer's own key. Rows from before them read "" and false.
	Region string `bun:"region,notnull"`
	BYOK   bool   `bun:"byok,notnull"`
	// BaseCostMicros is the cost at the vendor's rates, for reference, so the margin on the
	// row is CostMicros - BaseCostMicros.
	BaseCostMicros int64 `bun:"base_cost_micros,notnull"`
	// Billable is false for free and local work, which counts toward no spend.
	Billable     bool   `bun:"billable,notnull"`
	Success      bool   `bun:"success,notnull"`
	ErrorCode    string `bun:"error_code,nullzero"`
	ErrorMessage string `bun:"error_message,nullzero"`
	// ToShip is true until the usage service has the row; ShipError is why it refused it.
	ToShip    bool       `bun:"to_ship,notnull"`
	ShippedAt *time.Time `bun:"shipped_at"`
	ShipError string     `bun:"ship_error,notnull"`
}

// Validate refuses a request no row could be written for: every row names its customer, its
// modality and the unit of work it is.
func (r *Request) Validate() error {
	return validate("request").
		required("customer_id", r.CustomerID).
		required("modality", r.Modality).
		check(r.UsageID != uuid.Nil, "usage_id", "is required").
		done()
}

// Quantities is what the row is priced by: the typed columns as pricing meters, the prompt
// split into tokens read fresh, read from cache and written to it, then Meters. Zero
// quantities are left out.
func (r Request) Quantities() pricing.Quantities {
	used := pricing.Quantities{}
	add := func(meter pricing.Meter, quantity int64) {
		if quantity != 0 {
			used[meter] += quantity
		}
	}
	// Cached and cache-written tokens are part of the prompt, so they are billed once at
	// their own rate rather than twice.
	fresh := max(r.InputTokens-r.CachedInputTokens-r.Meters[pricing.CacheWriteInputTokens], 0)
	add(pricing.AudioMs, r.AudioMs)
	add(pricing.Characters, r.Characters)
	add(pricing.InputTokens, fresh)
	add(pricing.CachedInputTokens, r.CachedInputTokens)
	add(pricing.OutputTokens, r.OutputTokens)
	add(pricing.Images, r.Images)
	for meter, quantity := range r.Meters {
		add(meter, quantity)
	}
	return used
}

// InputParts splits an LLM prompt by what it carried. No provider reports this, so it is
// estimated from the request and scaled to the provider's count: the parts sum to the
// prompt's tokens, and only the split between them is a guess.
type InputParts struct {
	InstructionTokens    int64 `bun:"instruction_tokens,notnull"`
	MessageTokens        int64 `bun:"message_tokens,notnull"`
	ToolDefinitionTokens int64 `bun:"tool_definition_tokens,notnull"`
	ToolUseTokens        int64 `bun:"tool_use_tokens,notnull"`
	ImageTokens          int64 `bun:"image_tokens,notnull"`
	VideoTokens          int64 `bun:"video_tokens,notnull"`
}

// ErrorCancelled is the error code of a request its caller gave up on, whether before the
// provider answered or by closing the stream it had opened. It is not a success, but it is no
// failure either, so the rollups count it as a request and for what it cost and leave it out
// of errors, uptime and latency.
const ErrorCancelled = "cancelled"

// ErrorRefused is the error code of a response the router's prompt injection screen refused.
// The provider answered as asked, so like a cancelled request it is no failure of the
// provider's, and the rollups leave it out of errors, uptime and latency.
const ErrorRefused = "refused"

// measured is the SQL condition that a request says something about its provider, neither
// cancelled nor refused, on the request row the prefix names.
func measured(prefix string) string {
	return "COALESCE(" + prefix + "error_code, '') NOT IN ('" + ErrorCancelled + "', '" + ErrorRefused + "')"
}

// requestsPrimaryKey is named for the table's first name: the table was renamed and its key
// was not.
const requestsPrimaryKey = "stt_requests_pkey"

// SpendBucket is what one group cost in one bucket, across every modality. Value is a
// modality or a cost label value; the row with Other is everything outside the biggest few,
// with Value "other", and the empty string is spend that carried no such label, so the rows
// still add up to the total. A label whose value is "other" is a row of its own, without Other.
type SpendBucket struct {
	Bucket          time.Time `bun:"bucket"`
	Value           string    `bun:"value"`
	Other           bool      `bun:"-"`
	CostMicrosTotal int64     `bun:"cost_micros_total"`
	RequestCount    int64     `bun:"request_count"`
}

// TagKeySummary is one cost label key, with what it covers and its largest values. It is
// what lets a reader be shown the breakdown that means something: the keys are the
// customer's own, so which of them identifies a product and which an end customer is
// only visible in how many values each was used with and how much of the bill it covers.
type TagKeySummary struct {
	Key             string
	ValueCount      int64
	CostMicrosTotal int64
	RequestCount    int64
	// Coverage is the share of the window's requests carrying this key, from 0 to 1.
	Coverage  float64
	TopValues []TagValueSummary
}

// TagValueSummary is one value of a cost label key, and its share of what that key covers.
type TagValueSummary struct {
	Value           string
	CostMicrosTotal int64
	RequestCount    int64
	Share           float64
}

// UsageFilter narrows usage to who asked for it: the key or origin it was started with, the end
// user it was for, and categories every row carries, a team among them as team:<name>. The zero
// filter narrows nothing.
//
// OrganizationID widens it instead, from the customer's own app to the rows of every app in
// Apps that bill that organization; empty keeps it to the customer.
type UsageFilter struct {
	APIKey         string
	UserID         string
	Categories     []string
	OrganizationID string
	Apps           []string
}

// where is the filter as conditions on requests, each opening with AND, and their arguments.
func (f UsageFilter) where() (string, []any) {
	var conditions strings.Builder
	var args []any
	if f.APIKey != "" {
		conditions.WriteString("\n  AND api_key = ?")
		args = append(args, f.APIKey)
	}
	if f.UserID != "" {
		conditions.WriteString("\n  AND user_id = ?")
		args = append(args, f.UserID)
	}
	if len(f.Categories) > 0 {
		conditions.WriteString("\n  AND usage_categories @> ?")
		args = append(args, pgdialect.Array(f.Categories))
	}
	return conditions.String(), args
}

// VendorCostAttribute is the row attribute a vendor's own figure for what the work cost is
// kept in, in micros, by the providers whose answers carry one.
const VendorCostAttribute = "vendor_cost_micros"

// VendorCostModel is, for one model, how far the cost the price list gave each row is from what
// the vendor said it charged: each row's gap as a fraction of the vendor's figure, negative
// where the list billed less, and the rows with the widest gaps.
type VendorCostModel struct {
	Modality string
	Provider string
	Model    string
	Gaps     []float64
	Widest   []VendorCostRow
}

// VendorCostRow is one row with the vendor's figure beside the list's cost at the vendor's
// rates. UsageID is empty on a database whose requests have no usage id.
type VendorCostRow struct {
	ID               int64     `json:"id"`
	UsageID          string    `json:"usage_id,omitempty"`
	StartedAt        time.Time `json:"started_at"`
	VendorCostMicros int64     `json:"vendor_cost_micros"`
	BaseCostMicros   int64     `json:"base_cost_micros"`
	Gap              float64   `json:"gap"`
}

// Requests are the units of work every shard records, in its own database: what billing,
// spend, health and the usage service are all read from.
type Requests struct {
	store *Store
}

// InsertMany writes rows that are not written yet, in one transaction in this shard's
// database, and returns the usage ids it wrote. A row whose usage id is already there is
// skipped, so writing a batch again after a failure whose commit did land writes nothing twice.
// A row InsertMany wrote has its ID set, and one with an agent a provider_request agent log
// row, in the same transaction. With one database each row it wrote also counts toward the
// grants it names in it (addGrantUses); with shards the grants are the caller's to count, apart.
func (r *Requests) InsertMany(ctx context.Context, requests []Request) ([]uuid.UUID, error) {
	for i := range requests {
		if err := prepareRequest(&requests[i]); err != nil {
			return nil, err
		}
	}
	if len(requests) == 0 {
		return nil, nil
	}
	// The log rows take a lock per customer, so two routers writing batches that name the same
	// customers in another order would deadlock. Written in customer order, every batch takes
	// them in one order.
	sorted := slices.Clone(requests)
	slices.SortStableFunc(sorted, func(a, b Request) int { return strings.Compare(a.CustomerID, b.CustomerID) })

	sharded := r.store.Sharded()
	var written []writtenRequest
	err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Scanned apart from the models: a row skipped by the conflict returns nothing, so the
		// rows that come back do not line up with the ones sent.
		_, err := tx.NewInsert().Model(&sorted).
			On("CONFLICT (usage_id) DO NOTHING").
			Returning("id, usage_id").
			Exec(ctx, &written)
		if err != nil {
			return err
		}
		// One database: the rows count toward their grants in the transaction that writes them.
		if !sharded {
			if _, err := addGrantUses(ctx, tx, GrantUsesOf(sorted, usageIDsOf(written))); err != nil {
				return err
			}
		}
		return r.store.AgentLogs.logRequests(ctx, tx, toLog(sorted, written))
	})
	if err != nil {
		if IDTaken(err) {
			if advanced := r.advanceIDs(ctx); advanced != nil {
				err = errors.Join(err, advanced)
			}
		}
		return nil, stack.Wrap(fmt.Errorf("store: record requests: %w", err))
	}

	ids := usageIDsOf(written)
	byUsage := make(map[uuid.UUID]int64, len(written))
	for _, row := range written {
		byUsage[row.UsageID] = row.ID
	}
	for i := range requests {
		id, ok := byUsage[requests[i].UsageID]
		if !ok {
			continue
		}
		// Only the first of two rows sent with one usage id was written.
		delete(byUsage, requests[i].UsageID)
		requests[i].ID = id
	}
	return ids, nil
}

// toLog is the rows of sorted that were written and name an agent, with their ids: the first of
// two sent with one usage id, as InsertMany numbers them.
func toLog(sorted []Request, written []writtenRequest) []Request {
	byUsage := make(map[uuid.UUID]int64, len(written))
	for _, row := range written {
		byUsage[row.UsageID] = row.ID
	}
	var logged []Request
	for _, request := range sorted {
		id, ok := byUsage[request.UsageID]
		if !ok {
			continue
		}
		delete(byUsage, request.UsageID)
		if request.AgentID != "" {
			request.ID = id
			logged = append(logged, request)
		}
	}
	return logged
}

func usageIDsOf(written []writtenRequest) []uuid.UUID {
	ids := make([]uuid.UUID, len(written))
	for i, row := range written {
		ids[i] = row.UsageID
	}
	return ids
}

// advanceIDs moves the requests id sequence past the highest id written: a row carried in with
// its own id, as a data move from before usage ids did, leaves the sequence behind it, and
// every id it would still hand out is taken. It never sets the sequence below where the
// statement read it, but under a race it can still move it back: a writer drawing ids between
// that read and the setval may draw past the highest id read, and the ids after it are then
// handed out again. That costs whoever draws one another try, one of the writer's immediate
// retries, never a row.
func (r *Requests) advanceIDs(ctx context.Context) error {
	_, err := r.store.shard.ExecContext(ctx, `
SELECT setval(pg_get_serial_sequence('requests', 'id'), written)
FROM (SELECT max(id) AS written FROM requests) AS highest
WHERE written > COALESCE(pg_sequence_last_value(pg_get_serial_sequence('requests', 'id')::regclass), 0)`)
	if err != nil {
		return fmt.Errorf("store: advance request ids: %w", err)
	}
	return nil
}

// writtenRequest is what InsertMany reads back of a row it wrote.
type writtenRequest struct {
	ID      int64     `bun:"id"`
	UsageID uuid.UUID `bun:"usage_id"`
}

// prepareRequest checks a request before it is written and fills what defaults.
func prepareRequest(request *Request) error {
	request.ErrorMessage = SafeLogText(request.ErrorMessage)
	if err := request.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if request.StartedAt.IsZero() {
		request.StartedAt = time.Now().UTC()
	}
	return nil
}

// Unwritable reports whether writing the same rows again can only fail the same way: a row
// is invalid, or Postgres refused its data, a constraint or its size. Anything else, a
// timeout, a lost connection, a lock, may pass on a later try. So may an id already taken
// (IDTaken): the next try draws new ids.
func Unwritable(err error) bool {
	var invalid *ValidationError
	if errors.As(err, &invalid) {
		return true
	}
	var refused pgdriver.Error
	if !errors.As(err, &refused) {
		return false
	}
	if IDTaken(err) {
		return false
	}
	code := refused.Field('C')
	return strings.HasPrefix(code, "22") || strings.HasPrefix(code, "23") || strings.HasPrefix(code, "54")
}

// IDTaken reports whether InsertMany failed on a request id another row already has, which a
// row carried in with its own id leaves behind. InsertMany has moved the sequence past it by
// then, so trying again at once writes the rows.
func IDTaken(err error) bool {
	return constraint(err) == requestsPrimaryKey
}

// callsOf is the call each request happened in, in their order: the agent's latest call of the
// customer started by the request, the one it names when it names one. A request in no call
// has an empty one. The log row names the call's session and the config it ran.
func callsOf(ctx context.Context, db bun.IDB, requests []Request) ([]requestCall, error) {
	customers := make([]string, len(requests))
	agents := make([]string, len(requests))
	callIDs := make([]string, len(requests))
	started := make([]string, len(requests))
	for i, request := range requests {
		customers[i], agents[i], callIDs[i] = request.CustomerID, request.AgentID, request.CallID
		started[i] = request.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	var found []struct {
		Ordinal   int    `bun:"ordinal"`
		ConfigID  string `bun:"config_id"`
		SessionID string `bun:"session_id"`
	}
	err := db.NewRaw(`
SELECT r.ordinal, c.config_id, c.session_id
FROM unnest(?::text[], ?::text[], ?::text[], ?::timestamptz[]) WITH ORDINALITY
    AS r (customer_id, agent_id, call_id, started_at, ordinal)
CROSS JOIN LATERAL (
    SELECT COALESCE(s.config_id, '') AS config_id, COALESCE(calls.session_id, '') AS session_id
    FROM calls LEFT JOIN agent_sessions AS s ON s.customer_id = calls.customer_id AND s.id = calls.session_id
    WHERE calls.customer_id = r.customer_id AND calls.agent_id = r.agent_id
        AND calls.started_at <= r.started_at AND (r.call_id = '' OR calls.call_id = r.call_id)
    ORDER BY calls.started_at DESC
    LIMIT 1) AS c`,
		pgdialect.Array(customers), pgdialect.Array(agents), pgdialect.Array(callIDs), pgdialect.Array(started),
	).Scan(ctx, &found)
	if err != nil {
		return nil, err
	}
	calls := make([]requestCall, len(requests))
	for _, call := range found {
		calls[call.Ordinal-1] = requestCall{configID: call.ConfigID, sessionID: call.SessionID}
	}
	return calls, nil
}

// requestCall is the session, and the config it ran, of the call a request happened in.
type requestCall struct {
	configID  string
	sessionID string
}

func nullIfEmpty(text string) *string {
	if text == "" {
		return nil
	}
	return &text
}

// reshipBatch is the most rows one statement of Reship queues, so a long span does not hold
// every row's lock in one transaction.
const reshipBatch = 10_000

// ToShip is up to limit of this shard's rows still to reach the usage service that started
// more than settle ago, oldest first, after the row after when it is not nil: the rows started
// since are the writer's to ship.
func (r *Requests) ToShip(ctx context.Context, settle time.Duration, after *Request, limit int) ([]Request, error) {
	var rows []Request
	query := r.store.shard.NewSelect().Model(&rows).
		Where("to_ship").
		Where("started_at < now() - make_interval(secs => ?)", settle.Seconds()).
		OrderExpr("started_at, id").
		Limit(limit)
	if after != nil {
		query = query.Where("(started_at, id) > (?, ?)", after.StartedAt, after.ID)
	}
	if err := query.Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: requests to ship: %w", err))
	}
	return rows, nil
}

// Shipped is which of the usage ids this shard holds a row for that the usage service has, as
// whichever router's sweeper shipped it recorded.
func (r *Requests) Shipped(ctx context.Context, usageIDs []uuid.UUID) ([]uuid.UUID, error) {
	if len(usageIDs) == 0 {
		return nil, nil
	}
	var shipped []uuid.UUID
	err := r.store.shard.NewSelect().Model((*Request)(nil)).Column("usage_id").
		Where("usage_id IN (?)", bun.In(usageIDs)).
		Where("shipped_at IS NOT NULL").
		Scan(ctx, &shipped)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: shipped: %w", err))
	}
	return shipped, nil
}

// Ship marks what the usage service answered for rows it was sent: shipped are the usage ids
// it has, and rejected the ones it refused, each with why. Neither is sent again until
// somebody reships it.
func (r *Requests) Ship(ctx context.Context, shipped []uuid.UUID, rejected map[uuid.UUID]string) error {
	if len(shipped) == 0 && len(rejected) == 0 {
		return nil
	}
	ids := make([]string, 0, len(rejected))
	reasons := make([]string, 0, len(rejected))
	for id, reason := range rejected {
		ids, reasons = append(ids, id.String()), append(reasons, reason)
	}
	err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := boundShipping(ctx, tx); err != nil {
			return err
		}
		if len(shipped) > 0 {
			if _, err := tx.NewUpdate().Model((*Request)(nil)).
				Set("to_ship = false").Set("shipped_at = now()").
				Where("usage_id IN (?)", bun.In(shipped)).
				Exec(ctx); err != nil {
				return err
			}
		}
		if len(rejected) > 0 {
			if _, err := tx.ExecContext(ctx, `
UPDATE requests AS r SET to_ship = false, ship_error = v.reason
FROM unnest(?::uuid[], ?::text[]) AS v(usage_id, reason)
WHERE r.usage_id = v.usage_id`, pgdialect.Array(ids), pgdialect.Array(reasons)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record shipment: %w", err))
	}
	return nil
}

// SetOrganization writes the organization on rows written without one.
func (r *Requests) SetOrganization(ctx context.Context, ids []uuid.UUID, organizationID string) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := boundShipping(ctx, tx); err != nil {
			return err
		}
		_, err := tx.NewUpdate().Model((*Request)(nil)).
			Set("organization_id = ?", organizationID).
			Where("usage_id IN (?)", bun.In(ids)).
			Where("organization_id = ''").
			Exec(ctx)
		return err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set requests organization: %w", err))
	}
	return nil
}

// shippingLockWait bounds how long a shipping write waits on another session's row locks, and
// shippingIdle how long its transaction may sit idle before Postgres ends the session. A
// router whose network went silent mid-transaction keeps its locks only that long, rather than
// until the kernel's keepalive gives up on the pool's connection, two hours on.
const (
	shippingLockWait = 5 * time.Second
	shippingIdle     = 15 * time.Second
)

// boundShipping sets shippingLockWait and shippingIdle for the rest of tx.
func boundShipping(ctx context.Context, tx bun.Tx) error {
	_, err := tx.ExecContext(ctx, "SELECT set_config('lock_timeout', ?, true), set_config('idle_in_transaction_session_timeout', ?, true)",
		fmt.Sprintf("%dms", shippingLockWait.Milliseconds()), fmt.Sprintf("%dms", shippingIdle.Milliseconds()))
	return err
}

// Unshipped is how many of this shard's rows with an organization are still to reach the usage
// service, and how long ago the oldest of them started: zero when there is none. A row with
// none is left out: the sweeper skips it until its app joins one, and reports it as unowned.
func (r *Requests) Unshipped(ctx context.Context) (int64, time.Duration, error) {
	var rows int64
	var oldest float64
	err := r.store.shard.NewSelect().Model((*Request)(nil)).
		ColumnExpr("count(*)").
		ColumnExpr("COALESCE(EXTRACT(EPOCH FROM now() - min(r.started_at)), 0)::float8").
		Where("r.to_ship").
		Where("r.organization_id <> ''").
		Scan(ctx, &rows, &oldest)
	if err != nil {
		return 0, 0, stack.Wrap(fmt.Errorf("store: unshipped: %w", err))
	}
	return rows, time.Duration(max(oldest, 0) * float64(time.Second)), nil
}

// UnownedApp is an app's rows in this shard that wait to ship because the app has joined no
// organization.
type UnownedApp struct {
	App             string
	Rows            int64
	OldestStartedAt time.Time
	CostMicros      int64
	// Groups are the rows by what wrote them, most rows first.
	Groups []UnownedGroup
}

// UnownedGroup is the waiting rows of an app written by one key for one modality and model.
type UnownedGroup struct {
	APIKey     string `json:"api_key"`
	Modality   string `json:"modality"`
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	Rows       int64  `json:"rows"`
	CostMicros int64  `json:"cost_micros"`
}

// UnownedUsage is the apps of this shard whose rows wait to ship with no organization, by app.
// An app that has joined one is left out: the next sweep writes it on its rows.
func (r *Requests) UnownedUsage(ctx context.Context) ([]UnownedApp, error) {
	var groups []struct {
		App string `bun:"customer_id"`
		UnownedGroup
		Oldest time.Time `bun:"oldest"`
	}
	err := r.store.shard.NewRaw(`
SELECT customer_id, api_key, modality, provider, model, count(*) AS rows, sum(cost_micros) AS cost_micros, min(started_at) AS oldest
FROM requests WHERE to_ship AND shipped_at IS NULL AND organization_id = ''
GROUP BY 1, 2, 3, 4, 5 ORDER BY customer_id, rows DESC, api_key, modality, provider, model`).Scan(ctx, &groups)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: unowned usage: %w", err))
	}
	if len(groups) == 0 {
		return nil, nil
	}
	apps := make([]string, 0, len(groups))
	for _, group := range groups {
		apps = append(apps, group.App)
	}
	var memberships []string
	if err := r.store.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").Where("app_id IN (?)", bun.In(apps)).
		Scan(ctx, &memberships); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: unowned usage: %w", err))
	}
	joined := make(map[string]bool, len(memberships))
	for _, app := range memberships {
		joined[app] = true
	}
	var unowned []UnownedApp
	for _, group := range groups {
		if joined[group.App] {
			continue
		}
		if len(unowned) == 0 || unowned[len(unowned)-1].App != group.App {
			unowned = append(unowned, UnownedApp{App: group.App, OldestStartedAt: group.Oldest})
		}
		app := &unowned[len(unowned)-1]
		app.Rows += group.Rows
		app.CostMicros += group.CostMicros
		if group.Oldest.Before(app.OldestStartedAt) {
			app.OldestStartedAt = group.Oldest
		}
		app.Groups = append(app.Groups, group.UnownedGroup)
	}
	return unowned, nil
}

// Reship queues this shard's rows started since then to be sent to the usage service again:
// the rows it has (shipped_at set), or with rejected the rows it refused (ship_error set),
// clearing why. A row it never had nor refused is left alone: one written before the router
// shipped, or carried in by a data move, which the deployment that recorded it bills. So is a
// row from before usage ids, which the service could not record once, one already queued,
// and one shipped since Reship started, so a sweeper shipping meanwhile cannot keep it going.
// It walks the rows by id, so each is looked at once. It returns how many it queued.
func (r *Requests) Reship(ctx context.Context, since time.Time, rejected bool) (int64, error) {
	var started time.Time
	var last sql.NullInt64
	if err := r.store.shard.QueryRowContext(ctx, "SELECT now(), (SELECT min(id) - 1 FROM requests WHERE started_at >= ?)", since).
		Scan(&started, &last); err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: reship: %w", err))
	}
	if !last.Valid {
		return 0, nil
	}
	var queued int64
	for {
		batch := r.store.shard.NewSelect().Model((*Request)(nil)).
			Column("r.id").
			Where("r.id > ? AND r.started_at >= ?", last.Int64, since).
			Where("r.usage_id IS NOT NULL AND NOT r.to_ship").
			Order("r.id").
			Limit(reshipBatch)
		if rejected {
			batch.Where("r.ship_error <> ''")
		} else {
			batch.Where("r.shipped_at IS NOT NULL AND r.shipped_at < ?", started)
		}
		var ids []int64
		err := r.store.shard.NewUpdate().With("batch", batch).Model((*Request)(nil)).
			TableExpr("batch").
			Set("to_ship = true").
			Set("ship_error = ''").
			Where("r.id = batch.id").
			Returning("r.id").
			Scan(ctx, &ids)
		if err != nil {
			return queued, stack.Wrap(fmt.Errorf("store: reship: %w", err))
		}
		queued += int64(len(ids))
		if len(ids) < reshipBatch {
			return queued, nil
		}
		last.Int64 = slices.Max(ids)
	}
}

// CountByUser is how many requests on this shard name an app's end user.
func (r *Requests) CountByUser(ctx context.Context, appID, userID string) (int64, error) {
	count, err := r.store.shard.NewSelect().Model((*Request)(nil)).Where("customer_id = ?", appID).Where("user_id = ?", userID).Count(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: user requests: %w", err))
	}
	return int64(count), nil
}

// RenameUser replaces an app's end user id with replacement on this shard's requests,
// writeBatch of them per statement, and in the changes recorded of their requests in each of
// those statements (renamedChanges), and answers how many rows it rewrote. A request recorded
// after the last statement keeps its change as it is.
func (r *Requests) RenameUser(ctx context.Context, appID, userID, replacement string) (int64, error) {
	if replacement == userID {
		// Nothing to rewrite, and every batch would match the rows it just wrote.
		return 0, nil
	}
	db := r.store.shard
	var renamed int64
	for {
		// A data-modifying WITH runs to the end whether or not the statement reads it.
		changes := renamedChanges(db, appID, "requests", userID, replacement)
		batch := db.NewSelect().Model((*Request)(nil)).Column("id").
			Where("customer_id = ?", appID).Where("user_id = ?", userID).Limit(writeBatch)
		result, err := db.NewUpdate().With("changes", changes).Model((*Request)(nil)).Set("user_id = ?", replacement).
			Where("id IN (?)", batch).Exec(ctx)
		if err != nil {
			return renamed, stack.Wrap(fmt.Errorf("store: rename user requests: %w", err))
		}
		n, err := result.RowsAffected()
		renamed += n
		if err != nil {
			return renamed, stack.Wrap(fmt.Errorf("store: rename user requests: %w", err))
		}
		if n < int64(writeBatch) {
			return renamed, nil
		}
	}
}

// spendGroupByModality is the group_by that means "where the money went" rather than
// "what it was spent on".
const spendGroupByModality = "modality"

// The group_by values that are who asked rather than a cost label key, from the columns every
// row carries. A label key never holds a ':', so none of them shadows a label of the same name.
const (
	SpendGroupByApp      = "who:app"
	SpendGroupByOrigin   = "who:origin"
	SpendGroupByUser     = "who:user"
	SpendGroupByTeam     = "who:team"
	SpendGroupByCategory = "who:category"
)

// spendGroups is the value each group_by that is not a cost label groups by, and the join it
// needs. A user is only unique within its app, so across apps it is <app>/<user>; a team is
// the name in a row's team: category; a category leaves the team out, and a row counts in each
// of its others.
func spendGroups(groupBy string, acrossApps bool) (value, join string, ok bool) {
	switch groupBy {
	case spendGroupByModality:
		return "modality", "", true
	case SpendGroupByApp:
		return "customer_id", "", true
	case SpendGroupByOrigin:
		return "api_key", "", true
	case SpendGroupByUser:
		if acrossApps {
			return "CASE WHEN user_id = '' THEN '' ELSE customer_id || '/' || user_id END", "", true
		}
		return "user_id", "", true
	case SpendGroupByTeam:
		return "COALESCE((SELECT substr(c, 6) FROM unnest(usage_categories) AS c WHERE c ILIKE 'team:%' LIMIT 1), '')", "", true
	case SpendGroupByCategory:
		return "COALESCE(spend_category.name, '')",
			"\nLEFT JOIN LATERAL unnest(usage_categories) AS spend_category(name) ON spend_category.name NOT ILIKE 'team:%'", true
	}
	return "", "", false
}

// spendOther is the value of the row every group outside the biggest few is summed into,
// which SpendBucket.Other tells apart from a label of that value.
const spendOther = "other"

// Spend returns what one customer spent per bucket and group, oldest bucket first. It is the
// whole bill rather than one modality's share of it, which is what a spend trend is read as.
//
// groupBy is "modality", who asked (SpendGroupByApp and the rest), or a cost label key. Only
// the limit biggest values over the whole window keep a group of their own: a label such as
// customer_id has as many values as the customer has customers, and a chart of all of them
// says nothing. The rest are summed into a row marked Other, and requests carrying no such label into
// the empty value, so the rows still add up to the total; grouped by category, a row counts
// once in each of its categories.
//
// Reads the request rows rather than the rollups, so today's spend is there before a
// rollup has run.
func (r *Requests) Spend(
	ctx context.Context,
	customerID, groupBy string,
	granularity Granularity,
	from, to time.Time,
	limit int,
	tags map[string]string,
	who UsageFilter,
) ([]SpendBucket, error) {
	if !granularity.Valid() {
		return nil, stack.Wrap(fmt.Errorf("store: unknown granularity %q", granularity))
	}
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	if limit < 1 {
		return nil, stack.Wrap(errors.New("store: limit must be at least 1"))
	}
	tags = orEmptyMap(tags)
	filter, err := json.Marshal(tags)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer spend: encode tag filter: %w", err))
	}

	acrossApps := who.OrganizationID != ""
	valueExpr, join, known := spendGroups(groupBy, acrossApps)
	args := []any{}
	if !known {
		valueExpr = "COALESCE(tags->>?, '')"
		args = append(args, groupBy)
	}
	scope := "customer_id = ?"
	args = append(args, customerID)
	if acrossApps {
		scope = "customer_id IN (?) AND organization_id = ?"
		args[len(args)-1] = bun.In(append([]string{customerID}, who.Apps...))
		args = append(args, who.OrganizationID)
	}
	args = append(args, from, to, string(filter))
	narrowed, narrowing := who.where()
	args = append(args, narrowing...)

	source := r.store.spendRows()
	query := fmt.Sprintf(`
SELECT
    date_trunc('%s', started_at, 'UTC') AS bucket,
    %s AS value,
    COALESCE(SUM(cost_micros), 0) AS cost_micros_total,
    %s AS request_count
FROM %s%s
WHERE %s
  AND started_at >= ? AND started_at < ?
  AND tags @> ?::jsonb%s
GROUP BY 1, 2`, granularity.truncateUnit(), valueExpr, source.requests, source.table, join, scope, narrowed)

	var rows []spendRow
	if err := source.db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer spend: %w", err))
	}
	return foldSpend([][]spendRow{rows}, limit), nil
}

// tagValueRow is what one value of one cost label key was spent on.
type tagValueRow struct {
	Key             string `bun:"key"`
	Value           string `bun:"value"`
	CostMicrosTotal int64  `bun:"cost_micros_total"`
	RequestCount    int64  `bun:"request_count"`
}

// topTagValues is how many values of a key come back with it. Enough to see what drives the
// key's spend, few enough that a key naming an end customer does not return a database.
const topTagValues = 10

// TagKeys returns which cost label keys one customer's spend carries, biggest spend first,
// each with its ten largest values.
//
// Cost labels are the customer's own, so nothing here knows in advance whether spend is
// broken down by product, by environment or by the end customer it was incurred for. What
// tells them apart is how many values a key was used with and how much of the traffic
// carries it, which is what this reports.
func (r *Requests) TagKeys(
	ctx context.Context,
	customerID string,
	from, to time.Time,
	tags map[string]string,
) ([]TagKeySummary, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	tags = orEmptyMap(tags)
	filter, err := json.Marshal(tags)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer tag keys: encode tag filter: %w", err))
	}

	// Coverage is measured against every request in the window, labelled or not, because a
	// key on half the traffic breaks down half the bill and reading it as the whole of it
	// is the mistake this number exists to prevent. Runtime is spend and not requests.
	type shardTags struct {
		rows, total int64
		values      []tagValueRow
	}
	source := r.store.spendRows()
	var rows, total int64
	err = source.db.NewSelect().
		TableExpr(source.table).
		ColumnExpr(source.rows).
		ColumnExpr(source.requests).
		Where("customer_id = ?", customerID).
		Where("started_at >= ?", from).
		Where("started_at < ?", to).
		Where("tags @> ?::jsonb", string(filter)).
		Scan(ctx, &rows, &total)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer tag keys: %w", err))
	}
	if rows == 0 {
		return nil, nil
	}
	var values []tagValueRow
	err = source.db.NewSelect().TableExpr(source.table+" AS r").
		ColumnExpr("tag.key AS key, tag.value AS value").
		ColumnExpr("SUM(r.cost_micros) AS cost_micros_total").
		ColumnExpr(source.requests+" AS request_count").
		Join("CROSS JOIN LATERAL jsonb_each_text(r.tags) AS tag(key, value)").
		Where("r.customer_id = ?", customerID).
		Where("r.started_at >= ? AND r.started_at < ?", from, to).
		Where("r.tags @> ?::jsonb", string(filter)).
		Group("tag.key", "tag.value").
		OrderExpr("tag.key, tag.value").
		Scan(ctx, &values)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer tag keys: %w", err))
	}

	byKey := map[string]*TagKeySummary{}
	ranked := map[string][]tagValueRow{}
	var keys []string
	for _, row := range values {
		summary := byKey[row.Key]
		if summary == nil {
			summary = &TagKeySummary{Key: row.Key}
			byKey[row.Key] = summary
			keys = append(keys, row.Key)
		}
		summary.ValueCount++
		summary.CostMicrosTotal += row.CostMicrosTotal
		summary.RequestCount += row.RequestCount
		ranked[row.Key] = append(ranked[row.Key], row)
	}
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Or(cmp.Compare(byKey[b].CostMicrosTotal, byKey[a].CostMicrosTotal),
			cmp.Compare(byKey[b].RequestCount, byKey[a].RequestCount), strings.Compare(a, b))
	})

	var summaries []TagKeySummary
	for _, name := range keys {
		key := byKey[name]
		if total > 0 {
			key.Coverage = float64(key.RequestCount) / float64(total)
		}
		rows := ranked[name]
		slices.SortFunc(rows, func(a, b tagValueRow) int {
			return cmp.Or(cmp.Compare(b.CostMicrosTotal, a.CostMicrosTotal),
				cmp.Compare(b.RequestCount, a.RequestCount), strings.Compare(a.Value, b.Value))
		})
		for _, row := range rows[:min(topTagValues, len(rows))] {
			// A deployment with no prices configured records every row at zero, so a share of
			// spend would be a division by nothing. Requests are what is left to rank by.
			var share float64
			switch {
			case key.CostMicrosTotal > 0:
				share = float64(row.CostMicrosTotal) / float64(key.CostMicrosTotal)
			case key.RequestCount > 0:
				share = float64(row.RequestCount) / float64(key.RequestCount)
			}
			key.TopValues = append(key.TopValues, TagValueSummary{
				Value:           row.Value,
				CostMicrosTotal: row.CostMicrosTotal,
				RequestCount:    row.RequestCount,
				Share:           share,
			})
		}
		summaries = append(summaries, *key)
	}
	return summaries, nil
}

// CountByModel returns how many requests each "provider/model" served for a modality since a
// time, across every customer. It is what makes a model popular, so it counts calls rather
// than spend, and is read from the raw requests so it needs no rollup.
func (r *Requests) CountByModel(ctx context.Context, modality string, since time.Time) (map[string]int64, error) {
	if modality == "" {
		return nil, stack.Wrap(errors.New("store: modality is required"))
	}

	type modelRow struct {
		Provider string `bun:"provider"`
		Model    string `bun:"model"`
		Requests int64  `bun:"requests"`
	}
	source := r.store.spendRows()
	var rows []modelRow
	err := source.db.NewSelect().
		TableExpr(source.table).
		Column("provider", "model").
		ColumnExpr(source.rows+" AS requests").
		Where("modality = ?", modality).
		Where("started_at >= ?", since).
		Group("provider", "model").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: model requests: %w", err))
	}

	counts := map[string]int64{}
	for _, row := range rows {
		counts[row.Provider+"/"+row.Model] += row.Requests
	}
	return counts, nil
}

// vendorCosts is every row since a time whose vendor reported what it charged, with the list's
// cost at the vendor's rates: base_cost_micros once something writes it, else cost_micros,
// which is the same figure for a row priced before either was written. A malformed figure is skipped,
// never cast, so one bad row cannot fail the read. A row on the customer's own key is left out:
// its vendor bills the customer, so the list puts no cost at the vendor's rates on it.
const vendorCosts = `
SELECT id, usage_id, modality, provider, model, started_at, vendor, base
FROM (
    SELECT id, coalesce(usage_id::text, '') AS usage_id, modality, provider, model, started_at,
           CASE WHEN attributes->>'vendor_cost_micros' ~ '^[0-9]{1,18}$'
                THEN (attributes->>'vendor_cost_micros')::bigint END AS vendor,
           CASE WHEN price_version = '' THEN cost_micros ELSE base_cost_micros END AS base
    FROM requests
    WHERE started_at >= ? AND attributes->>'vendor_cost_micros' IS NOT NULL AND NOT byok
) AS reported
WHERE vendor > 0`

// VendorCostGaps reads this shard's rows since a time whose vendor reported what it charged,
// per model, with up to widest of the rows whose gap is widest; it is run in each region. The
// shard is read once, in a read-only transaction that gives up after timeout: the read takes
// every row since then off requests_started_idx, and a large shard must not hold a connection
// for long.
func (r *Requests) VendorCostGaps(ctx context.Context, since time.Time, widest int, timeout time.Duration) ([]VendorCostModel, error) {
	models, err := vendorCostGaps(ctx, r.store.shard, since, widest, timeout)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: vendor cost gaps: %w", err))
	}
	slices.SortFunc(models, func(a, b VendorCostModel) int {
		return strings.Compare(a.Modality+"/"+a.Provider+"/"+a.Model, b.Modality+"/"+b.Provider+"/"+b.Model)
	})
	return models, nil
}

// widestOf is the rows with the widest gaps, widest first, at most n of them; ties go to the
// lower id, so the same rows are picked every time.
func widestOf(rows []VendorCostRow, n int) []VendorCostRow {
	slices.SortFunc(rows, func(a, b VendorCostRow) int {
		if gapA, gapB := max(a.Gap, -a.Gap), max(b.Gap, -b.Gap); gapA != gapB {
			if gapA > gapB {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.ID, b.ID)
	})
	if len(rows) > n {
		rows = rows[:n:n]
	}
	return rows
}

// vendorCostGaps is VendorCostGaps on one shard's database: one pass over its rows, the gaps
// and the widest rows kept per model as they stream by.
func vendorCostGaps(ctx context.Context, db bun.IDB, since time.Time, widest int, timeout time.Duration) ([]VendorCostModel, error) {
	var models []VendorCostModel
	err := db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		// At least a millisecond: 0 would turn the timeout off.
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", max(timeout.Milliseconds(), 1))); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, vendorCosts, since)
		if err != nil {
			return err
		}
		defer rows.Close()
		byModel := map[string]int{}
		for rows.Next() {
			var modality, provider, model string
			var row VendorCostRow
			if err := rows.Scan(&row.ID, &row.UsageID, &modality, &provider, &model, &row.StartedAt, &row.VendorCostMicros, &row.BaseCostMicros); err != nil {
				return err
			}
			row.Gap = float64(row.BaseCostMicros-row.VendorCostMicros) / float64(row.VendorCostMicros)
			key := modality + "/" + provider + "/" + model
			i, ok := byModel[key]
			if !ok {
				i = len(models)
				byModel[key] = i
				models = append(models, VendorCostModel{Modality: modality, Provider: provider, Model: model})
			}
			found := &models[i]
			found.Gaps = append(found.Gaps, row.Gap)
			found.Widest = append(found.Widest, row)
			// Pruned as it grows, so a model with a million rows keeps a few dozen of them.
			if len(found.Widest) >= 4*widest {
				found.Widest = widestOf(found.Widest, widest)
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	for i := range models {
		models[i].Widest = widestOf(models[i].Widest, widest)
	}
	return models, nil
}

// requestLogEntry is the provider_request agent log row of a written request in call, as the
// trigger this replaced wrote it.
func requestLogEntry(request Request, call requestCall) AgentLog {
	outcome, severity := "completed", "info"
	switch {
	case request.ErrorCode == ErrorCancelled:
		// A caller closing its own request is routine, a superseded flow decision or a
		// barge-in, and no failure, as the rollups count it.
		outcome = "cancelled"
	case !request.Success:
		outcome, severity = "failed", "error"
	}
	return AgentLog{
		CustomerID: request.CustomerID, ConfigID: call.configID, AgentID: request.AgentID, SessionID: call.sessionID,
		Source: "system", Severity: severity, EventType: "provider_request",
		Message:    request.Modality + " request " + outcome,
		OccurredAt: request.StartedAt, IngestedAt: time.Now().UTC(),
		Details: map[string]any{
			"provider": request.Provider, "model": request.Model, "request_id": strconv.FormatInt(request.ID, 10),
			"duration_ms": request.LatencyMs, "input_tokens": request.InputTokens, "output_tokens": request.OutputTokens,
			"cost_micros": request.CostMicros, "error_code": nullIfEmpty(request.ErrorCode),
			"error_message": nullIfEmpty(request.ErrorMessage),
		},
	}
}
