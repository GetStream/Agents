package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// EventDelivery is one forward of a provider delivery to one destination.
type EventDelivery struct {
	bun.BaseModel `bun:"table:connector_event_deliveries,alias:cedl"`

	DestinationID string `bun:"destination_id,pk"`
	// ID is the Standard Webhooks webhook-id, the same on every attempt.
	ID string `bun:"id,pk"`
	// Headers are the provider's own headers the receiver verifies the body with, as sent.
	Headers map[string]string `bun:"headers,type:jsonb,notnull"`
	// Body is the provider's raw body, unchanged.
	Body          []byte    `bun:"body,notnull"`
	Attempts      int       `bun:"attempts,notnull"`
	NextAttemptAt time.Time `bun:"next_attempt_at,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull"`
	// ProviderHeadersUntil is when the provider's signature headers in Headers stop verifying
	// at a receiver that checks them. Nil when they do not age.
	ProviderHeadersUntil *time.Time `bun:"provider_headers_until"`
}

// Validate refuses a delivery the table refuses. Its destination is left out: Queue writes one
// row per destination, each naming its own.
func (d *EventDelivery) Validate() error {
	return validate("event delivery").
		required("id", d.ID).
		check(d.Body != nil, "body", "is required").
		check(d.Attempts >= 0, "attempts", "is negative").
		check(!d.NextAttemptAt.IsZero(), "next_attempt_at", "is required").
		done()
}

// ClaimedEventDelivery is a delivery a worker took, with what sending it needs of its
// destination.
type ClaimedEventDelivery struct {
	EventDelivery
	CustomerID           string     `bun:"customer_id"`
	ConnectorID          string     `bun:"connector_id"`
	URL                  string     `bun:"url"`
	SecretSealed         []byte     `bun:"secret_sealed"`
	KEKVersion           int        `bun:"kek_version"`
	PreviousSecretSealed []byte     `bun:"previous_secret_sealed"`
	PreviousKEKVersion   int        `bun:"previous_kek_version"`
	PreviousUntil        *time.Time `bun:"previous_until"`
}

// EventDeliveries are the forwards to event destinations not yet sent, a queue.
type EventDeliveries struct {
	store *Store
}

// Queue queues one provider delivery for each of the customer's destinations of
// the connector whose forward is one of forwards (ForwardAll, ForwardUnhandled). A delivery
// already queued for a destination under the same id is not queued again. It returns how many
// it queued.
func (r *EventDeliveries) Queue(ctx context.Context, customerID, connectorID string, forwards []string, delivery EventDelivery) (int, error) {
	if err := delivery.Validate(); err != nil {
		return 0, stack.Wrap(err)
	}
	if customerID == "" || connectorID == "" || len(forwards) == 0 {
		return 0, stack.Wrap(errors.New("store: a customer, a connector id and the forwards that take a delivery are required"))
	}
	headers, err := json.Marshal(delivery.Headers)
	if err != nil {
		return 0, stack.Wrap(err)
	}
	var headersUntil *time.Time
	if delivery.ProviderHeadersUntil != nil {
		until := delivery.ProviderHeadersUntil.UTC()
		headersUntil = &until
	}
	result, err := r.store.global.NewRaw(`
INSERT INTO connector_event_deliveries (destination_id, id, headers, body, attempts, next_attempt_at, provider_headers_until)
SELECT ced.id, ?, ?::jsonb, ?, 0, ?, ?
FROM connector_event_destinations AS ced
WHERE ced.customer_id = ? AND ced.connector_id = ? AND ced.forward IN (?)
ON CONFLICT (destination_id, id) DO NOTHING`,
		delivery.ID, string(headers), delivery.Body, delivery.NextAttemptAt.UTC(), headersUntil, customerID, connectorID, bun.In(forwards)).Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: queue event deliveries: %w", err))
	}
	queued, _ := result.RowsAffected()
	return int(queued), nil
}

// Claim takes at most limit deliveries due at now, oldest due first, moves each one's next
// attempt to leaseUntil, so no other worker takes it while this one sends it, and counts the
// attempt. Rows another worker is claiming at the same moment are skipped, not waited for.
//
// It takes at most perDestination deliveries of one destination, less the ones of it the
// caller is still sending (sending, by destination id). So one destination that never answers
// holds perDestination of the caller's sends, and other destinations' deliveries are taken
// past its own.
func (r *EventDeliveries) Claim(ctx context.Context, now time.Time, limit, perDestination int, sending map[string]int, leaseUntil time.Time) ([]ClaimedEventDelivery, error) {
	return r.claim(ctx, claimEventDeliveriesQuery, now, limit, perDestination, sending, leaseUntil)
}

// claimEventDeliveriesQuery is Claim's statement. Postgres refuses FOR UPDATE
// beside a window function in one SELECT, so ranked numbers each destination's due rows first
// and due locks the ones within the cap.
//
// due checks next_attempt_at on the row it locks, not only in ranked, which reads the rows as
// they were when the statement started. When another router's claim commits in between,
// Postgres locks the row's newer version and checks only due's own WHERE on it again («the
// second updater ... re-evaluates its WHERE condition»: Read Committed Isolation Level,
// https://www.postgresql.org/docs/16/transaction-iso.html, opened October 7, 2026). Without
// the check in due, a row another router just leased is leased again (AI-924 review of #778).
//
// It is not queue.go's claim: a delivery is known by two columns, the cap needs ranked's
// window, and the destination's secrets come back in the same statement through a join the
// claim's UPDATE has no place for. Like queue.go's claim it counts the attempt, so a delivery
// whose forwarder dies between the claim and the send still runs out of attempts.
const claimEventDeliveriesQuery = `
WITH sending AS (
    SELECT * FROM unnest(?::text[], ?::int[]) AS s (destination_id, count)
), ranked AS (
    SELECT cedl.destination_id, cedl.id,
        row_number() OVER (PARTITION BY cedl.destination_id ORDER BY cedl.next_attempt_at, cedl.id)
            + coalesce(sending.count, 0) AS slot
    FROM connector_event_deliveries AS cedl
    LEFT JOIN sending ON sending.destination_id = cedl.destination_id
    WHERE cedl.next_attempt_at <= ?
), due AS (
    SELECT cedl.destination_id, cedl.id FROM connector_event_deliveries AS cedl
    JOIN ranked ON ranked.destination_id = cedl.destination_id AND ranked.id = cedl.id
    WHERE ranked.slot <= ? AND cedl.next_attempt_at <= ?
    ORDER BY cedl.next_attempt_at
    LIMIT ?
    FOR UPDATE OF cedl SKIP LOCKED
)
UPDATE connector_event_deliveries AS cedl
SET next_attempt_at = ?, attempts = cedl.attempts + 1
FROM due, connector_event_destinations AS ced
WHERE cedl.destination_id = due.destination_id AND cedl.id = due.id AND ced.id = cedl.destination_id
RETURNING cedl.destination_id, cedl.id, cedl.headers, cedl.body, cedl.attempts, cedl.next_attempt_at, cedl.created_at,
    cedl.provider_headers_until, ced.customer_id, ced.connector_id, ced.url, ced.secret_sealed, ced.kek_version,
    ced.previous_secret_sealed, ced.previous_kek_version, ced.previous_until`

// claim runs query, Claim's statement or a test's variant of it with a pause in it, with
// Claim's arguments.
func (r *EventDeliveries) claim(ctx context.Context, query string, now time.Time, limit, perDestination int, sending map[string]int, leaseUntil time.Time) ([]ClaimedEventDelivery, error) {
	claimed := []ClaimedEventDelivery{}
	if limit < 1 || perDestination < 1 {
		return claimed, nil
	}
	busy, counts := make([]string, 0, len(sending)), make([]int, 0, len(sending))
	for destination, count := range sending {
		busy, counts = append(busy, destination), append(counts, count)
	}
	err := r.store.global.NewRaw(query, pgdialect.Array(busy), pgdialect.Array(counts), now.UTC(), perDestination, now.UTC(), limit,
		leaseUntil.UTC()).Scan(ctx, &claimed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: claim event deliveries: %w", err))
	}
	return claimed, nil
}

// NextDue is when the delivery due first is due, any router's, leased ones at the
// end of their lease. found is false when no delivery is queued at all.
func (r *EventDeliveries) NextDue(ctx context.Context) (next time.Time, found bool, err error) {
	var at sql.NullTime
	if err := r.store.global.NewSelect().Model((*EventDelivery)(nil)).ColumnExpr("min(next_attempt_at)").Scan(ctx, &at); err != nil {
		return time.Time{}, false, stack.Wrap(fmt.Errorf("store: next event delivery: %w", err))
	}
	return at.Time, at.Valid, nil
}

// Finish removes a delivery the destination took, refused, or that ran out of
// retries. One the destination was deleted under is already gone, which is not an error.
func (r *EventDeliveries) Finish(ctx context.Context, destinationID, id string) error {
	_, err := r.store.global.NewDelete().Model((*EventDelivery)(nil)).
		Where("destination_id = ?", destinationID).
		Where("id = ?", id).
		Exec(ctx)
	return stack.Wrap(err)
}

// Release gives back the claim of delivery that made no attempt: the attempt its claim counted
// is taken back, and the delivery is due again after, on Postgres's clock, for a router that can
// send it. It changes the row only while that claim still holds it, by the attempts and the
// lease the claim wrote: a release whose lease ran out, and whose row another router claimed
// since, would otherwise undo the newer claim.
func (r *EventDeliveries) Release(ctx context.Context, delivery ClaimedEventDelivery, after time.Duration) error {
	_, err := r.store.global.NewUpdate().Model((*EventDelivery)(nil)).
		Set("attempts = greatest(attempts - 1, 0)").
		Set("next_attempt_at = now() + make_interval(secs => ?)", after.Seconds()).
		Where("destination_id = ?", delivery.DestinationID).
		Where("id = ?", delivery.ID).
		Where("attempts = ?", delivery.Attempts).
		Where("next_attempt_at = ?", delivery.NextAttemptAt).
		Exec(ctx)
	return stack.Wrap(err)
}

// Retry records when the next attempt of a delivery is due. The attempt that failed was
// counted by its claim.
func (r *EventDeliveries) Retry(ctx context.Context, destinationID, id string, at time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*EventDelivery)(nil)).
		Set("next_attempt_at = ?", at.UTC()).
		Where("destination_id = ?", destinationID).
		Where("id = ?", id).
		Exec(ctx)
	return stack.Wrap(err)
}
