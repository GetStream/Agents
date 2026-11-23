package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ReviewLog is one move a use case made, and who made it.
type ReviewLog struct {
	bun.BaseModel `bun:"table:dlc_review_logs,alias:rl"`

	ID         string `bun:"id,pk"`
	UseCaseID  string `bun:"use_case_id,notnull"`
	CustomerID string `bun:"customer_id,notnull"`
	// Actor is app, staff or vendor, and ActorName who exactly, where known.
	Actor      string `bun:"actor,notnull"`
	ActorName  string `bun:"actor_name,notnull"`
	FromStatus string `bun:"from_status,notnull"`
	ToStatus   string `bun:"to_status,notnull"`
	Notes      string `bun:"notes,notnull"`
	// VendorPayload is what the vendor answered, kept as it came.
	VendorPayload json.RawMessage `bun:"vendor_payload,type:jsonb,nullzero"`
	CreatedAt     time.Time       `bun:"created_at,notnull"`
	SubmittedAt   *time.Time      `bun:"submitted_at"`
	ApprovedAt    *time.Time      `bun:"approved_at"`
}

// Validate refuses a move nobody made.
func (l *ReviewLog) Validate() error {
	return validate("review log").
		required("use_case_id", l.UseCaseID).
		required("customer_id", l.CustomerID).
		required("actor", l.Actor).
		done()
}

// ReviewLogs are the moves use cases made, in the shared database.
type ReviewLogs struct {
	store *Store
}

// Select pages through what happened to a use case, oldest first.
func (r *ReviewLogs) Select(ctx context.Context, customerID, useCaseID string, limit int, after *CreatedPosition) ([]ReviewLog, error) {
	logs := []ReviewLog{}
	query := r.store.global.NewSelect().Model(&logs).
		Where("customer_id = ?", customerID).
		Where("use_case_id = ?", useCaseID)
	if after != nil {
		query = query.Where("(created_at, id) > (?, ?)", after.At, after.ID)
	}
	err := query.Order("created_at", "id").Limit(DLCLimit(limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: review logs: %w", err))
	}
	return logs, nil
}
