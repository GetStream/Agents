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

// Where a knowledge url has got to.
const (
	// KnowledgeURLPending means it has been added but not yet read.
	KnowledgeURLPending = "pending"
	// KnowledgeURLIndexed means the page is in the knowledge base and can be looked up.
	KnowledgeURLIndexed = "indexed"
	// KnowledgeURLFailed means it could not be read, and Error says why.
	KnowledgeURLFailed = "failed"
)

// KnowledgeURL is a page an agent's knowledge base is kept filled from.
//
// The passages are in turbopuffer rather than here; this is the subscription to them. It
// carries how many were written because that is what makes removing them exact: the ids
// are the url and a position, so knowing the count is knowing which ids to delete.
type KnowledgeURL struct {
	bun.BaseModel `bun:"table:knowledge_urls,alias:ku"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// Namespace is the knowledge base the page is read into.
	Namespace string `bun:"namespace,notnull"`
	URL       string `bun:"url,notnull"`
	// Title is what the page called itself, as of the last time it was read.
	Title string `bun:"title,notnull"`
	// DeclaredTitle and Description are what the subscription says the page is. They are
	// the caller's words and are left alone by a read, which is why the title a page gives
	// itself is kept apart from the one it was subscribed with.
	DeclaredTitle string `bun:"declared_title,notnull"`
	Description   string `bun:"description,notnull"`
	// State is pending, indexed or failed.
	State string `bun:"state,notnull"`
	// Error is why the last read failed, and is empty otherwise.
	Error string `bun:"error,notnull"`
	// Passages is how many the page was last cut into.
	Passages int `bun:"passages,notnull"`
	// LastIndexedAt is when it was last read successfully. Nil means never.
	LastIndexedAt *time.Time `bun:"last_indexed_at"`
	// RefreshHours is how often the page is read again on its own. Zero is never.
	RefreshHours int        `bun:"refresh_hours,notnull"`
	CreatedAt    time.Time  `bun:"created_at,notnull"`
	UpdatedAt    time.Time  `bun:"updated_at,notnull"`
	DeletedAt    *time.Time `bun:"deleted_at"`
}

// Validate refuses a page the table would refuse.
func (k *KnowledgeURL) Validate() error {
	return validate("knowledge url").
		required("customer_id", k.CustomerID).
		required("namespace", k.Namespace).
		required("url", k.URL).
		oneOf("state", k.State, KnowledgeURLPending, KnowledgeURLIndexed, KnowledgeURLFailed).
		check(k.Passages >= 0, "passages", "is negative").
		check(k.RefreshHours >= 0, "refresh_hours", "is negative").
		done()
}

// ErrNoKnowledgeURL is what asking for a page that is not there returns, which is what
// tells a subscription removed since from a database that did not answer.
var ErrNoKnowledgeURL = errors.New("store: no such knowledge url")

// KnowledgeURLFilter picks which of a customer's pages Select returns.
type KnowledgeURLFilter struct {
	// Namespace narrows them to one knowledge base. Empty is every one of them.
	Namespace string
}

// KnowledgeURLs are the pages knowledge bases are kept filled from, in the shared database.
type KnowledgeURLs struct {
	store *Store
}

// Insert stores a new page to keep a knowledge base filled from, and fills in its id and
// timestamps. It starts pending, because nothing has been read yet.
func (r *KnowledgeURLs) Insert(ctx context.Context, page *KnowledgeURL) error {
	if page.State == "" {
		page.State = KnowledgeURLPending
	}
	if err := page.Validate(); err != nil {
		return stack.Wrap(err)
	}

	page.ID = newID()
	now := time.Now().UTC()
	page.CreatedAt = now
	page.UpdatedAt = now
	page.DeletedAt = nil

	if _, err := r.store.global.NewInsert().Model(page).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create knowledge url: %w", err))
	}
	return nil
}

// GetByURL returns the page a knowledge base is already filled from at this url, and
// ErrNoKnowledgeURL when it is not.
func (r *KnowledgeURLs) GetByURL(ctx context.Context, customerID, namespace, address string) (KnowledgeURL, error) {
	if customerID == "" || namespace == "" || address == "" {
		return KnowledgeURL{}, stack.Wrap(errors.New("store: a customer, a namespace and a url are required"))
	}

	var page KnowledgeURL
	err := r.store.global.NewSelect().Model(&page).
		Where("customer_id = ?", customerID).
		Where("namespace = ?", namespace).
		Where("url = ?", address).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return KnowledgeURL{}, unknownKnowledgeURL(address)
	}
	if err != nil {
		return KnowledgeURL{}, stack.Wrap(fmt.Errorf("store: subscribed knowledge url: %w", err))
	}
	return page, nil
}

// Update records what reading a page made of it: where it got to, what it was called, how
// many passages it became and when. The declaration is saved with it, so subscribing to a page
// again can say something different about it.
func (r *KnowledgeURLs) Update(ctx context.Context, page *KnowledgeURL) error {
	if page.ID == "" {
		return stack.Wrap(&ValidationError{Model: "knowledge url", Field: "id", Reason: "is required"})
	}
	if err := page.Validate(); err != nil {
		return stack.Wrap(err)
	}

	page.UpdatedAt = time.Now().UTC()

	result, err := r.store.global.NewUpdate().Model(page).
		Column("title", "declared_title", "description", "refresh_hours",
			"state", "error", "passages", "last_indexed_at", "updated_at").
		Where("id = ?", page.ID).
		Where("customer_id = ?", page.CustomerID).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save knowledge url: %w", err))
	}
	updated, err := updatedOne(result, "save knowledge url")
	if err != nil {
		return err
	}
	if !updated {
		return unknownKnowledgeURL(page.ID)
	}
	return nil
}

// Delete marks a page as gone. Its passages are the caller's to remove, since they are not in
// this database.
func (r *KnowledgeURLs) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a knowledge url id are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*KnowledgeURL)(nil)).
		Set("deleted_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete knowledge url: %w", err))
	}
	updated, err := updatedOne(result, "delete knowledge url")
	if err != nil {
		return err
	}
	if !updated {
		return unknownKnowledgeURL(id)
	}
	return nil
}

// Get returns one page a customer subscribes to.
func (r *KnowledgeURLs) Get(ctx context.Context, customerID, id string) (KnowledgeURL, error) {
	if customerID == "" || id == "" {
		return KnowledgeURL{}, stack.Wrap(errors.New("store: a customer and a knowledge url id are required"))
	}

	var page KnowledgeURL
	err := r.store.global.NewSelect().Model(&page).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return KnowledgeURL{}, unknownKnowledgeURL(id)
	}
	if err != nil {
		return KnowledgeURL{}, stack.Wrap(fmt.Errorf("store: knowledge url: %w", err))
	}
	return page, nil
}

// SelectDue returns pages, of every customer, whose refresh interval has passed since they
// were last read. A read that failed counts, since every read is saved, so a broken page is
// tried once an interval rather than on every sweep. A page still waiting on its first read is
// not due: that read is already queued.
func (r *KnowledgeURLs) SelectDue(ctx context.Context, now time.Time, limit int) ([]KnowledgeURL, error) {
	var pages []KnowledgeURL
	err := r.store.global.NewSelect().Model(&pages).
		Where("deleted_at IS NULL").
		Where("refresh_hours > 0").
		Where("state <> ?", KnowledgeURLPending).
		Where("updated_at + refresh_hours * interval '1 hour' <= ?", now).
		Order("updated_at ASC").
		Limit(limit).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: due knowledge urls: %w", err))
	}
	return pages, nil
}

// Select returns the pages a customer subscribes to, newest first.
func (r *KnowledgeURLs) Select(ctx context.Context, customerID string, filter KnowledgeURLFilter) ([]KnowledgeURL, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}

	var pages []KnowledgeURL
	query := r.store.global.NewSelect().Model(&pages).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL")
	if filter.Namespace != "" {
		query = query.Where("namespace = ?", filter.Namespace)
	}
	if err := query.Order("created_at DESC").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer knowledge urls: %w", err))
	}
	return pages, nil
}

func unknownKnowledgeURL(id string) error {
	return stack.Wrap(fmt.Errorf("%w: %s", ErrNoKnowledgeURL, id))
}
