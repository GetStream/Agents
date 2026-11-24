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

// KnowledgeDocument is a document a knowledge base was filled with, posted or synced
// rather than read from a page. Like a KnowledgeURL it carries how many passages it was
// cut into, so removing it, or what a shorter version no longer covers, is exact.
type KnowledgeDocument struct {
	bun.BaseModel `bun:"table:knowledge_documents,alias:kd"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Namespace  string `bun:"namespace,notnull"`
	// Source is what the passages are keyed by, and what a reader would call the document.
	Source string `bun:"source,notnull"`
	// Passages covers every passage that may be stored for the document: while its passages
	// are written, and after that write failed, both the old version's and the new one's.
	Passages int `bun:"passages,notnull"`
	// Text is the document as it was posted, or as its file was read. Listings leave it out.
	Text string `bun:"text,notnull"`
	// State is pending while an uploaded file waits to be read or posted text is written,
	// failed when that could not be done, and indexed otherwise. Error says why it failed.
	State string `bun:"state,notnull"`
	Error string `bun:"error,notnull"`
	// Pages is how many pages the uploaded file had, 0 for text and for a file without pages.
	Pages int `bun:"pages,notnull"`
	// ContentType and Bytes are what was sent: the file's type as the upload named it, and
	// its size.
	ContentType string `bun:"content_type,notnull"`
	Bytes       int64  `bun:"bytes,notnull"`
	// ObjectKey is the uploaded file in the knowledge bucket while it waits to be read, and
	// empty once it has been. A read that finds another key here was overtaken by a newer
	// upload of the same source, and leaves the row to it.
	ObjectKey string    `bun:"object_key,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Where a knowledge document has got to.
const (
	// KnowledgeDocumentPending is an uploaded file not read yet, or posted text whose
	// passages are being written.
	KnowledgeDocumentPending = "pending"
	// KnowledgeDocumentIndexed is a document whose passages can be looked up.
	KnowledgeDocumentIndexed = "indexed"
	// KnowledgeDocumentFailed is an uploaded file that could not be read, or posted text
	// whose write stopped part way, and Error says why. The passages written before the
	// failure can still be looked up.
	KnowledgeDocumentFailed = "failed"
)

// Validate refuses a document the table would refuse.
func (k *KnowledgeDocument) Validate() error {
	return validate("knowledge document").
		required("customer_id", k.CustomerID).
		required("namespace", k.Namespace).
		required("source", k.Source).
		oneOf("state", k.State, KnowledgeDocumentPending, KnowledgeDocumentIndexed, KnowledgeDocumentFailed).
		check(k.Passages >= 0, "passages", "is negative").
		check(k.Pages >= 0, "pages", "is negative").
		check(k.Bytes >= 0, "bytes", "is negative").
		done()
}

// ErrNoKnowledgeDocument is what asking for a document that is not there returns, which is
// what tells a document removed while its file was read from a database that did not answer.
var ErrNoKnowledgeDocument = errors.New("store: no such knowledge document")

// KnowledgeDocumentFilter picks which of a customer's documents Select returns.
type KnowledgeDocumentFilter struct {
	// Namespace narrows them to one knowledge base. Empty is every one of them.
	Namespace string
	// WithText reads the text each was posted as, for a caller that compares the text rather
	// than lists it: what a sync would write over, and what a change records. A listing
	// leaves it out.
	WithText bool
}

// KnowledgeDocuments are the documents knowledge bases were filled with, in the shared
// database.
type KnowledgeDocuments struct {
	store *Store
}

// Upsert records how many passages a document was cut into. A source the knowledge base
// already has is updated in place and keeps its id, since posting it again replaced the same
// passages rather than adding a second copy. The row takes the state and error the caller
// sets.
func (r *KnowledgeDocuments) Upsert(ctx context.Context, document *KnowledgeDocument) error {
	if err := document.Validate(); err != nil {
		return stack.Wrap(err)
	}

	document.ID = newID()
	now := time.Now().UTC()
	document.CreatedAt = now
	document.UpdatedAt = now
	document.ObjectKey = ""

	// Text posted over a file still waiting to be read is the newer of the two, so the row
	// lets go of the file and the read that finds it gone leaves the row alone.
	_, err := r.store.global.NewInsert().Model(document).
		On("CONFLICT (customer_id, namespace, source) DO UPDATE").
		Set("passages = EXCLUDED.passages").
		Set("text = EXCLUDED.text").
		Set("state = EXCLUDED.state").
		Set("error = EXCLUDED.error").
		Set("pages = EXCLUDED.pages").
		Set("content_type = EXCLUDED.content_type").
		Set("bytes = EXCLUDED.bytes").
		Set("object_key = EXCLUDED.object_key").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("id, created_at").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save knowledge document: %w", err))
	}
	return nil
}

// Pend records a file uploaded to a knowledge base, waiting in the bucket under
// document.ObjectKey to be read. A source the base already has keeps its id and its passages,
// which are still looked up until the file's replace them. The row comes back with the
// passages and pages it still has.
func (r *KnowledgeDocuments) Pend(ctx context.Context, document *KnowledgeDocument) error {
	document.State = KnowledgeDocumentPending
	document.Error = ""
	if err := document.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if document.ObjectKey == "" {
		return stack.Wrap(&ValidationError{Model: "knowledge document", Field: "object_key", Reason: "is required for a file waiting to be read"})
	}

	document.ID = newID()
	now := time.Now().UTC()
	document.CreatedAt = now
	document.UpdatedAt = now

	_, err := r.store.global.NewInsert().Model(document).
		On("CONFLICT (customer_id, namespace, source) DO UPDATE").
		Set("state = EXCLUDED.state").
		Set("error = EXCLUDED.error").
		Set("content_type = EXCLUDED.content_type").
		Set("bytes = EXCLUDED.bytes").
		Set("object_key = EXCLUDED.object_key").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("id, created_at, passages, pages").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: pend knowledge file: %w", err))
	}
	return nil
}

// Cover raises the document's passage count to cover passages about to be written for its
// file, under the same condition as Finish, so a write that stops part way leaves a row whose
// delete removes every passage either version wrote.
func (r *KnowledgeDocuments) Cover(ctx context.Context, customerID, id, objectKey string, passages int) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*KnowledgeDocument)(nil)).
		Set("passages = GREATEST(passages, ?)", passages).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("object_key = ?", objectKey).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: cover knowledge file: %w", err))
	}
	return updatedOne(result, "cover knowledge file")
}

// Finish records a file read into its passages, as long as the row still waits for that file.
// It is false when the document was removed, or uploaded or posted again, while the file was
// read: the row is no longer this read's to write.
func (r *KnowledgeDocuments) Finish(ctx context.Context, document *KnowledgeDocument, objectKey string) (bool, error) {
	document.UpdatedAt = time.Now().UTC()
	document.State = KnowledgeDocumentIndexed
	document.Error = ""
	document.ObjectKey = ""
	result, err := r.store.global.NewUpdate().Model(document).
		Column("text", "passages", "pages", "state", "error", "object_key", "updated_at").
		Where("id = ?", document.ID).
		Where("customer_id = ?", document.CustomerID).
		Where("object_key = ?", objectKey).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: finish knowledge file: %w", err))
	}
	return updatedOne(result, "finish knowledge file")
}

// Fail records why a file could not be read, under the same condition as Finish. A last
// failure marks the document failed and lets go of the file; an earlier one leaves it pending,
// to be tried again.
func (r *KnowledgeDocuments) Fail(ctx context.Context, customerID, id, objectKey, reason string, last bool) (bool, error) {
	query := r.store.global.NewUpdate().Model((*KnowledgeDocument)(nil)).
		Set("error = ?", reason).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("object_key = ?", objectKey)
	if last {
		query = query.Set("state = ?", KnowledgeDocumentFailed).Set("object_key = ''")
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: fail knowledge file: %w", err))
	}
	return updatedOne(result, "fail knowledge file")
}

// Get returns one document a customer filled a knowledge base with, its text included.
func (r *KnowledgeDocuments) Get(ctx context.Context, customerID, id string) (KnowledgeDocument, error) {
	if customerID == "" || id == "" {
		return KnowledgeDocument{}, stack.Wrap(errors.New("store: a customer and a knowledge document id are required"))
	}

	var document KnowledgeDocument
	err := r.store.global.NewSelect().Model(&document).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return KnowledgeDocument{}, unknownKnowledgeDocument(id)
	}
	if err != nil {
		return KnowledgeDocument{}, stack.Wrap(fmt.Errorf("store: knowledge document: %w", err))
	}
	return document, nil
}

// Select returns the documents a customer filled its knowledge bases with, most recently
// written first. Their text is left out unless the filter asks for it.
func (r *KnowledgeDocuments) Select(ctx context.Context, customerID string, filter KnowledgeDocumentFilter) ([]KnowledgeDocument, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}

	var documents []KnowledgeDocument
	query := r.store.global.NewSelect().Model(&documents).Where("customer_id = ?", customerID)
	if !filter.WithText {
		query = query.ExcludeColumn("text")
	}
	if filter.Namespace != "" {
		query = query.Where("namespace = ?", filter.Namespace)
	}
	if err := query.Order("updated_at DESC").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer knowledge documents: %w", err))
	}
	return documents, nil
}

// Delete forgets a document. Its passages are the caller's to remove, since they are not in
// this database.
func (r *KnowledgeDocuments) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a knowledge document id are required"))
	}

	result, err := r.store.global.NewDelete().Model((*KnowledgeDocument)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete knowledge document: %w", err))
	}
	deleted, err := updatedOne(result, "delete knowledge document")
	if err != nil {
		return err
	}
	if !deleted {
		return unknownKnowledgeDocument(id)
	}
	return nil
}

// DeleteBase forgets every document and marks every page of one knowledge base as gone, and
// returns how many of each there were. Their passages are the caller's to remove, since they
// are not in this database. The pages are another repository's rows, written here so the two
// go in one transaction.
func (r *KnowledgeDocuments) DeleteBase(ctx context.Context, customerID, namespace string) (documents, pages int64, err error) {
	if customerID == "" || namespace == "" {
		return 0, 0, stack.Wrap(errors.New("store: a customer and a namespace are required"))
	}

	err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		deleted, err := tx.NewDelete().Model((*KnowledgeDocument)(nil)).
			Where("customer_id = ?", customerID).
			Where("namespace = ?", namespace).
			Exec(ctx)
		if err != nil {
			return err
		}
		if documents, err = deleted.RowsAffected(); err != nil {
			return err
		}
		marked, err := tx.NewUpdate().Model((*KnowledgeURL)(nil)).
			Set("deleted_at = ?", time.Now().UTC()).
			Where("customer_id = ?", customerID).
			Where("namespace = ?", namespace).
			Where("deleted_at IS NULL").
			Exec(ctx)
		if err != nil {
			return err
		}
		pages, err = marked.RowsAffected()
		return err
	})
	if err != nil {
		return 0, 0, stack.Wrap(fmt.Errorf("store: delete knowledge base: %w", err))
	}
	return documents, pages, nil
}

// updatedOne is whether result changed exactly one row.
func updatedOne(result sql.Result, doing string) (bool, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: %s: %w", doing, err))
	}
	return affected == 1, nil
}

func unknownKnowledgeDocument(id string) error {
	return stack.Wrap(fmt.Errorf("%w: %s", ErrNoKnowledgeDocument, id))
}
