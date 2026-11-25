package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
	// ObjectKey is what holds the row: the uploaded file in the knowledge bucket while it waits
	// to be read, posts of its text (Upsert) or its removal (Take). It is empty once a file has
	// been read. Whoever writes or deletes the document's passages takes the row under its key
	// first, and what it writes on the row afterwards only while the key is still there. One that
	// finds another key was overtaken, and what it did to the passages may have met what the
	// other did: a removal and a post do theirs again, and a read makes the removal do it.
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

// postKey begins the ObjectKey of a document last written by a post of its text. A file's key
// begins with its customer, so it is never one.
const postKey = "posting:"

// Upsert records text posted as a document whose passages are to be written, or may have been,
// and holds the row under document.ObjectKey, which Finish takes. The posts of one source
// share a key, which an upload and a removal change. A source the knowledge base already has
// is updated in place and keeps its id, since posting it again replaced the same passages
// rather than adding a second copy, and its count still covers the passages it had. The row
// takes the state and error the caller sets.
func (r *KnowledgeDocuments) Upsert(ctx context.Context, document *KnowledgeDocument) error {
	if err := document.Validate(); err != nil {
		return stack.Wrap(err)
	}

	document.ID = newID()
	now := time.Now().UTC()
	document.CreatedAt = now
	document.UpdatedAt = now
	document.ObjectKey = postKey + newID()

	// Text posted over a file still waiting to be read is the newer of the two, so the row
	// lets go of the file and the read that finds it gone leaves the row alone.
	_, err := r.store.global.NewInsert().Model(document).
		On("CONFLICT (customer_id, namespace, source) DO UPDATE").
		Set("passages = GREATEST(kd.passages, EXCLUDED.passages)").
		Set("text = EXCLUDED.text").
		Set("state = EXCLUDED.state").
		Set("error = EXCLUDED.error").
		Set("pages = EXCLUDED.pages").
		Set("content_type = EXCLUDED.content_type").
		Set("bytes = EXCLUDED.bytes").
		Set("object_key = CASE WHEN kd.object_key LIKE ? THEN kd.object_key ELSE EXCLUDED.object_key END", postKey+"%").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("id, created_at, object_key").
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

// Finish records a file read, or text posted, into its passages, as long as objectKey still
// holds the row. It is false when the document was removed or uploaded again meanwhile, or the
// file's was posted: the row is no longer this write's. A post leaves its key for the others it
// shares it with.
func (r *KnowledgeDocuments) Finish(ctx context.Context, document *KnowledgeDocument, objectKey string) (bool, error) {
	document.UpdatedAt = time.Now().UTC()
	document.State = KnowledgeDocumentIndexed
	document.Error = ""
	document.ObjectKey = ""
	if strings.HasPrefix(objectKey, postKey) {
		document.ObjectKey = objectKey
	}
	result, err := r.store.global.NewUpdate().Model(document).
		Column("text", "passages", "pages", "state", "error", "content_type", "bytes", "object_key", "updated_at").
		Where("id = ?", document.ID).
		Where("customer_id = ?", document.CustomerID).
		Where("object_key = ?", objectKey).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: finish knowledge document: %w", err))
	}
	return updatedOne(result, "finish knowledge document")
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

// removalKey begins the ObjectKey of a document being removed. A file's key begins with its
// customer, so it is never one.
const removalKey = "removing:"

// removalUnfinished is the error of a document whose removal has not finished.
const removalUnfinished = "Its removal did not finish. Delete it again."

// BeingRemoved is whether a removal holds the document.
func (k *KnowledgeDocument) BeingRemoved() bool { return strings.HasPrefix(k.ObjectKey, removalKey) }

// Take begins the removal of a document, and returns the key the removal holds it under and
// how many passages to remove. The row lets go of its file, as it does for a newer upload, so
// a read of it finds it was overtaken. The row says failed until it is removed, so a removal
// that stops part way leaves a document that is listed, not shown as found, and can be removed
// again.
func (r *KnowledgeDocuments) Take(ctx context.Context, customerID, id string) (key string, passages int, err error) {
	if customerID == "" || id == "" {
		return "", 0, stack.Wrap(errors.New("store: a customer and a knowledge document id are required"))
	}

	var taken []KnowledgeDocument
	err = r.store.global.NewUpdate().Model((*KnowledgeDocument)(nil)).
		Set("object_key = CASE WHEN object_key LIKE ? THEN object_key ELSE ? END", removalKey+"%", removalKey+newID()).
		Set("state = ?", KnowledgeDocumentFailed).
		Set("error = ?", removalUnfinished).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Returning("object_key, passages").
		Scan(ctx, &taken)
	if err != nil {
		return "", 0, stack.Wrap(fmt.Errorf("store: take knowledge document: %w", err))
	}
	if len(taken) == 0 {
		return "", 0, unknownKnowledgeDocument(id)
	}
	return taken[0].ObjectKey, taken[0].Passages, nil
}

// FailRemoval says a document's removal did not finish, unless a file uploaded since waits to
// be read. It holds the row as Take does, so a post that took the row from the removal writes
// its passages again, as the failed removal may have deleted them.
func (r *KnowledgeDocuments) FailRemoval(ctx context.Context, customerID, id string) error {
	_, err := r.store.global.NewUpdate().Model((*KnowledgeDocument)(nil)).
		Set("object_key = ?", removalKey+newID()).
		Set("state = ?", KnowledgeDocumentFailed).
		Set("error = ?", removalUnfinished).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("(object_key = '' OR object_key LIKE ?)", postKey+"%").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: fail knowledge document removal: %w", err))
	}
	return nil
}

// Interrupt makes the removal a document is under take its passages away again, for a read
// that wrote passages of them after the removal began, which the row counts if it did not. It
// is false when no removal holds the document.
func (r *KnowledgeDocuments) Interrupt(ctx context.Context, customerID, id string, passages int) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*KnowledgeDocument)(nil)).
		Set("object_key = ?", removalKey+newID()).
		Set("passages = GREATEST(passages, ?)", passages).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("object_key LIKE ?", removalKey+"%").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: interrupt knowledge document removal: %w", err))
	}
	return updatedOne(result, "interrupt knowledge document removal")
}

// Delete forgets a document whose removal still holds it under key, once its passages are
// gone, since they are not in this database. It is false when the removal no longer holds it,
// and then it may have passages again.
func (r *KnowledgeDocuments) Delete(ctx context.Context, customerID, id, key string) (bool, error) {
	if customerID == "" || id == "" || key == "" {
		return false, stack.Wrap(errors.New("store: a customer, a knowledge document id and its removal's key are required"))
	}

	result, err := r.store.global.NewDelete().Model((*KnowledgeDocument)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("object_key = ?", key).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: delete knowledge document: %w", err))
	}
	return updatedOne(result, "delete knowledge document")
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
