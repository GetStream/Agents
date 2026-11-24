package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/danielgtaylor/huma/v2"
)

// defaultChangeLimit and maxChangeLimit bound one page of changes. A page is held in
// memory on both sides, and a row can be a whole agent response, so the ceiling is there
// to keep a caller from asking for more than either end can hold.
const (
	defaultChangeLimit = 500
	maxChangeLimit     = 1000
)

// exportWritesWait bounds how long an export waits for the writes open when it started
// (Store.WaitForOpenWrites): twice shippingIdle (internal/store/shipping.go), the idle limit
// shipping sets on its own transaction, a write transaction of the router that is not
// meant to sit open. Postgres cannot say whose a transaction still open past that is, and
// an export is a rare call whose caller can come back, so it is told to rather than held.
const exportWritesWait = 30 * time.Second

// exportLineWait bounds the write of one line of an export. The export reads in one
// transaction, which holds a database connection until the last line is written, so a client
// that stops reading lets go of it after this rather than holding it for as long as it likes.
// A line is one row, which a client still reading takes in well under a minute.
const exportLineWait = time.Minute

// exportWait bounds a whole export. Its rows are read in one repeatable-read transaction, and
// Postgres vacuums none of the rows that snapshot can see while it is open. An hour is a
// choice: it reads 36 GB at 10 MB/s.
const exportWait = time.Hour

// maxExports bounds the exports running on one router at once, each holding a connection of
// its pool: a tenth of the 40 the store opens (store.maxConns), so the rest are left to
// requests.
const maxExports = 4

// maxImportLine bounds one line of an import, which is read into memory whole. A line is one
// row, and the largest row the router writes is a conversation document's text, read from a
// file of at most maxFetchedDocument (20 MiB): 64 MiB holds that with room for its JSON
// escaping.
const maxImportLine = 64 << 20

// maxImports bounds the imports running on one router at once. Each holds a buffer of up to
// maxImportLine, grown by doubling, and the copy of a row that decoding its line makes: 128 MiB
// or more an import at its largest line, so four hold over half a GiB.
const maxImports = 4

// importLineWait bounds the read of one line of an import, which holds one of maxImports and
// its app's place until it ends, so a client that stops sending lets go of them after this. A
// line of maxImportLine arrives within it at over a megabyte a second.
const importLineWait = time.Minute

// errExportRunning refuses a second export of an app while one is running on this router. Each
// export holds a connection of this router's pool until its client has read it all.
var errExportRunning = conflict("an export of this app is already running: wait for it to finish")

// errExportTooLong is an export whose exportWait passed before its first row was read.
var errExportTooLong = unavailable("the export ran out of time before its first row: try again")

// errExportsBusy refuses an export while maxExports are running on this router.
var errExportsBusy = unavailable("this router is running as many exports as it takes at once: try again later")

// errImportRunning refuses a second import of an app while one is running on this router.
var errImportRunning = APIError{Type: ErrorTypeConflict, Code: codeImportRunning,
	Message: "an import of this app is already running: wait for it to finish"}

// errImportStalled ends an import whose client sent no line for importLineWait.
var errImportStalled = newAPIError(ErrorTypeTimeout, "the import's next line did not arrive in time: send the body without pausing")

// errImportsBusy refuses an import while maxImports are running on this router.
var errImportsBusy = unavailable("this router is running as many imports as it takes at once: try again later")

// errWritesOpen is a capture start, or the export after it, that gave up on a write from before
// the start (store.ErrWritesOpen).
var errWritesOpen = unavailable("a write that started before the changes were recorded is still open: try again")

// errShardedSource refuses a move off a router with shards. A shard's tables (calls, call
// bridges, recordings, sessions, responses, requests and their stats) record no changes
// (watchDataChanges), so a write to one after the export would be lost by a move that says
// it lost nothing.
var errShardedSource = notConfigured("moving a customer off a router with shards is not supported yet: " +
	"changes to the shards' calls, sessions, responses, requests and stats after an export cannot be followed")

// dataLine is one line of an export.
//
// Rows, and then a last line naming the cursor their changes carry on from. It is last
// rather than first because it is also what says the export finished: a stream that died
// halfway has no cursor, and importing one without noticing would leave somebody with
// half their data and no way to tell. A line carrying a change rather than a row is what
// replaying onto the other deployment sends, so one endpoint takes both and a move is the
// same request twice.
type dataLine struct {
	Cursor   *int64            `json:"cursor,omitempty"`
	Customer string            `json:"customer,omitempty"`
	At       *time.Time        `json:"at,omitempty"`
	Table    string            `json:"table,omitempty"`
	Row      json.RawMessage   `json:"row,omitempty"`
	Change   *store.DataChange `json:"change,omitempty"`
}

// exportData writes everything the calling app has, as one JSON object per line.
func (s *Server) exportData(w http.ResponseWriter, r *http.Request) {
	customerID, ok := s.dataMover(w, r)
	if !ok {
		return
	}
	if s.store.HasShards() {
		writeError(w, errShardedSource)
		return
	}
	if _, running := s.exporting.LoadOrStore(customerID, struct{}{}); running {
		writeError(w, errExportRunning)
		return
	}
	defer s.exporting.Delete(customerID)
	select {
	case s.exports <- struct{}{}:
	default:
		writeError(w, errExportsBusy)
		return
	}
	defer func() { <-s.exports }()
	ends := time.Now().Add(s.exportWait)

	// Recording starts before the snapshot is read rather than after, so a write that
	// lands while the export is being streamed is in one of the two and not in neither.
	// The start and the wait after it share one bound.
	deadline := time.Now().Add(s.exportWritesWait)
	if err := s.store.StartDataCapture(r.Context(), customerID, s.dataRetention, s.exportWritesWait); err != nil {
		if errors.Is(err, store.ErrWritesOpen) {
			writeError(w, errWritesOpen)
			return
		}
		s.logger.Error("could not start recording changes", "customer", customerID, "error", err)
		writeError(w, unavailable("changes cannot be recorded, so an export would be a copy nothing could catch up from"))
		return
	}
	// A write whose trigger looked before the recording started recorded nothing, so the
	// snapshot is read only once it has committed and is in the rows.
	if err := s.store.WaitForOpenWrites(r.Context(), time.Until(deadline)); err != nil {
		if errors.Is(err, store.ErrWritesOpen) {
			writeError(w, errWritesOpen)
			return
		}
		writeFailure(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	encoder := json.NewEncoder(w)
	controller := http.NewResponseController(w)
	encode := func(line dataLine) error {
		if err := controller.SetWriteDeadline(time.Now().Add(min(s.exportLineWait, time.Until(ends)))); err != nil {
			return fmt.Errorf("api: export write deadline: %w", err)
		}
		return encoder.Encode(line)
	}

	// A failure before the first line is still an error document. One after it is a
	// truncated stream, which the missing cursor line on the end is what says.
	written := int64(0)
	ctx, cancel := context.WithDeadline(r.Context(), ends)
	defer cancel()
	cursor, err := s.store.ExportCustomer(ctx, customerID, func(table string, row json.RawMessage) error {
		written++
		return encode(dataLine{Table: table, Row: row})
	})
	if err != nil {
		if written == 0 {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				writeError(w, errExportTooLong)
				return
			}
			writeFailure(w, r, err)
			return
		}
		s.logger.Error("export stopped partway", "customer", customerID, "error", err)
		return
	}

	now := time.Now().UTC()
	if err := encode(dataLine{Cursor: &cursor, Customer: customerID, At: &now}); err != nil {
		s.logger.Debug("export was not finished", "customer", customerID, "error", err)
		return
	}
	s.logger.Info("exported a customer", "customer", customerID, "rows", written, "cursor", cursor)
}

// importData writes an export, or a batch of changes, into this deployment.
func (s *Server) importData(w http.ResponseWriter, r *http.Request) {
	customerID, ok := s.dataMover(w, r)
	if !ok {
		return
	}

	// An export carries each row as the source had it when it was read, so writing one over an
	// app this deployment already holds would undo whatever was done to that row here since.
	full := false
	if raw := r.URL.Query().Get("full"); raw != "" {
		var err error
		if full, err = strconv.ParseBool(raw); err != nil {
			writeError(w, invalidRequest("full is true or false"))
			return
		}
	}
	if _, running := s.importing.LoadOrStore(customerID, struct{}{}); running {
		writeError(w, errImportRunning)
		return
	}
	defer s.importing.Delete(customerID)
	select {
	case s.imports <- struct{}{}:
	default:
		writeError(w, errImportsBusy)
		return
	}
	defer func() { <-s.imports }()

	// Asked at the first row rather than up front, so a batch of changes costs no lookup; once
	// that row is written the app holds it, so the rows after it are not asked about.
	checked := full

	var cursor *int64
	rows := int64(0)
	tables := map[string]int64{}

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(nil, maxImportLine)
	controller := http.NewResponseController(w)
	for {
		if err := controller.SetReadDeadline(time.Now().Add(s.importLineWait)); err != nil {
			writeFailure(w, r, fmt.Errorf("api: import read deadline: %w", err))
			return
		}
		if !scanner.Scan() {
			break
		}
		// The deadline is for the client's next line, not for writing this one: left on the
		// connection, it would end net/http's read after the body, which cancels the request.
		if err := controller.SetReadDeadline(time.Time{}); err != nil {
			writeFailure(w, r, fmt.Errorf("api: import read deadline: %w", err))
			return
		}
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var line dataLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			writeError(w, invalidRequest("this is not an export: an import is one JSON object per line: "+err.Error()))
			return
		}

		switch {
		case line.Change != nil:
			if err := s.store.ApplyChanges(r.Context(), customerID, []store.DataChange{*line.Change}); err != nil {
				writeImportFailure(w, r, err)
				return
			}
			if !line.Change.Skipped() {
				rows++
				tables[line.Change.Table]++
			}
		case line.Table != "":
			if !checked {
				table, err := s.store.HoldingTable(r.Context(), customerID)
				if err != nil {
					writeFailure(w, r, err)
					return
				}
				if table != "" {
					writeError(w, conflict("this app already has data here (a row in "+table+"), and an export's rows "+
						"would be written over what was done to them here since: import the changes since its cursor, "+
						"or pass full=true to copy everything again over what was done here"))
					return
				}
				checked = true
			}
			if err := s.store.ImportRow(r.Context(), customerID, line.Table, line.Row); err != nil {
				writeImportFailure(w, r, err)
				return
			}
			rows++
			tables[line.Table]++
		case line.Cursor != nil:
			cursor = line.Cursor
		}
	}
	if errors.Is(scanner.Err(), bufio.ErrTooLong) {
		writeError(w, payloadTooLarge(fmt.Sprintf("a line of an import is at most %d MiB", maxImportLine>>20)))
		return
	}
	if errors.Is(scanner.Err(), os.ErrDeadlineExceeded) {
		writeError(w, errImportStalled)
		return
	}
	if err := scanner.Err(); err != nil {
		writeError(w, invalidRequest("this is not an export: "+err.Error()))
		return
	}

	s.logger.Info("imported a customer", "customer", customerID, "rows", rows)
	writeJSON(w, map[string]any{"rows": rows, "tables": tables, "cursor": cursor})
}

// writeImportFailure answers a line the store did not write: an export the caller can correct
// with what to change, and anything else, such as a database that failed, whose text names
// where the database is, as a failure of ours that names nothing.
func writeImportFailure(w http.ResponseWriter, r *http.Request, err error) {
	var invalid *store.ValidationError
	if errors.Is(err, store.ErrInvalidImport) || errors.Is(err, store.ErrNotACustomConnector) || errors.As(err, &invalid) {
		writeError(w, invalidRequest(err.Error()))
		return
	}
	writeFailure(w, r, err)
}

// listDataChanges returns what has happened to the calling app's rows since a cursor.
func (s *Server) listDataChanges(w http.ResponseWriter, r *http.Request) {
	customerID, ok := s.dataMover(w, r)
	if !ok {
		return
	}
	if s.store.HasShards() {
		writeError(w, errShardedSource)
		return
	}

	after, err := strconv.ParseInt(nonEmpty(r.URL.Query().Get("after"), "0"), 10, 64)
	if err != nil || after < 0 {
		writeError(w, invalidRequest("after is the cursor the last page ended at"))
		return
	}
	limit := defaultChangeLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxChangeLimit {
			writeError(w, invalidRequest("limit is between 1 and "+strconv.Itoa(maxChangeLimit)))
			return
		}
	}

	// Following the changes is the other half of the move, so it says the customer is
	// still going rather than letting the recording expire under them.
	if err := s.store.StartDataCapture(r.Context(), customerID, s.dataRetention, s.exportWritesWait); err != nil {
		if errors.Is(err, store.ErrWritesOpen) {
			writeError(w, errWritesOpen)
			return
		}
		writeFailure(w, r, err)
		return
	}

	changes, cursor, err := s.store.Changes(r.Context(), customerID, after, limit)
	if errors.Is(err, store.ErrChangesExpired) {
		writeError(w, gone("the changes since that cursor are no longer kept: export again"))
		return
	}
	if err != nil {
		writeFailure(w, r, err)
		return
	}

	writeJSON(w, map[string]any{
		"changes": changes,
		"cursor":  cursor,
		// Fewer changes than were asked for means the end of what has happened so far,
		// which is the moment a switchover is safe.
		"caught_up": len(changes) < limit,
	})
}

// dataMover answers who is moving their data, and writes the refusal itself when nobody
// may.
//
// Three refusals, in the order they stop being guesses. A caller nobody authenticated has
// no data here. A device holding a token its own backend minted may hold a conversation
// and may not walk off with the customer's account, which is the ordinary server-side
// rule. And a deployment running without authentication must refuse outright: there the
// tenant is a header, so serving an export would mean handing any caller any customer's
// data for the price of naming them.
func (s *Server) dataMover(w http.ResponseWriter, r *http.Request) (string, bool) {
	customerID, known := CustomerFrom(r.Context())
	if !known {
		writeError(w, errMissingCustomer)
		return "", false
	}
	if s.refuseClientSide(w, r) {
		return "", false
	}
	if s.authMode == auth.NoAuth {
		writeError(w, forbidden("moving data is refused while this router runs without authentication: "+
			"the customer is whatever the caller says it is, so an export would be anybody's"))
		return "", false
	}
	if s.store == nil {
		writeError(w, unavailable("there is no database here to move"))
		return "", false
	}
	return customerID, true
}

func nonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// DataChange One thing that happened to one row of the calling app's data.
type DataChange struct {
	At    time.Time               `json:"at"`
	Key   map[string]interface{}  `json:"key" doc:"What identifies the row, which is all a delete has." nullable:"false"`
	Op    DataChangeOp            `json:"op" enum:"insert,update,delete"`
	Row   *map[string]interface{} `json:"row,omitempty" doc:"The row as it now reads, absent for a delete and never carrying a credential."`
	Seq   int64                   `json:"seq" doc:"Where this sits in the order changes happened, and the cursor to resume from."`
	Table string                  `json:"table" doc:"Which table the row is in." example:"agent_configs"`
}

func (*DataChange) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Description = "One thing that happened to one row of the calling app's data."
	return schema
}
