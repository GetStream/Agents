package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

func logCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}
func readLogCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if id < 0 {
		return 0, fmt.Errorf("negative cursor")
	}
	return id, err
}
func (s *Server) logFilter(w http.ResponseWriter, r *http.Request) (string, store.AgentLogFilter, bool) {
	customer, ok := CustomerFrom(r.Context())
	if !ok {
		writeError(w, errMissingCustomer)
		return "", store.AgentLogFilter{}, false
	}
	if s.refuseClientSide(w, r) {
		return "", store.AgentLogFilter{}, false
	}
	if s.store == nil {
		writeError(w, unavailable("logging storage unavailable"))
		return "", store.AgentLogFilter{}, false
	}
	q := r.URL.Query()
	f := store.AgentLogFilter{ConfigID: q.Get("config_id"), SessionID: q.Get("session_id"), UserID: q.Get("user_id"), Search: q.Get("q"), Severity: q.Get("severity"), Limit: 250}
	// The least serious level to show, not the only one: `warn` is how a reader
	// asks for the problems without the running commentary.
	if f.Severity != "" && f.Severity != "error" && f.Severity != "warn" && f.Severity != "info" {
		writeError(w, invalidRequest("invalid severity"))
		return customer, f, false
	}
	if len(f.Search) > 256 {
		writeError(w, invalidRequest("search too long"))
		return customer, f, false
	}
	if q.Get("source") != "" {
		for _, v := range strings.Split(q.Get("source"), ",") {
			if v != "user" && v != "agent" && v != "tool" && v != "system" {
				writeError(w, invalidRequest("invalid source"))
				return customer, f, false
			}
			f.Sources = append(f.Sources, v)
		}
	}
	for key, target := range map[string]*time.Time{"from": &f.From, "to": &f.To} {
		if q.Get(key) != "" {
			v, err := time.Parse(time.RFC3339Nano, q.Get(key))
			if err != nil {
				writeError(w, invalidRequest("invalid time"))
				return customer, f, false
			}
			*target = v
		}
	}
	if !f.To.IsZero() && !f.From.IsZero() && !f.To.After(f.From) {
		writeError(w, invalidRequest("invalid time range"))
		return customer, f, false
	}
	if q.Get("limit") != "" {
		v, err := strconv.Atoi(q.Get("limit"))
		if err != nil || v < 1 || v > 250 {
			writeError(w, invalidRequest("limit must be 1 to 250"))
			return customer, f, false
		}
		f.Limit = v
	}
	return customer, f, true
}
func logJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}
func (s *Server) listAgentLogs(w http.ResponseWriter, r *http.Request) {
	customer, f, ok := s.logFilter(w, r)
	if !ok {
		return
	}
	before, err := readLogCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, invalidRequest("invalid cursor"))
		return
	}
	high, err := s.store.AgentLogs.HighWater(r.Context(), customer)
	if err != nil {
		writeFailure(w, r, err)
		return
	}
	f.Before = high + 1
	if before > 0 && before < f.Before {
		f.Before = before
	}
	limit := f.Limit
	f.Limit++
	rows, err := s.store.AgentLogs.Select(r.Context(), customer, f)
	if err != nil {
		writeFailure(w, r, err)
		return
	}
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	next := ""
	if more {
		next = logCursor(rows[len(rows)-1].ID)
	}
	logJSON(w, map[string]any{"items": rows, "has_more": more, "next_cursor": next, "resume_cursor": logCursor(high), "dropped_logs": s.store.LogDrops.Load(), "coverage": "Structured logs are recorded from logging enablement. Earlier session history may contain additional events."})
}
func (s *Server) getAgentLog(w http.ResponseWriter, r *http.Request) {
	customer, _, ok := s.logFilter(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, invalidRequest("invalid log id"))
		return
	}
	entry, err := s.store.AgentLogs.Get(r.Context(), customer, id)
	if errors.Is(err, store.ErrNoAgentLog) {
		writeError(w, notFound("log not found"))
		return
	}
	if err != nil {
		writeFailure(w, r, err)
		return
	}
	logJSON(w, entry)
}
func (s *Server) streamAgentLogs(w http.ResponseWriter, r *http.Request) {
	customer, f, ok := s.logFilter(w, r)
	if !ok {
		return
	}
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("cursor")
	}
	after, err := readLogCursor(cursor)
	if err != nil || cursor == "" {
		writeError(w, invalidRequest("resume cursor required"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	send := func(event string, id int64, data any) error {
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		raw, _ := json.Marshal(data)
		if _, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", logCursor(id), event, raw); err != nil {
			return err
		}
		return controller.Flush()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		high, err := s.store.AgentLogs.HighWater(r.Context(), customer)
		if err != nil {
			// A reader that left ends it; anything else is the database's, for the access line.
			if r.Context().Err() == nil {
				recordFailure(r.Context(), err, stack.Trace(err))
			}
			return
		}
		if high < after {
			_ = send("reset", high, map[string]string{"message": "Log history changed; reload to resume."})
			return
		}
		f.After = after
		f.Before = high + 1
		f.Forward = true
		f.Limit = 250
		rows, err := s.store.AgentLogs.Select(r.Context(), customer, f)
		if err != nil {
			// A reader that left ends it; anything else is the database's, for the access line.
			if r.Context().Err() == nil {
				recordFailure(r.Context(), err, stack.Trace(err))
			}
			return
		}
		if len(rows) > 0 {
			after = rows[len(rows)-1].ID
			if err := send("logs", after, rows); err != nil {
				return
			}
		}
		if len(rows) == 250 {
			continue
		}
		after = high
		if err := send("checkpoint", after, map[string]any{"dropped_logs": s.store.LogDrops.Load()}); err != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
