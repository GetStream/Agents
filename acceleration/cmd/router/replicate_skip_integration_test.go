//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// definitionRow is a custom connector definition of acme's under id, with the schemes given.
func definitionRow(id, schemes string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"customer_id":"acme","id":%q,"revision":1,"name":"Acme","category":"","description":"",`+
		`"created_at":"2026-11-01T00:00:00Z","manifest":{"id":%q,"revision":1,"name":"Acme","endpoints":{"mcp":"https://mcp.acme.example/mcp"},`+
		`"schemes":%s,"sources":[{"kind":"mcp","endpoint":"mcp"}]}}`, id, id, schemes))
}

// A copy and a page of changes each holding a custom connector definition with
// oauth2_client_credentials carry on past it, write the rest and log the row each left out. A
// delete that carries one in its payload left nothing out.
func (s *ReplicateIDsSuite) TestAReplicateLeavesOutAClientCredentialsDefinitionAndSaysWhich() {
	copied, copiedSkip, followed, followedSkip := "custom_copied", "custom_copied_cc", "custom_followed", "custom_followed_cc"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/data/export":
			fmt.Fprintf(w, `{"table":"connector_definitions","row":%s}`+"\n", definitionRow(copiedSkip, `["oauth2_client_credentials"]`))
			fmt.Fprintf(w, `{"table":"connector_definitions","row":%s}`+"\n", definitionRow(copied, `["bearer"]`))
			fmt.Fprintln(w, `{"cursor":1,"customer":"acme"}`)
		case "/v1/data/changes":
			change := func(id, schemes string) store.DataChange {
				return store.DataChange{Table: "connector_definitions", Op: store.ChangeInsert,
					Key: json.RawMessage(fmt.Sprintf(`{"customer_id":"acme","id":%q,"revision":1}`, id)), Payload: definitionRow(id, schemes), At: time.Now().UTC()}
			}
			deleted := change("custom_deleted_cc", `["oauth2_client_credentials"]`)
			deleted.Op = store.ChangeDelete
			page := changePage{Changes: []store.DataChange{change(followedSkip, `["oauth2_client_credentials"]`), change(followed, `["bearer"]`), deleted},
				Cursor: 3, CaughtUp: true}
			s.NoError(json.NewEncoder(w).Encode(page))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	source := &deployment{url: server.URL, key: sourceKey, secret: sourceSecret, authType: auth.AuthTypeServer, client: server.Client()}
	var logged bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logged, nil))

	customerID, cursor, err := copyEverything(s.ctx, source, s.target, s.reserved(), "", false, time.Hour, logger)
	s.Require().NoError(err)
	s.Require().NoError(follow(s.ctx, source, s.target, s.reserved(), customerID, cursor, time.Millisecond, true, logger))

	for _, id := range []string{copied, followed} {
		_, err := s.target.ConnectorDefinitions.Latest(s.ctx, "acme", id)
		s.NoError(err, id)
	}
	var left []string
	for line := range strings.Lines(logged.String()) {
		var record struct {
			Msg  string `json:"msg"`
			Row  string `json:"row"`
			Rows *int   `json:"rows"`
		}
		s.Require().NoError(json.Unmarshal([]byte(line), &record))
		if record.Msg == skippedRowMessage {
			left = append(left, record.Row)
		}
		if record.Msg == "copied a customer" {
			// The row left out is not one copied.
			s.Require().NotNil(record.Rows)
			s.Equal(1, *record.Rows)
		}
	}
	s.Equal([]string{`connector_definitions:{"id":"custom_copied_cc","revision":1}`, `connector_definitions:{"id":"custom_followed_cc","revision":1}`}, left)
	for _, id := range []string{copiedSkip, followedSkip} {
		_, err := s.target.ConnectorDefinitions.Latest(s.ctx, "acme", id)
		s.ErrorIs(err, store.ErrNoConnectorDefinition, id)
	}
}
