//go:build integration

package store

import (
	"time"

	"github.com/uptrace/bun"
)

// invocation records one call of connection's, started at offset past s.base.
func (s *StoreSuite) invocation(connection ConnectorConnection, offset time.Duration, errorType string) ConnectorInvocation {
	row := &ConnectorInvocation{
		CustomerID: connection.CustomerID, ConnectionID: connection.ID, ConnectorID: connection.ConnectorID,
		ConfigID: "config", Binding: "acme", Tool: "search", SessionID: "session",
		StartedAt: s.base.Add(offset), LatencyMs: 12, ErrorType: errorType,
	}
	s.Require().NoError(s.store.ConnectorInvocations.Insert(s.ctx, row))
	return *row
}

func (s *StoreSuite) TestAConnectionsInvocationsAreListedNewestFirstAndPaged() {
	connection := s.connection("acme-app", nil)
	first := s.invocation(connection, time.Second, "")
	second := s.invocation(connection, 2*time.Second, InvocationExternalServer)
	third := s.invocation(connection, 3*time.Second, InvocationDenied)

	page, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID, Limit: 2})
	s.Require().NoError(err)
	s.Require().Len(page, 3, "one more than the limit says there is another page")
	rest, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID, Limit: 2, After: &InvocationPosition{StartedAt: page[1].StartedAt, ID: page[1].ID}})

	s.Require().NoError(err)

	s.Equal([]string{third.ID, second.ID}, []string{page[0].ID, page[1].ID})
	s.Require().Len(rest, 1)
	s.Equal(first.ID, rest[0].ID)
	s.Equal(InvocationDenied, page[0].ErrorType)
	s.Equal(int64(12), page[0].LatencyMs)
}

func (s *StoreSuite) TestAnInvocationIsOnlyItsOwnCustomersAndConnections() {
	connection := s.connection("acme-app", nil)
	other := s.connection("acme-app", nil)
	s.invocation(connection, time.Second, "")

	ofOther, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: other.ID, Limit: 0, After: nil})
	s.Require().NoError(err)
	ofStranger, err := s.store.ConnectorInvocations.Select(s.ctx, "other-app", InvocationFilter{ConnectionID: connection.ID, Limit: 0, After: nil})
	s.Require().NoError(err)

	s.Empty(ofOther)
	s.Empty(ofStranger)
}

func (s *StoreSuite) TestAnInvocationKeepsItsProviderStatusAndErrorMessage() {
	connection := s.connection("acme-app", nil)
	status := 403
	failed := &ConnectorInvocation{
		CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme", Tool: "search",
		StartedAt: s.base, ErrorType: InvocationCustomerAuth, ProviderStatus: &status,
		ErrorMessage: "the server refused the request with 403 Forbidden",
	}
	s.Require().NoError(s.store.ConnectorInvocations.Insert(s.ctx, failed))
	answered := s.invocation(connection, time.Second, "")

	rows, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID, Limit: 0, After: nil})
	s.Require().NoError(err)

	s.Require().Len(rows, 2)
	s.Equal(answered.ID, rows[0].ID)
	s.Nil(rows[0].ProviderStatus)
	s.Empty(rows[0].ErrorMessage)
	s.Equal(failed.ID, rows[1].ID)
	s.Equal(&status, rows[1].ProviderStatus)
	s.Equal("the server refused the request with 403 Forbidden", rows[1].ErrorMessage)
}

// baseInvocation is ConnectorInvocation as the router before
// 20261031130000_connector_invocation_failures.sql has it (11fae4b1), without the provider's
// status and the error message.
type baseInvocation struct {
	bun.BaseModel `bun:"table:connector_invocations,alias:ci"`

	ID           string    `bun:"id,pk"`
	CustomerID   string    `bun:"customer_id,notnull"`
	ConnectionID string    `bun:"connection_id,notnull"`
	ConnectorID  string    `bun:"connector_id,notnull"`
	ConfigID     string    `bun:"config_id,notnull"`
	Binding      string    `bun:"binding,notnull"`
	Tool         string    `bun:"tool,notnull"`
	SessionID    string    `bun:"session_id,notnull"`
	StartedAt    time.Time `bun:"started_at,notnull"`
	LatencyMs    int64     `bun:"latency_ms,notnull"`
	ErrorType    string    `bun:"error_type,notnull"`
}

// TestARouterBeforeTheMigrationStillWritesAndReadsItsRows: the migration only adds columns
// that are nullable or have a default, so the base router's insert, with its column list, and
// its read of the rows the new one wrote both work.
func (s *StoreSuite) TestARouterBeforeTheMigrationStillWritesAndReadsItsRows() {
	connection := s.connection("acme-app", nil)
	old := &baseInvocation{
		ID: newID(), CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme", ConfigID: "config",
		Binding: "acme", Tool: "search", StartedAt: s.base, LatencyMs: 7, ErrorType: InvocationDenied,
	}
	_, err := s.store.GlobalDB().NewInsert().Model(old).Exec(s.ctx)
	s.Require().NoError(err)
	status := 502
	s.Require().NoError(s.store.ConnectorInvocations.Insert(s.ctx, &ConnectorInvocation{
		CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme", Tool: "search",
		StartedAt: s.base.Add(time.Second), ErrorType: InvocationExternalServer, ProviderStatus: &status,
		ErrorMessage: "Bad Gateway",
	}))

	rows, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID, Limit: 0, After: nil})
	s.Require().NoError(err)
	var read []baseInvocation
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&read).Where("connection_id = ?", connection.ID).
		Order("started_at").Scan(s.ctx))

	s.Require().Len(rows, 2)
	s.Equal(old.ID, rows[1].ID)
	s.Nil(rows[1].ProviderStatus)
	s.Empty(rows[1].ErrorMessage)
	s.Require().Len(read, 2)
	s.Equal(old.ID, read[0].ID)
	s.Equal(InvocationExternalServer, read[1].ErrorType)
}

func (s *StoreSuite) TestAnInvocationWithAnUnknownErrorTypeIsRefused() {
	connection := s.connection("acme-app", nil)

	err := s.store.ConnectorInvocations.Insert(s.ctx, &ConnectorInvocation{
		CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme", Tool: "search",
		StartedAt: s.base, ErrorType: "timeout",
	})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("error_type", invalid.Field)
	rows, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID})
	s.Require().NoError(err)
	s.Empty(rows, "nothing was stored")
}

func (s *StoreSuite) TestInvocationsInsertedTogetherAreAllStoredOrNone() {
	connection := s.connection("acme-app", nil)
	row := ConnectorInvocation{CustomerID: "acme-app", ConnectionID: connection.ID, Tool: "search", StartedAt: s.base}
	refused := row
	refused.ErrorType = "timeout"

	s.Require().NoError(s.store.ConnectorInvocations.InsertMany(s.ctx, []ConnectorInvocation{row, row, row}))
	err := s.store.ConnectorInvocations.InsertMany(s.ctx, []ConnectorInvocation{row, refused})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	rows, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID})
	s.Require().NoError(err)
	s.Len(rows, 3, "each row of the first batch has an id of its own, and the refused batch left none")
}

func (s *StoreSuite) TestConnectorInvocationQueryBudgets() {
	connection := s.connection("acme-app", nil)
	for i := range 30 {
		s.invocation(connection, time.Duration(i)*time.Second, "")
	}

	s.Equal(1, s.countQueries(func() { s.invocation(connection, time.Minute, "") }), "Insert")
	s.Equal(1, s.countQueries(func() {
		rows := make([]ConnectorInvocation, 50)
		for i := range rows {
			rows[i] = ConnectorInvocation{CustomerID: "acme-app", ConnectionID: connection.ID, Tool: "search", StartedAt: s.base}
		}
		s.Require().NoError(s.store.ConnectorInvocations.InsertMany(s.ctx, rows))
	}), "InsertMany")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorInvocations.Insert(s.ctx, &ConnectorInvocation{CustomerID: "acme-app",
			ConnectionID: connection.ID, Tool: "search", StartedAt: s.base, Arguments: []ArgumentShape{{Name: "q", Type: "string"}}}))
	}), "Insert with arguments")
	s.Equal(1, s.countQueries(func() {
		rows, err := s.store.ConnectorInvocations.Select(s.ctx, "acme-app", InvocationFilter{ConnectionID: connection.ID, Limit: 10})
		s.Require().NoError(err)
		s.Len(rows, 11)
	}), "Select")
}
