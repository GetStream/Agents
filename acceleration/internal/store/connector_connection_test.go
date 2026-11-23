//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing/fstest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/uptrace/bun"
)

// connection creates a connection to the acme built-in, app-owned unless change says
// otherwise, after seeding acme at revision 1.
func (s *StoreSuite) connection(customerID string, change func(*ConnectorConnection)) ConnectorConnection {
	s.seed(acmeManifest)
	connection := appConnection()
	connection.CustomerID = customerID
	if change != nil {
		change(connection)
	}
	s.Require().NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection))
	return *connection
}

// userOwned makes a connection the named user's.
func userOwned(userID string) func(*ConnectorConnection) {
	return func(connection *ConnectorConnection) {
		connection.OwnerType = OwnerUser
		connection.OwnerID = userID
	}
}

// bind makes a live agent config of the customer's carry bindings, written as T20 will
// write them. The store has no write for the column until then.
func (s *StoreSuite) bind(customerID, bindings string) string {
	config := &AgentConfig{CustomerID: customerID, Name: "agent-" + newID()}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET connectors = ?::jsonb WHERE id = ?", bindings, config.ID)
	s.Require().NoError(err)
	return config.ID
}

func fixedBinding(connectionID string) string {
	return `[{"name": "acme", "connector_id": "acme", "connection": {"type": "fixed", "connection_id": "` + connectionID + `"},` +
		` "tools": [{"name": "search", "schema_digest": "` + strings.Repeat("a", 64) + `"}], "required": true}]`
}

// insertRaw writes a connection row the way a writer that skips the store would, such as a
// data move's import, so only the database's own rules stand in its way.
func (s *StoreSuite) insertRaw(ownerType, ownerID string) error {
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO connector_connections (id, customer_id, connector_id, definition_revision, owner_type, owner_id, auth_scheme)"+
			" VALUES (?, 'acme-app', 'acme', 1, ?, ?, 'test_key')", newID(), ownerType, ownerID)
	return err
}

func (s *StoreSuite) TestACreatedConnectionIsPendingAtRevisionOneWithNoCredentials() {
	created := s.connection("acme-app", func(connection *ConnectorConnection) {
		connection.Inputs = map[string]string{"region": "eu"}
		connection.Label = "Support workspace"
	})

	found, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", created.ID)
	s.Require().NoError(err)
	s.Equal(created.ID, found.ID)
	s.Equal("acme", found.ConnectorID)
	s.Equal(1, found.DefinitionRevision)
	s.Equal(OwnerApp, found.OwnerType)
	s.Empty(found.OwnerID)
	s.Equal("test_key", found.AuthScheme)
	s.Equal(map[string]string{"region": "eu"}, found.Inputs)
	s.Equal(map[string]string{}, found.Metadata)
	s.Equal("Support workspace", found.Label)
	s.Equal(ConnectionPending, found.Status)
	s.Equal(1, found.Revision)
	s.Empty(found.CredentialsSealed)
	s.Zero(found.CredentialsKEKVersion)
	s.Equal([]string{}, found.GrantedScopes)
	s.True(created.CreatedAt.Equal(found.CreatedAt), "the row handed back is the row a read returns")
	s.Nil(found.DeletedAt)
}

func (s *StoreSuite) TestARegisteredAuthSchemeItsConnectorDoesNotListIsRefused() {
	s.seed(acmeManifest)
	connection := appConnection()
	connection.AuthScheme = "test_unlisted"

	err := s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection)

	s.ErrorIs(err, ErrSchemeNotAllowed)
	s.ErrorContains(err, `acme revision 1 does not list auth scheme "test_unlisted"`)
}

func (s *StoreSuite) TestTheDatabaseRefusesAnAppOwnedConnectionNamingAUser() {
	s.ErrorContains(s.insertRaw(OwnerApp, "alice"), "connector_connections_owner")
}

func (s *StoreSuite) TestTheDatabaseRefusesAUserOwnedConnectionNamingNoUser() {
	s.ErrorContains(s.insertRaw(OwnerUser, ""), "connector_connections_owner")
}

func (s *StoreSuite) TestTheDatabaseRefusesAThirdKindOfOwner() {
	s.ErrorContains(s.insertRaw("organization", "acme-org"), "connector_connections_owner")
}

func (s *StoreSuite) TestTheDatabaseTakesBothOwners() {
	s.NoError(s.insertRaw(OwnerApp, ""))
	s.NoError(s.insertRaw(OwnerUser, "alice"))
}

func (s *StoreSuite) TestAConnectionCannotPinARevisionThatDoesNotExist() {
	s.seed(acmeManifest)
	connection := appConnection()
	connection.DefinitionRevision = 2

	err := s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection)

	s.ErrorIs(err, ErrNoConnectorDefinition)
	s.ErrorContains(err, "acme revision 2")
}

func (s *StoreSuite) TestAConnectionCanPinAnOlderRevision() {
	s.seed(acmeManifest)
	s.seed(acmeChanged)
	connection := appConnection()

	s.Require().NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection))

	found, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(1, found.DefinitionRevision, "a new revision does not move a connection made from the old one")
}

func (s *StoreSuite) TestAConnectionCanPinTheCustomersOwnDefinition() {
	custom, err := s.store.ConnectorDefinitions.Revise(s.ctx, "acme-app",
		parsed(s.T(), strings.Replace(acmeManifest, "id: acme", "id: custom_acme", 1)))

	s.Require().NoError(err)
	connection := appConnection()
	connection.ConnectorID = custom.ID

	s.NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection))
}

func (s *StoreSuite) TestAConnectionCannotPinAnotherCustomersDefinition() {
	_, err := s.store.ConnectorDefinitions.Revise(s.ctx, "other-app",
		parsed(s.T(), strings.Replace(acmeManifest, "id: acme", "id: custom_acme", 1)))

	s.Require().NoError(err)
	connection := appConnection()
	connection.ConnectorID = "custom_acme"

	s.ErrorIs(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection), ErrNoConnectorDefinition)
}

func (s *StoreSuite) TestAConnectionIsOnlyItsOwnCustomers() {
	mine := s.connection("acme-app", nil)

	_, err := s.store.ConnectorConnections.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)

	listed, err := s.store.ConnectorConnections.Select(s.ctx, "other-app", ConnectionFilter{OwnerType: OwnerApp})
	s.Require().NoError(err)
	s.Empty(listed)

	_, err = s.store.ConnectorConnections.Referenced(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)

	s.ErrorIs(s.store.ConnectorConnections.Delete(s.ctx, "other-app", mine.ID), ErrNoConnectorConnection)
	_, err = s.store.ConnectorConnections.Get(s.ctx, "acme-app", mine.ID)
	s.NoError(err, "another customer's delete leaves it alone")
}

func (s *StoreSuite) TestListingByOwnerReturnsOnlyThatOwnersConnectionsNewestFirst() {
	first := s.connection("acme-app", userOwned("alice"))
	s.connection("acme-app", userOwned("bob"))
	s.connection("acme-app", nil)
	second := s.connection("acme-app", userOwned("alice"))

	listed, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerUser, OwnerID: "alice"})
	s.Require().NoError(err)
	s.Require().Len(listed, 2)
	s.Equal(second.ID, listed[0].ID)
	s.Equal(first.ID, listed[1].ID)

	apps, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerApp})
	s.Require().NoError(err)
	s.Len(apps, 1, "the app's own connections are not any user's, and the other way round")
}

func (s *StoreSuite) TestListingByOwnerNarrowsToOneConnector() {
	_, err := s.store.ConnectorDefinitions.Revise(s.ctx, "acme-app",
		parsed(s.T(), strings.Replace(acmeManifest, "id: acme", "id: custom_acme", 1)))

	s.Require().NoError(err)
	s.connection("acme-app", nil)
	custom := s.connection("acme-app", func(connection *ConnectorConnection) { connection.ConnectorID = "custom_acme" })

	listed, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerApp, ConnectorID: "custom_acme"})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(custom.ID, listed[0].ID)
}

func (s *StoreSuite) TestListingByOwnerPagesFromWhereTheLastPageEnded() {
	oldest := s.connection("acme-app", nil)
	middle := s.connection("acme-app", nil)
	newest := s.connection("acme-app", nil)

	page, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerApp, Limit: 1})
	s.Require().NoError(err)
	s.Require().Len(page, 2, "one more than the limit says there is another page")
	s.Equal(newest.ID, page[0].ID)

	next, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{
		OwnerType: OwnerApp, Limit: 2,
		After: &ConnectionPosition{CreatedAt: page[0].CreatedAt, ID: page[0].ID},
	})

	s.Require().NoError(err)
	s.Require().Len(next, 2)
	s.Equal(middle.ID, next[0].ID)
	s.Equal(oldest.ID, next[1].ID)
}

func (s *StoreSuite) TestADeletedConnectionCannotBeRead() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	_, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestADeletedConnectionIsNotListed() {
	kept := s.connection("acme-app", userOwned("alice"))
	deleted := s.connection("acme-app", userOwned("alice"))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", deleted.ID))

	listed, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerUser, OwnerID: "alice"})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(kept.ID, listed[0].ID)
}

func (s *StoreSuite) TestADeletedConnectionIsNotAskedAboutReferences() {
	connection := s.connection("acme-app", nil)
	s.bind("acme-app", fixedBinding(connection.ID))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	_, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestADeletedConnectionCannotBeDeletedAgain() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	s.ErrorIs(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID), ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAnUnforcedDeleteRefusesABoundConnectionAndLeavesItLive() {
	connection := s.connection("acme-app", nil)
	s.bind("acme-app", fixedBinding(connection.ID))

	s.ErrorIs(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID), ErrConnectorConnectionBound)
	found, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err, "a refused delete leaves the connection live")
	s.Equal(ConnectionPending, found.Status)
}

func (s *StoreSuite) TestAnUnforcedDeleteDeletesAnUnboundConnection() {
	connection := s.connection("acme-app", nil)
	other := s.connection("acme-app", nil)
	s.bind("acme-app", fixedBinding(other.ID))

	s.Require().NoError(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID))
	_, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAnUnforcedDeleteOfAMissingConnectionIsNotFound() {
	deleted := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", deleted.ID))
	others := s.connection("other-app", nil)
	s.bind("other-app", fixedBinding(others.ID))

	s.ErrorIs(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", newID()), ErrNoConnectorConnection)
	s.ErrorIs(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", deleted.ID), ErrNoConnectorConnection)
	s.ErrorIs(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", others.ID), ErrNoConnectorConnection,
		"another customer's connection is not found, bound or not")
}

// The race a check before the delete had: a bind that commits after the check and before the
// UPDATE. LOCK TABLE in SHARE mode holds every UPDATE of the table at its ROW EXCLUSIVE lock
// and lets a plain SELECT through (https://www.postgresql.org/docs/current/explicit-locking.html,
// table 13.2). Postgres takes that lock while it parses the UPDATE and only then the snapshot
// the UPDATE runs with (exec_simple_query in src/backend/tcop/postgres.c: execution does not
// reuse "a snapshot that has been acquired before locking any of the tables mentioned in the
// query"). So a delete that checked in a SELECT of its own has checked when it stops here,
// and one that checks inside the UPDATE has not. A wait on the connection's row lock is the
// next test's.
func (s *StoreSuite) TestABindThatCommitsBeforeTheDeleteTakesItsSnapshotStopsIt() {
	connection := s.connection("acme-app", nil)
	locker := s.router()
	held, err := locker.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer held.Rollback() //nolint:errcheck // after the commit below there is nothing to roll back
	_, err = held.ExecContext(s.ctx, "LOCK TABLE connector_connections IN SHARE MODE")
	s.Require().NoError(err)

	deleter := s.router()
	deleted := make(chan error, 1)
	go func() { deleted <- deleter.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID) }()
	// The two seconds and the ten milliseconds are assertTheWaitEnded's (credentials_test.go).
	s.Require().Eventually(func() bool { return s.waitingForTable("connector_connections") == 1 },
		2*time.Second, 10*time.Millisecond, "the delete waits for the table lock")
	s.bind("acme-app", fixedBinding(connection.ID))
	s.Require().NoError(held.Commit())

	s.ErrorIs(<-deleted, ErrConnectorConnectionBound)
	_, err = s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.NoError(err, "the bound connection stays live")
}

// waitingForTable counts the callers in this database queued for a lock on the table
// (https://www.postgresql.org/docs/current/view-pg-locks.html).
func (s *StoreSuite) waitingForTable(table string) int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_locks
WHERE locktype = 'relation' AND relation = ?::regclass AND NOT granted
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`, table).Scan(&count))
	return count
}

// waitingForALock counts the backends of this database waiting for a heavyweight lock, a
// table's or a row's (wait_event_type Lock,
// https://www.postgresql.org/docs/current/monitoring-stats.html#WAIT-EVENT-TABLE).
func (s *StoreSuite) waitingForALock() int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&count))
	return count
}

// begin opens a transaction on a router of its own, rolled back when the test ends unless it
// was committed.
func (s *StoreSuite) begin() bun.Tx {
	held, err := s.router().GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = held.Rollback() })
	return held
}

// E2a in #746. Another transaction holds the connection's row lock, so the delete waits at
// its own lock. A bind that does not lock (s.bind writes the column directly) commits during
// the wait. The delete's UPDATE starts after the wait, with a snapshot that holds the bind.
func (s *StoreSuite) TestABindThatCommitsWhileTheDeleteWaitsForTheRowStopsIt() {
	connection := s.connection("acme-app", nil)
	held := s.begin()
	_, err := held.ExecContext(s.ctx, "SELECT id FROM connector_connections WHERE id = ? FOR UPDATE", connection.ID)
	s.Require().NoError(err)

	deleter := s.router()
	deleted := make(chan error, 1)
	go func() { deleted <- deleter.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID) }()
	// The two seconds and the ten milliseconds are assertTheWaitEnded's (credentials_test.go).
	s.Require().Eventually(func() bool { return s.waitingForALock() == 1 },
		2*time.Second, 10*time.Millisecond, "the delete waits for the row")
	s.bind("acme-app", fixedBinding(connection.ID))
	s.Require().NoError(held.Commit())

	s.ErrorIs(<-deleted, ErrConnectorConnectionBound)
	_, err = s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.NoError(err, "the bound connection stays live")
}

// E3a in #746. The config save has locked the connection and is held at its INSERT by a
// SHARE lock on agent_configs, which blocks an INSERT and lets the delete's read of the
// table through (https://www.postgresql.org/docs/current/explicit-locking.html, table
// 13.2). The delete waits for the save, then sees its binding.
func (s *StoreSuite) TestADeleteWaitsForABindInProgressAndIsRefused() {
	connection := s.connection("acme-app", nil)
	held := s.begin()
	_, err := held.ExecContext(s.ctx, "LOCK TABLE agent_configs IN SHARE MODE")
	s.Require().NoError(err)

	saver := s.router()
	saved := make(chan error, 1)
	go func() {
		saved <- saver.AgentConfigs.Insert(s.ctx, &AgentConfig{CustomerID: "acme-app", Name: "bound",
			Connectors: []ConnectorBinding{fixed(connection.ID)}})
	}()
	s.Require().Eventually(func() bool { return s.waitingForTable("agent_configs") == 1 },
		2*time.Second, 10*time.Millisecond, "the save waits at its INSERT")
	deleter := s.router()
	deleted := make(chan error, 1)
	go func() { deleted <- deleter.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID) }()
	s.Eventually(func() bool { return s.waitingForALock() == 2 },
		2*time.Second, 10*time.Millisecond, "the delete waits for the save's lock on the connection")
	s.Require().NoError(held.Commit())

	s.Require().NoError(<-saved)
	s.ErrorIs(<-deleted, ErrConnectorConnectionBound)
	_, err = s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.NoError(err, "the bound connection stays live")
}

// E3b in #746. The delete has locked the connection and is held at its UPDATE by a SHARE
// lock on connector_connections, which a SELECT FOR UPDATE's ROW SHARE lock passes (table
// 13.2). The config save waits for the delete, then finds no live connection to bind.
func (s *StoreSuite) TestABindWaitsForADeleteInProgressAndIsRefused() {
	connection := s.connection("acme-app", nil)
	held := s.begin()
	_, err := held.ExecContext(s.ctx, "LOCK TABLE connector_connections IN SHARE MODE")
	s.Require().NoError(err)

	deleter := s.router()
	deleted := make(chan error, 1)
	go func() { deleted <- deleter.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connection.ID) }()
	s.Require().Eventually(func() bool { return s.waitingForTable("connector_connections") == 1 },
		2*time.Second, 10*time.Millisecond, "the delete waits at its UPDATE")
	saver := s.router()
	saved := make(chan error, 1)
	go func() {
		saved <- saver.AgentConfigs.Insert(s.ctx, &AgentConfig{CustomerID: "acme-app", Name: "bound",
			Connectors: []ConnectorBinding{fixed(connection.ID)}})
	}()
	s.Eventually(func() bool { return s.waitingForALock() == 2 },
		2*time.Second, 10*time.Millisecond, "the save waits for the delete's lock on the connection")
	s.Require().NoError(held.Commit())

	s.NoError(<-deleted)
	s.ErrorIs(<-saved, ErrNoConnectorConnection)
	configs, err := s.store.AgentConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Empty(configs, "no binding to the deleted connection is stored")
}

// The bind's lock is FOR KEY SHARE, which a plain UPDATE's FOR NO KEY UPDATE does not wait
// for (https://www.postgresql.org/docs/current/explicit-locking.html, table 13.3). FOR SHARE
// would hold a credential save until the config save commits.
func (s *StoreSuite) TestABindInProgressDoesNotHoldUpACredentialSave() {
	connection := s.connection("acme-app", nil)
	held := s.begin()
	_, err := held.ExecContext(s.ctx, "LOCK TABLE agent_configs IN SHARE MODE")
	s.Require().NoError(err)

	saver := s.router()
	saved := make(chan error, 1)
	go func() {
		saved <- saver.AgentConfigs.Insert(s.ctx, &AgentConfig{CustomerID: "acme-app", Name: "bound",
			Connectors: []ConnectorBinding{fixed(connection.ID)}})
	}()
	s.Require().Eventually(func() bool { return s.waitingForTable("agent_configs") == 1 },
		2*time.Second, 10*time.Millisecond, "the save waits at its INSERT, holding the connection")
	credentials := make(chan error, 1)
	go func() {
		connection.Revision = 2
		connection.CredentialsSealed = []byte("sealed for revision 2")
		connection.CredentialsKEKVersion = 1
		credentials <- s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1)
	}()
	select {
	case err := <-credentials:
		s.NoError(err)
	case <-time.After(2 * time.Second):
		s.Fail("the credential save waits for the config save")
	}
	s.Require().NoError(held.Commit())

	s.NoError(<-saved)
}

func (s *StoreSuite) TestDeletingAConnectionDropsItsCredentials() {
	connection := s.connection("acme-app", nil)
	// The revisioned save that writes credentials is T8's; this is the state it leaves.
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE connector_connections SET credentials_sealed = 'sealed credentials', credentials_kek_version = 1, status = ?, expires_at = now() WHERE id = ?",
		ConnectionConnected, connection.ID)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	var sealed []byte
	var version int
	var status string
	var expires *time.Time
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT credentials_sealed, credentials_kek_version, status, expires_at FROM connector_connections WHERE id = ?",
		connection.ID).Scan(&sealed, &version, &status, &expires))
	s.Empty(sealed)
	s.Zero(version)
	s.Equal(ConnectionDisconnected, status)
	s.Nil(expires)
}

func (s *StoreSuite) TestAFixedBindingReferencesItsConnection() {
	connection := s.connection("acme-app", nil)
	s.bind("acme-app", fixedBinding(connection.ID))

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.True(referenced)
}

func (s *StoreSuite) TestAnUnboundConnectionIsNotReferenced() {
	connection := s.connection("acme-app", nil)
	other := s.connection("acme-app", nil)
	s.bind("acme-app", fixedBinding(other.ID))

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.False(referenced)
}

func (s *StoreSuite) TestASessionBindingReferencesNoConnection() {
	connection := s.connection("acme-app", userOwned("alice"))
	// A session binding names no connection; it carries an id here only to show the type is
	// what decides.
	s.bind("acme-app", `[{"name": "acme", "connector_id": "acme", "connection": {"type": "session", "connection_id": "`+connection.ID+`"}}]`)

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.False(referenced)
}

func (s *StoreSuite) TestADeletedConfigReferencesNothing() {
	connection := s.connection("acme-app", nil)
	configID := s.bind("acme-app", fixedBinding(connection.ID))
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", configID))

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.False(referenced)
}

func (s *StoreSuite) TestAnotherCustomersConfigReferencesNothingOfMine() {
	connection := s.connection("acme-app", nil)
	s.bind("other-app", fixedBinding(connection.ID))

	referenced, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.False(referenced)
}

func (s *StoreSuite) TestTheStateItselfIsNotStored() {
	s.attempt(s.connection("acme-app", nil), "bearer-state")

	var stored int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM connector_authorization_attempts WHERE to_jsonb(connector_authorization_attempts)::text LIKE '%bearer-state%'").Scan(&stored))
	s.Zero(stored)
}

// lineManifest is a built-in whose events arrive at one URL for every customer: verified with
// the operator's secret, each message naming the unit that received it, the shape of the
// WhatsApp fixture (internal/connectors/core/testdata/manifests/whatsapp.yaml) cut down.
const lineManifest = `
id: line
revision: 1
name: Line
schemes: [test_key]
client:
  env: LINE
channel:
  verifier:
    kind: hmac_header
    secret: operator
    header: X-Signature
    algorithm: sha256
    encoding: hex
    signed: "{body}"
  format: json
  messages:
    provider_unit_id: $.unit
    thread_key:
      - name: from
        path: $.from
    author_id: $.from
    provider_message_id: $.id
    text: $.text
  reply:
    url: https://line.example/{provider_unit_id}/messages
    body:
      to: "{thread.from}"
      text: "{text}"
`

// ownLineManifest is lineManifest verified with the customer's own provider app instead, so
// each customer's events arrive at a URL of their own.
var ownLineManifest = strings.Replace(strings.Replace(lineManifest, "id: line", "id: own_line", 1),
	"secret: operator", "secret: provider_app", 1)

// lineConnection creates a connected, app-owned connection of the customer's to a built-in
// channel connector, seeding it from raw first.
func (s *StoreSuite) lineConnection(customerID, raw string) ConnectorConnection {
	manifest := parsed(s.T(), raw)
	s.Require().NoError(s.store.ConnectorDefinitions.Seed(s.ctx, fstest.MapFS{manifest.ID + ".yaml": {Data: []byte(raw)}}))
	connection := appConnection()
	connection.CustomerID = customerID
	connection.ConnectorID = manifest.ID
	s.Require().NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection))
	s.moveTo(*connection, ConnectionConnected)
	return *connection
}

// moveTo saves a new status onto a connection the way the credential store does, under its
// lock: a consent moves it to connected, a revocation to needs_reauthorization.
func (s *StoreSuite) moveTo(connection ConnectorConnection, status string) {
	s.Require().NoError(s.store.ConnectorConnections.WithLocked(s.ctx, connection.CustomerID, connection.ID,
		func(locked *ConnectorConnection, _ func() error) (bool, error) {
			locked.Status = status
			return true, nil
		}))
}

func (s *StoreSuite) TestAConnectionIsFoundByItsConnectorAndProviderUnitWithoutItsCustomer() {
	connection := s.lineConnection("acme-app", lineManifest)
	s.lineConnection("acme-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "106540352242922"))

	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "106540352242922")

	s.Require().NoError(err)
	s.Equal(connection.ID, found.ID)
	s.Equal("acme-app", found.CustomerID, "the event names no customer; the connection does")
	s.Equal("106540352242922", found.ProviderUnitID)
}

func (s *StoreSuite) TestAProviderUnitNoConnectionHoldsIsNotFound() {
	connection := s.lineConnection("acme-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1"))

	_, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-2")

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAProviderUnitAnotherCustomerHoldsIsRefused() {
	first := s.lineConnection("acme-app", lineManifest)
	second := s.lineConnection("other-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", first.ID, "unit-1"))

	err := s.store.ConnectorConnections.SetProviderUnit(s.ctx, "other-app", second.ID, "unit-1")

	s.ErrorIs(err, ErrProviderUnitTaken)
	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(first.ID, found.ID, "the first holder keeps the unit")
}

func (s *StoreSuite) TestDeletingAConnectionFreesItsProviderUnit() {
	first := s.lineConnection("acme-app", lineManifest)
	second := s.lineConnection("other-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", first.ID, "unit-1"))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", first.ID))

	_, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.ErrorIs(err, ErrNoConnectorConnection, "a deleted connection is not routed to")

	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "other-app", second.ID, "unit-1"))
	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(second.ID, found.ID)
}

func (s *StoreSuite) TestARevokedConnectionFreesItsProviderUnitForTheNextConsent() {
	revoked := s.lineConnection("acme-app", lineManifest)
	next := s.lineConnection("other-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", revoked.ID, "unit-1"))

	s.moveTo(revoked, ConnectionNeedsReauthorization)

	_, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.ErrorIs(err, ErrNoConnectorConnection, "an ended grant is not routed to")
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "other-app", next.ID, "unit-1"))
	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(next.ID, found.ID)

	s.moveTo(revoked, ConnectionConnected)
	err = s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", revoked.ID, "unit-1")
	s.ErrorIs(err, ErrProviderUnitTaken, "a reconnect proves the unit again, and it is taken now")
}

func (s *StoreSuite) TestASaveThatStaysConnectedKeepsTheProviderUnitTheRowHolds() {
	connection := s.lineConnection("acme-app", lineManifest)
	readBefore, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1"))

	// A refresh that read the row before the unit was stored.
	readBefore.Revision++
	readBefore.CredentialsSealed = []byte("refreshed")
	readBefore.CredentialsKEKVersion = 1
	s.Require().NoError(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &readBefore, readBefore.Revision-1))

	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(connection.ID, found.ID)
}

func (s *StoreSuite) TestValidatedToolsSurviveACredentialsSaveThatReadTheRowBefore() {
	connection := s.connection("acme-app", nil)
	readBefore, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	tools := []ConnectorTool{{Name: "search", Description: "Finds a message.", InputSchema: map[string]any{"type": "object"}, SchemaDigest: "digest-1"}}
	checked := time.Now()
	s.Require().NoError(s.store.ConnectorConnections.SetTools(s.ctx, "acme-app", connection.ID, tools, "list-1", checked))

	readBefore.Revision++
	readBefore.CredentialsSealed = []byte("renewed")
	readBefore.CredentialsKEKVersion = 1
	s.Require().NoError(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &readBefore, readBefore.Revision-1))

	found, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(tools, found.CachedTools)
	s.Equal("list-1", found.ToolsDigest)
	s.Require().NotNil(found.ToolsCheckedAt)
	s.WithinDuration(checked, *found.ToolsCheckedAt, time.Millisecond)
}

func (s *StoreSuite) TestToolsAreNotStoredOnADeletedConnection() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	err := s.store.ConnectorConnections.SetTools(s.ctx, "acme-app", connection.ID, nil, "list-1", time.Now())

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAConnectionThatIsNotConnectedTakesNoProviderUnit() {
	connection := s.lineConnection("acme-app", lineManifest)
	s.moveTo(connection, ConnectionPending)

	err := s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1")

	s.ErrorIs(err, ErrConnectorConnectionNotConnected)
	_, err = s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.ErrorIs(err, ErrNoConnectorConnection)
}

// A revocation's save holds the row while the unit is being set: the set read the connection
// as connected, waits for the row, and then finds the grant ended.
func (s *StoreSuite) TestARevocationThatCommitsWhileTheUnitIsSetWins() {
	connection := s.lineConnection("acme-app", lineManifest)
	held := s.begin()
	_, err := held.ExecContext(s.ctx, "UPDATE connector_connections SET status = ?, provider_unit_id = NULL WHERE id = ?",
		ConnectionNeedsReauthorization, connection.ID)
	s.Require().NoError(err)

	setter := s.router()
	set := make(chan error, 1)
	go func() { set <- setter.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1") }()
	// The two seconds and the ten milliseconds are assertTheWaitEnded's (credentials_test.go).
	s.Require().Eventually(func() bool { return s.waitingForALock() == 1 },
		2*time.Second, 10*time.Millisecond, "the set waits for the row")
	s.Require().NoError(held.Commit())

	s.ErrorIs(<-set, ErrConnectorConnectionChanged)
	_, err = s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestTheSameProviderUnitUnderAnotherConnectorIsAllowed() {
	other := strings.Replace(lineManifest, "id: line", "id: line_two", 1)
	first := s.lineConnection("acme-app", lineManifest)
	second := s.lineConnection("other-app", other)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", first.ID, "unit-1"))

	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "other-app", second.ID, "unit-1"))

	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line_two", "unit-1")
	s.Require().NoError(err)
	s.Equal(second.ID, found.ID)
	found, err = s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(first.ID, found.ID)
}

func (s *StoreSuite) TestANewProviderUnitReplacesAndFreesTheOldOne() {
	connection := s.lineConnection("acme-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1"))

	s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-2"))

	_, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.ErrorIs(err, ErrNoConnectorConnection)
	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-2")
	s.Require().NoError(err)
	s.Equal(connection.ID, found.ID)
}

func (s *StoreSuite) TestAnotherCustomersConnectionTakesNoProviderUnitFromMe() {
	theirs := s.lineConnection("other-app", lineManifest)

	err := s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", theirs.ID, "unit-1")

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestADeletedConnectionTakesNoProviderUnit() {
	connection := s.lineConnection("acme-app", lineManifest)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	err := s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1")

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAConnectorWhoseEventsURLNamesTheCustomerTakesNoProviderUnit() {
	own := s.lineConnection("acme-app", ownLineManifest)
	tools := s.connection("acme-app", nil)

	s.ErrorIs(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", own.ID, "T0001"), ErrNoSharedWebhook,
		"two customers' own apps may sit in one workspace")
	s.ErrorIs(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", tools.ID, "T0001"), ErrNoSharedWebhook,
		"a connector with no channel has no events to route")
}

func (s *StoreSuite) TestACustomDefinitionTakesNoProviderUnit() {
	custom, err := s.store.ConnectorDefinitions.Revise(s.ctx, "acme-app",
		parsed(s.T(), strings.Replace(lineManifest, "id: line", "id: custom_line", 1)))

	s.Require().NoError(err)
	connection := appConnection()
	connection.ConnectorID = custom.ID
	s.Require().NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, connection))

	err = s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", connection.ID, "unit-1")

	s.ErrorIs(err, ErrNoSharedWebhook, "one customer's definition does not speak for the operator's app")
}

func (s *StoreSuite) TestCustomersRacingForOneProviderUnitLeaveOneHolder() {
	// Each customer is a router of its own with a pool of its own, connected before they all
	// start at once, so the writes overlap in Postgres rather than queue on one pool.
	const customers = 8
	connections := make([]ConnectorConnection, customers)
	routers := make([]*Store, customers)
	for i := range customers {
		connections[i] = s.lineConnection("app-"+newID(), lineManifest)
		router, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { router.Close() })
		s.Require().NoError(router.Ping(s.ctx))
		routers[i] = router
	}
	errs := make([]error, customers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = router.ConnectorConnections.SetProviderUnit(s.ctx, connections[i].CustomerID, connections[i].ID, "unit-1")
		}()
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, err := range errs {
		if err == nil {
			s.Equal(-1, winner, "a second customer also took the unit")
			winner = i
			continue
		}
		s.True(errors.Is(err, ErrProviderUnitTaken), "a customer that loses is told the unit is taken: %v", err)
	}
	s.Require().NotEqual(-1, winner)
	found, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
	s.Require().NoError(err)
	s.Equal(connections[winner].ID, found.ID)
}

// TestAUsersGrantedConnectionsAreHerLiveOnesThatHoldCredentials: what the user delete revokes
// is one user's live connections with a grant, not another user's, the app's, another
// customer's, a pending one or a deleted one.
func (s *StoreSuite) TestAUsersGrantedConnectionsAreHerLiveOnesThatHoldCredentials() {
	first := s.connection("acme-app", userOwned("alice"))
	second := s.connection("acme-app", userOwned("alice"))
	s.connection("acme-app", userOwned("alice"))
	gone := s.connection("acme-app", userOwned("alice"))
	bob := s.connection("acme-app", userOwned("bob"))
	app := s.connection("acme-app", nil)
	elsewhere := s.connection("other-app", userOwned("alice"))
	for _, connection := range []ConnectorConnection{first, second, gone, bob, app, elsewhere} {
		s.granted(connection)
	}
	s.deletedKeepingCredentials(gone)

	found, err := s.store.ConnectorConnections.SelectGrantedOfUser(s.ctx, "acme-app", "alice")

	s.Require().NoError(err)
	s.Equal([]string{first.ID, second.ID}, connectionIDs(found))
}

// deletedKeepingCredentials marks connection deleted and leaves its credentials, which no
// delete does: a row a query must leave out for being deleted alone.
func (s *StoreSuite) deletedKeepingCredentials(connection ConnectorConnection) {
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET deleted_at = now() WHERE id = ?", connection.ID)
	s.Require().NoError(err)
}

func connectionIDs(connections []ConnectorConnection) []string {
	ids := make([]string, 0, len(connections))
	for _, connection := range connections {
		ids = append(ids, connection.ID)
	}
	return ids
}

// granted gives a connection stored credentials, as a consent leaves it.
func (s *StoreSuite) granted(connection ConnectorConnection) {
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE connector_connections SET credentials_sealed = 'sealed credentials', credentials_kek_version = 1, status = ? WHERE id = ?",
		ConnectionConnected, connection.ID)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestDeletingAUsersConnectionsRemovesEveryOneOfThemAndNothingElse() {
	live := s.connection("acme-app", userOwned("alice"))
	s.granted(live)
	gone := s.connection("acme-app", userOwned("alice"))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", gone.ID))
	pending := s.connection("acme-app", userOwned("alice"))
	s.attempt(pending, "state-of-alices-consent")
	s.invocation(live, time.Second, "")
	bob := s.connection("acme-app", userOwned("bob"))
	app := s.connection("acme-app", nil)
	elsewhere := s.connection("other-app", userOwned("alice"))

	deleted, err := s.store.ConnectorConnections.DeleteUser(s.ctx, "acme-app", "alice")

	s.Require().NoError(err)
	s.ElementsMatch([]DeletedConnection{
		{ID: live.ID, ConnectorID: "acme", OwnerType: OwnerUser, HadGrant: true},
		{ID: gone.ID, ConnectorID: "acme", OwnerType: OwnerUser},
		{ID: pending.ID, ConnectorID: "acme", OwnerType: OwnerUser},
	}, deleted)
	var left int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM connector_connections WHERE customer_id = 'acme-app' AND owner_id = 'alice'").Scan(&left))
	s.Zero(left, "the soft deleted row, which still held the owner and account ids, is gone too")
	var invocations, attempts int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM connector_invocations WHERE connection_id = ?", live.ID).Scan(&invocations))
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM connector_authorization_attempts WHERE connection_id = ?", pending.ID).Scan(&attempts))
	s.Zero(invocations)
	s.Zero(attempts)
	for _, kept := range []ConnectorConnection{bob, app, elsewhere} {
		_, err := s.store.ConnectorConnections.Get(s.ctx, kept.CustomerID, kept.ID)
		s.NoError(err, "another user's, the app's and another customer's stay")
	}
}

// TestDeletingAUsersConnectionsDeletesTheirEventSubscriptions: the subscriptions' sealed secrets and
// callback tokens go with the connections; another user's, the app's and another customer's stay.
func (s *StoreSuite) TestDeletingAUsersConnectionsDeletesTheirEventSubscriptions() {
	first := s.connection("acme-app", userOwned("alice"))
	second := s.connection("acme-app", userOwned("alice"))
	bob := s.connection("acme-app", userOwned("bob"))
	app := s.connection("acme-app", nil)
	elsewhere := s.connection("other-app", userOwned("alice"))
	hers := []ConnectionEventSubscription{
		s.eventSubscription("acme-app", first.ID, "config-1", "key-1"),
		s.eventSubscription("acme-app", first.ID, "config-1", "key-2"),
		s.eventSubscription("acme-app", second.ID, "config-1", "key-1"),
	}
	kept := []ConnectionEventSubscription{
		s.eventSubscription("acme-app", bob.ID, "config-1", "key-1"),
		s.eventSubscription("acme-app", app.ID, "config-1", "key-1"),
		s.eventSubscription("other-app", elsewhere.ID, "config-1", "key-1"),
		// A row of another customer that names her connection's id: the customer is part of the match.
		s.eventSubscription("other-app", first.ID, "config-2", "key-1"),
	}

	_, err := s.store.ConnectorConnections.DeleteUser(s.ctx, "acme-app", "alice")

	s.Require().NoError(err)
	for _, sub := range hers {
		_, err := s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, sub.Token)
		s.ErrorIs(err, ErrNoConnectionEventSubscription, "her subscription %s is gone", sub.Token)
	}
	for _, sub := range kept {
		got, err := s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, sub.Token)
		s.Require().NoError(err, "subscription %s stays", sub.Token)
		s.Equal(sub.ID, got.ID)
	}
}

// TestAUserDeleteThatFailsKeepsTheirEventSubscriptions: the subscriptions go in the delete's transaction,
// so a delete that fails leaves them with the connections.
func (s *StoreSuite) TestAUserDeleteThatFailsKeepsTheirEventSubscriptions() {
	hers := s.connection("acme-app", userOwned("alice"))
	sub := s.eventSubscription("acme-app", hers.ID, "config-1", "key-1")
	s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, &ConnectorAuditEvent{
		CustomerID: "acme-app", ConnectionID: hers.ID, ConnectorID: "acme", OwnerType: OwnerUser,
		Action: AuditGrantCreated, Reason: AuditReasonConsent, Revision: 2,
	}))
	// Another transaction holds her audit row, so the delete's last statement waits until its deadline.
	holder, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.ExecContext(s.ctx, "SELECT 1 FROM connector_audit WHERE connection_id = ? FOR UPDATE", hers.ID)
	s.Require().NoError(err)
	ctx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
	defer cancel()

	_, err = s.store.ConnectorConnections.DeleteUser(ctx, "acme-app", "alice")

	s.Require().ErrorContains(err, "unlink a user's connector audit")
	s.Require().NoError(holder.Rollback())
	_, err = s.store.ConnectorConnections.Get(s.ctx, "acme-app", hers.ID)
	s.Require().NoError(err, "the connection stays")
	_, err = s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, sub.Token)
	s.NoError(err, "so does its subscription")
}

// TestDeletingAUsersConnectionsUnlinksTheirAuditFromTheUser: the rows stay, with no id that
// leads back to the user; another user's keep theirs.
func (s *StoreSuite) TestDeletingAUsersConnectionsUnlinksTheirAuditFromTheUser() {
	hers := s.connection("acme-app", userOwned("alice"))
	his := s.connection("acme-app", userOwned("bob"))
	for _, connection := range []ConnectorConnection{hers, his} {
		s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, &ConnectorAuditEvent{
			CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme", OwnerType: OwnerUser,
			Action: AuditGrantCreated, Reason: AuditReasonConsent, Revision: 2,
			RequestID: "request", SessionID: "session", AttemptID: "attempt",
		}))
	}

	_, err := s.store.ConnectorConnections.DeleteUser(s.ctx, "acme-app", "alice")

	s.Require().NoError(err)
	kept, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: hers.ID})
	s.Require().NoError(err)
	s.Require().Len(kept, 1, "the row stays")
	s.Empty(kept[0].RequestID)
	s.Empty(kept[0].SessionID)
	s.Empty(kept[0].AttemptID)
	s.Equal(AuditGrantCreated, kept[0].Action)
	others, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: his.ID})
	s.Require().NoError(err)
	s.Require().Len(others, 1)
	s.Equal("session", others[0].SessionID)
}

func (s *StoreSuite) TestDeletingTheConnectionsOfAUserWithNoneDeletesNothing() {
	s.connection("acme-app", userOwned("bob"))

	deleted, err := s.store.ConnectorConnections.DeleteUser(s.ctx, "acme-app", "alice")

	s.Require().NoError(err)
	s.Empty(deleted)
}

func (s *StoreSuite) TestAConnectionIsUsedByTheFixedBindingsOfLiveConfigsThatNameIt() {
	bound := s.connection("acme-app", nil)
	unbound := s.connection("acme-app", nil)
	config := s.bind("acme-app", fixedBinding(bound.ID))
	s.bind("acme-app", `[{"name": "acme", "connector_id": "acme", "connection": {"type": "session", "connection_id": "`+unbound.ID+`"}}]`)
	deleted := s.bind("acme-app", fixedBinding(bound.ID))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET deleted_at = now() WHERE id = ?", deleted)
	s.Require().NoError(err)
	s.bind("other-app", fixedBinding(bound.ID))
	s.bind("acme-app", `{"not": "a list"}`)

	uses, err := s.store.ConnectorConnections.Uses(s.ctx, "acme-app", []string{bound.ID, unbound.ID})

	s.Require().NoError(err)
	s.Require().Len(uses[bound.ID], 1, "a deleted config and another customer's do not use it")
	s.Equal(config, uses[bound.ID][0].ConfigID)
	s.Equal("acme", uses[bound.ID][0].Binding)
	s.Empty(uses[unbound.ID], "a session binding names no connection")
}

func (s *StoreSuite) TestAnotherCustomersWriteToAConnectionIdChangesNothing() {
	writes := map[string]func(id string) error{
		"PutClient": func(id string) error {
			return s.store.ConnectorConnections.PutClient(s.ctx, "other-app", id, ConnectorConnectionClient{
				Registration: core.ClientDCR, ClientID: "their-client"})
		},
		"PutValidation": func(id string) error {
			return s.store.ConnectorConnections.PutValidation(s.ctx, "other-app", id, ConnectorConnectionValidation{
				Revision: 99, Status: "failed", Code: "401", Error: "theirs", CheckedAt: time.Now().UTC().Truncate(time.Microsecond)})
		},
	}
	for name, write := range writes {
		s.Run(name, func() {
			s.SetupTest()
			connection := s.connection("acme-app", userOwned("alice"))
			before := s.storedConnection(connection.ID)

			s.Require().NoError(write(connection.ID))

			s.Equal(before, s.storedConnection(connection.ID), "acme-app's connection is as it was")
		})
	}
}

func (s *StoreSuite) TestALaterValidationReplacesTheLastOne() {
	connection := s.connection("acme-app", userOwned("alice"))
	first := time.Now().UTC().Truncate(time.Microsecond)
	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Status: "failed", Code: "503", Error: "Service Unavailable", CheckedAt: first}))

	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Status: "connected", CheckedAt: first.Add(time.Second)}))

	got := s.storedConnection(connection.ID).Validation
	s.Equal("connected", got.Status)
	s.Empty(got.Code)
	s.Empty(got.Error)
	s.True(first.Add(time.Second).Equal(got.CheckedAt))
}

// TestAValidationThatRanEarlierNeverReplacesALaterOne: of two validates at once, the one that
// ran first may write last; what the later one found stays.
func (s *StoreSuite) TestAValidationThatRanEarlierNeverReplacesALaterOne() {
	connection := s.connection("acme-app", userOwned("alice"))
	later := time.Now().UTC().Truncate(time.Microsecond)
	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Status: "connected", CheckedAt: later}))

	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Status: "failed", Code: "500", CheckedAt: later.Add(-time.Second)}))

	got := s.storedConnection(connection.ID).Validation
	s.Equal("connected", got.Status)
	s.True(later.Equal(got.CheckedAt))
}

// TestAValidationOfOlderCredentialsNeverReplacesOneOfNewerOnes: a validate of credentials
// replaced while it ran writes last, after a validate of the new ones; even though it finished
// later, what the new credentials' validate found stays.
func (s *StoreSuite) TestAValidationOfOlderCredentialsNeverReplacesOneOfNewerOnes() {
	connection := s.connection("acme-app", userOwned("alice"))
	checked := time.Now().UTC().Truncate(time.Microsecond)
	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Revision: 3, Status: "connected", CheckedAt: checked}))

	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Revision: 2, Status: "failed", Code: "503", CheckedAt: checked.Add(time.Second)}))

	got := s.storedConnection(connection.ID).Validation
	s.Equal("connected", got.Status)
	s.Equal(3, got.Revision)
}

// TestAValidationOfNewerCredentialsReplacesOneThatRanLater: the newer credentials' result
// replaces the older ones' whatever the clocks of the two routers say.
func (s *StoreSuite) TestAValidationOfNewerCredentialsReplacesOneThatRanLater() {
	connection := s.connection("acme-app", userOwned("alice"))
	checked := time.Now().UTC().Truncate(time.Microsecond)
	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Revision: 2, Status: "failed", Code: "503", CheckedAt: checked}))

	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
		Revision: 3, Status: "connected", CheckedAt: checked.Add(-time.Second)}))

	got := s.storedConnection(connection.ID).Validation
	s.Equal("connected", got.Status)
	s.Equal(3, got.Revision)
}

func (s *StoreSuite) TestAConnectionNeverValidatedHasNoValidation() {
	connection := s.connection("acme-app", userOwned("alice"))

	got := s.storedConnection(connection.ID).Validation
	s.Empty(got)
}

// withAccount gives a connection the metadata a consent for team and user would leave.
func withAccount(team, user string) func(*ConnectorConnection) {
	return func(connection *ConnectorConnection) {
		connection.Metadata = map[string]string{"team_id": team, "user_id": user}
	}
}

func (s *StoreSuite) TestConnectionsByIdentityFindTheAccountInEveryCustomer() {
	mine := s.connection("acme-app", withAccount("T1", "U1"))
	theirs := s.connection("other-app", withAccount("T1", "U1"))
	s.connection("acme-app", withAccount("T1", "U2"))
	s.connection("acme-app", withAccount("T2", "U1"))

	refs, err := s.store.ConnectorConnections.SelectByIdentity(s.ctx, "acme", nil, map[string]string{"team_id": "T1", "user_id": "U1"})

	s.Require().NoError(err)
	s.ElementsMatch([]core.ConnectionRef{
		{CustomerID: "acme-app", ConnectionID: mine.ID},
		{CustomerID: "other-app", ConnectionID: theirs.ID},
	}, refs)
}

// Fewer parts than the identity, such as a workspace uninstall that names no user, are every
// account that has them.
func (s *StoreSuite) TestConnectionsByIdentityWithFewerPartsFindEveryAccountWithThem() {
	alice := s.connection("acme-app", withAccount("T1", "U1"))
	bob := s.connection("acme-app", withAccount("T1", "U2"))
	s.connection("acme-app", withAccount("T2", "U1"))

	refs, err := s.store.ConnectorConnections.SelectByIdentity(s.ctx, "acme", nil, map[string]string{"team_id": "T1"})

	s.Require().NoError(err)
	s.ElementsMatch([]core.ConnectionRef{
		{CustomerID: "acme-app", ConnectionID: alice.ID},
		{CustomerID: "acme-app", ConnectionID: bob.ID},
	}, refs)
}

func (s *StoreSuite) TestConnectionsByIdentityLeaveOutADeletedConnection() {
	gone := s.connection("acme-app", withAccount("T1", "U1"))
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", gone.ID))

	refs, err := s.store.ConnectorConnections.SelectByIdentity(s.ctx, "acme", nil, map[string]string{"team_id": "T1"})

	s.Require().NoError(err)
	s.Empty(refs)
}

func (s *StoreSuite) TestConnectionsByIdentityNeedAPart() {
	_, err := s.store.ConnectorConnections.SelectByIdentity(s.ctx, "acme", nil, nil)

	s.ErrorContains(err, "at least one identity part are required")
}

func (s *StoreSuite) TestConnectorConnectionQueryBudgets() {
	var connections []ConnectorConnection
	for range 30 {
		connection := s.connection("acme-app", userOwned("alice"))
		s.granted(connection)
		connections = append(connections, connection)
	}
	ids := connectionIDs(connections)
	connection := s.storedConnection(connections[0].ID)
	now := time.Now().UTC()

	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.Insert(s.ctx, testSchemes, appConnection()))
	}), "Insert")
	s.Equal(1, s.countQueries(func() { s.storedConnection(connection.ID) }), "Get")
	line := s.lineConnection("acme-app", lineManifest)
	s.Equal(3, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.SetProviderUnit(s.ctx, "acme-app", line.ID, "unit-1"))
	}), "SetProviderUnit")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorConnections.GetByProviderUnit(s.ctx, "line", "unit-1")
		s.Require().NoError(err)
	}), "GetByProviderUnit")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.SetTools(s.ctx, "acme-app", connection.ID, []ConnectorTool{}, "digest", now))
	}), "SetTools")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerUser, OwnerID: "alice", Limit: 10})
		s.Require().NoError(err)
		s.Len(listed, 11)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		granted, err := s.store.ConnectorConnections.SelectGrantedOfUser(s.ctx, "acme-app", "alice")
		s.Require().NoError(err)
		s.Len(granted, 30)
	}), "SelectGrantedOfUser")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorConnections.Uses(s.ctx, "acme-app", ids)
		s.Require().NoError(err)
	}), "Uses")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorConnections.Referenced(s.ctx, "acme-app", connection.ID)
		s.Require().NoError(err)
	}), "Referenced")
	for _, id := range ids {
		s.Require().NoError(s.store.ConnectorConnections.PutClient(s.ctx, "acme-app", id, ConnectorConnectionClient{
			Registration: core.ClientDCR, ClientID: "dcr-client"}))
		s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, "acme-app", id, ConnectorConnectionValidation{
			Revision: 1, Status: "ok", CheckedAt: now}))
	}
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.PutClient(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionClient{
			Registration: core.ClientDCR, ClientID: "dcr-client"}))
	}), "PutClient")
	s.Equal(1, s.countQueries(func() {
		granted, err := s.store.ConnectorConnections.SelectGrantedOfClient(s.ctx, "acme-app", "acme", core.ClientDCR, "dcr-client")
		s.Require().NoError(err)
		s.Len(granted, 30)
	}), "SelectGrantedOfClient")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.ctx, connection.CustomerID, connection.ID, ConnectorConnectionValidation{
			Revision: 1, Status: "ok", CheckedAt: now.Add(time.Second)}))
	}), "PutValidation")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.ConnectorConnections.Select(s.ctx, "acme-app", ConnectionFilter{OwnerType: OwnerUser, OwnerID: "alice", Limit: 50})
		s.Require().NoError(err)
		for _, connection := range listed {
			s.Equal("dcr-client", connection.Client.ClientID)
			s.Equal("ok", connection.Validation.Status)
		}
	}), "Select reads each connection's client and last validation with it")
	s.Equal(3, s.countQueries(func() {
		saved := s.storedConnection(connection.ID)
		saved.Revision++
		saved.CredentialsSealed = []byte("rotated")
		s.Require().NoError(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &saved, saved.Revision-1))
	})-1, "SaveAtRevision")
	s.Equal(3, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) { return false, nil }))
	}), "WithLocked, unchanged")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorConnections.SelectByIdentity(s.ctx, "acme", map[string]string{"workspace": "acme"}, nil)
		s.Require().NoError(err)
	}), "SelectByIdentity")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connections[1].ID))
	}), "Delete")
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorConnections.DeleteUnbound(s.ctx, "acme-app", connections[2].ID))
	}), "DeleteUnbound")
	s.Equal(7, s.countQueries(func() {
		_, err := s.store.ConnectorConnections.DeleteUser(s.ctx, "acme-app", "alice")
		s.Require().NoError(err)
	}), "DeleteUser")
}
