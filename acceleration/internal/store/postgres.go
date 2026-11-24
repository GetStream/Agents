package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Postgres error codes the store tells apart.
const (
	foreignKeyViolation = "23503"
	uniqueViolation     = "23505"
)

// isViolation reports whether err is Postgres refusing a write with the given code.
func isViolation(err error, code string) bool {
	var refused pgdriver.Error
	return errors.As(err, &refused) && refused.Field('C') == code
}

// IsForeignKeyViolation reports whether err is Postgres refusing a row whose parent row is
// missing.
func IsForeignKeyViolation(err error) bool {
	return isViolation(err, foreignKeyViolation)
}

// Transient reports whether err is a failure that may pass if the same statement is sent
// again: the connection dropped or was refused, the statement or the context ran out of
// time, the server is shutting down or full, a write reached a primary a failover demoted
// (read-only transaction), or Postgres gave up on a serialization or a deadlock. A closed
// pool, a violated constraint, a bad statement and a local failure such as a missing file
// are not.
func Transient(err error) bool {
	var refused pgdriver.Error
	if errors.As(err, &refused) {
		switch code := refused.Field('C'); code {
		// 57014 is a statement that statement_timeout or an operator's pg_cancel_backend stopped.
		// The driver sends no cancel of its own: a context that ran out is a net timeout.
		case readOnlyTransaction, "40001", "40P01", "53300", "57014", "57P01", "57P02", "57P03":
			return true
		default:
			return strings.HasPrefix(code, "08")
		}
	}
	// A dial, read or write that failed is an *net.OpError; a deadline, the context's
	// included, is a net.Error that timed out.
	var op *net.OpError
	var network net.Error
	return errors.As(err, &op) || errors.As(err, &network) && network.Timeout() ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// readOnlyTransaction is the code of a write refused as read-only, which a primary a failover
// demoted answers every write with.
const readOnlyTransaction = "25006"

// primaryOnly is a connector whose connections close once a write on one is refused as
// read-only. pgdriver keeps a connection after an ERROR, so a retry of the write would reach
// the demoted backend again, and so would every write after it; a fresh connection reaches
// the new primary.
type primaryOnly struct{ *pgdriver.Connector }

func (c primaryOnly) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return primaryConn{conn.(*pgdriver.Conn)}, nil
}

// primaryConn is a connection of primaryOnly's. A transaction's statements go through it too.
type primaryConn struct{ *pgdriver.Conn }

func (c primaryConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.ExecContext(ctx, query, args)
	return result, c.closeIfReadOnly(err)
}

func (c primaryConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.QueryContext(ctx, query, args)
	return rows, c.closeIfReadOnly(err)
}

// closeIfReadOnly closes the connection if err is a write refused as read-only, so the pool
// drops it, as the driver closes one it finds broken. It returns err as it is.
func (c primaryConn) closeIfReadOnly(err error) error {
	if isViolation(err, readOnlyTransaction) {
		_ = c.Close()
	}
	return err
}

// oneRow turns an update or delete that matched nothing into missing.
func oneRow(result sql.Result, missing error) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: rows affected: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(missing)
	}
	return nil
}

// nullable keeps an empty string out of a column that means "nothing happened" by being
// null rather than blank.
func nullable(text string) any {
	if text == "" {
		return nil
	}
	return text
}

// likeEscaper makes typed text match as itself rather than as a pattern: backslash is LIKE's
// default escape.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
