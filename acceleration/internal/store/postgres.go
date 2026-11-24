package store

import (
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

// constraint is the name of the unique constraint an error violated, empty for anything
// else.
func constraint(err error) string {
	var failure pgdriver.Error
	if errors.As(err, &failure) && failure.Field('C') == uniqueViolation {
		return failure.Field('n')
	}
	return ""
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
		case "25006", "40001", "40P01", "53300", "57014", "57P01", "57P02", "57P03":
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

// orEmpty is list, or an empty one when it is nil, for a column or a JSON value that holds
// an empty list and never NULL.
func orEmpty[S ~[]E, E any](list S) S {
	if list == nil {
		return S{}
	}
	return list
}

// orEmptyMap is orEmpty for a map.
func orEmptyMap[M ~map[K]V, K comparable, V any](values M) M {
	if values == nil {
		return M{}
	}
	return values
}

// likeEscaper makes typed text match as itself rather than as a pattern: backslash is LIKE's
// default escape.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
