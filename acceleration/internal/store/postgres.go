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
	"syscall"

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

// Answered reports whether err is Postgres answering a statement, rather than a failure
// before the statement reached it.
func Answered(err error) bool {
	var answer pgdriver.Error
	return errors.As(err, &answer)
}

// Transient reports whether err is a failure that may pass if the same statement is sent
// again: the connection dropped or was refused, the statement timed out on either side, the
// server is shutting down or full, a write reached a primary a failover demoted (read-only
// transaction), or Postgres gave up on a serialization or a deadlock. A closed pool, a
// violated constraint and a bad statement are not.
func Transient(err error) bool {
	var refused pgdriver.Error
	if errors.As(err, &refused) {
		switch code := refused.Field('C'); code {
		// 57014 is a statement cancelled by statement_timeout, or by the driver once the
		// context ran out: the same wait as the context.DeadlineExceeded below.
		case "25006", "40001", "40P01", "53300", "57014", "57P01", "57P02", "57P03":
			return true
		default:
			return strings.HasPrefix(code, "08")
		}
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
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
