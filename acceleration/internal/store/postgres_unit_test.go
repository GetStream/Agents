package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestTransientTellsAFailureAnotherTryCanPassFromOneItCannot(t *testing.T) {
	for code, transient := range map[string]bool{
		"25006": true,  // read-only transaction: a primary a failover demoted
		"40001": true,  // serialization failure
		"40P01": true,  // deadlock
		"53300": true,  // too many connections
		"57014": true,  // statement_timeout or pg_cancel_backend
		"57P01": true,  // admin shutdown
		"57P02": true,  // crash shutdown
		"57P03": true,  // cannot connect now
		"08006": true,  // connection failure
		"08P01": true,  // protocol violation
		"23503": false, // foreign key violation
		"42P01": false, // undefined table
		"28P01": false, // invalid password
		"55P03": false, // lock not available
	} {
		t.Run(code, func(t *testing.T) {
			err := pingPostgres(t, refusingServer(t, code), time.Second)
			var answer pgdriver.Error
			require.ErrorAs(t, err, &answer)
			require.Equal(t, code, answer.Field('C'))
			require.Equal(t, transient, Transient(err), "%v", err)
		})
	}

	t.Run("a server that never answers", func(t *testing.T) {
		err := pingPostgres(t, silentServer(t), 200*time.Millisecond)
		require.Error(t, err)
		require.True(t, Transient(err), "%v", err)
	})
	// A context that ran out is what the pool answers a caller left waiting for a connection.
	for _, cut := range []error{driver.ErrBadConn, io.EOF, io.ErrUnexpectedEOF, context.DeadlineExceeded} {
		t.Run(cut.Error(), func(t *testing.T) {
			err := fmt.Errorf("write: %w", cut)
			require.True(t, Transient(err), "%v", err)
		})
	}
	t.Run("a port nobody listens on", func(t *testing.T) {
		err := pingPostgres(t, "127.0.0.1:1", 2*time.Second)
		var dial *net.OpError
		require.ErrorAs(t, err, &dial)
		require.True(t, Transient(err), "%v", err)
	})
	t.Run("a closed pool", func(t *testing.T) {
		db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN("postgres://postgres@127.0.0.1:1/router?sslmode=disable")))
		require.NoError(t, db.Close())
		err := db.PingContext(context.Background())
		require.Error(t, err)
		require.False(t, Transient(err), "%v", err)
	})
	t.Run("a failure of the caller's own", func(t *testing.T) {
		require.False(t, Transient(errors.New("store: a customer is required")))
		require.False(t, Transient(context.Canceled))
		_, missing := os.Open(filepath.Join(t.TempDir(), "missing"))
		require.ErrorIs(t, missing, syscall.ENOENT)
		require.False(t, Transient(missing), "%v", missing)
		require.False(t, Transient(syscall.EACCES))
	})
}

// pingPostgres is a pool's first statement against the server at addr, given wait.
func pingPostgres(t *testing.T, addr string, wait time.Duration) error {
	db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN("postgres://postgres@" + addr + "/router?sslmode=disable")))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	return db.PingContext(ctx)
}

// refusingServer answers every startup message with a FATAL ErrorResponse carrying code.
func refusingServer(t *testing.T, code string) string {
	return serve(t, func(conn net.Conn) {
		if readStartup(conn) != nil {
			return
		}
		var fields []byte
		for _, field := range []struct {
			kind  byte
			value string
		}{{'S', "FATAL"}, {'V', "FATAL"}, {'C', code}, {'M', "refused"}} {
			fields = append(append(append(fields, field.kind), field.value...), 0)
		}
		fields = append(fields, 0)
		message := binary.BigEndian.AppendUint32([]byte{'E'}, uint32(4+len(fields)))
		_, _ = conn.Write(append(message, fields...))
	})
}

// silentServer takes connections and never says a word.
func silentServer(t *testing.T) string {
	return serve(t, func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) })
}

// serve runs answer on every connection to a local listener until the test ends.
func serve(t *testing.T, answer func(net.Conn)) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var answering sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		answering.Wait()
	})
	answering.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			answering.Go(func() {
				defer conn.Close()
				answer(conn)
			})
		}
	})
	return listener.Addr().String()
}

// readStartup reads the client's startup message: a length that counts itself, then the rest.
func readStartup(conn net.Conn) error {
	var length [4]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return err
	}
	_, err := io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(length[:]))-4)
	return err
}
