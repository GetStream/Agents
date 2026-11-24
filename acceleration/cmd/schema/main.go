// Command schema writes acceleration/schema: the tables each migration set leaves, as the
// catalogue describes them. It migrates two empty databases of its own on the testing
// Postgres (internal/config/testing.yaml), so a change to the migrations shows in review as a
// change to those files. Run it from acceleration/:
//
//	go run ./cmd/schema
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "schema:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		return errors.New("ROUTER_POSTGRES_DSN is not set")
	}
	global, shard := testenv.Database(dsn, "schema_global"), testenv.Database(dsn, "schema_shard")
	for _, own := range []string{global, shard} {
		if err := empty(ctx, own); err != nil {
			return err
		}
	}

	described, err := store.OpenShards(store.Options{DSN: global, ShardDSN: shard})
	if err != nil {
		return err
	}
	defer described.Close()
	if err := described.Migrate(ctx); err != nil {
		return err
	}
	globalSchema, shardSchema, err := described.Schema(ctx)
	if err != nil {
		return err
	}
	if err := os.MkdirAll("schema", 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("schema", "global.sql"), []byte(globalSchema), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join("schema", "shard.sql"), []byte(shardSchema), 0o644)
}

// empty drops the schema of a database of this command's own, which must be a test one.
func empty(ctx context.Context, dsn string) error {
	db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	defer db.Close()
	var name string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name); err != nil {
		return err
	}
	if !strings.HasSuffix(name, "_test") {
		return fmt.Errorf("refusing to drop the schema of %s, which is not a test database", name)
	}
	_, err := db.ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	return err
}
