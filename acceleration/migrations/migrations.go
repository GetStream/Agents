// Package migrations embeds the goose SQL migrations so they ship inside the binary.
//
// There are two sets. global is the database every shard shares: tenants, configs,
// connectors, numbers, limits. shard is each shard's own: sessions, responses, calls, call
// legs, requests and their rollups. A deployment of one database runs both on it. Each set
// records its versions in a table of its own, so a version is unique within its set only.
// Each starts with one initial migration, the history before it squashed into the schema it
// left.
package migrations

import (
	"embed"
	"io/fs"
)

//go:embed global/*.sql
var global embed.FS

//go:embed shard/*.sql
var shard embed.FS

// Global holds the shared database's migrations, ordered by their timestamped names.
var Global = sub(global, "global")

// Shard holds each shard's migrations, ordered by their timestamped names.
var Shard = sub(shard, "shard")

func sub(files embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(files, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
