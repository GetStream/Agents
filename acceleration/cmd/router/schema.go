package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

const schemaUsage = `usage: router schema drop-strays

  drop-strays   drop the empty tables each database kept of the other migration set from
                before the split, and list the ones holding rows, which it leaves`

func runSchema(args []string, settings config.Config, out io.Writer) error {
	if len(args) == 0 || args[0] != "drop-strays" {
		return errors.New(schemaUsage)
	}
	ctx := context.Background()
	pgStore, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	defer pgStore.Close()
	dropped, left, err := pgStore.DropStrays(ctx)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "dropped %d tables: %s\nleft %d holding rows: %s\n",
		len(dropped), strings.Join(dropped, ", "), len(left), strings.Join(left, ", "))
	return err
}
