package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMoveCursorValidate(t *testing.T) {
	valid := func() MoveCursor { return MoveCursor{CustomerID: "acme", Source: "eu", ReadAt: time.Now()} }
	cursor := valid()
	require.NoError(t, cursor.Validate())

	for field, broken := range map[string]func(*MoveCursor){
		"customer_id": func(c *MoveCursor) { c.CustomerID = "" },
		"source":      func(c *MoveCursor) { c.Source = "" },
		"read_at":     func(c *MoveCursor) { c.ReadAt = time.Time{} },
	} {
		cursor := valid()
		broken(&cursor)
		var invalid *ValidationError
		require.True(t, errors.As(cursor.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
