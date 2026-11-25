package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPriceChangeValidate(t *testing.T) {
	valid := func() PriceChange {
		return PriceChange{Name: "2027-01", Kind: PriceChangeAPI, EffectiveAt: time.Now()}
	}
	change := valid()
	require.NoError(t, change.Validate())

	for field, broken := range map[string]func(*PriceChange){
		"name":         func(c *PriceChange) { c.Name = "" },
		"kind":         func(c *PriceChange) { c.Kind = "manual" },
		"effective_at": func(c *PriceChange) { c.EffectiveAt = time.Time{} },
	} {
		change := valid()
		broken(&change)
		var invalid *ValidationError
		require.True(t, errors.As(change.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
