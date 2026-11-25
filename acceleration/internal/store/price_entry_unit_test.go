package store

import (
	"errors"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/stretchr/testify/require"
)

func TestPriceEntryValidate(t *testing.T) {
	rates := map[pricing.Meter]pricing.Rate{pricing.InputTokens: {USD: 1, Per: 1_000_000}}
	valid := func() PriceEntry {
		return PriceEntry{Change: "2027-01", SKU: "llm/gpt", Region: "global", Rates: rates, Prices: rates}
	}
	entry := valid()
	require.NoError(t, entry.Validate())
	for name, unpriced := range map[string]PriceEntry{
		"free":      {Change: "2027-01", SKU: "llm/gpt", Region: "global", Rates: rates, Free: true},
		"withdrawn": {Change: "2027-01", SKU: "llm/gpt", Region: "global", Rates: rates, Withdrawn: true},
		"no rates":  {Change: "2027-01", SKU: "llm/gpt", Region: "global"},
	} {
		require.NoError(t, unpriced.Validate(), name)
	}

	for field, broken := range map[string]func(*PriceEntry){
		"change": func(e *PriceEntry) { e.Change = "" },
		"sku":    func(e *PriceEntry) { e.SKU = "" },
		"region": func(e *PriceEntry) { e.Region = "" },
		"prices": func(e *PriceEntry) { e.Prices = nil },
	} {
		entry := valid()
		broken(&entry)
		var invalid *ValidationError
		require.True(t, errors.As(entry.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
