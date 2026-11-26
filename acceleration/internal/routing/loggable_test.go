package routing

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

func TestALostRowIsLoggedWithEveryTextFieldMasked(t *testing.T) {
	const id = "sip-15550008808"
	fields := map[string]func(*store.Request){
		"Modality":         func(r *store.Request) { r.Modality = id },
		"OrganizationTier": func(r *store.Request) { r.OrganizationTier = id },
		"Provider":         func(r *store.Request) { r.Provider = id },
		"Model":            func(r *store.Request) { r.Model = id },
		"OperationID":      func(r *store.Request) { r.OperationID = id },
		"Purpose":          func(r *store.Request) { r.Purpose = id },
		"TurnID":           func(r *store.Request) { r.TurnID = id },
		"PriceVersion":     func(r *store.Request) { r.PriceVersion = id },
		"SKU":              func(r *store.Request) { r.SKU = id },
		"Region":           func(r *store.Request) { r.Region = id },
		"ErrorCode":        func(r *store.Request) { r.ErrorCode = id },
		"ShipError":        func(r *store.Request) { r.ShipError = id },
	}
	for name, set := range fields {
		t.Run(name, func(t *testing.T) {
			var request store.Request
			set(&request)

			row, err := json.Marshal(loggable(request))

			require.NoError(t, err)
			require.NotContains(t, string(row), "5550008808")
		})
	}
}

func TestTwoKeysMaskedAlikeAreBothLogged(t *testing.T) {
	request := store.Request{
		Tags: map[string]string{
			"sip-15550002222": "call +15550004444", "sip-15550001111": "first", "sip-15550005555": "third", "plain": "kept",
		},
		// The masked key sorts before the one it masks to.
		Attributes: map[string]string{"+1•••33": "named so already", "+15550003333": "masked"},
	}

	logged := loggable(request)

	require.Equal(t, map[string]string{
		"phone_caller": "first", "phone_caller#2": "call +1•••44", "phone_caller#3": "third", "plain": "kept",
	}, logged.Tags,
		"every value is kept, under its masked key in key order, and an unmasked key keeps its name")
	require.Equal(t, map[string]string{"+1•••33": "named so already", "+1•••33#2": "masked"}, logged.Attributes,
		"a key masking leaves alone keeps its name over one masked to it")
	row, err := json.Marshal(logged)
	require.NoError(t, err)
	for _, digits := range []string{"5550001111", "5550002222", "5550003333", "5550004444", "5550005555"} {
		require.NotContains(t, string(row), digits)
	}
}

func TestALostRowLogsNoEndUserEmailOrNumber(t *testing.T) {
	for id, want := range map[string]string{
		"ali@example.com":      "masked:•••@example.com",
		"Ali.Khan@Example.COM": "masked:•••@Example.COM",
		"phone:15550001101":    "masked:phone:•••01",
		"phone:+15550001101":   "masked:phone:+1•••01",
		"sip-+15550001101":     "masked:phone_caller",
		"user-42":              "user-42",
	} {
		logged := loggable(store.Request{UserID: id})

		require.Equal(t, want, logged.UserID, id)
	}
}
