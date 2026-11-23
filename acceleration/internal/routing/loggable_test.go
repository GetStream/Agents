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
