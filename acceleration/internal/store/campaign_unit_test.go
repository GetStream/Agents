package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCampaignValidate(t *testing.T) {
	valid := func() Campaign {
		return Campaign{CustomerID: "app", Name: "renewals", Concurrency: 1, State: Draft, Type: Standard}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*Campaign){
		"customer_id": func(m *Campaign) { m.CustomerID = "" },
		"name":        func(m *Campaign) { m.Name = "" },
		"concurrency": func(m *Campaign) { m.Concurrency = maxConcurrency + 1 },
		"state":       func(m *Campaign) { m.State = "asleep" },
		"type":        func(m *Campaign) { m.Type = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
