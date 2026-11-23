package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContactValidate(t *testing.T) {
	valid := func() Contact {
		return Contact{CampaignID: "cp", ToNumber: "+15550000001", Stage: Enrolled}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*Contact){
		"campaign_id": func(m *Contact) { m.CampaignID = "" },
		"to_number":   func(m *Contact) { m.ToNumber = "" },
		"stage":       func(m *Contact) { m.Stage = "lost" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
