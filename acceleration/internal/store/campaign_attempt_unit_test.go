package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCampaignAttemptValidate(t *testing.T) {
	valid := func() CampaignAttempt {
		return CampaignAttempt{CampaignID: "cp", ContactID: "cc", StartedAt: time.Now(), Dial: 1}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*CampaignAttempt){
		"campaign_id":    func(m *CampaignAttempt) { m.CampaignID = "" },
		"contact_id":     func(m *CampaignAttempt) { m.ContactID = "" },
		"started_at":     func(m *CampaignAttempt) { m.StartedAt = time.Time{} },
		"dial":           func(m *CampaignAttempt) { m.Dial = 0 },
		"classify_state": func(m *CampaignAttempt) { m.ClassifyState = "later" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
