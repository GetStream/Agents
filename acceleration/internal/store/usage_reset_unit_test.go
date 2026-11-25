package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func TestUsageResetValidate(t *testing.T) {
	valid := UsageReset{OrganizationID: "org-1", Subject: usage.SubjectApp, AppID: "app-1", Interval: usage.Monthly, WindowStart: time.Now()}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*UsageReset){
		"organization_id": func(r *UsageReset) { r.OrganizationID = "" },
		"subject":         func(r *UsageReset) { r.Subject = "planet" },
		"interval":        func(r *UsageReset) { r.Interval = "hourly" },
		"window_start":    func(r *UsageReset) { r.WindowStart = time.Time{} },
		"baseline_micros": func(r *UsageReset) { r.BaselineMicros = -1 },
	} {
		reset := valid
		broken(&reset)
		var invalid *ValidationError
		require.True(t, errors.As(reset.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
