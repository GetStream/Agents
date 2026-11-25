package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActivityDeltaValidate(t *testing.T) {
	valid := ActivityDelta{AppID: "app-1", Area: "llm", Client: "python", Day: time.Now()}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*ActivityDelta){
		"app_id": func(d *ActivityDelta) { d.AppID = "" },
		"area":   func(d *ActivityDelta) { d.Area = "" },
		"client": func(d *ActivityDelta) { d.Client = "" },
		"day":    func(d *ActivityDelta) { d.Day = time.Time{} },
	} {
		delta := valid
		broken(&delta)
		var invalid *ValidationError
		require.True(t, errors.As(delta.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
