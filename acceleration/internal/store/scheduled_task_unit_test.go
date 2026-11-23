package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheduledTaskValidate(t *testing.T) {
	valid := func() ScheduledTask {
		return ScheduledTask{CustomerID: "acme", ID: "st-1", UserID: "sam", Title: "Water the plants", State: ScheduledTaskActive}
	}
	task := valid()
	require.NoError(t, task.Validate())

	for field, broken := range map[string]func(*ScheduledTask){
		"customer_id": func(t *ScheduledTask) { t.CustomerID = "" },
		"id":          func(t *ScheduledTask) { t.ID = "" },
		"user_id":     func(t *ScheduledTask) { t.UserID = "" },
		"title":       func(t *ScheduledTask) { t.Title = " " },
		"state":       func(t *ScheduledTask) { t.State = "sleeping" },
	} {
		task := valid()
		broken(&task)
		var invalid *ValidationError
		require.True(t, errors.As(task.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
