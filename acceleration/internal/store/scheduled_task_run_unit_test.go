package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheduledTaskRunValidate(t *testing.T) {
	valid := func() ScheduledTaskRun {
		return ScheduledTaskRun{CustomerID: "acme", TaskID: "st-1", Trigger: ScheduledTaskTriggerManual, State: ScheduledTaskRunRunning}
	}
	run := valid()
	require.NoError(t, run.Validate())

	for field, broken := range map[string]func(*ScheduledTaskRun){
		"customer_id": func(r *ScheduledTaskRun) { r.CustomerID = "" },
		"task_id":     func(r *ScheduledTaskRun) { r.TaskID = "" },
		"trigger":     func(r *ScheduledTaskRun) { r.Trigger = "cron" },
		"state":       func(r *ScheduledTaskRun) { r.State = "queued" },
	} {
		run := valid()
		broken(&run)
		var invalid *ValidationError
		require.True(t, errors.As(run.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
