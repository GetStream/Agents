package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImageJobValidate(t *testing.T) {
	valid := func() ImageJob {
		return ImageJob{ID: "job-one", CustomerID: "acme-app", CID: "messaging:one", AgentUser: "agent", RequestKey: "key-one", Attribution: []byte(`{}`), State: ImageJobQueued}
	}
	job := valid()
	require.NoError(t, job.Validate())

	for field, broken := range map[string]func(*ImageJob){
		"id":          func(j *ImageJob) { j.ID = "" },
		"customer_id": func(j *ImageJob) { j.CustomerID = "" },
		"cid":         func(j *ImageJob) { j.CID = "" },
		"agent_user":  func(j *ImageJob) { j.AgentUser = "" },
		"request_key": func(j *ImageJob) { j.RequestKey = "" },
		"attribution": func(j *ImageJob) { j.Attribution = nil },
		"state":       func(j *ImageJob) { j.State = "drawing" },
	} {
		j := valid()
		broken(&j)
		var invalid *ValidationError
		require.True(t, errors.As(j.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
