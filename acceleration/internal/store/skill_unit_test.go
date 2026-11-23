package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkillValidate(t *testing.T) {
	valid := func() Skill {
		return Skill{CustomerID: "app", ConfigID: "config", Name: "refund"}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*Skill){
		"customer_id": func(m *Skill) { m.CustomerID = "" },
		"config_id":   func(m *Skill) { m.ConfigID = "" },
		"name":        func(m *Skill) { m.Name = "" },
		"deadline_ms": func(m *Skill) { m.DeadlineMs = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
