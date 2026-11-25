package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSIPTrunkValidate(t *testing.T) {
	valid := func() SIPTrunk {
		return SIPTrunk{ID: "t1", CustomerID: "app", Transport: "tcp"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*SIPTrunk){
		"id":          func(m *SIPTrunk) { m.ID = "" },
		"customer_id": func(m *SIPTrunk) { m.CustomerID = "" },
		"transport":   func(m *SIPTrunk) { m.Transport = "sctp" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
