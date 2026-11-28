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

func TestASIPTrunkUnderEitherKeyHasAPassword(t *testing.T) {
	routerKey := SIPTrunk{PasswordSealed: []byte("sealed"), PasswordKEKVersion: 1}
	dataKey := SIPTrunk{PasswordSealed: []byte("sealed"), PasswordDataKeyVersion: new(1)}

	require.True(t, routerKey.HasPassword())
	require.Nil(t, routerKey.DataKeyVersion())
	require.True(t, dataKey.HasPassword())
	require.Equal(t, new(1), dataKey.DataKeyVersion())
}

func TestASIPTrunkADataMoveLeftHasNoPassword(t *testing.T) {
	// A data move sets kek version 0 and no data key version, and the sealed bytes of a password
	// set again before may survive it.
	require.False(t, SIPTrunk{PasswordSealed: []byte("sealed")}.HasPassword())
	require.False(t, SIPTrunk{}.HasPassword())
	require.False(t, SIPTrunk{PasswordDataKeyVersion: new(1)}.HasPassword(), "a version names no bytes to open")
}

func TestASIPTrunkABuildBeforeDataKeysSavedAgainIsUnderTheRoutersKey(t *testing.T) {
	// That build seals under the router's key and leaves the data key version as it was.
	saved := SIPTrunk{PasswordSealed: []byte("sealed"), PasswordKEKVersion: 2, PasswordDataKeyVersion: new(1)}

	require.Nil(t, saved.DataKeyVersion())
	require.True(t, saved.HasPassword())
}
