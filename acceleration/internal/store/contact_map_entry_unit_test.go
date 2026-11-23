package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContactMapEntryValidate(t *testing.T) {
	valid := func() ContactMapEntry {
		return ContactMapEntry{CustomerID: "acme-app", ConfigID: "agent-one", Kind: ContactPhone, Address: "+15550100"}
	}
	entry := valid()
	require.NoError(t, entry.Validate())

	for field, broken := range map[string]func(*ContactMapEntry){
		"customer_id": func(e *ContactMapEntry) { e.CustomerID = "" },
		"config_id":   func(e *ContactMapEntry) { e.ConfigID = "" },
		"kind":        func(e *ContactMapEntry) { e.Kind = "email" },
		"address":     func(e *ContactMapEntry) { e.Address = "" },
	} {
		e := valid()
		broken(&e)
		var invalid *ValidationError
		require.True(t, errors.As(e.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
