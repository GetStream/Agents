package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrganizationValidate(t *testing.T) {
	require.NoError(t, (&Organization{Name: "acme"}).Validate())
	var invalid *ValidationError
	require.True(t, errors.As((&Organization{Name: " "}).Validate(), &invalid))
	require.Equal(t, "name", invalid.Field)
}
