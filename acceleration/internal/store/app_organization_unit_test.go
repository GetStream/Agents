package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppOrganizationValidate(t *testing.T) {
	require.NoError(t, (&AppOrganization{AppID: "app-1", OrganizationID: "org-1"}).Validate())
	for field, row := range map[string]AppOrganization{
		"app_id":          {OrganizationID: "org-1"},
		"organization_id": {AppID: "app-1"},
	} {
		var invalid *ValidationError
		require.True(t, errors.As(row.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
