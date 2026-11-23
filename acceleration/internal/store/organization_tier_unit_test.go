package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrganizationTierValidate(t *testing.T) {
	valid := OrganizationTier{OrganizationID: "org-1", Tier: "pro", SeenAt: time.Now()}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*OrganizationTier){
		"organization_id": func(o *OrganizationTier) { o.OrganizationID = "" },
		"tier":            func(o *OrganizationTier) { o.Tier = "" },
		"seen_at":         func(o *OrganizationTier) { o.SeenAt = time.Time{} },
	} {
		tier := valid
		broken(&tier)
		var invalid *ValidationError
		require.True(t, errors.As(tier.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
