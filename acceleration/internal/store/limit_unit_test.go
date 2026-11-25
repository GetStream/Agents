package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func TestLimitValidate(t *testing.T) {
	require.NoError(t, (&Limit{OrganizationID: "org-1", Subject: usage.SubjectAPIKey, Level: LevelAPIKey, AppID: "app-1", APIKey: "k1", Interval: usage.Daily}).Validate())
	require.NoError(t, (&Limit{OrganizationID: "org-1", Subject: usage.SubjectCategory, Level: LevelOrganization, Category: "team:a", Interval: usage.Monthly}).Validate())

	for name, test := range map[string]struct {
		limit Limit
		field string
	}{
		"a key row with no key":      {Limit{Subject: usage.SubjectAPIKey, Level: LevelAPIKey, AppID: "app-1"}, "app_id"},
		"an app row with no app":     {Limit{Subject: usage.SubjectApp, Level: LevelApp}, "app_id"},
		"a row with no organization": {Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization}, "organization_id"},
		"a global level":             {Limit{Subject: usage.SubjectOrganization, Level: "global"}, "level"},
		"a tier level":               {Limit{Subject: usage.SubjectOrganization, Level: "tier"}, "level"},
		"an app limited per user":    {Limit{Subject: usage.SubjectApp, Level: LevelUser, AppID: "app-1", UserID: "u1"}, "level"},
		"a category with none":       {Limit{Subject: usage.SubjectCategory, Level: LevelOrganization}, "category"},
		"a category on an app":       {Limit{Subject: usage.SubjectApp, Level: LevelApp, AppID: "app-1", Category: "team:a"}, "category"},
		"an unknown subject":         {Limit{Subject: "planet", Level: LevelOrganization}, "subject"},
		"an hourly limit":            {Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization, Interval: "hourly"}, "interval"},
		"a negative amount":          {Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization, LimitMicros: -1}, "limit_micros"},
	} {
		limit := test.limit
		if limit.Interval == "" {
			limit.Interval = usage.Daily
		}
		if name != "a row with no organization" {
			limit.OrganizationID = "org-1"
		}
		var invalid *ValidationError
		require.True(t, errors.As(limit.Validate(), &invalid), name)
		require.Equal(t, test.field, invalid.Field, name)
	}
}
