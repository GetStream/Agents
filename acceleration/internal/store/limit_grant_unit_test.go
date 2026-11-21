package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func TestLimitGrantValidate(t *testing.T) {
	valid := func(subject usage.Subject) LimitGrant {
		grant := LimitGrant{OrganizationID: "org-1", Subject: subject, AmountMicros: 1, Reason: "launch"}
		switch subject {
		case usage.SubjectApp:
			grant.AppID = "app-1"
		case usage.SubjectAPIKey:
			grant.AppID, grant.APIKey = "app-1", "k1"
		case usage.SubjectUser:
			grant.AppID, grant.UserID = "app-1", "u1"
		case usage.SubjectCategory:
			grant.Category = "team:a"
		case usage.SubjectAppCategory:
			grant.AppID, grant.Category = "app-1", "team:a"
		}
		return grant
	}
	for _, subject := range []usage.Subject{usage.SubjectOrganization, usage.SubjectApp, usage.SubjectAPIKey, usage.SubjectUser, usage.SubjectCategory, usage.SubjectAppCategory} {
		grant := valid(subject)
		require.NoError(t, grant.Validate(), subject)
	}

	past := time.Now().Add(-time.Minute)
	for name, test := range map[string]struct {
		subject usage.Subject
		broken  func(*LimitGrant)
		field   string
	}{
		"no organization":            {usage.SubjectOrganization, func(g *LimitGrant) { g.OrganizationID = "" }, "organization_id"},
		"an unknown subject":         {usage.SubjectOrganization, func(g *LimitGrant) { g.Subject = "planet" }, "subject"},
		"an app grant with no app":   {usage.SubjectApp, func(g *LimitGrant) { g.AppID = "" }, "app_id"},
		"an org grant naming an app": {usage.SubjectOrganization, func(g *LimitGrant) { g.AppID = "app-1" }, "app_id"},
		"a key grant with no key":    {usage.SubjectAPIKey, func(g *LimitGrant) { g.APIKey = "" }, "app_id"},
		"a user grant with no user":  {usage.SubjectUser, func(g *LimitGrant) { g.UserID = "" }, "app_id"},
		"a category with none":       {usage.SubjectCategory, func(g *LimitGrant) { g.Category = "" }, "category"},
		"a category on an app grant": {usage.SubjectApp, func(g *LimitGrant) { g.Category = "team:a" }, "category"},
		"nothing granted":            {usage.SubjectOrganization, func(g *LimitGrant) { g.AmountMicros = 0 }, "amount_micros"},
		"negative use":               {usage.SubjectOrganization, func(g *LimitGrant) { g.UsedMicros = -1 }, "used_micros"},
		"no reason":                  {usage.SubjectOrganization, func(g *LimitGrant) { g.Reason = "" }, "reason"},
		"already expired":            {usage.SubjectOrganization, func(g *LimitGrant) { g.ExpiresAt = &past }, "expires_at"},
	} {
		grant := valid(test.subject)
		test.broken(&grant)
		var invalid *ValidationError
		require.True(t, errors.As(grant.Validate(), &invalid), name)
		require.Equal(t, test.field, invalid.Field, name)
	}
}
