package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppValidate(t *testing.T) {
	valid := App{OrganizationID: "org-1", Name: "voice"}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*App){
		"organization_id": func(a *App) { a.OrganizationID = "" },
		"name":            func(a *App) { a.Name = " " },
	} {
		app := valid
		broken(&app)
		var invalid *ValidationError
		require.True(t, errors.As(app.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
