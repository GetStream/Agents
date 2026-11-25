package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserPhoneValidate(t *testing.T) {
	valid := func() UserPhone {
		return UserPhone{CustomerID: "acme-app", UserID: "user-one", Phone: "+12025550199"}
	}
	phone := valid()
	require.NoError(t, phone.Validate())

	for field, broken := range map[string]func(*UserPhone){
		"customer_id": func(p *UserPhone) { p.CustomerID = "" },
		"user_id":     func(p *UserPhone) { p.UserID = "" },
		"phone":       func(p *UserPhone) { p.Phone = "555-0199" },
	} {
		p := valid()
		broken(&p)
		var invalid *ValidationError
		require.True(t, errors.As(p.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
