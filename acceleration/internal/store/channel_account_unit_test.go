package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelAccountValidate(t *testing.T) {
	valid := func() ChannelAccount {
		return ChannelAccount{CustomerID: "acme-app", Kind: "whatsapp", E164: "+15550001111", Token: "token-one"}
	}
	account := valid()
	require.NoError(t, account.Validate())

	for field, broken := range map[string]func(*ChannelAccount){
		"customer_id": func(a *ChannelAccount) { a.CustomerID = "" },
		"kind":        func(a *ChannelAccount) { a.Kind = "telegram" },
		"e164":        func(a *ChannelAccount) { a.E164 = "" },
		"token":       func(a *ChannelAccount) { a.Token = "" },
		"kek_version": func(a *ChannelAccount) { a.SecretsKEKVersion = -1 },
	} {
		a := valid()
		broken(&a)
		var invalid *ValidationError
		require.True(t, errors.As(a.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
