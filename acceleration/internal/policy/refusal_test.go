package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A refusal rebuilt from its code is of the kind Code named, checked or not as it says, and reads
// as that kind's own words: a delivery that reads it back answers the money as it was refused.
func TestARefusalRebuiltFromItsCodeIsOfThatKind(t *testing.T) {
	for _, tc := range []struct {
		code    string
		kind    error
		message string
	}{
		{"account_suspended", ErrSuspended, "this organization is suspended; contact support"},
		{"insufficient_credits", ErrNoCredits, "no credit left"},
		{"top_up_required", ErrTopUpRequired, "a top-up is required first"},
		{"system_limit_reached", ErrSystemLimit, "the organization's spend hard limit is reached"},
		{"spend_limit_reached", ErrBudgetSpent, "the spend limit is reached"},
		{"user_limit_reached", ErrUserLimit, "the user's spend limit is reached"},
		{"spend_check_unavailable", ErrSpendCheckUnavailable, "could not share spend with the other routers; retry shortly"},
	} {
		for _, unchecked := range []bool{false, true} {
			got := Refusal(tc.code, unchecked)

			require.ErrorIs(t, got, tc.kind, tc.code)
			require.Equal(t, tc.code, Code(got))
			require.Equal(t, unchecked, Transient(got), tc.code)
			require.Equal(t, tc.message, got.Error())
		}
	}
}

// A code no refusal is known by rebuilds none.
func TestACodeOfNoRefusalRebuildsNone(t *testing.T) {
	for _, code := range []string{"", "prompt_injection", "Insufficient_Credits", "insufficient_credits "} {
		require.NoError(t, Refusal(code, false), code)
	}
}
