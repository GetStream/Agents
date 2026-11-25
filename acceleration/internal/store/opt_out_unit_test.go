package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptOutValidate(t *testing.T) {
	valid := func() OptOut {
		return OptOut{CustomerID: "app", Recipient: "+15550000001", Channel: "sms"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*OptOut){
		"customer_id": func(m *OptOut) { m.CustomerID = "" },
		"recipient":   func(m *OptOut) { m.Recipient = "" },
		"channel":     func(m *OptOut) { m.Channel = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}

// Catches an opt-out kept under a number as it was written, which the E.164 number dialled or
// texted never matches; a country guessed for a number that names none, which would match
// somebody else; and a handle that is no number (an iMessage email, a Slack id, a vanity
// word) read as one.
func TestOptOutRecipient(t *testing.T) {
	for _, c := range []struct {
		written, region, want string
		kind                  recipientKind
	}{
		{"+12025550123", "", "+12025550123", recipientNumber},
		{"12025550123", "", "+12025550123", recipientNumber},
		{" +1 202 555 0123 ", "", "+12025550123", recipientNumber},
		{"1 (202) 555-0123", "", "+12025550123", recipientNumber},
		{"tel:+12025550123", "", "+12025550123", recipientNumber},
		{"whatsapp:447911123456", "", "+447911123456", recipientNumber},
		{"447911123456", "", "+447911123456", recipientNumber},
		{"(202) 555-0123", "US", "+12025550123", recipientNumber},
		{"3475550103", "US", "+13475550103", recipientNumber},
		{"0612345678", "FR", "+33612345678", recipientNumber},
		{"+33 6 12 34 56 78", "US", "+33612345678", recipientNumber},
		// Norway's 47 91 23 45 67 is no North American number: its only valid reading.
		{"4791234567", "", "+4791234567", recipientNumber},
		// A national number names no country, so with none known it is kept as written.
		{"(202) 555-0123", "", "(202) 555-0123", recipientUnread},
		{"9198765432", "", "9198765432", recipientUnread},
		{"13812345678", "", "13812345678", recipientUnread},
		{"0612345678", "", "0612345678", recipientUnread},
		{"555-1110", "", "555-1110", recipientUnread},
		{"555-1110", "US", "555-1110", recipientUnread},
		{"+15551110000", "", "+15551110000", recipientUnread},
		{" 12345 ", "", "12345", recipientUnread},
		// Valid in China and, read with a 1 in front, in North America: two readings.
		{"13312345678", "CN", "13312345678", recipientUnread},
		{"jane@icloud.com", "", "jane@icloud.com", recipientHandle},
		{" jane@icloud.com ", "", "jane@icloud.com", recipientHandle},
		{"12025550123@icloud.com", "US", "12025550123@icloud.com", recipientHandle},
		{"U0123456789", "", "U0123456789", recipientHandle},
		{"U024BE7LH", "US", "U024BE7LH", recipientHandle},
		{"1-800-FLOWERS", "US", "1-800-FLOWERS", recipientHandle},
		{"6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", "", "6f1c2d3e-4b5a-4c7d-8e9f-0a1b2c3d4e5f", recipientHandle},
		{"not a number", "", "not a number", recipientHandle},
	} {
		got, kind := optOutRecipient(c.written, c.region)
		require.Equal(t, c.want, got, "%q in %q", c.written, c.region)
		require.Equal(t, c.kind, kind, "%q in %q", c.written, c.region)
	}
}

// Catches the line's country being ignored, or taken from something other than the line.
func TestOptOutRecipientReadsTheSenderInTheLinesCountry(t *testing.T) {
	require.Equal(t, "+13475550103", OptOutRecipient("(347) 555-0103", "+12025550100"))
	require.Equal(t, "+33612345678", OptOutRecipient("06 12 34 56 78", "+33140000000"))
	require.Equal(t, "06 12 34 56 78", OptOutRecipient("06 12 34 56 78", "not a line"))
}

// Catches a lookup that matches only the E.164 form and misses a row stored as it was written
// before recipients were normalized, or that repeats one.
func TestKeptUnder(t *testing.T) {
	require.Equal(t, []string{"+12025550123", "12025550123"}, keptUnder("12025550123"))
	require.Equal(t, []string{"+12025550123"}, keptUnder("+12025550123", " +12025550123"))
	require.Equal(t, []string{"+13475550103", "(347) 555-0103"}, keptUnder("+13475550103", "(347) 555-0103"))
}
