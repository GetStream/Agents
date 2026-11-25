package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpisodeValidate(t *testing.T) {
	valid := func() Episode {
		return Episode{CustomerID: "acme-app", ContactID: "contact-one", Source: EpisodeSMS, ThreadChannel: "agent:thread-one", CardMessageID: "episode-one", Status: episodeInProgress}
	}
	episode := valid()
	require.NoError(t, episode.Validate())

	for field, broken := range map[string]func(*Episode){
		"customer_id":     func(e *Episode) { e.CustomerID = "" },
		"contact_id":      func(e *Episode) { e.ContactID = "" },
		"source":          func(e *Episode) { e.Source = "fax" },
		"thread_channel":  func(e *Episode) { e.ThreadChannel = "" },
		"session_id":      func(e *Episode) { e.SessionID = "session-one" },
		"card_message_id": func(e *Episode) { e.CardMessageID = "" },
		"status":          func(e *Episode) { e.Status = "open" },
	} {
		e := valid()
		broken(&e)
		var invalid *ValidationError
		require.True(t, errors.As(e.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
