package api

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// An inline job is answered in the response and never stored. A provider's failure is told as
// a stored job's is, since its text can carry the vendor's body, its URL or the key it was sent.
func TestInlineResultTellsAProvidersFailureWithNoneOfItsText(t *testing.T) {
	key := "sk-test-" + rand.Text()
	failure := errors.New("deepgram: 401 Unauthorized from https://api.deepgram.com/v1/listen\nAuthorization: Bearer " + key)

	job := inlineResult(context.Background(), store.Recording{}, "deepgram", "nova", nil, failure)

	require.Equal(t, "failed", job.Status)
	require.Equal(t, store.RecordingFailure, job.Error)
}

// A refusal is the caller's to fix, so it is told in its own words, without a key it echoed.
func TestInlineResultTellsARefusalInItsOwnWords(t *testing.T) {
	key := "sk-test-" + rand.Text()
	refusal := routing.Refused(fmt.Errorf("routing: unknown target %q\nAuthorization: Bearer %s", "nova-9", key))

	job := inlineResult(context.Background(), store.Recording{}, "deepgram", "nova", nil, refusal)

	require.Equal(t, "failed", job.Status)
	require.Equal(t, "routing: unknown target \"nova-9\"\n[redacted]", job.Error)
}
