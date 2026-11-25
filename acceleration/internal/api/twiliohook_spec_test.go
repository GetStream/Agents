package api

import (
	"net/http"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// TestTheTwilioCallHookIsDeclared is the handwritten call webhook: public, named in the spec
// with every answer it gives, in the committed document, and a hook, so nothing it carries is
// read as who is calling.
func TestTheTwilioCallHookIsDeclared(t *testing.T) {
	item := (&Server{}).newAPI(chi.NewRouter()).OpenAPI().Paths[twilioCallHookPath]
	require.NotNil(t, item)
	require.NotNil(t, item.Post)
	require.Equal(t, "receiveTwilioCall", item.Post.OperationID)
	require.Equal(t, http.MethodPost, item.Post.Method)
	require.NotNil(t, item.Post.Security)
	require.Empty(t, item.Post.Security)
	for _, code := range []string{"200", "400", "401", "404", "413"} {
		require.Contains(t, item.Post.Responses, code)
	}
	require.Contains(t, item.Post.Responses["200"].Content, "application/xml")

	committed, err := os.ReadFile("../../api/openapi.yaml")
	require.NoError(t, err)
	require.Contains(t, string(committed), "/v1/phone/hooks/twilio:")

	require.True(t, isHook(twilioCallHookPath))
	require.True(t, isHook(twilioEventsHookPath), "the events hook is under the call hook's path")
}
