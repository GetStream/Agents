package api

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
)

func TestAModelKeyTheDeploymentCannotOpenIsNoRefusalOfTheCaller(t *testing.T) {
	sealed := fmt.Errorf("%w: custom/acme-llm: %w", llmrouter.ErrCustomModelKey,
		errors.New("auth: open secret: cipher: message authentication failed"))

	answered := settingsRefused(sealed)

	_, refused := errors.AsType[APIError](answered)
	require.False(t, refused, "a key the deployment cannot open is answered as its own failure, not a 400 quoting the sealer")
	require.ErrorIs(t, answered, llmrouter.ErrCustomModelKey)
}

func TestADatabaseThatFailedReadingAModelIsNoRefusalOfTheCaller(t *testing.T) {
	answered := settingsRefused(fmt.Errorf("agent: start llm: store: custom model: %w", driver.ErrBadConn))

	_, refused := errors.AsType[APIError](answered)
	require.False(t, refused, "a database that failed is answered as the router's own failure, not a 400 quoting it")
}

func TestASettingTheCallerGotWrongIsRefusedWithItsWords(t *testing.T) {
	answered := settingsRefused(errors.New(`session: "fast" is no turn detection mode`))

	refusal, refused := errors.AsType[APIError](answered)
	require.True(t, refused)
	require.Equal(t, http.StatusBadRequest, refusal.GetStatus())
	require.Contains(t, refusal.Message, `"fast" is no turn detection mode`)
}
