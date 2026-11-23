package session

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/stretchr/testify/suite"

	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// StreamKeysSuite is what a session that is not incognito is refused with on a deployment
// whose own Stream app is missing or refuses its keys (AI-3566): before, the caller read
// Stream's own words, «QueryUsers failed with error: "api_key not valid"», and nothing said
// that a Stream app or incognito: true was what it took.
type StreamKeysSuite struct{ suite.Suite }

func TestStreamKeysSuite(t *testing.T) { suite.Run(t, new(StreamKeysSuite)) }

// streamAnswering is what the getstream client returns when Stream answers status with body,
// as it does for a key it does not know.
func (s *StreamKeysSuite) streamAnswering(status int, body string) error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	s.T().Cleanup(server.Close)
	client, err := getstream.NewClient("your_stream_api_key_here", "your_stream_api_secret_here", getstream.WithBaseUrl(server.URL))
	s.Require().NoError(err)
	failed := persistent.CreateMissingUsers(context.Background(), client,
		map[string]getstream.UserRequest{"agent-" + s.T().Name(): {ID: "agent-" + s.T().Name()}})
	s.Require().Error(failed)
	return fmt.Errorf("conversation: create: %w", failed)
}

func (s *StreamKeysSuite) TestKeysStreamRefusesAreTheDeploymentsToFix() {
	// Stream's answer to the placeholder key .env.example ships, as it gave it on 2026-11-23.
	refused := s.streamAnswering(http.StatusUnauthorized,
		`{"StatusCode":401,"code":2,"message":"QueryUsers failed with error: \"api_key not valid\""}`)

	unkept := unkeptIn(refused, false)

	s.Require().ErrorIs(unkept, ErrStreamKeysRejected)
	s.ErrorIs(unkept, refused, "Stream's answer stays for the log")
}

func (s *StreamKeysSuite) TestADeploymentWithNoStreamAppIsTheDeploymentsToFix() {
	unkept := unkeptIn(fmt.Errorf("conversation: %w", streamapp.ErrNoIdentity), false)

	s.ErrorIs(unkept, ErrNoStreamKeys)
}

func (s *StreamKeysSuite) TestTheRefusalsNameTheSettingsAndIncognito() {
	for _, refusal := range []error{ErrNoStreamKeys, ErrStreamKeysRejected} {
		s.Contains(refusal.Error(), "STREAM_API_KEY", refusal)
		s.Contains(refusal.Error(), "STREAM_API_SECRET", refusal)
		s.Contains(refusal.Error(), "incognito: true", refusal)
	}
}

func (s *StreamKeysSuite) TestInAppModeTheAppIsTheCustomers() {
	// The deployment's variables are no help to a customer whose own app refuses its keys.
	refused := s.streamAnswering(http.StatusUnauthorized, `{"StatusCode":401,"code":2,"message":"api_key not valid"}`)

	s.NoError(unkeptIn(refused, true))
	s.NoError(unkeptIn(streamapp.ErrNoIdentity, true))
}

func (s *StreamKeysSuite) TestAnyOtherStreamFailureIsLeftAlone() {
	failed := s.streamAnswering(http.StatusInternalServerError, `{"StatusCode":500,"code":-1,"message":"boom"}`)

	s.NoError(unkeptIn(failed, false))
	s.NoError(unkeptIn(errors.New("conversation: closed"), false))
	s.NoError(unkeptIn(nil, false))
}
