package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	s.Equal(ErrStreamKeysRejected.Error(), unkept.Error(), "a caller answering with Error() tells only our words")
	s.Equal(refused, errors.Unwrap(unkept))
	s.Contains(unkept.(slog.LogValuer).LogValue().String(), "api_key not valid", "the log keeps Stream's answer")
}

func (s *StreamKeysSuite) TestATokenNotSignedWithTheKeysSecretIsTheDeploymentsToFix() {
	// A real api key with the wrong secret: the Chat backend's invalidTokenSignature.
	refused := s.streamAnswering(http.StatusUnauthorized,
		`{"StatusCode":401,"code":43,"message":"QueryUsers failed with error: \"JWTAuth error: signature is not valid\""}`)

	s.ErrorIs(unkeptIn(refused, false), ErrStreamKeysRejected)
}

func (s *StreamKeysSuite) TestADeploymentWithNoStreamAppIsTheDeploymentsToFix() {
	cause := fmt.Errorf("conversation: %w", streamapp.ErrNoIdentity)

	unkept := unkeptIn(cause, false)

	s.Require().ErrorIs(unkept, ErrNoStreamKeys)
	s.Equal(ErrNoStreamKeys.Error(), unkept.Error())
	s.ErrorIs(unkept, streamapp.ErrNoIdentity)
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

func (s *StreamKeysSuite) TestAnyOtherStreamRefusalIsLeftAlone() {
	for name, answer := range map[string]struct {
		status int
		body   string
	}{
		"a 401 for another reason": {http.StatusUnauthorized, `{"StatusCode":401,"code":40,"message":"token expired"}`},
		"a 403 not allowed":        {http.StatusForbidden, `{"StatusCode":403,"code":17,"message":"not allowed"}`},
		"a 403 with code 2":        {http.StatusForbidden, `{"StatusCode":403,"code":2,"message":"product not enabled"}`},
		"a 429":                    {http.StatusTooManyRequests, `{"StatusCode":429,"code":9,"message":"too many requests"}`},
		"a 500":                    {http.StatusInternalServerError, `{"StatusCode":500,"code":-1,"message":"boom"}`},
	} {
		s.NoError(unkeptIn(s.streamAnswering(answer.status, answer.body), false), name)
	}
	s.NoError(unkeptIn(errors.New("conversation: closed"), false))
	s.NoError(unkeptIn(nil, false))
}
