package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type syncInput struct {
	Body struct {
		Name string `json:"name" minLength:"1"`
	}
}

// served is one Huma operation behind the request id and the access log, as Handler wraps
// every operation; failure is what the operation returns.
func served(t *testing.T, failure func() error) (http.Handler, *bytes.Buffer) {
	t.Helper()
	logged := &bytes.Buffer{}
	server := &Server{logger: slog.New(slog.NewTextHandler(logged, nil))}
	router := chi.NewRouter()
	api := humachi.New(router, huma.DefaultConfig("test", "0"))
	huma.Register(api, huma.Operation{OperationID: "syncAgent", Method: http.MethodPost, Path: "/v1/agents/sync"},
		func(context.Context, *syncInput) (*struct{}, error) { return nil, failure() })
	return withRequestID(server.withRequestLog(router, router)), logged
}

func postSync(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/agents/sync", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func lookUpConfig() error {
	return stack.Wrap(fmt.Errorf("store: agent config by name: %w",
		errors.New("dial tcp 10.0.0.7:5432: connection refused")))
}

func TestARequestIsAnsweredAndLoggedWithItsRequestID(t *testing.T) {
	handler, logged := served(t, func() error { return nil })

	response := postSync(handler, `{"name":"jean"}`)

	id := response.Header().Get(RequestIDHeader)
	require.NoError(t, uuid.Validate(id), "a request that arrived without one is given one")
	require.Contains(t, logged.String(), "request_id="+id)
}

func TestARequestIDFromAProxyIsKeptAndAnUnreadableOneReplaced(t *testing.T) {
	handler, _ := served(t, func() error { return nil })
	for sent, kept := range map[string]bool{
		"gw-7f3a91":                     true,
		"two words":                     false,
		strings.Repeat("x", 129):        false,
		"ends-with-newline\nforged=log": false,
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/agents/sync", strings.NewReader(`{"name":"jean"}`))
		request.Header.Set(RequestIDHeader, sent)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		answered := response.Header().Get(RequestIDHeader)
		if kept {
			require.Equal(t, sent, answered)
		} else {
			require.NoError(t, uuid.Validate(answered), "replaced, not repeated: %q", sent)
		}
	}
}

func TestAServerErrorIsLoggedWhereItEnteredAndAnsweredWithTheRequestIDAlone(t *testing.T) {
	handler, logged := served(t, lookUpConfig)

	response := postSync(handler, `{"name":"jean"}`)

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"error":{"message":"something went wrong","type":"internal","code":"internal_error",
		"doc_url":"https://getstream.io/agents/docs/api/errors/#internal_error"}}`, response.Body.String())
	require.NotContains(t, response.Body.String(), "10.0.0.7", "the caller is not told what the database said")

	line := logged.String()
	require.Contains(t, line, "level=ERROR")
	require.Contains(t, line, "request_id="+response.Header().Get(RequestIDHeader))
	require.Contains(t, line, `error="store: agent config by name: dial tcp 10.0.0.7:5432: connection refused"`)
	require.Contains(t, line, `stack="github.com/GetStream/Vision-Agents/acceleration/internal/api.lookUpConfig\n`,
		"the stack starts where the error was wrapped, not where it reached Huma")
}

// The error is the driver's own, from a real query to a database that is not there: what it
// names (the host, the port, the database, the user) is what a caller must never be told.
func TestADatabaseFailureIsAnsweredAsInternalWithNothingOfTheDatabase(t *testing.T) {
	broken, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	handler, logged := served(t, func() error {
		_, err := broken.Voices.Get(context.Background(), "acme", "voice-1")
		return err
	})

	response := postSync(handler, `{"name":"jean"}`)

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"error":{"message":"something went wrong","type":"internal","code":"internal_error",
		"doc_url":"https://getstream.io/agents/docs/api/errors/#internal_error"}}`, response.Body.String())
	for _, detail := range []string{"127.0.0.1", ":1", "agents_prod", "router_app", "hunter2", "dial", "store:"} {
		require.NotContains(t, response.Body.String(), detail)
	}
	id := response.Header().Get(RequestIDHeader)
	require.NoError(t, uuid.Validate(id), "the request id is what finds the failure in the log")

	line := logged.String()
	require.Contains(t, line, "level=ERROR")
	require.Contains(t, line, "request_id="+id)
	require.Contains(t, line, "127.0.0.1:1", "what the database said is kept for whoever looks into it")
	require.Contains(t, line, `stack="github.com/GetStream/Vision-Agents/acceleration/internal/store.`,
		"the stack starts in the store, where the driver's error entered the code base")
	require.NotContains(t, line, "hunter2", "the log is no place for the database password either")
}

func TestACredentialTheKeyStoreCouldNotCheckIsAnsweredAsInternalNotAsAWrongKey(t *testing.T) {
	broken, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	lookup := broken.APIKeys.Lookup(nil)
	key, _, err := auth.NewCredential(auth.Live)
	require.NoError(t, err)
	refused := false
	logged := &bytes.Buffer{}
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		AuthMode: auth.Custom,
		Auth: auth.Func(func(ctx context.Context, _ *http.Request) (auth.Principal, error) {
			if refused {
				return auth.Principal{}, auth.ErrUnauthenticated
			}
			_, err := lookup(ctx, key)
			return auth.Principal{}, err
		}),
		Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	require.NoError(t, err)
	ask := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agents/configs", nil))
		return response
	}

	response := ask()

	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"code":"internal_error"`)
	for _, detail := range []string{"127.0.0.1", "agents_prod", "hunter2", "dial", "store:"} {
		require.NotContains(t, response.Body.String(), detail)
	}
	require.Contains(t, logged.String(), "127.0.0.1:1", "the outage is in the log, not hidden as a wrong key")

	refused = true
	require.Equal(t, http.StatusUnauthorized, ask().Code, "a key the store refused is still the caller's to fix")
}

func TestACallerThatHangsUpWhileItsKeyIsCheckedIsNoFailure(t *testing.T) {
	broken, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	lookup := broken.APIKeys.Lookup(nil)
	key, _, err := auth.NewCredential(auth.Live)
	require.NoError(t, err)
	logged := &bytes.Buffer{}
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		AuthMode: auth.Custom,
		Auth: auth.Func(func(ctx context.Context, _ *http.Request) (auth.Principal, error) {
			_, err := lookup(ctx, key)
			return auth.Principal{}, err
		}),
		Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	require.NoError(t, err)
	ctx, hangUp := context.WithCancel(context.Background())
	hangUp()
	response := httptest.NewRecorder()
	gone := map[string]string{"route": "/v1/agents/configs", "status": "4xx"}
	answered := map[string]string{"route": "/v1/agents/configs", "status": "2xx"}
	goneBefore, answeredBefore := sumOf(t, "router_http_requests_total", gone), sumOf(t, "router_http_requests_total", answered)

	server.Handler().ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/agents/configs", nil))

	require.Equal(t, 499, response.Code, response.Body.String())
	require.NotContains(t, logged.String(), "level=ERROR", "a caller hanging up is not an outage")
	require.Equal(t, goneBefore+1, sumOf(t, "router_http_requests_total", gone), "a caller that gave up is counted")
	require.Equal(t, answeredBefore, sumOf(t, "router_http_requests_total", answered), "nor as an answer")
}

func TestWhetherACallerIsAGuestCouldNotBeRead(t *testing.T) {
	broken, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	logged := &bytes.Buffer{}
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store:    broken,
		AuthMode: auth.Custom,
		Auth: auth.Func(func(context.Context, *http.Request) (auth.Principal, error) {
			return auth.Principal{AppID: "app", UserID: "guest-1", Kind: auth.KindAuthenticated}, nil
		}),
		Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	require.NoError(t, err)
	ask := func(ctx context.Context) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/agents/configs", nil))
		return response
	}
	gone, hangUp := context.WithCancel(context.Background())
	hangUp()

	require.Equal(t, 499, ask(gone).Code)
	require.NotContains(t, logged.String(), "level=ERROR", "a caller hanging up is not an outage")

	response := ask(context.Background())
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Contains(t, response.Body.String(), `"code":"internal_error"`)
	require.NotContains(t, response.Body.String(), "127.0.0.1")
	require.Contains(t, logged.String(), "127.0.0.1:1")
	require.Contains(t, logged.String(), "store: minted guest")
}

func TestAKeyStoreDownIsAFailureEvenIfTheCallerHasGone(t *testing.T) {
	broken, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	lookup := broken.APIKeys.Lookup(nil)
	key, _, err := auth.NewCredential(auth.Live)
	require.NoError(t, err)
	logged := &bytes.Buffer{}
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		AuthMode: auth.Custom,
		// The lookup fails on its own, as an unreachable store does, not for the caller leaving.
		Auth: auth.Func(func(context.Context, *http.Request) (auth.Principal, error) {
			_, err := lookup(context.Background(), key)
			return auth.Principal{}, err
		}),
		Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	require.NoError(t, err)
	ctx, hangUp := context.WithCancel(context.Background())
	hangUp()

	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/agents/configs", nil))

	require.Contains(t, logged.String(), "level=ERROR", "an outage is logged whoever is left to hear of it")
	require.Contains(t, logged.String(), "127.0.0.1:1")
}

func TestALookupCancelledWhileTheCallerWaitsIsStillAFailure(t *testing.T) {
	logged := &bytes.Buffer{}
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		AuthMode: auth.Custom,
		Auth: auth.Func(func(context.Context, *http.Request) (auth.Principal, error) {
			return auth.Principal{}, stack.Wrap(fmt.Errorf("store: get api key: %w", context.Canceled))
		}),
		Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	require.NoError(t, err)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agents/configs", nil))

	require.Equal(t, http.StatusInternalServerError, response.Code, "the caller is still there to be told")
	require.Contains(t, logged.String(), "level=ERROR")
}

func TestAServerErrorNeverWrappedIsLoggedWithTheStackItReachedHumaOn(t *testing.T) {
	handler, logged := served(t, func() error { return errors.New("never wrapped") })

	response := postSync(handler, `{"name":"jean"}`)

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Contains(t, logged.String(), `error="never wrapped"`)
	require.Contains(t, logged.String(), "danielgtaylor/huma", "the fallback names the operation Huma was running")
}

func TestARefusalIsLoggedWithItsReasonAndNoStack(t *testing.T) {
	handler, logged := served(t, func() error { return nil })

	response := postSync(handler, `{"name":""}`)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "expected length >= 1", "a refusal still says what to fix")
	require.Contains(t, logged.String(), "error=")
	require.NotContains(t, logged.String(), "stack=")
}
