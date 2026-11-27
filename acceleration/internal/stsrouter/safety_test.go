package stsrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts/openairealtime"
)

// The factory hands the identifier routing chose to the vendor, which sends it as the socket opens.
func (s *STSRouterSuite) TestTheRealtimeFactoryOpensOpenAIsSocketWithTheSafetyIdentifier() {
	headers := make(chan http.Header, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"session.created","session":{}}`))
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"session.updated","session":{}}`))
		_, _, _ = conn.ReadMessage()
	}))
	s.T().Cleanup(server.Close)
	s.T().Setenv("OPENAI_API_KEY", "sk-test")
	vendor := openairealtime.OpenAI
	vendor.URL = "ws://" + strings.TrimPrefix(server.URL, "http://")

	provider, err := realtime(vendor)(routing.Spec{Model: "gpt-realtime-2.1", SafetyIdentifier: "5f1d0c"})
	s.Require().NoError(err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	defer func() { _ = provider.Close() }()

	s.Equal([]string{"5f1d0c"}, (<-headers).Values("OpenAI-Safety-Identifier"))
}
