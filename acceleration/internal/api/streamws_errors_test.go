package api

import (
	"crypto/rand"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

// received is what write sends on a stream socket, as its caller reads it: the messages, then
// the close frame, nil when the socket was dropped without one.
func received(t *testing.T, write func(out *socket) error) ([]string, *websocket.CloseError) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	wrote := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			wrote <- err
			return
		}
		defer connection.Close()
		wrote <- write(&socket{connection: connection})
	}))
	t.Cleanup(server.Close)
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer connection.Close()
	var messages []string
	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			require.NoError(t, <-wrote)
			var closed *websocket.CloseError
			if errors.As(err, &closed) && closed.Code != websocket.CloseAbnormalClosure {
				return messages, closed
			}
			return messages, nil
		}
		messages = append(messages, string(message))
	}
}

// sentOver is the one message write sends on a stream socket.
func sentOver(t *testing.T, write func(out *socket) error) string {
	t.Helper()
	messages, _ := received(t, write)
	require.Len(t, messages, 1)
	return messages[0]
}

// failedAndEnded is the error frame and the close frame a stream socket sends for a failure
// that ends it: one opening the provider's session, or one the session ended on.
func failedAndEnded(t *testing.T, failure error) (string, *websocket.CloseError) {
	t.Helper()
	messages, closed := received(t, func(out *socket) error {
		out.failed(failure)
		out.end(failure)
		return nil
	})
	require.Len(t, messages, 1)
	require.NotNil(t, closed)
	return messages[0], closed
}

// errorFrames is the error frame each stream socket sends for a provider's failure.
func errorFrames(t *testing.T, failure error) map[string]string {
	t.Helper()
	sent, ok := sttFrame(stt.Error{Err: failure, Context: "stream"})
	transcribed := string(marshalled(t, sent, ok))
	sent, ok = llmFrame(llm.ResponseFailed{ResponseID: "r1", Err: failure, Context: "stream"})
	responded := string(marshalled(t, sent, ok))
	return map[string]string{
		"stt": transcribed,
		"llm": responded,
		"tts": sentOver(t, func(out *socket) error {
			return writeTTS(out, tts.Error{SynthesisID: "s1", Err: failure, Context: "stream"}, nil)
		}),
		"sts": sentOver(t, func(out *socket) error {
			return writeSTS(out, sts.Error{ResponseID: "r1", Err: failure, Context: "stream"})
		}),
	}
}

func TestAStreamSocketErrorNeverCarriesACredentialARefusalEchoed(t *testing.T) {
	key, token := rand.Text(), "sk-test-"+rand.Text()
	echoed := routing.Refused(errors.New(`routing: unknown target "wss://live.example/ws?key=` + key + `"` + "\nAuthorization: Bearer " + token))
	for socket, sent := range errorFrames(t, echoed) {
		t.Run(socket, func(t *testing.T) {
			require.NotContains(t, sent, key)
			require.NotContains(t, sent, token)
			require.Contains(t, sent, `routing: unknown target`)
		})
	}
}

func TestAStreamSocketErrorARefusalSentIsTheRefusalInEveryFrame(t *testing.T) {
	refusal := routing.Refused(errors.New("routing: down/news does not take category"))
	for socket, sent := range errorFrames(t, refusal) {
		t.Run(socket, func(t *testing.T) {
			require.Contains(t, sent, `"error":"routing: down/news does not take category"`)
		})
	}
}

func TestAStreamSocketErrorAVendorAnsweredSaysNothingOfIt(t *testing.T) {
	answered := errors.New(`gemini: http 429: {"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`)
	// gorilla's WriteJSON ends a message with a newline.
	want := map[string]string{
		"stt": `{"context":"stream","error":"something went wrong","fatal":false,"type":"error"}`,
		"llm": `{"context":"stream","error":"something went wrong","fatal":false,"id":"r1","type":"error"}`,
		"tts": `{"context":"stream","error":"something went wrong","fatal":false,"id":"s1","type":"error"}` + "\n",
		"sts": `{"context":"stream","error":"something went wrong","fatal":false,"id":"r1","type":"error"}` + "\n",
	}
	require.Equal(t, want, errorFrames(t, answered))
}

func TestAStreamSocketThatIsRefusedNeverSaysACredentialInItsClose(t *testing.T) {
	key := rand.Text()
	_, closed := failedAndEnded(t, routing.Refused(errors.New(`routing: unknown target "wss://live.example/ws?key=`+key+`"`)))

	require.Equal(t, websocket.ClosePolicyViolation, closed.Code)
	require.Equal(t, `routing: unknown target "wss://live.example/ws?key=[redacted]"`, closed.Text)
}

func TestAStreamSocketThatFailsAtItsVendorSaysNothingOfIt(t *testing.T) {
	sent, closed := failedAndEnded(t, errors.New(`gemini: http 503: {"error":"backend unavailable"}`))

	require.Equal(t, `{"error":"something went wrong","type":"error"}`+"\n", sent)
	require.Equal(t, websocket.CloseInternalServerErr, closed.Code)
	require.Equal(t, somethingWentWrong, closed.Text)
}

func TestAStreamSocketThatFailsOnTheNetworkSaysNothingOfIt(t *testing.T) {
	_, failure := net.Dial("tcp", "vendor.invalid:443")
	require.Error(t, failure)

	sent, closed := failedAndEnded(t, failure)

	require.Equal(t, `{"error":"something went wrong","type":"error"}`+"\n", sent)
	require.Equal(t, websocket.CloseInternalServerErr, closed.Code)
	require.Equal(t, somethingWentWrong, closed.Text)
	for socket, sent := range errorFrames(t, failure) {
		t.Run(socket, func(t *testing.T) {
			require.NotContains(t, sent, "vendor.invalid")
			require.Contains(t, sent, somethingWentWrong)
		})
	}
}

// A failure frame on a stream socket is not cut, so a refusal naming what the caller sent
// stays whole, but it never carries the key a provider echoed.
func TestAStreamSocketFailureFrameKeepsALongRefusalWholeWithoutItsKey(t *testing.T) {
	key, long := rand.Text(), strings.Repeat("x", 200<<10)
	sent := sentOver(t, func(out *socket) error {
		out.failed(routing.Refused(errors.New(`routing: unknown target "` + long + `" from wss://live.example/ws?key=` + key)))
		return nil
	})

	require.NotContains(t, sent, key)
	require.Contains(t, sent, long)
}
