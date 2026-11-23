package api

import (
	"errors"
	"net"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// closeCause is why a socket closed (a session socket, /v1/dispatch, /v1/{modality}/stream),
// logged as close_reason on its access line.
type closeCause string

const (
	// closedByClient is a caller that sent a close frame, or a command that closed the session.
	closedByClient closeCause = "client_closed"
	// clientGone is a connection that broke with no close frame: it dropped, or a write to it
	// failed.
	clientGone closeCause = "client_gone"
	// timedOut is a caller that answered no ping within pongWait, or sent no start frame within
	// startWait.
	timedOut closeCause = "timeout"
	// sessionEnded is a session that ended, for the end_reason logged beside it.
	sessionEnded closeCause = "session_ended"
	// refusedByRouter is a caller the router refused: a start frame, a session it would not
	// open, a frame it could not read or one past maxSocketMessage.
	refusedByRouter closeCause = "refused"
	// serverFailed is a failure of the router's own, or of a provider a stream socket could not
	// start.
	serverFailed closeCause = "server_error"
	// routerShutdown is a session the router stopped as it went away, or a dispatch worker it
	// sent to reconnect elsewhere.
	routerShutdown closeCause = "shutdown"
)

// socketEnd is why a socket closed: the cause, the code of the close frame the router sent or
// the caller did (0 when neither did), and the session's end reason when the session ended.
type socketEnd struct {
	cause     closeCause
	code      int
	endReason session.EndReason
}

// fields are socketEnd's fields on the access line, none for a request that opened no socket.
func (e socketEnd) fields() []any {
	if e.cause == "" {
		return nil
	}
	fields := []any{"close_reason", string(e.cause)}
	if e.code != 0 {
		fields = append(fields, "close_code", e.code)
	}
	if e.endReason != "" {
		fields = append(fields, "end_reason", string(e.endReason))
	}
	return fields
}

// readEnd is why a socket's reader stopped on err. gorilla reports a connection that dropped
// as a close with 1006, which no peer may send (RFC 6455 section 7.4.1,
// https://www.rfc-editor.org/rfc/rfc6455#section-7.4.1), and answers a frame past the read
// limit with 1009 itself.
func readEnd(err error) socketEnd {
	var closed *websocket.CloseError
	var failed net.Error
	switch {
	case errors.Is(err, websocket.ErrReadLimit):
		return socketEnd{cause: refusedByRouter, code: websocket.CloseMessageTooBig}
	case errors.As(err, &closed) && closed.Code != websocket.CloseAbnormalClosure:
		return socketEnd{cause: closedByClient, code: closed.Code}
	case errors.As(err, &failed) && failed.Timeout():
		return socketEnd{cause: timedOut}
	}
	return socketEnd{cause: clientGone}
}

// sessionEnd is why a socket closed with code once its session's events stopped: a router
// restarting (1012, endingOf) or the session's end.
func sessionEnd(code int) socketEnd {
	if code == websocket.CloseServiceRestart {
		return socketEnd{cause: routerShutdown, code: code}
	}
	return socketEnd{cause: sessionEnded, code: code}
}
