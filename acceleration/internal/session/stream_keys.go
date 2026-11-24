package session

import (
	"errors"
	"log/slog"
	"net/http"

	getstream "github.com/GetStream/getstream-go/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// ErrNoStreamKeys is a conversation that is not incognito, asked of a deployment that has no
// Stream app to keep it in: the router keeps no history of its own, so a self-hosted router
// needs a Stream app's keys for every session that is not incognito.
var ErrNoStreamKeys = errors.New("session: this deployment has no Stream app, and a session that is not " +
	"incognito is kept in Stream Chat: set stream.api_key and stream.api_secret (STREAM_API_KEY, " +
	"STREAM_API_SECRET) to a Stream app's keys, or open the session with incognito: true")

// ErrStreamKeysRejected is a conversation that is not incognito, asked of a deployment whose
// Stream app keys Stream refuses, such as the placeholders .env.example ships.
var ErrStreamKeysRejected = errors.New("session: Stream refused this deployment's Stream app keys, and a " +
	"session that is not incognito is kept in Stream Chat: set stream.api_key and stream.api_secret " +
	"(STREAM_API_KEY, STREAM_API_SECRET) to a Stream app's keys, or open the session with incognito: true")

// Stream's error codes for a request its keys do not authenticate, from the Chat backend's
// lib/core/errors: an api key it does not know, and a token not signed with that key's secret.
const (
	streamCodeAccessKey        = 2
	streamCodeInvalidSignature = 43
)

// UnkeptError is ErrNoStreamKeys or ErrStreamKeysRejected with what Stream or the router said.
// It reads as the refusal alone, so every caller that answers with err.Error() tells its
// caller only our words; the cause stays for errors.Unwrap, errors.Is and the log.
type UnkeptError struct {
	Refusal error
	Cause   error
}

func (e *UnkeptError) Error() string { return e.Refusal.Error() }

func (e *UnkeptError) Is(target error) bool { return target == e.Refusal }

func (e *UnkeptError) Unwrap() error { return e.Cause }

// LogValue is the refusal and its cause, so a log line keeps what Stream answered.
func (e *UnkeptError) LogValue() slog.Value {
	return slog.StringValue(e.Refusal.Error() + ": " + e.Cause.Error())
}

// unkeptIn is the refusal for a conversation that could not be opened because the deployment's
// own Stream app is missing or refuses its keys, and nil for any other failure. Only in
// deployment mode: in app mode the app and its keys are the customer's, which keepable and
// the gateway answer for, and naming the deployment's variables would send them to settings
// they cannot change.
func unkeptIn(err error, perApp bool) error {
	if err == nil || perApp {
		return nil
	}
	if errors.Is(err, streamapp.ErrNoIdentity) {
		return &UnkeptError{Refusal: ErrNoStreamKeys, Cause: err}
	}
	if refused, ok := errors.AsType[*getstream.StreamError](err); ok && refused.StatusCode == http.StatusUnauthorized &&
		(refused.Code == streamCodeAccessKey || refused.Code == streamCodeInvalidSignature) {
		return &UnkeptError{Refusal: ErrStreamKeysRejected, Cause: err}
	}
	return nil
}
