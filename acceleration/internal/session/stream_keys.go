package session

import (
	"errors"
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

// unkeptIn is the refusal for a conversation that could not be opened because the deployment's
// own Stream app is missing or refuses its keys, and nil for any other failure. Only in
// deployment mode: in app mode the app and its keys are the customer's, which keepable and
// the gateway answer for, and naming the deployment's variables would send them to settings
// they cannot change. The cause is joined so the log keeps Stream's answer; the caller is told
// only the sentinel's words.
func unkeptIn(err error, perApp bool) error {
	if err == nil || perApp {
		return nil
	}
	if errors.Is(err, streamapp.ErrNoIdentity) {
		return errors.Join(ErrNoStreamKeys, err)
	}
	if refused, ok := errors.AsType[*getstream.StreamError](err); ok &&
		(refused.StatusCode == http.StatusUnauthorized || refused.StatusCode == http.StatusForbidden) {
		return errors.Join(ErrStreamKeysRejected, err)
	}
	return nil
}
