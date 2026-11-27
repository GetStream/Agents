package routing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
)

// safetyPurpose names the key the router's keyring derives for safety identifiers (auth.Sealer.Derive).
const safetyPurpose = "routing: safety identifier v1"

// SafetyKey is Options.SafetyKey from the router's keyring. Every node holds the same keyring
// and so derives the same key, which keeps a user's safety identifier the same whichever node
// serves them. It is derived from the current key encryption key, so a rotation changes every
// identifier. Nil with no keyring, and then no identifier is sent.
func SafetyKey(secrets *auth.Sealer) []byte {
	if secrets == nil {
		return nil
	}
	key, _ := secrets.Derive(safetyPurpose)
	return key
}

// safetyIdentifier returns the Spec.SafetyIdentifier for one request on one instance. It is empty
// on a customer's own key or model, because that account is the customer's, and on a router with
// no key. Otherwise it is built from the end user the work is attributed to, whether their own
// token or the app's backend made the request. An anonymous caller's name is left out, because
// nobody checked it and they could pick another user's. Their work hashes with an "anonymous"
// marker beside the app id, so it gets an identifier of its own: a block on what an anonymous
// caller does never reaches the app's own work.
func (r *Router[P]) safetyIdentifier(request Request, config ProviderConfig) string {
	if len(r.safetyKey) == 0 || config.BYOK || config.Provider == CustomProvider {
		return ""
	}
	party := ""
	if request.Attribution.CallerKind == CallerAnonymous {
		party = CallerAnonymous
	}
	return safetyIdentifier(r.safetyKey, request.CustomerID, party, request.Attribution.UserID)
}

// safetyIdentifier returns the hex HMAC-SHA256 of the app id, the caller kind ("anonymous" or
// empty) and the user id: 64 characters, under the 128 OpenAI accepts. The app id and the caller
// kind go in with their length first, so app "a" with user "bc" and app "ab" with user "c" do not
// share one.
func safetyIdentifier(key []byte, customerID, party, userID string) string {
	mac := hmac.New(sha256.New, key)
	for _, field := range []string{customerID, party} {
		mac.Write(binary.AppendUvarint(nil, uint64(len(field))))
		mac.Write([]byte(field))
	}
	mac.Write([]byte(userID))
	return hex.EncodeToString(mac.Sum(nil))
}
