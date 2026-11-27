package routing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
)

// safetyPurpose is what the router's keyring derives the safety key for (auth.Sealer.Derive).
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

// safetyIdentifier is Spec.SafetyIdentifier for the request on one instance. The user is the
// verified one the work is attributed to, so an anonymous caller, whose name nobody checked and
// who could pick another user's, gets the app's identifier alone.
func (r *Router[P]) safetyIdentifier(request Request, config ProviderConfig) string {
	if config.BYOK {
		return ""
	}
	return safetyIdentifier(r.safetyKey, request.CustomerID, request.Attribution.UserID)
}

// safetyIdentifier is HMAC-SHA256 of the app id and the user id, in hex: 64 characters, under
// the 128 OpenAI accepts. The app id goes in with its length first, so app "a" with user "bc"
// and app "ab" with user "c" do not share one.
func safetyIdentifier(key []byte, customerID, userID string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(binary.AppendUvarint(nil, uint64(len(customerID))))
	mac.Write([]byte(customerID))
	mac.Write([]byte(userID))
	return hex.EncodeToString(mac.Sum(nil))
}
