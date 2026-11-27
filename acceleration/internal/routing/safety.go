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
// verified one the work is attributed to. An anonymous caller's name is one nobody checked, and
// they could pick another user's, so it is left out; their work is set apart from the app's own
// instead, so a block on what an anonymous caller does leaves the app's own work running. The
// other kinds go in alike: a user is one user whether their token or the app's backend asked.
// A customer's own key gets none: that account is the customer's.
func (r *Router[P]) safetyIdentifier(request Request, config ProviderConfig) string {
	if len(r.safetyKey) == 0 || config.BYOK {
		return ""
	}
	party := ""
	if request.Attribution.CallerKind == CallerAnonymous {
		party = CallerAnonymous
	}
	return safetyIdentifier(r.safetyKey, request.CustomerID, party, request.Attribution.UserID)
}

// safetyIdentifier is HMAC-SHA256 of the app id, the party and the user id, in hex: 64
// characters, under the 128 OpenAI accepts. The app id and the party go in with their length
// first, so app "a" with user "bc" and app "ab" with user "c" do not share one.
func safetyIdentifier(key []byte, customerID, party, userID string) string {
	mac := hmac.New(sha256.New, key)
	for _, field := range []string{customerID, party} {
		mac.Write(binary.AppendUvarint(nil, uint64(len(field))))
		mac.Write([]byte(field))
	}
	mac.Write([]byte(userID))
	return hex.EncodeToString(mac.Sum(nil))
}
