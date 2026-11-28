package phone

import (
	"context"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
)

// trunkRecord is the record label of Stream trunk passwords in the appkeys counters.
const trunkRecord = "stream_trunk"

// The counter starts here so a dashboard or alert on the stream_trunk series reads 0 from the
// router's start, not "no data" until the first fallback or open under the router's key.
func init() { appkeys.StartCounting(trunkRecord) }

// trunkPasswordRecord is the password of the customer's Stream trunk, as the appkeys counters
// and log lines name it.
func trunkPasswordRecord(customerID, trunkID string) appkeys.Record {
	return appkeys.Record{Kind: trunkRecord, CustomerID: customerID, Attrs: []slog.Attr{slog.String("trunk", trunkID)}}
}

// sealTrunkPassword seals the password of the customer's Stream trunk, with aad naming its row.
// With sealing on it goes under the app's data key. With sealing off, no data keys, or KMS
// unavailable, it goes under the router's key, so a trunk is never lost to KMS being down.
func (s *Service) sealTrunkPassword(ctx context.Context, customerID, trunkID, password string,
	aad []byte) (appkeys.Sealed, error) {
	return s.dataKeys.SealRecord(ctx, trunkPasswordRecord(customerID, trunkID), []byte(password), aad,
		func() ([]byte, int, error) {
			blob, err := s.sealer.SealWithAAD(password, aad)
			return blob, s.sealer.CurrentVersion(), err
		})
}

// openTrunkPassword opens the password under the key its row names: the app's data key at
// dataKeyVersion, or the router's key at kekVersion when dataKeyVersion is nil. A password sealed
// under the data key never falls back to the router's key, so any failure to open it is an error.
// The error can name the KMS key: log it and never return it to a caller.
func (s *Service) openTrunkPassword(ctx context.Context, customerID, trunkID string, sealed []byte, kekVersion int,
	dataKeyVersion *int, aad []byte) (string, error) {
	// A data key seal stores kek version 0. A build from before data keys can save the trunk
	// again: it seals under the router's key at its current version and leaves the old data key
	// version in place. So a nonzero kek version means the router's key sealed the row.
	if kekVersion != 0 {
		dataKeyVersion = nil
	}
	plain, err := s.dataKeys.OpenRecord(ctx, trunkPasswordRecord(customerID, trunkID), dataKeyVersion, sealed, aad,
		func() ([]byte, error) {
			password, err := s.sealer.OpenWithAADVersion(sealed, aad, kekVersion)
			return []byte(password), err
		})
	return string(plain), err
}
