package phone

import (
	"context"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// trunkRecord is the record label of Stream trunk passwords in the appkeys counters.
	trunkRecord = "stream_trunk"
	// callBridgeRecord is the record label of the trunk passwords parked call bridges keep.
	callBridgeRecord = "call_bridge"
)

// The counters start here so a dashboard or alert on these series reads 0 from the router's
// start, not "no data" until the first fallback or open under the router's key.
func init() {
	appkeys.StartCounting(trunkRecord)
	appkeys.StartCounting(callBridgeRecord)
}

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
	if !underDataKey(kekVersion, dataKeyVersion) {
		dataKeyVersion = nil
	}
	plain, err := s.dataKeys.OpenRecord(ctx, trunkPasswordRecord(customerID, trunkID), dataKeyVersion, sealed, aad,
		func() ([]byte, error) {
			password, err := s.sealer.OpenWithAADVersion(sealed, aad, kekVersion)
			return []byte(password), err
		})
	return string(plain), err
}

// underDataKey says whether the app's data key sealed a trunk password. A data key seal stores
// kek version 0. A build from before data keys can save the trunk again: it seals under the
// router's key at its current version and leaves the old data key version in place. So a nonzero
// kek version means the router's key sealed the row.
func underDataKey(kekVersion int, dataKeyVersion *int) bool {
	return dataKeyVersion != nil && kekVersion == 0
}

// resealTrunkPassword seals password again under the app's data key when the router's key sealed
// it and sealing is on, and answers false otherwise. A failure is logged and answers false too,
// since the password opened and the call or line can still use it. The next use tries again.
func (s *Service) resealTrunkPassword(ctx context.Context, customerID, trunkID, password string, kekVersion int,
	dataKeyVersion *int, aad []byte) (appkeys.Sealed, bool) {
	if underDataKey(kekVersion, dataKeyVersion) {
		return appkeys.Sealed{}, false
	}
	sealed, err := s.dataKeys.ResealRecord(ctx, trunkPasswordRecord(customerID, trunkID), []byte(password), aad)
	if err != nil {
		s.logger.WarnContext(ctx, "left a trunk password under the router's key, since it could not be sealed "+
			"again under the app's data key", "app_id", customerID, "trunk", trunkID, "error", err)
		return appkeys.Sealed{}, false
	}
	// ResealRecord returns a Sealed without a data key version, and seals nothing, when sealing is
	// off or the app has no data keys.
	return sealed, sealed.DataKeyVersion != nil
}

// resealStoredTrunkPassword is resealTrunkPassword for a password that stays in its row, and
// writes the new blob over the opened one.
func (s *Service) resealStoredTrunkPassword(ctx context.Context, customerID, trunkID, password string, opened []byte,
	kekVersion int, dataKeyVersion *int, aad []byte) {
	sealed, ok := s.resealTrunkPassword(ctx, customerID, trunkID, password, kekVersion, dataKeyVersion, aad)
	if !ok {
		return
	}
	if _, err := s.store.StreamTrunks.ResealPassword(ctx, customerID, trunkID, opened, sealed.Blob,
		*sealed.DataKeyVersion); err != nil {
		s.logger.WarnContext(ctx, "left a trunk password under the router's key, since the resealed one "+
			"could not be saved", "app_id", customerID, "trunk", trunkID, "error", err)
	}
}

// parkedPasswordRecord is the trunk password a call bridge keeps for the call callID, as the
// appkeys counters and log lines name it. The bridge's token never goes in a log line, since it
// is the whole of the vendor's authentication when it fetches the bridge.
func parkedPasswordRecord(customerID, callID string) appkeys.Record {
	return appkeys.Record{Kind: callBridgeRecord, CustomerID: customerID, Attrs: []slog.Attr{slog.String("call_id", callID)}}
}

// sealParkedPassword seals the trunk password of a bridge parked for the call callID. With
// sealing on it goes under the app's data key, and under the router's key otherwise, or while
// KMS is unavailable, so a call is never refused because KMS is down.
func (s *Service) sealParkedPassword(ctx context.Context, customerID, callID, token, password string) (appkeys.Sealed, error) {
	aad := callBridgeAAD(customerID, token)
	return s.dataKeys.SealRecord(ctx, parkedPasswordRecord(customerID, callID), []byte(password), aad,
		func() ([]byte, int, error) {
			blob, err := s.sealer.SealWithAAD(password, aad)
			if err != nil {
				// The router's key fails the same way on every retry, so mark the error as lasting and do not retry the call.
				return nil, 0, lastingError{err}
			}
			return blob, s.sealer.CurrentVersion(), nil
		})
}

// openParkedPassword opens the trunk password of a claimed bridge under the key its row names.
// The bridge's row is gone by then, so a password under the router's key is counted and never
// sealed again. Like openTrunkPassword, the error can name the KMS key and goes to the log only.
func (s *Service) openParkedPassword(ctx context.Context, bridge store.CallBridge, token string) (string, error) {
	dataKeyVersion := bridge.TrunkDataKeyVersion
	if !underDataKey(bridge.TrunkKEKVersion, dataKeyVersion) {
		dataKeyVersion = nil
	}
	aad := callBridgeAAD(bridge.CustomerID, token)
	plain, err := s.dataKeys.OpenRecord(ctx, parkedPasswordRecord(bridge.CustomerID, bridge.CallID), dataKeyVersion,
		bridge.TrunkPasswordSealed, aad, func() ([]byte, error) {
			password, err := s.sealer.OpenWithAADVersion(bridge.TrunkPasswordSealed, aad, bridge.TrunkKEKVersion)
			return []byte(password), err
		})
	return string(plain), err
}
