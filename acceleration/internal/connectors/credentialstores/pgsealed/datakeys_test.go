//go:build integration

package pgsealed

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// sealedOnBase is credentials(tokens{"base-access", "base-refresh"}) that pgsealed sealed under
// "test key one" at version 1, for acme-app's connection conn-sealed-on-base at revision 2,
// before data keys existed (26dc6389e).
const sealedOnBase = "Ek8LIeKhdhxbiRP02CC8uqNCcMokaZoYQofEDjXjakzoFGUkJBzUBAMCVxHhZemlwVx7den1TAqsNLy92jCBgXj4NEG" +
	"sySxnv0qj/kyuwEcJjlw7b6Jn0qeLaHKw3THZQ7WBmF9x/m/gmI0RlnVSRLU2CvX+7USEY4rPaQTi74O9Prsk+o0qiDqN"

// withDataKeys is a credential store sealing with s.v1 that opens data key rows through kms.
func (s *PGSealedSuite) withDataKeys(kms *appkeystest.KMS) *CredentialStore {
	return s.dataKeyStore(kms, appkeystest.Options())
}

// sealing is a credential store that seals new credentials under the app's data key through kms.
func (s *PGSealedSuite) sealing(kms *appkeystest.KMS) *CredentialStore {
	options := appkeystest.Options()
	options.Seal = true
	return s.dataKeyStore(kms, options)
}

func (s *PGSealedSuite) dataKeyStore(kms *appkeystest.KMS, options appkeys.Options) *CredentialStore {
	keys, err := appkeys.New(s.db, kms, options)
	s.Require().NoError(err)
	credentialStore, err := New(s.db, s.v1, WithDataKeys(keys))
	s.Require().NoError(err)
	return credentialStore
}

// write stores held as ref's new credentials through credentialStore.
func (s *PGSealedSuite) write(credentialStore *CredentialStore, ref core.ConnectionRef, held core.StoredCredentials) error {
	return credentialStore.Update(s.ctx, ref, func(state *core.CredentialState, _ func() error) (bool, error) {
		state.Credentials = held
		return true, nil
	})
}

// TestWithSealingOnNewCredentialsAreSealedUnderTheAppsDataKey: the write makes the app's key and
// holds it, so the next use asks KMS nothing. The router's key alone does not open the row.
func (s *PGSealedSuite) TestWithSealingOnNewCredentialsAreSealedUnderTheAppsDataKey() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	sealing := s.sealing(kms)

	s.Require().NoError(s.write(sealing, ref, credentials(tokens{Access: "b", Refresh: "s"})))
	stored := s.stored(ref)
	s.Equal(new(1), stored.CredentialsDataKeyVersion)
	s.Zero(stored.CredentialsKEKVersion, "a build that knows no data keys finds a key version it does not hold")
	s.Equal(1, kms.Wraps())
	s.Equal(tokens{Access: "b", Refresh: "s"}, opened(s.held(sealing, ref).Credentials))
	s.Zero(kms.Unwraps())
	_, err := s.v1.OpenWithAADVersion(stored.CredentialsSealed, credentialsAAD(ref, stored.Revision), 1)
	s.Error(err)
}

func (s *PGSealedSuite) TestWithSealingOnEmptyCredentialsMakeNoDataKey() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})

	sealing := s.sealing(kms)
	s.Require().NoError(s.write(sealing, ref, core.StoredCredentials{}))
	s.held(sealing, ref)
	stored := s.stored(ref)
	s.Empty(stored.CredentialsSealed)
	s.Zero(stored.CredentialsKEKVersion)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Zero(kms.Wraps())
	_, err := s.db.AppDataKeys.Get(s.ctx, ref.CustomerID, 1)
	s.ErrorIs(err, store.ErrNoAppDataKey)
}

// TestARefreshThatKMSCannotSealForKeepsTheRotatedTokenUnderTheRoutersKey: the provider has
// already spent the old refresh token, so failing the write would leave the connection with
// none that works. The row is sealed as on the base, with no data key version, and opens on a
// router without data keys.
func (s *PGSealedSuite) TestARefreshThatKMSCannotSealForKeepsTheRotatedTokenUnderTheRoutersKey() {
	for _, failure := range []struct {
		name   string
		fail   func(*appkeystest.KMS)
		reason string
	}{
		{"refused", func(kms *appkeystest.KMS) { kms.Fail(errors.New("kms: permission denied on the key")) }, "kms_error"},
		{"no answer", func(kms *appkeystest.KMS) { kms.Hold() }, "kms_timeout"},
	} {
		s.Run(failure.name, func() {
			logs := s.captureLogs()
			kms := appkeystest.NewKMS()
			defer kms.Release()
			options := appkeystest.Options()
			options.Seal, options.KMSTimeout = true, 50*time.Millisecond
			sealing := s.dataKeyStore(kms, options)
			endpoint := s.rotating("refresh-0", nil, answer)
			ref := s.connected("app-"+rand.Text(), tokens{Access: "access-0", Refresh: "refresh-0"}, time.Now().Add(-time.Minute))
			fallbacks := s.sealFallbacks()
			failure.fail(kms)

			resolved, err := resolve(s.ctx, sealing, ref, endpoint)
			s.Require().NoError(err)
			s.Equal(tokens{Access: "access-1", Refresh: "refresh-1"}, resolved)
			stored := s.stored(ref)
			s.Equal(store.ConnectionConnected, stored.Status)
			s.Equal(1, stored.CredentialsKEKVersion)
			s.Nil(stored.CredentialsDataKeyVersion)
			s.Equal(tokens{Access: "access-1", Refresh: "refresh-1"}, opened(s.held(s.credentialStore(s.v1), ref).Credentials))
			s.Equal(fallbacks+1, s.sealFallbacks())
			s.Equal(1, strings.Count(logs.String(), `level=WARN msg="sealed a secret under the router's key`))
			s.Contains(logs.String(), "record=connector_credentials app_id="+ref.CustomerID+" connection="+ref.ConnectionID+
				" reason="+failure.reason)
			s.Equal(int32(1), endpoint.refreshes.Load())
			_, err = s.db.AppDataKeys.Get(s.ctx, ref.CustomerID, 1)
			s.ErrorIs(err, store.ErrNoAppDataKey)
		})
	}
}

// captureLogs sends slog.Default() to the buffer it returns until the test ends. Build the
// credential store after it, since appkeys keeps the logger it was built with.
func (s *PGSealedSuite) captureLogs() *bytes.Buffer {
	logs, previous := &bytes.Buffer{}, slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	s.T().Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// underDataKey seals ref's credentials again under a new data key of its app, at key version 0
// and data key version 1, and returns the blob.
func (s *PGSealedSuite) underDataKey(ref core.ConnectionRef, kms *appkeystest.KMS) []byte {
	connection := s.stored(ref)
	aad := credentialsAAD(ref, connection.Revision)
	plain, err := s.v1.OpenWithAADVersion(connection.CredentialsSealed, aad, connection.CredentialsKEKVersion)
	s.Require().NoError(err)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, kms, ref.CustomerID, 1), []byte(plain),
		appkeystest.RecordAAD("connector_credentials", ref.CustomerID, aad))
	s.putDataKeyBlob(ref, blob)
	return blob
}

func (s *PGSealedSuite) putDataKeyBlob(ref core.ConnectionRef, blob []byte) {
	_, err := s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET credentials_sealed = ?,"+
		" credentials_kek_version = 0, credentials_data_key_version = 1 WHERE id = ?", blob, ref.ConnectionID)
	s.Require().NoError(err)
}

func (s *PGSealedSuite) TestCredentialsUnderTheAppsDataKeyOpenAndAnUnchangedUseKeepsThem() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	blob := s.underDataKey(ref, kms)

	state := s.held(s.withDataKeys(kms), ref)
	s.Equal(tokens{Access: "a", Refresh: "r"}, opened(state.Credentials))
	s.Equal(store.ConnectionConnected, state.Status)
	s.Equal(1, kms.Unwraps())
	stored := s.stored(ref)
	s.True(bytes.Equal(blob, stored.CredentialsSealed), "a use that changed nothing does not seal again under the router's key")
	s.Equal(new(1), stored.CredentialsDataKeyVersion)
}

// TestCredentialsUnderASecondDataKeyOpenWithThatKey: an app with keys v1 and v2 whose row is
// sealed under v2 opens with v2, not with the first key.
func (s *PGSealedSuite) TestCredentialsUnderASecondDataKeyOpenWithThatKey() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	connection := s.stored(ref)
	aad := credentialsAAD(ref, connection.Revision)
	plain, err := s.v1.OpenWithAADVersion(connection.CredentialsSealed, aad, connection.CredentialsKEKVersion)
	s.Require().NoError(err)
	appkeystest.AddKey(s.T(), s.db, kms, ref.CustomerID, 1)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, kms, ref.CustomerID, 2), []byte(plain),
		appkeystest.RecordAAD("connector_credentials", ref.CustomerID, aad))
	_, err = s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET credentials_sealed = ?,"+
		" credentials_kek_version = 0, credentials_data_key_version = 2 WHERE id = ?", blob, ref.ConnectionID)
	s.Require().NoError(err)

	state := s.held(s.withDataKeys(kms), ref)
	s.Equal(tokens{Access: "a", Refresh: "r"}, opened(state.Credentials))
	s.Equal(store.ConnectionConnected, state.Status)
}

// TestWithSealingTurnedOffADataKeyRowOpensAndNewCredentialsGoUnderTheRoutersKey is the
// rollback: the KMS key stays set, so a row sealed under the data key still opens, and a write
// leaves a row that a router without data keys opens, as on the base.
func (s *PGSealedSuite) TestWithSealingTurnedOffADataKeyRowOpensAndNewCredentialsGoUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	s.Require().NoError(s.write(s.sealing(kms), ref, credentials(tokens{Access: "on", Refresh: "on"})))
	off := s.withDataKeys(kms)
	s.Equal(tokens{Access: "on", Refresh: "on"}, opened(s.held(off, ref).Credentials))

	s.Require().NoError(s.write(off, ref, credentials(tokens{Access: "b", Refresh: "s"})))
	s.Equal(1, kms.Wraps())
	stored := s.stored(ref)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal(1, stored.CredentialsKEKVersion)
	s.Equal(tokens{Access: "b", Refresh: "s"}, opened(s.held(s.credentialStore(s.v1), ref).Credentials))
}

func (s *PGSealedSuite) TestADataKeyBlobInAnotherAppsRowDoesNotOpen() {
	kms := appkeystest.NewKMS()
	theirs := s.connected("app-"+rand.Text(), tokens{Access: "theirs", Refresh: "theirs"}, time.Time{})
	mine := s.connected("app-"+rand.Text(), tokens{Access: "mine", Refresh: "mine"}, time.Time{})
	s.underDataKey(mine, kms)
	s.putDataKeyBlob(mine, s.underDataKey(theirs, kms))

	s.assertUnreadableBy(s.withDataKeys(kms), mine)
}

func (s *PGSealedSuite) TestAKMSFailureLeavesTheConnectionAsItIsAndItsReturnRestoresIt() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	blob := s.underDataKey(ref, kms)
	kms.Fail(errors.New("kms: unavailable"))

	err := s.withDataKeys(kms).Update(s.ctx, ref, func(*core.CredentialState, func() error) (bool, error) {
		s.Fail("fn does not run on credentials no key opened")
		return false, nil
	})
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, ErrKeyUnavailable, "the API answers it as a key fault, with nothing recorded")
	s.ErrorContains(err, "the key service is unavailable")
	stored := s.stored(ref)
	s.Equal(store.ConnectionConnected, stored.Status, "a KMS fault is not the connection's")
	s.Empty(stored.LastError)
	s.True(bytes.Equal(blob, stored.CredentialsSealed))

	kms.Fail(nil)
	s.Equal(tokens{Access: "a", Refresh: "r"}, opened(s.held(s.withDataKeys(kms), ref).Credentials))
}

// TestADataKeyRowOnARouterWithoutDataKeysIsAnErrorNotAReconnect: the setting is missing, not
// the connection's credentials, so setting it again restores the connection.
func (s *PGSealedSuite) TestADataKeyRowOnARouterWithoutDataKeysIsAnErrorNotAReconnect() {
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	s.underDataKey(ref, appkeystest.NewKMS())

	err := s.credentialStore(s.v1).Update(s.ctx, ref, func(*core.CredentialState, func() error) (bool, error) {
		s.Fail("fn does not run on credentials no key opened")
		return false, nil
	})
	s.ErrorIs(err, appkeys.ErrNoKeys)
	s.ErrorIs(err, ErrKeyUnavailable, "the API answers it as a key fault, with nothing recorded")
	s.Equal(store.ConnectionConnected, s.stored(ref).Status)
}

// TestCredentialsSealedOnTheBaseOpenAsBeforeAndNoDataKeyIsMade: with or without data keys, a
// row under the router's key opens as it did before them, with no KMS call and no data key.
func (s *PGSealedSuite) TestCredentialsSealedOnTheBaseOpenAsBeforeAndNoDataKeyIsMade() {
	made := s.connected("acme-app", tokens{Access: "a", Refresh: "r"}, time.Time{})
	sealed, err := base64.StdEncoding.DecodeString(sealedOnBase)
	s.Require().NoError(err)
	_, err = s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET id = 'conn-sealed-on-base',"+
		" credentials_sealed = ? WHERE id = ?", sealed, made.ConnectionID)
	s.Require().NoError(err)
	ref := core.ConnectionRef{CustomerID: "acme-app", ConnectionID: "conn-sealed-on-base"}
	kms := appkeystest.NewKMS()

	for _, credentialStore := range []*CredentialStore{s.credentialStore(s.v1), s.withDataKeys(kms)} {
		state := s.held(credentialStore, ref)
		s.Equal(tokens{Access: "base-access", Refresh: "base-refresh"}, opened(state.Credentials))
		s.Equal(store.ConnectionConnected, state.Status)
		s.True(bytes.Equal(sealed, s.stored(ref).CredentialsSealed))
	}
	s.Require().NoError(s.credentialStore(s.v1).Update(s.ctx, ref, func(state *core.CredentialState, _ func() error) (bool, error) {
		state.Credentials = credentials(tokens{Access: "b", Refresh: "s"})
		return true, nil
	}))
	stored := s.stored(ref)
	s.Equal(1, stored.CredentialsKEKVersion)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Zero(kms.Unwraps())
	var keys int
	s.Require().NoError(s.db.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM app_data_keys WHERE customer_id = 'acme-app'").Scan(&keys))
	s.Zero(keys)
}

// TestWithNothingConfiguredAWriteAndAUseDoWhatTheyDidOnTheBase: the expected values come from
// the same steps run on 13478814c, before the seal and the open moved into appkeys.
func (s *PGSealedSuite) TestWithNothingConfiguredAWriteAndAUseDoWhatTheyDidOnTheBase() {
	plain := s.credentialStore(s.v1)
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	before := s.stored(ref)
	opens, fallbacks := s.kekOpened(), s.sealFallbacks()

	s.Require().NoError(s.write(plain, ref, credentials(tokens{Access: "b", Refresh: "s"})))
	s.Equal(opens+1, s.kekOpened(), "the write opened the credentials it replaced")
	s.Equal(tokens{Access: "b", Refresh: "s"}, opened(s.held(plain, ref).Credentials))
	s.Equal(opens+2, s.kekOpened())
	s.Equal(fallbacks, s.sealFallbacks())
	stored := s.stored(ref)
	s.Equal(before.Revision+1, stored.Revision)
	s.Equal(1, stored.CredentialsKEKVersion)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal(store.ConnectionConnected, stored.Status)
	_, err := s.v1.OpenWithAADVersion(stored.CredentialsSealed, credentialsAAD(ref, stored.Revision), 1)
	s.Require().NoError(err, "a router without data keys opens what this build sealed")
	_, err = s.db.AppDataKeys.Get(s.ctx, ref.CustomerID, 1)
	s.ErrorIs(err, store.ErrNoAppDataKey)
}

// TestACallerThatGoesAwayDuringTheFirstWrapDoesNotDowngradeTheSeal: KMS is slow, not down, when
// the caller cancels its request during the wrap of the app's first key. The key is still made
// and the credentials go under it, with no fallback to the router's key.
func (s *PGSealedSuite) TestACallerThatGoesAwayDuringTheFirstWrapDoesNotDowngradeTheSeal() {
	kms := appkeystest.NewKMS()
	defer kms.Release()
	options := appkeystest.Options()
	options.Seal, options.KMSTimeout = true, 10*time.Second
	sealing := s.dataKeyStore(kms, options)
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	fallbacks := s.sealFallbacks()
	kms.Hold()

	ctx, cancel := context.WithCancel(s.ctx)
	go func() {
		s.Eventually(func() bool { return kms.Wraps() == 1 }, 5*time.Second, time.Millisecond)
		cancel()
		// KMS answers a moment after the caller went away.
		time.Sleep(20 * time.Millisecond)
		kms.Release()
	}()
	err := sealing.Update(ctx, ref, func(state *core.CredentialState, _ func() error) (bool, error) {
		state.Credentials = credentials(tokens{Access: "b", Refresh: "s"})
		return true, nil
	})
	s.Require().NoError(err)
	stored := s.stored(ref)
	s.Require().NotNil(stored.CredentialsDataKeyVersion, "the credentials went under the router's key")
	s.Equal(1, *stored.CredentialsDataKeyVersion)
	s.Equal(fallbacks, s.sealFallbacks(), "a caller's cancel counted as a KMS fallback")
	_, err = s.db.AppDataKeys.Get(s.ctx, ref.CustomerID, 1)
	s.NoError(err)
}

// TestASealThatFailsForAnotherReasonThanKMSFailsTheWrite: the app has a key, another open is
// unwrapping it, and the writer stops waiting. That is no KMS outage, so the write fails and the
// row stays as it was, rather than falling back to the router's key.
func (s *PGSealedSuite) TestASealThatFailsForAnotherReasonThanKMSFailsTheWrite() {
	kms := appkeystest.NewKMS()
	defer kms.Release()
	options := appkeystest.Options()
	options.Seal, options.KMSTimeout = true, 10*time.Second
	sealing := s.dataKeyStore(kms, options)
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	appkeystest.AddKey(s.T(), s.db, kms, ref.CustomerID, 1)
	before := s.stored(ref)
	fallbacks := s.sealFallbacks()
	kms.Hold()
	other := s.connected(ref.CustomerID, tokens{Access: "o", Refresh: "p"}, time.Time{})
	go func() { _ = s.write(sealing, other, credentials(tokens{Access: "x", Refresh: "y"})) }()
	s.Require().Eventually(func() bool { return kms.Unwraps() == 1 }, 5*time.Second, time.Millisecond)

	ctx, cancel := context.WithCancel(s.ctx)
	time.AfterFunc(50*time.Millisecond, cancel)
	err := sealing.Update(ctx, ref, func(state *core.CredentialState, _ func() error) (bool, error) {
		state.Credentials = credentials(tokens{Access: "b", Refresh: "s"})
		return true, nil
	})
	s.Require().ErrorIs(err, context.Canceled)
	s.Equal(before.Revision, s.stored(ref).Revision, "the row is left as it was")
	s.Equal(fallbacks, s.sealFallbacks())
}
