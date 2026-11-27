//go:build integration

package pgsealed

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
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
	keys, err := appkeys.New(s.db, kms)
	s.Require().NoError(err)
	credentialStore, err := New(s.db, s.v1, WithDataKeys(keys))
	s.Require().NoError(err)
	return credentialStore
}

// underDataKey seals ref's credentials again under a new data key of its app, at key version 0
// and data key version 1, and returns the blob.
func (s *PGSealedSuite) underDataKey(ref core.ConnectionRef, kms *appkeystest.KMS) []byte {
	connection := s.stored(ref)
	aad := credentialsAAD(ref, connection.Revision)
	plain, err := s.v1.OpenWithAADVersion(connection.CredentialsSealed, aad, connection.CredentialsKEKVersion)
	s.Require().NoError(err)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, kms, ref.CustomerID, 1), []byte(plain), aad)
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

// TestNewCredentialsOnADataKeyRowAreSealedUnderTheRoutersKey: nothing seals under a data key
// yet, so a write leaves a row that a router without data keys opens.
func (s *PGSealedSuite) TestNewCredentialsOnADataKeyRowAreSealedUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	s.underDataKey(ref, kms)

	s.Require().NoError(s.withDataKeys(kms).Update(s.ctx, ref, func(state *core.CredentialState, _ func() error) (bool, error) {
		state.Credentials = credentials(tokens{Access: "b", Refresh: "s"})
		return true, nil
	}))
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
	s.ErrorIs(err, errNoDataKeys)
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
