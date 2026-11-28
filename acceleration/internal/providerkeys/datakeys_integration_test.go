//go:build integration

package providerkeys

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// sealedOnBase is "sk-ant-api03-sealed-on-the-base-7Kq2", which Seal on the commit before data
// keys sealed for acme-app's anthropic key in the us, under "provider keys base probe" at version 1.
const sealedOnBase = "Rw5XdAJjNJWTR9QyghXgbmtCrIsX2ssY31lhkP+4uFpWPJxBU5reHSG2kKUlcxrHj2oatUpHAc6rLHqF5P+e9Q=="

// DataKeysSuite seals and opens provider keys under the apps' data keys, wrapped by an
// in-process KMS, in the database the router keeps them in.
type DataKeysSuite struct {
	suite.Suite
	ctx    context.Context
	db     *store.Store
	sealer *auth.Sealer
	kms    *appkeystest.KMS
	app    string
}

func TestDataKeysSuite(t *testing.T) {
	suite.Run(t, new(DataKeysSuite))
}

func (s *DataKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	db, err := store.Open(dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.ctx))
	s.db = db
	s.sealer, err = auth.NewSealerWithKeyring(1, map[int]string{1: "provider keys base probe"})
	s.Require().NoError(err)
}

func (s *DataKeysSuite) SetupTest() {
	s.app = "app-" + rand.Text()
	s.kms = appkeystest.NewKMS()
}

// keys are data keys wrapped by the suite's KMS, sealing new keys under them when seal is set.
func (s *DataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.db, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// put seals key as the app's anthropic key in the us with keys, and stores it as the API does.
func (s *DataKeysSuite) put(keys *appkeys.Keys, key string) store.ProviderKey {
	row, err := Seal(s.ctx, s.sealer, keys, s.app, "anthropic", "us", key)
	s.Require().NoError(err)
	_, _, err = s.db.ProviderKeys.Upsert(s.ctx, &row)
	s.Require().NoError(err)
	return s.stored()
}

// stored is the app's anthropic key in the us as the database keeps it.
func (s *DataKeysSuite) stored() store.ProviderKey {
	rows, err := s.db.ProviderKeys.SelectAll(s.ctx, s.app)
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	return rows[0]
}

func (s *DataKeysSuite) TestWithSealingOnAKeyIsSealedUnderTheAppsDataKeyAndOpensForItsRowAlone() {
	keys := s.keys(true)

	row := s.put(keys, key)

	s.Require().NotNil(row.DataKeyVersion)
	s.Equal(1, *row.DataKeyVersion)
	s.Zero(row.KEKVersion)
	_, err := s.sealer.OpenWithAADVersion(row.APIKeySealed, AAD(s.app, "anthropic", "us"), 1)
	s.Error(err, "the router's key does not open it")
	opened, kept, err := NewKeyring(s.db.ProviderKeys, s.sealer, keys).CustomerKey(s.ctx, s.app, "anthropic", "us")
	s.Require().NoError(err)
	s.True(kept)
	s.Equal(key, opened)
	s.Equal(row, s.stored(), "a key already under the data key is not sealed again on use")
	moved := row
	moved.Region = "eu"
	_, err = Open(s.ctx, s.sealer, keys, moved)
	s.ErrorIs(err, appkeys.ErrDoesNotOpen, "a key moved to another region's row")
	s.NotErrorIs(err, routing.ErrCustomerKeyUnavailable, "a blob that is not the row's is no KMS fault")
}

func (s *DataKeysSuite) TestAKeyUnderTheAppsDataKeyFailsAsUnavailableWhenKMSFailsOrNoKeysAreSet() {
	s.put(s.keys(true), key)
	s.kms.Fail(errors.New("kms: deadline exceeded on projects/p/locations/l/keyRings/r/cryptoKeys/k"))
	cold := s.keys(true)

	for name, keys := range map[string]*appkeys.Keys{"kms fails": cold, "no data keys": nil} {
		opened, kept, err := NewKeyring(s.db.ProviderKeys, s.sealer, keys).CustomerKey(s.ctx, s.app, "anthropic", "us")

		s.ErrorIs(err, routing.ErrCustomerKeyUnavailable, name)
		s.False(kept, name)
		s.Empty(opened, name)
	}
}

func (s *DataKeysSuite) TestAKeyUnderTheRoutersKeyIsResealedOnItsFirstUseAndCountsOneKEKOpen() {
	s.put(nil, key)
	keys := s.keys(true)
	keyring := NewKeyring(s.db.ProviderKeys, s.sealer, keys)
	// The label is written out, since dashboards and alerts read provider_key by name.
	before := appkeystest.KEKOpens(s.T(), "provider_key")

	first, _, err := keyring.CustomerKey(s.ctx, s.app, "anthropic", "us")
	s.Require().NoError(err)
	afterFirst := appkeystest.KEKOpens(s.T(), "provider_key")
	second, _, err := keyring.CustomerKey(s.ctx, s.app, "anthropic", "us")
	s.Require().NoError(err)

	s.Equal(key, first)
	s.Equal(key, second)
	s.Equal(before+1, afterFirst, "the first use opened under the router's key")
	s.Equal(afterFirst, appkeystest.KEKOpens(s.T(), "provider_key"), "the second use opened under the data key")
	row := s.stored()
	s.Require().NotNil(row.DataKeyVersion)
	s.Zero(row.KEKVersion)
}

func (s *DataKeysSuite) TestAKeyThatCannotBeResealedIsStillUsedAndLeftUnderTheRoutersKey() {
	before := s.put(nil, key)
	keys := s.keys(true)
	s.kms.Fail(errors.New("kms: unavailable"))

	opened, kept, err := NewKeyring(s.db.ProviderKeys, s.sealer, keys).CustomerKey(s.ctx, s.app, "anthropic", "us")

	s.Require().NoError(err)
	s.True(kept)
	s.Equal(key, opened)
	s.Equal(before, s.stored(), "the row is as it was")
}

func (s *DataKeysSuite) TestWithSealingTurnedOffAKeyUnderTheDataKeyStillOpensAndANewOneGoesUnderTheRoutersKey() {
	s.put(s.keys(true), key)
	off := s.keys(false)
	keyring := NewKeyring(s.db.ProviderKeys, s.sealer, off)

	opened, _, err := keyring.CustomerKey(s.ctx, s.app, "anthropic", "us")
	s.Require().NoError(err)
	s.Equal(key, opened)

	row := s.put(off, "sk-ant-api03-put-after-rollback-0000")
	s.Nil(row.DataKeyVersion)
	s.Equal(1, row.KEKVersion)
	before := appkeystest.KEKOpens(s.T(), Record)
	opened, _, err = keyring.CustomerKey(s.ctx, s.app, "anthropic", "us")
	s.Require().NoError(err)
	s.Equal("sk-ant-api03-put-after-rollback-0000", opened)
	s.Nil(s.stored().DataKeyVersion, "nothing is resealed while sealing is off")
	s.Equal(before+1, appkeystest.KEKOpens(s.T(), Record))
}

// TestWithNothingConfiguredAKeyIsSealedAndOpensAsOnTheBase: the expected values come from Seal
// and Open run on the commit before data keys (sealedOnBase).
func (s *DataKeysSuite) TestWithNothingConfiguredAKeyIsSealedAndOpensAsOnTheBase() {
	blob, err := base64.StdEncoding.DecodeString(sealedOnBase)
	s.Require().NoError(err)
	onBase := store.ProviderKey{CustomerID: "acme-app", Provider: "anthropic", Region: "us", APIKeySealed: blob, KEKVersion: 1}

	opened, err := Open(s.ctx, s.sealer, nil, onBase)
	s.Require().NoError(err)
	s.Equal("sk-ant-api03-sealed-on-the-base-7Kq2", opened)
	row, err := Seal(s.ctx, s.sealer, nil, "acme-app", "anthropic", "us", "sk-ant-api03-sealed-on-the-base-7Kq2")
	s.Require().NoError(err)
	s.Equal(1, row.KEKVersion)
	s.Equal("7Kq2", row.KeyHint)
	s.Nil(row.DataKeyVersion)
	reopened, err := s.sealer.OpenWithAADVersion(row.APIKeySealed, AAD("acme-app", "anthropic", "us"), 1)
	s.Require().NoError(err, "the base opens what is sealed now")
	s.Equal("sk-ant-api03-sealed-on-the-base-7Kq2", reopened)
	s.True(appkeystest.Served(s.T(), Record), "both counters serve provider_key from the start")
}
