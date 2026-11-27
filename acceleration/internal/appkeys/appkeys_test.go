//go:build integration

package appkeys_test

import (
	"crypto/rand"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// AppKeysSuite opens secrets under data keys stored in Postgres and wrapped by a fake KMS. Each
// test uses apps of its own, so nothing is emptied between tests.
type AppKeysSuite struct {
	suite.Suite
	dsn  string
	db   *store.Store
	kms  *appkeystest.KMS
	keys *appkeys.Keys
}

func TestAppKeysSuite(t *testing.T) {
	suite.Run(t, new(AppKeysSuite))
}

func (s *AppKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.dsn = testenv.Database(dsn, "appkeys")
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.T().Context()))
	s.db = db
}

func (s *AppKeysSuite) SetupTest() {
	s.kms = appkeystest.NewKMS()
	keys, err := appkeys.New(s.db, s.kms)
	s.Require().NoError(err)
	s.keys = keys
}

func (s *AppKeysSuite) TestASecretSealedUnderTheAppsKeyOpensWithOneUnwrap() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))

	opened, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	s.Equal(1, s.kms.Unwraps())
}

func (s *AppKeysSuite) TestASecretUnderAnotherAADDoesNotOpen() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))

	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("another row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestABlobShorterThanANonceDoesNotOpen() {
	app := "app-" + rand.Text()
	appkeystest.AddKey(s.T(), s.db, s.kms, app, 1)

	_, err := s.keys.Open(s.T().Context(), app, 1, []byte("short"), []byte("row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
}

// TestASecretOfAnAppWithoutThatKeyDoesNotOpen: neither the app's other version nor another
// app's key stands in for the one missing.
func (s *AppKeysSuite) TestASecretOfAnAppWithoutThatKeyDoesNotOpen() {
	app, other := "app-"+rand.Text(), "app-"+rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))
	appkeystest.AddKey(s.T(), s.db, s.kms, other, 2)

	for _, missing := range []struct {
		app     string
		version int
	}{{app, 2}, {"app-" + rand.Text(), 1}} {
		_, err := s.keys.Open(s.T().Context(), missing.app, missing.version, sealed, []byte("row"))
		s.ErrorIs(err, appkeys.ErrDoesNotOpen)
		s.ErrorIs(err, store.ErrNoAppDataKey)
	}
	s.Zero(s.kms.Unwraps(), "no key, no KMS call")
}

func (s *AppKeysSuite) TestADatabaseFailureIsNeitherAnUnreadableSecretNorAKMSFault() {
	closed, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	keys, err := appkeys.New(closed, s.kms)
	s.Require().NoError(err)

	_, err = keys.Open(s.T().Context(), "app-"+rand.Text(), 1, make([]byte, 40), []byte("row"))
	s.Require().Error(err)
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

// TestAWrappedKeyCopiedToAnotherAppDoesNotUnwrap: the wrap AAD names the app, so KMS refuses a
// key moved to another app's row, and nothing opens with it there.
func (s *AppKeysSuite) TestAWrappedKeyCopiedToAnotherAppDoesNotUnwrap() {
	mine, theirs := "app-"+rand.Text(), "app-"+rand.Text()
	key := appkeystest.AddKey(s.T(), s.db, s.kms, theirs, 1)
	appkeystest.AddKey(s.T(), s.db, s.kms, mine, 1)
	s.copyWrappedKey(theirs, 1, mine, 1)

	_, err := s.keys.Open(s.T().Context(), mine, 1, appkeystest.Seal(s.T(), key, []byte("secret"), []byte("row")), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
}

// TestAWrappedKeyCopiedToAnotherVersionDoesNotUnwrap: the wrap AAD names the version too.
func (s *AppKeysSuite) TestAWrappedKeyCopiedToAnotherVersionDoesNotUnwrap() {
	app := "app-" + rand.Text()
	key := appkeystest.AddKey(s.T(), s.db, s.kms, app, 1)
	appkeystest.AddKey(s.T(), s.db, s.kms, app, 2)
	s.copyWrappedKey(app, 1, app, 2)

	_, err := s.keys.Open(s.T().Context(), app, 2, appkeystest.Seal(s.T(), key, []byte("secret"), []byte("row")), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestAKMSFailureIsUnavailableAndKeepsTheKMSError() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))
	failure := errors.New("kms: deadline exceeded")
	s.kms.Fail(failure)

	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, failure, "the KMS error is kept for the log")
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
}

// TestAWrappedValueThatIsNoAESKeyIsAFaultOfTheKeys: what KMS unwrapped is not a key, so the
// fault is in the keys, and a caller must not take the secret for unreadable.
func (s *AppKeysSuite) TestAWrappedValueThatIsNoAESKeyIsAFaultOfTheKeys() {
	app := "app-" + rand.Text()
	s.Require().NoError(s.db.AppDataKeys.Insert(s.T().Context(), &store.AppDataKey{
		CustomerID: app, Version: 1, WrappedKey: s.kms.Wrap([]byte("seven b"), appkeystest.WrapAAD(app, 1)),
	}))

	_, err := s.keys.Open(s.T().Context(), app, 1, make([]byte, 40), []byte("row"))
	s.Require().Error(err)
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestNewNeedsAStoreAndAKMS() {
	_, err := appkeys.New(nil, s.kms)
	s.Error(err)
	_, err = appkeys.New(s.db, nil)
	s.Error(err)
}

// copyWrappedKey puts one stored wrapped key in place of another, as somebody with write
// access to the table could.
func (s *AppKeysSuite) copyWrappedKey(fromApp string, fromVersion int, toApp string, toVersion int) {
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(), "UPDATE app_data_keys SET wrapped_key ="+
		" (SELECT wrapped_key FROM app_data_keys WHERE customer_id = ? AND version = ?) WHERE customer_id = ? AND version = ?",
		fromApp, fromVersion, toApp, toVersion)
	s.Require().NoError(err)
}
