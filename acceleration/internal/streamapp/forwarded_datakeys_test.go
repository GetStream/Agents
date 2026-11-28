//go:build integration

package streamapp

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// baseProbeBlobHex is what Keep on 5373f2c97, the commit before data keys, stored for app
// baseProbeApp: the bytes of baseProbeToken sealed under the suite's keyring, written in hex.
const (
	baseProbeBlobHex = "3e05611b0303121a886eedea419cfc8166ce1e253210931b66dc40611f0dee9e" +
		"54a3b4d9ca006fe53a75b6b8bf988bd1149dd23f80af30a7b335ee74d9a47c80" +
		"c87f0784aec16a98b2f6f49cafc8a1b973217d33462bd34913d2e442337f13a0" +
		"5f6ae68daf123526a3cb8e03ad102b321223a8f865d6931c3e8e3902ad4ec12a" +
		"52d9ac52f6"
	baseProbeApp = 4336000001
)

// tokenKind is the record label of the counters, written out so a rename fails here.
const tokenKind = "stream_app_token"

// baseProbeToken is the token the base probe kept: it expires two days after the suite's clock
// starts, and HS256 signs the same claims to the same bytes every time.
func (s *StoredSuite) baseProbeToken() string {
	return signed(jwt.MapClaims{"exp": time.Unix(1_700_000_000, 0).Add(48 * time.Hour).Unix()})
}

// dataKeys are data keys wrapped by kms, sealing new tokens under them when seal is set.
func (s *StoredSuite) dataKeys(kms *appkeystest.KMS, seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	return keys
}

// replica is a router of its own that keeps and opens tokens with keys, holding none in memory.
func (s *StoredSuite) replica(keys *appkeys.Keys) *Forwarded {
	forwarded, err := NewForwarded(ForwardedOptions{Store: s.store, Sealer: s.sealer, Keys: keys, BaseURL: s.stream.URL,
		Now: s.clock.Now})
	s.Require().NoError(err)
	return forwarded
}

// tokenOf is the token a replica with keys acts in the test's app with.
func (s *StoredSuite) tokenOf(keys *appkeys.Keys) (string, error) {
	identity, err := s.forwardedSource(s.replica(keys)).For(s.ctx, s.customer)
	return identity.Token.Reveal(), err
}

func (s *StoredSuite) row(app int64) store.StreamAppToken {
	row, err := s.store.StreamAppTokens.Get(s.ctx, app)
	s.Require().NoError(err)
	return row
}

// tokenAAD is the row AAD of the test's app's token, as forwarded.go binds it.
func (s *StoredSuite) tokenAAD() []byte {
	return boundTo(tokenPurpose, s.customer, s.app, forwardedKey)
}

// underDataKey writes the test's app a token sealed under version 1 of its data key, with the
// data key AAD laid out by hand, so a change to the layout that rows already sealed cannot
// follow fails here.
func (s *StoredSuite) underDataKey(kms *appkeystest.KMS, token string) {
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)
	sealed := appkeystest.Seal(s.T(), key, []byte(token), appkeystest.RecordAAD(tokenKind, s.customer, s.tokenAAD()))
	_, err := s.store.StreamAppTokens.Keep(s.ctx, &store.StreamAppToken{StreamAppPK: s.app, CustomerID: s.customer,
		APIKey: forwardedKey, Sealed: sealed, DataKeyVersion: new(1), ExpiresAt: s.clock.Now().Add(24 * time.Hour)})
	s.Require().NoError(err)
}

// logged collects what the package's loggers write while the test runs.
type logged struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logged) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logged) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (s *StoredSuite) TestWithSealingOnAKeptTokenIsSealedUnderTheAppsDataKeyAndAnAppActsWithIt() {
	var authorization string
	recording := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{}`))
	}))
	s.T().Cleanup(recording.Close)
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	token := s.serverToken(24 * time.Hour)

	kept, err := s.replica(keys).Keep(s.ctx, s.customer, forwardedKey, token)

	s.Require().NoError(err)
	s.True(kept)
	row := s.row(s.app)
	s.Equal(new(1), row.DataKeyVersion)
	s.Zero(row.KEKVersion)
	_, err = s.sealer.OpenWithAADVersion(row.Sealed, s.tokenAAD(), 1)
	s.Error(err, "the router's key does not open it")
	opened, err := keys.Open(s.ctx, s.customer, 1, row.Sealed, appkeystest.RecordAAD(tokenKind, s.customer, s.tokenAAD()))
	s.Require().NoError(err)
	s.Equal(token, string(opened), "the token is bound to its kind, its app and its row")

	other, err := NewForwarded(ForwardedOptions{Store: s.store, Sealer: s.sealer, Keys: s.dataKeys(kms, true),
		BaseURL: recording.URL, Now: s.clock.Now})
	s.Require().NoError(err)
	before := appkeystest.KEKOpens(s.T(), tokenKind)
	bound, err := NewClients(s.forwardedSource(other), ClientsOptions{Now: s.clock.Now}).For(s.ctx, s.customer)
	s.Require().NoError(err)
	_, _ = bound.Client.GetApp(s.ctx, nil)
	s.Equal(token, authorization, "another replica acts in the app with the token under the data key")
	s.Equal(before, appkeystest.KEKOpens(s.T(), tokenKind), "an open under the data key is no KEK open")
	s.Equal(row.Sealed, s.row(s.app).Sealed, "a token under the data key is not sealed again")
}

func (s *StoredSuite) TestATokenWrittenUnderTheAppsDataKeyOpensWithTheRecordLayout() {
	kms := appkeystest.NewKMS()
	token := s.serverToken(24 * time.Hour)
	s.underDataKey(kms, token)

	opened, err := s.tokenOf(s.dataKeys(kms, true))

	s.Require().NoError(err)
	s.Equal(token, opened)
}

func (s *StoredSuite) TestATokenKeptWhileKMSIsUnavailableIsSealedUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	kms.Fail(errors.New("kms: unavailable"))
	before := appkeystest.SealFallbacks(s.T(), tokenKind)
	token := s.serverToken(24 * time.Hour)

	_, err := s.replica(s.dataKeys(kms, true)).Keep(s.ctx, s.customer, forwardedKey, token)

	s.Require().NoError(err)
	row := s.row(s.app)
	s.Nil(row.DataKeyVersion)
	s.Equal(1, row.KEKVersion)
	opened, err := s.sealer.OpenWithAADVersion(row.Sealed, s.tokenAAD(), 1)
	s.Require().NoError(err)
	s.Equal(token, opened)
	s.Equal(before+1, appkeystest.SealFallbacks(s.T(), tokenKind))
}

func (s *StoredSuite) TestATokenUnderTheRoutersKeyIsResealedOnItsFirstUseAndCountsOneKEKOpen() {
	token := s.serverToken(24 * time.Hour)
	_, err := s.replica(nil).Keep(s.ctx, s.customer, forwardedKey, token)
	s.Require().NoError(err)
	before := s.row(s.app)
	kms := appkeystest.NewKMS()
	opens := appkeystest.KEKOpens(s.T(), tokenKind)

	first, err := s.tokenOf(s.dataKeys(kms, true))

	s.Require().NoError(err)
	s.Equal(token, first)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), tokenKind))
	after := s.row(s.app)
	s.Equal(new(1), after.DataKeyVersion, "the token moved to the app's data key on its use")
	s.Zero(after.KEKVersion)
	s.NotEqual(before.Sealed, after.Sealed)
	s.Equal(before.ExpiresAt, after.ExpiresAt)
	second, err := s.tokenOf(s.dataKeys(kms, true))
	s.Require().NoError(err)
	s.Equal(token, second, "another replica opens the resealed token")
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), tokenKind), "the token counted one KEK open, on its first use")
}

func (s *StoredSuite) TestATokenThatCannotBeResealedIsStillUsedAndLeftUnderTheRoutersKey() {
	token := s.serverToken(24 * time.Hour)
	_, err := s.replica(nil).Keep(s.ctx, s.customer, forwardedKey, token)
	s.Require().NoError(err)
	before := s.row(s.app)
	kms := appkeystest.NewKMS()
	kms.Fail(errors.New("kms: unavailable"))

	opened, err := s.tokenOf(s.dataKeys(kms, true))

	s.Require().NoError(err)
	s.Equal(token, opened)
	s.Equal(before, s.row(s.app))
}

func (s *StoredSuite) TestATokenUnderTheAppsDataKeyThatDoesNotOpenFailsWithoutSayingWhy() {
	kms := appkeystest.NewKMS()
	s.underDataKey(kms, s.serverToken(24*time.Hour))
	failing := appkeystest.NewKMS()
	failing.Fail(errors.New("kms: projects/p/locations/l/keyRings/r/cryptoKeys/k is unavailable"))
	var logs logged
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	s.T().Cleanup(func() { slog.SetDefault(previous) })

	for name, keys := range map[string]*appkeys.Keys{
		"no data keys":    nil,
		"KMS unavailable": s.dataKeys(failing, true),
	} {
		_, err := s.tokenOf(keys)

		s.ErrorIs(err, errTokenDoesNotOpen, name)
		s.ErrorContains(err, "see the router log for the cause", name)
		s.NotErrorIs(err, ErrNoIdentity, "%s: an app with a token is not one with none", name)
		s.NotContains(err.Error(), "app_keys_kms_key", name)
		s.NotContains(err.Error(), "cryptoKeys", name)
	}
	s.Contains(logs.String(), appkeys.ErrNoKeys.Error(), "the cause goes to the log")
	s.Contains(logs.String(), "cryptoKeys/k")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_app_tokens SET token_data_key_version = 2 WHERE stream_app_pk = ?", s.app)
	s.Require().NoError(err)
	_, err = s.tokenOf(s.dataKeys(kms, true))
	s.ErrorIs(err, errTokenDoesNotOpen, "a row that names a data key the app lacks does not open")
}

func (s *StoredSuite) TestATokenMovedToAnotherAppDoesNotOpen() {
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	_, err := s.replica(keys).Keep(s.ctx, s.customer, forwardedKey, s.serverToken(24*time.Hour))
	s.Require().NoError(err)
	sealed := s.row(s.app).Sealed
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_app_tokens SET api_key = 'another-key' WHERE stream_app_pk = ?", s.app)
	s.Require().NoError(err)

	_, err = s.tokenOf(keys)

	s.ErrorIs(err, errTokenDoesNotOpen, "a token is bound to the key it was kept with")
	s.Equal(sealed, s.row(s.app).Sealed)
}

func (s *StoredSuite) TestATokenAnOlderBuildKeptOverOneUnderTheDataKeyOpensAndMovesToTheDataKey() {
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	_, err := s.replica(keys).Keep(s.ctx, s.customer, forwardedKey, s.serverToken(24*time.Hour))
	s.Require().NoError(err)
	// A build from before data keys keeps a newer token without token_data_key_version, so the row
	// keeps the old version beside a kek_version.
	newer := s.serverToken(48 * time.Hour)
	sealed, err := s.sealer.SealWithAAD(newer, s.tokenAAD())
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_app_tokens SET token_sealed = ?, kek_version = 1, "+
		"expires_at = ? WHERE stream_app_pk = ?", sealed, s.clock.Now().Add(48*time.Hour), s.app)
	s.Require().NoError(err)
	opens := appkeystest.KEKOpens(s.T(), tokenKind)

	opened, err := s.tokenOf(keys)

	s.Require().NoError(err)
	s.Equal(newer, opened)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), tokenKind))
	after := s.row(s.app)
	s.Zero(after.KEKVersion, "the token moved to the data key")
	s.Equal(new(1), after.DataKeyVersion)
}

func (s *StoredSuite) TestWithSealingTurnedOffATokenUnderTheDataKeyStillOpensAndNothingIsResealed() {
	kms := appkeystest.NewKMS()
	token := s.serverToken(24 * time.Hour)
	_, err := s.replica(s.dataKeys(kms, true)).Keep(s.ctx, s.customer, forwardedKey, token)
	s.Require().NoError(err)
	off := s.dataKeys(kms, false)

	opened, err := s.tokenOf(off)

	s.Require().NoError(err)
	s.Equal(token, opened, "a rollback of the setting still opens tokens under the data key")
	newer := s.serverToken(48 * time.Hour)
	_, err = s.replica(off).Keep(s.ctx, s.customer, forwardedKey, newer)
	s.Require().NoError(err)
	row := s.row(s.app)
	s.Nil(row.DataKeyVersion, "a new token goes under the router's key")
	s.Equal(1, row.KEKVersion)
	opened, err = s.tokenOf(off)
	s.Require().NoError(err)
	s.Equal(newer, opened)
	s.Equal(row, s.row(s.app), "with sealing off a token under the router's key stays there")
}

func (s *StoredSuite) TestWithNothingConfiguredATokenIsSealedAndOpensAsOnTheBase() {
	s.app, s.customer = baseProbeApp, CustomerOf(baseProbeApp)
	sealed, err := hex.DecodeString(baseProbeBlobHex)
	s.Require().NoError(err)
	_, err = s.store.StreamAppTokens.Keep(s.ctx, &store.StreamAppToken{StreamAppPK: s.app, CustomerID: s.customer,
		APIKey: forwardedKey, Sealed: sealed, KEKVersion: 1, ExpiresAt: s.clock.Now().Add(48 * time.Hour)})
	s.Require().NoError(err)
	opens := appkeystest.KEKOpens(s.T(), tokenKind)

	opened, err := s.tokenOf(nil)

	s.Require().NoError(err)
	s.Equal(s.baseProbeToken(), opened, "a token the base kept opens")
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), tokenKind))
	row := s.row(s.app)
	s.Equal(sealed, row.Sealed, "nothing is resealed")
	s.Nil(row.DataKeyVersion)

	newer := s.serverToken(72 * time.Hour)
	_, err = s.replica(nil).Keep(s.ctx, s.customer, forwardedKey, newer)
	s.Require().NoError(err)
	row = s.row(s.app)
	s.Equal(1, row.KEKVersion, "a new token is sealed as the base sealed one")
	s.Nil(row.DataKeyVersion)
	again, err := s.sealer.OpenWithAADVersion(row.Sealed, s.tokenAAD(), 1)
	s.Require().NoError(err)
	s.Equal(newer, again, "the base's open, with the base's AAD, opens it")
}

func (s *StoredSuite) TestATokenSealedUnderTheRoutersKeyRecordsTheKeyVersionThatSealedIt() {
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "first-key", 2: "second-key"})
	s.Require().NoError(err)
	failing := appkeystest.NewKMS()
	failing.Fail(errors.New("kms: unavailable"))
	for i, c := range []struct {
		name string
		keys *appkeys.Keys
	}{
		{"no data keys", nil},
		{"sealing off", s.dataKeys(appkeystest.NewKMS(), false)},
		{"KMS unavailable", s.dataKeys(failing, true)},
	} {
		s.app += int64(i+1) * 7919
		s.customer = strconv.FormatInt(s.app, 10)
		token := s.serverToken(24 * time.Hour)
		options := ForwardedOptions{Store: s.store, Sealer: rotated, Keys: c.keys, BaseURL: s.stream.URL, Now: s.clock.Now}
		keeper, err := NewForwarded(options)
		s.Require().NoError(err, c.name)

		_, err = keeper.Keep(s.ctx, s.customer, forwardedKey, token)

		s.Require().NoError(err, c.name)
		s.Equal(2, s.row(s.app).KEKVersion, "%s: the current version of the keyring sealed it", c.name)
		other, err := NewForwarded(options)
		s.Require().NoError(err, c.name)
		identity, err := s.forwardedSource(other).For(s.ctx, s.customer)
		s.Require().NoError(err, "%s: another replica opens it", c.name)
		s.Equal(token, identity.Token.Reveal(), c.name)
	}
}

func (s *StoredSuite) TestAResealTheStoreRefusesIsLoggedWithItsCause() {
	token := s.serverToken(24 * time.Hour)
	_, err := s.replica(nil).Keep(s.ctx, s.customer, forwardedKey, token)
	s.Require().NoError(err)
	before := s.row(s.app)
	keys := s.dataKeys(appkeystest.NewKMS(), true)
	_, _, err = keys.Seal(s.ctx, s.customer, []byte("warms the data key"), nil)
	s.Require().NoError(err)
	var logs logged
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	s.T().Cleanup(func() { slog.SetDefault(previous) })
	// The first time the replica reads the clock is right after it read the row, so the
	// context ends when the token is open and the reseal is next.
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	forwarded, err := NewForwarded(ForwardedOptions{Store: s.store, Sealer: s.sealer, Keys: keys, BaseURL: s.stream.URL,
		Now: func() time.Time {
			cancel()
			return s.clock.Now()
		}})
	s.Require().NoError(err)

	identity, err := s.forwardedSource(forwarded).For(ctx, s.customer)

	s.Require().NoError(err)
	s.Equal(token, identity.Token.Reveal(), "the token opened, so it is used")
	s.Equal(before, s.row(s.app))
	s.Contains(logs.String(), "left an app's token under the router's key")
	s.Contains(logs.String(), context.Canceled.Error(), "the warning carries the store's error")
}
