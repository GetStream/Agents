//go:build integration

package channels

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// LineDataKeysSuite seals and opens lines' credentials under the apps' data keys, wrapped by an
// in-process KMS, and moves the ones under the router's key there on use.
type LineDataKeysSuite struct {
	suite.Suite
	ctx        context.Context
	store      *store.Store
	sealer     *auth.Sealer
	kms        *appkeystest.KMS
	customerID string
	// logged is what the services and the default logger, which Open logs to, wrote in the test.
	// The package's tests run one at a time, so no other test logs into it.
	logged *strings.Builder
}

func TestLineDataKeysSuite(t *testing.T) { suite.Run(t, new(LineDataKeysSuite)) }

func (s *LineDataKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "channels_datakeys"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.store = opened
	s.sealer, err = auth.NewSealerWithKeyring(1, map[int]string{1: "channel lines base probe"})
	s.Require().NoError(err)
}

func (s *LineDataKeysSuite) SetupTest() {
	s.customerID = "line-data-keys-" + rand.Text()
	s.kms = appkeystest.NewKMS()
	previous := slog.Default()
	s.T().Cleanup(func() { slog.SetDefault(previous) })
	s.logged = &strings.Builder{}
	slog.SetDefault(slog.New(slog.NewTextHandler(s.logged, nil)))
}

// keys are data keys wrapped by the suite's KMS, sealing new secrets under them when seal is
// set. Each call holds no key yet, as a router that just started.
func (s *LineDataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	return keys
}

func (s *LineDataKeysSuite) service(keys *appkeys.Keys) *Service {
	return &Service{store: s.store, secrets: s.sealer, dataKeys: keys, logger: slog.Default()}
}

// connected seals the credentials of a new SMS line with keys, as connecting a line does, and
// stores it.
func (s *LineDataKeysSuite) connected(keys *appkeys.Keys) store.ChannelAccount {
	account := store.ChannelAccount{CustomerID: s.customerID, Kind: string(SMS), E164: "+1555" + rand.Text()[:7],
		Token: "line-" + rand.Text()}
	s.Require().NoError(Seal(s.ctx, s.sealer, keys, &account, Secrets{Token: "telnyx-key", Signing: "a-public-key"}))
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &account))
	return account
}

func (s *LineDataKeysSuite) stored(account store.ChannelAccount) store.ChannelAccount {
	held, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	return held
}

// Catches a seal under the router's key, or under the data key without the line in its AAD: the
// router's key does not open the row, and the blob moved to another line of the app does not open.
func (s *LineDataKeysSuite) TestWithSealingOnALineIsSealedUnderTheAppsDataKeyAndOpensForItsLineAlone() {
	keys := s.keys(true)
	account := s.connected(keys)

	s.Require().NotNil(account.SecretsDataKeyVersion)
	s.Zero(account.SecretsKEKVersion)
	_, err := s.sealer.OpenWithAADVersion(account.SecretsSealed, []byte(s.customerID), 1)
	s.Error(err, "the router's key does not open it")
	line, _, err := s.service(keys).line(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)

	other := s.connected(keys)
	_, err = s.store.ChannelAccounts.Reseal(s.ctx, other, account.SecretsSealed, *account.SecretsDataKeyVersion)
	s.Require().NoError(err)
	_, _, err = s.service(keys).line(s.ctx, other.Token)
	s.ErrorIs(err, errUnopened)
	s.Equal(1, strings.Count(s.logged.String(), "could not open a channel line's credentials"), "the cause is logged once")
}

// Catches a change of the kind or of the row's part of the AAD, which would leave every line
// already sealed under a data key unopened.
func (s *LineDataKeysSuite) TestALineSealedWithTheRecordsAADUnderItsDataKeyOpens() {
	key := appkeystest.AddKey(s.T(), s.store, s.kms, s.customerID, 1)
	version := 1
	account := store.ChannelAccount{CustomerID: s.customerID, Kind: string(SMS), E164: "+15550100",
		SecretsDataKeyVersion: &version}
	account.SecretsSealed = appkeystest.Seal(s.T(), key, []byte(`{"token":"telnyx-key"}`),
		appkeystest.RecordAAD("channel_account", s.customerID, secretsAAD(s.customerID, "sms", "+15550100")))

	line, err := Open(s.ctx, s.sealer, s.keys(false), account)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
}

// AC: a KMS outage must not answer the provider 410, which stops its deliveries to a line that is
// fine. The answer is a 500 it retries, says nothing of the key, and the line stays as it was.
func (s *LineDataKeysSuite) TestADeliveryToALineWhoseDataKeyKMSCannotUnwrapIsA500AndTheLineStays() {
	account := s.connected(s.keys(true))
	restarted := s.service(s.keys(true))
	s.kms.Fail(errors.New("kms is down for the test"))

	answer := restarted.Receive(s.ctx, account.Token, http.Header{}, []byte(`{}`))

	s.Equal(http.StatusInternalServerError, answer.Status)
	s.Equal(failure(somethingWentWrong), answer.Body)
	s.Equal(account.SecretsSealed, s.stored(account).SecretsSealed)
	s.Nil(s.stored(account).DeletedAt)
	s.Equal(1, strings.Count(s.logged.String(), "could not open a channel line's credentials"), "the cause is logged once")
	s.NotContains(s.logged.String(), "could not read the line")
	s.Contains(s.logged.String(), "kms is down for the test")
	s.Contains(s.logged.String(), "account_id="+account.ID)
	s.kms.Fail(nil)
	line, _, err := restarted.line(s.ctx, account.Token)
	s.Require().NoError(err, "the line opens once KMS is back")
	s.Equal("telnyx-key", line.Token)
}

// Catches a line under the router's key that does not open being taken for a key fault: it is gone
// as before, so the provider stops delivering to it.
func (s *LineDataKeysSuite) TestADeliveryToALineUnderTheRoutersKeyThatDoesNotOpenIsStillGone() {
	account := store.ChannelAccount{CustomerID: s.customerID, Kind: string(SMS), E164: "+1555" + rand.Text()[:7],
		Token: "line-" + rand.Text(), SecretsSealed: []byte("not sealed by this router"), SecretsKEKVersion: 1}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &account))

	answer := s.service(s.keys(true)).Receive(s.ctx, account.Token, http.Header{}, []byte(`{}`))

	s.Equal(http.StatusGone, answer.Status)
}

// A router without data keys meets a line under one, as a rollback to a build without
// auth.app_keys_kms_key does. That is a setting to fix, not a line to drop.
func (s *LineDataKeysSuite) TestAWebhookCheckOfALineUnderADataKeyOnARouterWithoutDataKeysIsA500() {
	account := s.connected(s.keys(true))

	answer := s.service(nil).Challenge(s.ctx, account.Token, nil)

	s.Equal(http.StatusInternalServerError, answer.Status)
	s.Equal(failure(somethingWentWrong), answer.Body)
}

// Catches a scheduled delivery, which stores the error's text, being told the KMS cause or the
// setting.
func (s *LineDataKeysSuite) TestALineUnderADataKeyThatCannotOpenSaysNothingOfWhy() {
	account := s.connected(s.keys(true))

	_, err := Open(s.ctx, s.sealer, nil, account)

	s.ErrorIs(err, errKeysUnavailable)
	s.NotContains(err.Error(), "app_keys_kms_key")
	s.NotContains(err.Error(), "appkeys")
}

// AC: a line connected before data keys moves to the app's data key on its first use, counted
// once as an open under the router's key, and opens under the data key after that.
func (s *LineDataKeysSuite) TestALineUnderTheRoutersKeyIsResealedOnItsFirstUseAndCountsOneKEKOpen() {
	account := s.connected(nil)
	service := s.service(s.keys(true))
	before := appkeystest.KEKOpens(s.T(), "channel_account")

	line, _, err := service.line(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal(before+1, appkeystest.KEKOpens(s.T(), "channel_account"))
	moved := s.stored(account)
	s.Equal(new(1), moved.SecretsDataKeyVersion)
	s.Zero(moved.SecretsKEKVersion)

	line, _, err = service.line(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal("a-public-key", line.Signing)
	s.Equal(before+1, appkeystest.KEKOpens(s.T(), "channel_account"), "the second use opens under the data key")
	s.Equal(moved.SecretsSealed, s.stored(account).SecretsSealed, "a line under the data key is not sealed again")
}

// An older build reconnects a line under the router's key and leaves the data key version as it
// was. The row is under the router's key, so it opens there and then moves.
func (s *LineDataKeysSuite) TestALinePutByAnOlderBuildOverOneUnderTheDataKeyOpensAndMovesToTheDataKey() {
	account := s.connected(nil)
	_, err := s.store.ChannelAccounts.Reseal(s.ctx, account, account.SecretsSealed, 1)
	s.Require().NoError(err)
	older := s.stored(account)
	older.SecretsKEKVersion = 1
	_, err = s.store.GlobalDB().NewUpdate().Model(&older).Column("kek_version").WherePK().Exec(s.ctx)
	s.Require().NoError(err)

	line, _, err := s.service(s.keys(true)).line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Zero(s.stored(account).SecretsKEKVersion)
	s.NotEqual(account.SecretsSealed, s.stored(account).SecretsSealed)
}

// Catches a failed reseal failing the delivery: the credentials opened, so the line is answered
// and its row stays under the router's key.
func (s *LineDataKeysSuite) TestALineThatCannotBeResealedIsStillOpenedAndLeftUnderTheRoutersKey() {
	account := s.connected(nil)
	service := s.service(s.keys(true))
	s.kms.Fail(errors.New("kms is down for the test"))

	line, _, err := service.line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal(account.SecretsSealed, s.stored(account).SecretsSealed)
	s.Nil(s.stored(account).SecretsDataKeyVersion)
}

// A connect while KMS is down still stores the line, under the router's key and bound to the
// customer, and the fallback is counted under channel_account.
func (s *LineDataKeysSuite) TestALineIsSealedUnderTheRoutersKeyWhenKMSIsUnavailable() {
	keys := s.keys(true)
	s.kms.Fail(errors.New("kms is down for the test"))
	before := appkeystest.SealFallbacks(s.T(), "channel_account")

	account := s.connected(keys)

	s.Nil(account.SecretsDataKeyVersion)
	s.Equal(1, account.SecretsKEKVersion)
	_, err := s.sealer.OpenWithAADVersion(account.SecretsSealed, []byte(s.customerID), 1)
	s.NoError(err)
	s.Equal(before+1, appkeystest.SealFallbacks(s.T(), "channel_account"))
}

// Rollback: with sealing turned back off, a line under the data key still opens, a new line goes
// under the router's key, and nothing is resealed.
func (s *LineDataKeysSuite) TestWithSealingTurnedOffADataKeyLineStillOpensAndNothingIsResealed() {
	underDataKey := s.connected(s.keys(true))
	off := s.keys(false)
	underRouterKey := s.connected(off)

	for _, account := range []store.ChannelAccount{underDataKey, underRouterKey} {
		line, _, err := s.service(off).line(s.ctx, account.Token)
		s.Require().NoError(err)
		s.Equal("telnyx-key", line.Token)
		s.Equal(account.SecretsSealed, s.stored(account).SecretsSealed)
	}
	s.Nil(underRouterKey.SecretsDataKeyVersion)
	s.Equal(1, underRouterKey.SecretsKEKVersion)
}

// lineSealedOnBase is what the base (5373f2c97) stored for a line of app-base-probe: its API's
// SealWithAAD of the JSON, bound to the app, under the suite's keyring.
const lineSealedOnBase = "cb4b0feac724ed6503d2463c25a66c22acc3c17273a2d4d0e6797587ca8cb101befbbf3f51306cba50b49c80cb9dda629259dde737b3efb4fcbdcfa41d8630036a78fb3f3e31cc3a65c1b212f9b68b4ffda5829b64361d"

// Control: with nothing configured, a line the base sealed opens, and a new seal has the base's
// form, the router's key bound to the app alone.
func (s *LineDataKeysSuite) TestWithNothingConfiguredALineIsSealedAndOpensAsOnTheBase() {
	blob, err := hex.DecodeString(lineSealedOnBase)
	s.Require().NoError(err)
	line, err := Open(s.ctx, s.sealer, nil, store.ChannelAccount{CustomerID: "app-base-probe", Kind: "sms",
		E164: "+15550100", SecretsSealed: blob, SecretsKEKVersion: 1})
	s.Require().NoError(err)
	s.Equal("probe-line-token", line.Token)
	s.Equal("probe-line-signing", line.Signing)

	account := s.connected(nil)
	s.Nil(account.SecretsDataKeyVersion)
	s.Equal(1, account.SecretsKEKVersion)
	raw, err := s.sealer.OpenWithAADVersion(account.SecretsSealed, []byte(s.customerID), 1)
	s.Require().NoError(err)
	s.JSONEq(`{"token":"telnyx-key","signing":"a-public-key"}`, raw)
}

// Catches a seal error being dropped: a line of no app is not sealed.
func (s *LineDataKeysSuite) TestALineOfNoAppIsNotSealed() {
	account := store.ChannelAccount{Kind: string(SMS), E164: "+15550100"}

	s.Error(Seal(s.ctx, s.sealer, s.keys(true), &account, Secrets{Token: "telnyx-key"}))
	s.Nil(account.SecretsSealed)
}
