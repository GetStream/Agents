//go:build integration

package channels

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// LineSecretsSuite is how a line's sealed credentials are read back: bound to its customer, as
// lines are sealed, or bound to the line, as a later release seals them.
type LineSecretsSuite struct {
	suite.Suite
	ctx        context.Context
	store      *store.Store
	sealer     *auth.Sealer
	service    *Service
	customerID string
}

func TestLineSecretsSuite(t *testing.T) { suite.Run(t, new(LineSecretsSuite)) }

func (s *LineSecretsSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "channels_secrets"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.store = opened
	s.sealer, err = auth.NewSealer("line-secrets-kek")
	s.Require().NoError(err)
}

func (s *LineSecretsSuite) SetupTest() {
	s.customerID = "line-secrets-" + uuid.NewString()
	s.service = s.serviceSealedBy(s.sealer)
}

func (s *LineSecretsSuite) serviceSealedBy(sealer *auth.Sealer) *Service {
	return &Service{store: s.store, secrets: sealer, logger: slog.New(slog.DiscardHandler)}
}

// number is a line number no other test connects.
func (s *LineSecretsSuite) number() string {
	return "+1555" + uuid.NewString()[:7]
}

// connected stores the SMS line e164 holding sealed as its credentials and returns the row.
func (s *LineSecretsSuite) connected(e164 string, sealed []byte, version int) store.ChannelAccount {
	account := store.ChannelAccount{
		CustomerID: s.customerID, Kind: string(SMS), E164: e164,
		Token: "line-" + uuid.NewString(), SecretsSealed: sealed, SecretsKEKVersion: version,
	}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &account))
	return account
}

// legacy is a line's credentials sealed the way connect seals them: bound to the customer only.
func (s *LineSecretsSuite) legacy(sealer *auth.Sealer) []byte {
	sealed, err := sealer.SealWithAAD(`{"token":"telnyx-key","signing":"a-public-key"}`, []byte(s.customerID))
	s.Require().NoError(err)
	return sealed
}

func (s *LineSecretsSuite) stored(account store.ChannelAccount) store.ChannelAccount {
	held, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	return held
}

// bound is a line's credentials sealed bound to the line of customerID, kind and e164, as the
// release after this one seals them.
func (s *LineSecretsSuite) bound(customerID string, kind Kind, e164 string, credentials Secrets) []byte {
	raw, err := json.Marshal(credentials)
	s.Require().NoError(err)
	sealed, err := s.sealer.SealWithAAD(string(raw), secretsAAD(customerID, string(kind), e164))
	s.Require().NoError(err)
	return sealed
}

// Catches the line's own AAD naming only the customer, or a fallback that opens any blob of
// the customer's: credentials sealed for one line, put in another line's row, do not open.
func (s *LineSecretsSuite) TestCredentialsMovedFromAnotherLineDoNotOpen() {
	sealed := s.bound(s.customerID, SMS, s.number(), Secrets{Token: "telnyx-key"})
	second := s.connected(s.number(), sealed, s.sealer.CurrentVersion())

	_, _, err := s.service.line(s.ctx, second.Token)

	s.ErrorContains(err, "cannot be opened")
}

// Catches the kind or the customer missing from the line's AAD: the same number on another
// channel, or another app's line on the same number, is another line.
func (s *LineSecretsSuite) TestCredentialsSealedForTheSameNumberOnAnotherChannelOrAppDoNotOpen() {
	number := s.number()
	line := store.ChannelAccount{CustomerID: s.customerID, Kind: string(SMS), E164: number,
		SecretsKEKVersion: s.sealer.CurrentVersion()}

	line.SecretsSealed = s.bound(s.customerID, WhatsApp, number, Secrets{Token: "meta-token"})
	_, err := Open(s.sealer, line)
	s.Error(err, "a WhatsApp line's credentials on the SMS line of the same number")

	line.SecretsSealed = s.bound("another-app", SMS, number, Secrets{Token: "telnyx-key"})
	_, err = Open(s.sealer, line)
	s.Error(err, "another app's line of the same number")
}

// Catches a line the next release seals bound to itself not opening here, which would make
// that release impossible to roll back.
func (s *LineSecretsSuite) TestALineSealedBoundToItselfOpens() {
	number := s.number()
	account := s.connected(number, s.bound(s.customerID, SMS, number, Secrets{Token: "telnyx-key"}),
		s.sealer.CurrentVersion())

	line, _, err := s.service.line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
}

// Catches a line sealed bound only to its customer, as every line is, going dark or being
// written back on a read.
func (s *LineSecretsSuite) TestALineSealedBoundOnlyToItsCustomerOpensIsLeftAsItIs() {
	sealed := s.legacy(s.sealer)
	account := s.connected(s.number(), sealed, s.sealer.CurrentVersion())

	line, _, err := s.service.line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal("a-public-key", line.Signing)
	s.Equal(sealed, s.stored(account).SecretsSealed, "a read writes nothing")
}

// Catches a line bound only to its customer being opened under the current key rather than
// the one it was sealed under.
func (s *LineSecretsSuite) TestALegacyLineUnderAnOlderKeyOpensWithItsStoredVersion() {
	before, err := auth.NewSealer("old-key")
	s.Require().NoError(err)
	account := s.connected(s.number(), s.legacy(before), 1)
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)

	line, _, err := s.serviceSealedBy(rotated).line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal(1, s.stored(account).SecretsKEKVersion)
}

// Catches the parts of a line's AAD running together: a colon inside one part cannot make two
// lines give the same bytes.
func (s *LineSecretsSuite) TestTheLinesAADPartsDoNotRunTogether() {
	s.NotEqual(secretsAAD("app:sms", "whatsapp", "+15550100"), secretsAAD("app", "sms:whatsapp", "+15550100"))
	s.NotEqual(secretsAAD("app", "sms:+1555", "0100"), secretsAAD("app", "sms", "+1555:0100"))
}
