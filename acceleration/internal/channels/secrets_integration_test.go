//go:build integration

package channels

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// LineSecretsSuite is how a line's sealed credentials are read back: bound to the line, and
// still readable from a line sealed before they were, which is then sealed again bound to it.
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

// legacy is a line's credentials sealed the way lines were before they were bound to the
// line: bound to the customer only.
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

// Catches the line's own AAD naming only the customer, or a fallback that opens any blob of
// the customer's: credentials sealed for one line, put in another line's row, do not open.
func (s *LineSecretsSuite) TestCredentialsMovedFromAnotherLineDoNotOpen() {
	sealed, err := Seal(s.sealer, s.customerID, string(SMS), s.number(), Secrets{Token: "telnyx-key"})
	s.Require().NoError(err)
	second := s.connected(s.number(), sealed, s.sealer.CurrentVersion())

	_, _, err = s.service.line(s.ctx, second.Token)

	s.ErrorContains(err, "cannot be opened")
	s.Equal(sealed, s.stored(second).SecretsSealed, "nothing was sealed again from a blob that did not open")
}

// Catches the kind or the customer missing from the line's AAD: the same number on another
// channel, or another app's line on the same number, is another line.
func (s *LineSecretsSuite) TestCredentialsSealedForTheSameNumberOnAnotherChannelOrAppDoNotOpen() {
	number := s.number()
	line := store.ChannelAccount{CustomerID: s.customerID, Kind: string(SMS), E164: number,
		SecretsKEKVersion: s.sealer.CurrentVersion()}
	var err error

	line.SecretsSealed, err = Seal(s.sealer, s.customerID, string(WhatsApp), number, Secrets{Token: "meta-token"})
	s.Require().NoError(err)
	_, _, err = unseal(s.sealer, line)
	s.Error(err, "a WhatsApp line's credentials on the SMS line of the same number")

	line.SecretsSealed, err = Seal(s.sealer, "another-app", string(SMS), number, Secrets{Token: "telnyx-key"})
	s.Require().NoError(err)
	_, _, err = unseal(s.sealer, line)
	s.Error(err, "another app's line of the same number")
}

// Catches a line sealed bound to the line not opening, or being sealed again on every read.
func (s *LineSecretsSuite) TestALineSealedBoundToItselfOpensAndIsLeftAsItIs() {
	number := s.number()
	sealed, err := Seal(s.sealer, s.customerID, string(SMS), number, Secrets{Token: "telnyx-key"})
	s.Require().NoError(err)
	account := s.connected(number, sealed, s.sealer.CurrentVersion())

	line, _, err := s.service.line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal(sealed, s.stored(account).SecretsSealed)
}

// Catches a line connected before the binding going dark, or staying in the old form: it
// opens, and the row then holds its credentials bound to the line and nothing else.
func (s *LineSecretsSuite) TestALineSealedBoundOnlyToItsCustomerOpensAndIsSealedAgainBoundToTheLine() {
	account := s.connected(s.number(), s.legacy(s.sealer), s.sealer.CurrentVersion())

	line, _, err := s.service.line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	s.Equal("a-public-key", line.Signing)
	held := s.stored(account)
	_, err = s.sealer.OpenWithAADVersion(held.SecretsSealed, []byte(s.customerID), held.SecretsKEKVersion)
	s.Error(err, "the customer-only form is gone from the row")
	opened, legacy, err := unseal(s.sealer, held)
	s.Require().NoError(err)
	s.False(legacy)
	s.Equal(line, opened)
}

// Catches the old form being opened under the current key rather than the stored one, or
// sealed again under the old one: a rotation and the binding land on the same row.
func (s *LineSecretsSuite) TestALegacyLineUnderAnOlderKeyOpensAndIsSealedAgainUnderTheCurrentOne() {
	before, err := auth.NewSealer("old-key")
	s.Require().NoError(err)
	account := s.connected(s.number(), s.legacy(before), 1)
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)

	line, _, err := s.serviceSealedBy(rotated).line(s.ctx, account.Token)

	s.Require().NoError(err)
	s.Equal("telnyx-key", line.Token)
	held := s.stored(account)
	s.Equal(2, held.SecretsKEKVersion)
	_, legacy, err := unseal(rotated, held)
	s.Require().NoError(err)
	s.False(legacy)
}
