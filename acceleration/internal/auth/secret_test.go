package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

// KeyringSuite covers a sealer holding more than one key encryption key, which is how a
// key is rotated without reissuing what it sealed.
type KeyringSuite struct {
	suite.Suite
	record []byte
}

func TestKeyringSuite(t *testing.T) {
	suite.Run(t, new(KeyringSuite))
}

func (s *KeyringSuite) SetupTest() {
	s.record = []byte("tenant/connection/revision")
}

func (s *KeyringSuite) TestARowSealedUnderVersionOneOpensAfterVersionTwoIsAdded() {
	before, err := NewSealer("old-key")
	s.Require().NoError(err)
	sealed, err := before.SealWithAAD("connector secret", s.record)
	s.Require().NoError(err)

	rotated, err := NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)
	opened, err := rotated.OpenWithAADVersion(sealed, s.record, 1)
	s.Require().NoError(err)
	s.Equal("connector secret", opened)
}

func (s *KeyringSuite) TestNewRowsAreSealedUnderTheCurrentVersion() {
	rotated, err := NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)
	s.Equal(2, rotated.CurrentVersion())

	sealed, err := rotated.SealWithAAD("rotated secret", s.record)
	s.Require().NoError(err)
	opened, err := rotated.OpenWithAADVersion(sealed, s.record, 2)
	s.Require().NoError(err)
	s.Equal("rotated secret", opened)

	_, err = rotated.OpenWithAADVersion(sealed, s.record, 1)
	s.Error(err)
}

func (s *KeyringSuite) TestAWrongAADFailsToOpen() {
	sealer, err := NewSealer("a passphrase")
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD("connector secret", s.record)
	s.Require().NoError(err)

	_, err = sealer.OpenWithAADVersion(sealed, []byte("tenant/another-connection/revision"), 1)
	s.Error(err)
	_, err = sealer.Open(sealed)
	s.Error(err, "a secret bound to a record does not open without it")
}

func (s *KeyringSuite) TestARecordSealedBoundToItsRowOpensAsNotLegacy() {
	sealer, err := NewSealer("a passphrase")
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD("line secret", s.record)
	s.Require().NoError(err)
	before := testutil.ToFloat64(legacyOpens.WithLabelValues("bound-record"))

	opened, legacy, err := sealer.OpenWithLegacyAAD(sealed, s.record, []byte("tenant"), 1, "bound-record")
	s.Require().NoError(err)
	s.Equal("line secret", opened)
	s.False(legacy)
	s.InDelta(before, testutil.ToFloat64(legacyOpens.WithLabelValues("bound-record")), 0)
}

func (s *KeyringSuite) TestARecordSealedBoundOnlyToItsCustomerOpensAsLegacyAndIsCounted() {
	sealer, err := NewSealer("a passphrase")
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD("line secret", []byte("tenant"))
	s.Require().NoError(err)
	before := testutil.ToFloat64(legacyOpens.WithLabelValues("legacy-record"))

	opened, legacy, err := sealer.OpenWithLegacyAAD(sealed, s.record, []byte("tenant"), 1, "legacy-record")
	s.Require().NoError(err)
	s.Equal("line secret", opened)
	s.True(legacy)
	s.InDelta(before+1, testutil.ToFloat64(legacyOpens.WithLabelValues("legacy-record")), 0)
}

func (s *KeyringSuite) TestARecordOpeningUnderNeitherFailsWithTheErrorOfTheCurrentForm() {
	sealer, err := NewSealer("a passphrase")
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD("line secret", []byte("tenant/another-row"))
	s.Require().NoError(err)

	opened, legacy, err := sealer.OpenWithLegacyAAD(sealed, s.record, []byte("tenant"), 1, "moved-record")
	s.ErrorContains(err, "auth: open secret")
	s.Empty(opened)
	s.False(legacy)
	s.InDelta(0, testutil.ToFloat64(legacyOpens.WithLabelValues("moved-record")), 0)
}

func (s *KeyringSuite) TestALegacyRecordSealedUnderAnOlderKeyOpensWithItsStoredVersion() {
	before, err := NewSealer("old-key")
	s.Require().NoError(err)
	sealed, err := before.SealWithAAD("line secret", []byte("tenant"))
	s.Require().NoError(err)
	rotated, err := NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)

	opened, legacy, err := rotated.OpenWithLegacyAAD(sealed, s.record, []byte("tenant"), 1, "rotated-record")
	s.Require().NoError(err)
	s.Equal("line secret", opened)
	s.True(legacy)
	_, _, err = rotated.OpenWithLegacyAAD(sealed, s.record, []byte("tenant"), 2, "rotated-record")
	s.Error(err, "the stored version, not the current one, opens it")
}

func (s *KeyringSuite) TestAVersionNotInTheKeyringFailsToOpen() {
	sealer, err := NewSealer("a passphrase")
	s.Require().NoError(err)
	sealed, err := sealer.Seal("vas_live_s3cret")
	s.Require().NoError(err)

	_, err = sealer.OpenWithAADVersion(sealed, nil, 2)
	s.ErrorIs(err, ErrKeyVersionUnavailable)
	s.ErrorContains(err, "version 2")
}

func (s *KeyringSuite) TestTheCurrentVersionMustBeInTheKeyring() {
	_, err := NewSealerWithKeyring(2, map[int]string{1: "old-key"})
	s.ErrorContains(err, "version 2 is required")
}

func (s *KeyringSuite) TestAnEmptyKeyIsRefused() {
	_, err := NewSealerWithKeyring(2, map[int]string{1: "", 2: "new-key"})
	s.Error(err)
}

func (s *KeyringSuite) TestTwoProcessesHoldingOneKeyDeriveOneCredential() {
	first, err := NewSealer("shared-key")
	s.Require().NoError(err)
	second, err := NewSealer("shared-key")
	s.Require().NoError(err)

	presented, _ := first.Derive("node-forward-v1")
	_, accepted := second.Derive("node-forward-v1")
	s.Equal([][]byte{presented}, accepted)
	// What a peer is sent is not the key, nor the key the sealer encrypts with.
	key := sha256.Sum256([]byte("shared-key"))
	s.NotEqual(key[:], presented)
	s.NotEqual([]byte("shared-key"), presented)
	// Nor is it signed with the key the sealer encrypts with.
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("node-forward-v1"))
	s.NotEqual(mac.Sum(nil), presented)
}

func (s *KeyringSuite) TestAnotherKeyDerivesAnotherCredential() {
	ours, err := NewSealer("shared-key")
	s.Require().NoError(err)
	theirs, err := NewSealer("another-key")
	s.Require().NoError(err)

	presented, _ := theirs.Derive("node-forward-v1")
	_, accepted := ours.Derive("node-forward-v1")
	s.NotContains(accepted, presented)
}

func (s *KeyringSuite) TestAnotherPurposeDerivesAnotherCredential() {
	sealer, err := NewSealer("shared-key")
	s.Require().NoError(err)

	forward, _ := sealer.Derive("node-forward-v1")
	other, _ := sealer.Derive("node-forward-v2")
	s.NotEqual(forward, other)
}

func (s *KeyringSuite) TestARotatedKeyringAcceptsTheCredentialOfEveryKeyItHolds() {
	before, err := NewSealer("old-key")
	s.Require().NoError(err)
	rotated, err := NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)
	after, err := NewSealerWithKeyring(2, map[int]string{2: "new-key"})
	s.Require().NoError(err)

	older, _ := before.Derive("node-forward-v1")
	newer, _ := after.Derive("node-forward-v1")
	current, accepted := rotated.Derive("node-forward-v1")
	s.Equal(newer, current, "a rotated keyring presents its current key's credential")
	s.ElementsMatch([][]byte{older, newer}, accepted)
}

func (s *KeyringSuite) TestTheCurrentKeyInTheMiddleOfTheKeyringIsTheOneThatIsPresented() {
	keys := map[int]string{1: "key-one", 2: "key-two", 3: "key-three", 4: "key-four", 5: "key-five"}
	middle, err := NewSealerWithKeyring(3, keys)
	s.Require().NoError(err)
	alone, err := NewSealer("key-three")
	s.Require().NoError(err)

	// Many rounds, because the keys are held in a map and a lucky order must not pass.
	for range 20 {
		current, accepted := middle.Derive("node-forward-v1")
		again, _ := alone.Derive("node-forward-v1")
		s.Equal(again, current)
		s.Len(accepted, len(keys))
		s.Contains(accepted, current)
	}
}
