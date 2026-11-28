//go:build integration

package phone

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"os"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// withDataKeys gives the suite's service the router's key and the apps' data keys on kms, and
// seals new trunk passwords under the data keys when seal is true.
func (s *AppsSuite) withDataKeys(kms *appkeystest.KMS, seal bool) {
	s.sealed()
	s.service.dataKeys = s.dataKeys(kms, seal)
}

// dataKeys is a fresh appkeys.Keys on kms, holding no key yet, as a router that just started has.
func (s *AppsSuite) dataKeys(kms *appkeystest.KMS, seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	return keys
}

// anotherRouter is the suite's service as another router of the deployment runs it: the same
// router key and database, and data keys on kms that hold no key yet. A vendor's webhook can
// reach any router, not the one that sealed the trunk's password.
func (s *AppsSuite) anotherRouter(kms *appkeystest.KMS) *Service {
	service, err := NewService(ServiceOptions{Registry: s.registry, Store: s.store, Apps: s.apps,
		Sealer: s.service.sealer, DataKeys: s.dataKeys(kms, false)})
	s.Require().NoError(err)
	return service
}

// opensUnderTheRoutersKey is the trunk's password opened under the router's key, with the AAD
// written out as the base sealed it, so a test fails when a row the base wrote stops opening.
func (s *AppsSuite) opensUnderTheRoutersKey(trunk store.StreamTrunk) (string, error) {
	return s.service.sealer.OpenWithAADVersion(trunk.BridgePasswordSealed,
		[]byte("stream_trunks/"+s.customer+"/"+trunk.ID), trunk.BridgePasswordKEKVersion)
}

func (s *AppsSuite) TestAnInboundCallOnATrunkSealedUnderTheAppsDataKeyConnects() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	trunk := s.trunk()
	s.Require().NotNil(trunk.BridgePasswordDataKeyVersion, "the password is under the app's data key")
	s.Equal(1, *trunk.BridgePasswordDataKeyVersion)
	s.Equal(0, trunk.BridgePasswordKEKVersion)
	_, err = s.opensUnderTheRoutersKey(store.StreamTrunk{ID: trunk.ID, BridgePasswordSealed: trunk.BridgePasswordSealed,
		BridgePasswordKEKVersion: s.service.sealer.CurrentVersion()})
	s.Error(err, "the router's key does not open it")
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Require().NotEmpty(password)
	s.NotContains(string(trunk.BridgePasswordSealed), password)

	other := s.anotherRouter(kms)
	bridge, err := other.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, bridge.Password, "the call is sent to the trunk with its password")
	s.Equal(attached.TrunkID, bridge.Username)
	s.Equal(1, kms.Unwraps(), "a router that holds no key unwraps it once")

	again, err := other.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, again.Password)
	s.Equal(1, kms.Unwraps(), "the next call uses the key the router holds, with no KMS call")
}

func (s *AppsSuite) TestAnInboundCallFailsWhileKMSCannotUnwrapTheAppsDataKey() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	other := s.anotherRouter(kms)
	kms.Fail(errors.New("kms is down"))

	_, err = other.InboundBridge(s.ctx, "twilio", s.e164)

	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.NotErrorIs(err, ErrNoSealer, "a KMS fault is not a missing keyring, which tells the operator to attach again")
	s.NotErrorIs(err, store.ErrNoStoredBridge, "nor a number to attach again")
	kms.Fail(nil)
	bridge, err := other.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err, "the next call connects once KMS answers")
	s.NotEmpty(bridge.Password)
}

func (s *AppsSuite) TestATrunkMadeWhileKMSIsDownIsSealedUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))
	before := appkeystest.SealFallbacks(s.T(), trunkRecord)

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err, "a trunk is never lost to KMS being down")
	trunk := s.trunk()
	s.Nil(trunk.BridgePasswordDataKeyVersion)
	s.Equal(s.service.sealer.CurrentVersion(), trunk.BridgePasswordKEKVersion)
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	opened, err := s.opensUnderTheRoutersKey(trunk)
	s.Require().NoError(err)
	s.Equal(password, opened)
	s.Equal(before+1, appkeystest.SealFallbacks(s.T(), trunkRecord))
	bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err, "KMS is not asked to open a trunk under the router's key")
	s.Equal(password, bridge.Password)
}

func (s *AppsSuite) TestWithoutDataKeysATrunkPasswordIsSealedAsBefore() {
	s.sealed()
	s.attachesAsBefore()
}

func (s *AppsSuite) TestWithSealingOffATrunkPasswordIsSealedAsBefore() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, false)
	s.attachesAsBefore()
	s.Zero(kms.Wraps(), "no data key is made")
}

// attachesAsBefore attaches the suite's number and checks the row the base writes: no data key
// version, the router's current key version, and a blob that opens under the router's key with
// the trunk's AAD.
func (s *AppsSuite) attachesAsBefore() {
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	trunk := s.trunk()
	s.Nil(trunk.BridgePasswordDataKeyVersion)
	s.Equal(s.service.sealer.CurrentVersion(), trunk.BridgePasswordKEKVersion)
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Require().NotEmpty(password)
	opened, err := s.opensUnderTheRoutersKey(trunk)
	s.Require().NoError(err)
	s.Equal(password, opened)
	bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, bridge.Password)
}

// Turning sealing off again must not cut off the trunks sealed while it was on.
func (s *AppsSuite) TestATrunkSealedUnderTheDataKeyStillOpensWithSealingOff() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	sealed := s.trunk()
	s.Require().NotNil(sealed.BridgePasswordDataKeyVersion)
	s.service.dataKeys = s.dataKeys(kms, false)

	bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Equal(password, bridge.Password)
	joined, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err, "the next number joins the trunk, which opens its password")
	s.Equal(attached.TrunkID, joined.TrunkID)
	s.Equal(password, joined.Bridge.Password)
	s.Equal(sealed, s.trunk(), "the row is not rewritten")
}

// A number attached with no key to seal its trunk's password gets a trunk of its own once a
// key is there, and that trunk's password is sealed to the number under the app's data key.
func (s *AppsSuite) TestAnOwnTrunkMadeWithSealingOnIsSealedUnderTheAppsDataKey() {
	s.ownApp()
	s.own.OneTrunkPerNumber()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	trunk := s.trunk()
	s.Require().True(trunk.Own, "the trunk is the number's own")
	s.Require().NotNil(trunk.BridgePasswordDataKeyVersion)
	s.Equal(1, *trunk.BridgePasswordDataKeyVersion)
	password, _ := s.own.TrunkAuth(again.TrunkID)
	bridge, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, bridge.Password)
}

// A number on a trunk of its own whose password is under the app's data key is opened under
// it when the trunk is adopted, and the shared trunk's password is sealed under it again.
func (s *AppsSuite) TestAnOwnTrunkUnderTheAppsDataKeyIsAdoptedUnderIt() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)
	_, attached, bridge := s.onItsOwnTrunk()
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)
	ownAAD := appkeystest.RecordAAD(trunkRecord, s.customer, []byte("phone_numbers/"+s.customer+"/"+s.e164))
	version := 1
	group := store.TrunkGroup{CustomerID: s.customer, StreamAppPK: attached.StreamAppPK, Vendor: s.vendor.vendor}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		return tx.SaveTrunk(store.StreamTrunk{
			ID: attached.TrunkID, BridgeURI: bridge.URI, BridgeUsername: bridge.Username, Own: true,
			BridgePasswordSealed:         appkeystest.Seal(s.T(), key, []byte(bridge.Password), ownAAD),
			BridgePasswordDataKeyVersion: &version,
		})
	}))

	line, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(attached.TrunkID, line.TrunkID)
	s.Equal(bridge.Password, line.Bridge.Password)
	adopted := s.trunk()
	s.Require().NotNil(adopted.BridgePasswordDataKeyVersion, "the shared trunk's password stays under the data key")
	sharedAAD := appkeystest.RecordAAD(trunkRecord, s.customer, []byte("stream_trunks/"+s.customer+"/"+attached.TrunkID))
	s.Equal(bridge.Password, s.openUnder(key, adopted.BridgePasswordSealed, sharedAAD))
	inbound, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(bridge.Password, inbound.Password)
}

// openUnder opens a blob sealed under a data key the test holds, in the layout appkeystest.Seal
// writes, so the test reads what was sealed without the code under test opening it.
func (s *AppsSuite) openUnder(key, sealed, aad []byte) string {
	block, err := aes.NewCipher(key)
	s.Require().NoError(err)
	aead, err := cipher.NewGCM(block)
	s.Require().NoError(err)
	s.Require().Greater(len(sealed), aead.NonceSize())
	opened, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], aad)
	s.Require().NoError(err)
	return string(opened)
}

// A number joining a trunk whose password KMS cannot open fails, rather than being lined with
// an empty password that no call could use.
func (s *AppsSuite) TestANumberDoesNotJoinATrunkWhosePasswordKMSCannotOpen() {
	kms := appkeystest.NewKMS()
	s.withDataKeys(kms, true)
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.service = s.anotherRouter(kms)
	kms.Fail(errors.New("kms is down"))
	second := s.anotherNumber(s.customer, s.vendor.vendor)

	_, err = s.lineFor(s.customer, second)

	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID), "the trunk at Stream is left as it was")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, second)
	s.Require().NoError(err)
	s.Empty(held.StreamTrunkID)
}

// attachedUnderADataKey attaches the suite's number with sealing on, and answers its trunk's
// password as Stream made it.
func (s *AppsSuite) attachedUnderADataKey(kms *appkeystest.KMS) string {
	s.withDataKeys(kms, true)
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().NotNil(s.trunk().BridgePasswordDataKeyVersion)
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Require().NotEmpty(password)
	return password
}

// A row that names a data key version the app does not have fails, rather than being tried
// under the router's key.
func (s *AppsSuite) TestAnInboundCallOnATrunkNamingADataKeyTheAppLacksFails() {
	kms := appkeystest.NewKMS()
	s.attachedUnderADataKey(kms)
	_, err := s.store.GlobalDB().NewUpdate().Table("stream_trunks").Set("bridge_password_data_key_version = 2").
		Where("id = ?", s.trunk().ID).Exec(s.ctx)
	s.Require().NoError(err)

	bridge, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)

	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.Empty(bridge.Password)
}

// The data keys failing to read their database fail the open too, with no fallback.
func (s *AppsSuite) TestAnInboundCallFailsWhenTheDataKeysCannotReadTheirDatabase() {
	kms := appkeystest.NewKMS()
	s.attachedUnderADataKey(kms)
	closed, err := store.Open(testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_apps"))
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	keys, err := appkeys.New(closed, kms, appkeystest.Options())
	s.Require().NoError(err)
	other, err := NewService(ServiceOptions{Registry: s.registry, Store: s.store, Apps: s.apps,
		Sealer: s.service.sealer, DataKeys: keys})
	s.Require().NoError(err)

	bridge, err := other.InboundBridge(s.ctx, "twilio", s.e164)

	s.Error(err)
	s.NotErrorIs(err, ErrNoSealer)
	s.NotErrorIs(err, store.ErrNoStoredBridge)
	s.Empty(bridge.Password)
}

// A router from before data keys saves a trunk with its password under the router's key and
// leaves the data key version as it was. That row opens under the router's key.
func (s *AppsSuite) TestATrunkABuildBeforeDataKeysSavedAgainOpensUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	password := s.attachedUnderADataKey(kms)
	trunk := s.trunk()
	resealed, err := s.service.sealer.SealWithAAD(password, []byte("stream_trunks/"+s.customer+"/"+trunk.ID))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Table("stream_trunks").
		Set("bridge_password_sealed = ?", resealed).Set("bridge_password_kek_version = ?", s.service.sealer.CurrentVersion()).
		Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)
	s.Require().NotNil(s.trunk().BridgePasswordDataKeyVersion, "the old build leaves the column as it was")

	other := s.anotherRouter(kms)
	bridge, err := other.InboundBridge(s.ctx, "twilio", s.e164)

	s.Require().NoError(err)
	s.Equal(password, bridge.Password)
	s.Zero(kms.Unwraps(), "KMS is not asked for a password under the router's key")
	joined, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err, "a number joining the trunk opens it the same way")
	s.Equal(password, joined.Bridge.Password)
}

// Taking one number off a trunk another number still uses saves the trunk again. It must keep
// its data key version, or the next call to the trunk opens the password under the wrong key.
func (s *AppsSuite) TestTakingANumberOffATrunkUnderTheDataKeyLeavesItsPasswordOpening() {
	kms := appkeystest.NewKMS()
	password := s.attachedUnderADataKey(kms)
	trunkID := s.trunk().ID
	_, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	kept, err := s.store.StreamTrunks.Get(s.ctx, s.customer, trunkID)
	s.Require().NoError(err)
	s.Require().NotNil(kept.BridgePasswordDataKeyVersion, "the trunk's password is still under the data key")
	third, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)
	s.Equal(trunkID, third.TrunkID)
	s.Equal(password, third.Bridge.Password)
}
