//go:build integration

package phone

import (
	"errors"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// attachedBeforeDataKeys attaches the suite's number on a router with no data keys, as a trunk
// made before the feature is, and then gives the service data keys on kms. It answers the
// trunk's password as Stream made it and the row as the base wrote it.
func (s *AppsSuite) attachedBeforeDataKeys(kms *appkeystest.KMS, seal bool) (string, store.StreamTrunk) {
	s.sealed()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	trunk := s.trunk()
	s.Require().Nil(trunk.BridgePasswordDataKeyVersion, "the trunk starts under the router's key")
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Require().NotEmpty(password)
	s.service.dataKeys = s.dataKeys(kms, seal)
	return password, trunk
}

// sharedTrunkAAD is the AAD a shared trunk's password is sealed with under the app's data key,
// written out so a test fails when a reseal binds the blob to anything else.
func (s *AppsSuite) sharedTrunkAAD(trunkID string) []byte {
	return appkeystest.RecordAAD("stream_trunk", s.customer, []byte("stream_trunks/"+s.customer+"/"+trunkID))
}

// AC-4: the first inbound call on a trunk from before data keys moves its password to the
// app's data key, so only that call counts an open under the router's key.
func (s *AppsSuite) TestTwoInboundCallsOnATrunkFromBeforeDataKeysConnectAndTheFirstReseals() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, true)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)
	opens := appkeystest.KEKOpens(s.T(), "stream_trunk")

	first, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)

	s.Require().NoError(err)
	s.Equal(password, first.Password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), "stream_trunk"))
	resealed := s.trunk()
	s.Require().NotNil(resealed.BridgePasswordDataKeyVersion, "the password moved to the app's data key")
	s.Equal(1, *resealed.BridgePasswordDataKeyVersion)
	s.Zero(resealed.BridgePasswordKEKVersion)
	s.Equal(password, s.openUnder(key, resealed.BridgePasswordSealed, s.sharedTrunkAAD(before.ID)))

	second, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)

	s.Require().NoError(err)
	s.Equal(password, second.Password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), "stream_trunk"), "the second call opens under the data key")
	s.Equal(resealed, s.trunk(), "a password under the data key is not sealed again")
}

func (s *AppsSuite) TestInboundCallsConnectWhileKMSCannotResealTheirTrunk() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))
	opens := appkeystest.KEKOpens(s.T(), "stream_trunk")
	fallbacks := appkeystest.SealFallbacks(s.T(), "stream_trunk")

	for range 2 {
		bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
		s.Require().NoError(err, "the call connects on the password it opened")
		s.Equal(password, bridge.Password)
	}

	s.Equal(before, s.trunk(), "the row stays under the router's key")
	s.Equal(opens+2, appkeystest.KEKOpens(s.T(), "stream_trunk"))
	s.Equal(fallbacks, appkeystest.SealFallbacks(s.T(), "stream_trunk"), "a reseal does not fall back to the router's key")
	kms.Fail(nil)
	other, err := NewService(ServiceOptions{Registry: s.registry, Store: s.store, Apps: s.apps,
		Sealer: s.service.sealer, DataKeys: s.dataKeys(kms, true)})
	s.Require().NoError(err)
	_, err = other.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.NotNil(s.trunk().BridgePasswordDataKeyVersion, "the next call once KMS answers reseals the row")
}

// The expected row and counts were probed on the base (af72d76a5), where nothing reseals.
func (s *AppsSuite) TestWithSealingOffInboundCallsLeaveATrunkFromBeforeDataKeysAsItIs() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, false)
	s.callsTwiceAsBefore(password, before)
	s.Zero(kms.Wraps(), "no data key is made")
}

func (s *AppsSuite) TestWithoutDataKeysInboundCallsLeaveATrunkAsItIs() {
	password, before := s.attachedBeforeDataKeys(appkeystest.NewKMS(), false)
	s.service.dataKeys = nil
	s.callsTwiceAsBefore(password, before)
}

// callsTwiceAsBefore makes two inbound calls and a line on the suite's trunk, and checks what
// the base does: each opens under the router's key, counts one open, and the row is not rewritten.
func (s *AppsSuite) callsTwiceAsBefore(password string, before store.StreamTrunk) {
	opens := appkeystest.KEKOpens(s.T(), "stream_trunk")
	for range 2 {
		bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
		s.Require().NoError(err)
		s.Equal(password, bridge.Password)
	}
	line, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.Equal(password, line.Bridge.Password)

	s.Equal(before, s.trunk())
	s.Equal(opens+3, appkeystest.KEKOpens(s.T(), "stream_trunk"))
	opened, err := s.opensUnderTheRoutersKey(s.trunk())
	s.Require().NoError(err)
	s.Equal(password, opened)
}

// A number already on a shared trunk is lined again whenever it is attached. That open moves
// the password to the data key as an inbound call does.
func (s *AppsSuite) TestLiningANumberOnATrunkFromBeforeDataKeysResealsIt() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, true)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)

	line, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(before.ID, line.TrunkID)
	s.Equal(password, line.Bridge.Password)
	resealed := s.trunk()
	s.Require().NotNil(resealed.BridgePasswordDataKeyVersion)
	s.Zero(resealed.BridgePasswordKEKVersion)
	s.Equal(password, s.openUnder(key, resealed.BridgePasswordSealed, s.sharedTrunkAAD(before.ID)))
}

// A number joining a trunk from before data keys saves the trunk anyway, and saves it with the
// password under the data key.
func (s *AppsSuite) TestANumberJoiningATrunkFromBeforeDataKeysResealsIt() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, true)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)

	joined, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().NoError(err)
	s.Equal(before.ID, joined.TrunkID)
	s.Equal(password, joined.Bridge.Password)
	resealed := s.trunk()
	s.Require().NotNil(resealed.BridgePasswordDataKeyVersion)
	s.Zero(resealed.BridgePasswordKEKVersion)
	s.Equal(password, s.openUnder(key, resealed.BridgePasswordSealed, s.sharedTrunkAAD(before.ID)))
	bridge, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, bridge.Password)
}

func (s *AppsSuite) TestANumberJoinsATrunkFromBeforeDataKeysWhileKMSIsDown() {
	kms := appkeystest.NewKMS()
	password, before := s.attachedBeforeDataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))

	joined, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().NoError(err, "the join uses the password it opened")
	s.Equal(password, joined.Bridge.Password)
	after := s.trunk()
	s.Nil(after.BridgePasswordDataKeyVersion)
	s.Equal(before.BridgePasswordSealed, after.BridgePasswordSealed)
	s.Equal(before.BridgePasswordKEKVersion, after.BridgePasswordKEKVersion)
}

// A number on a trunk of its own from before data keys is adopted under the app's data key.
func (s *AppsSuite) TestAnOwnTrunkFromBeforeDataKeysIsAdoptedUnderTheAppsDataKey() {
	s.sealed()
	_, attached, bridge := s.onItsOwnTrunk()
	kms := appkeystest.NewKMS()
	s.service.dataKeys = s.dataKeys(kms, true)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)
	opens := appkeystest.KEKOpens(s.T(), "stream_trunk")

	line, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(bridge.Password, line.Bridge.Password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), "stream_trunk"))
	adopted := s.trunk()
	s.False(adopted.Own)
	s.Require().NotNil(adopted.BridgePasswordDataKeyVersion)
	s.Equal(bridge.Password, s.openUnder(key, adopted.BridgePasswordSealed, s.sharedTrunkAAD(attached.TrunkID)))
	inbound, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(bridge.Password, inbound.Password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), "stream_trunk"))
}

// An inbound call on a number with a trunk of its own reseals the password with the number's
// AAD, which is what that row opens with.
func (s *AppsSuite) TestAnInboundCallOnAnOwnTrunkFromBeforeDataKeysResealsItForTheNumber() {
	s.sealed()
	_, _, bridge := s.onItsOwnTrunk()
	kms := appkeystest.NewKMS()
	s.service.dataKeys = s.dataKeys(kms, true)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)

	inbound, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)

	s.Require().NoError(err)
	s.Equal(bridge.Password, inbound.Password)
	resealed := s.trunk()
	s.True(resealed.Own)
	s.Require().NotNil(resealed.BridgePasswordDataKeyVersion)
	ownAAD := appkeystest.RecordAAD("stream_trunk", s.customer, []byte("phone_numbers/"+s.customer+"/"+s.e164))
	s.Equal(bridge.Password, s.openUnder(key, resealed.BridgePasswordSealed, ownAAD))
	again, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(bridge.Password, again.Password)
}

// A row a build from before data keys saved again names a data key version but holds a blob
// under the router's key. Its first open moves it to the data key, as any router-key row.
func (s *AppsSuite) TestATrunkABuildBeforeDataKeysSavedAgainIsResealedOnItsNextCall() {
	kms := appkeystest.NewKMS()
	password := s.attachedUnderADataKey(kms)
	trunk := s.trunk()
	old, err := s.service.sealer.SealWithAAD(password, []byte("stream_trunks/"+s.customer+"/"+trunk.ID))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Table("stream_trunks").
		Set("bridge_password_sealed = ?", old).Set("bridge_password_kek_version = ?", s.service.sealer.CurrentVersion()).
		Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)

	bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)

	s.Require().NoError(err)
	s.Equal(password, bridge.Password)
	resealed := s.trunk()
	s.Zero(resealed.BridgePasswordKEKVersion, "the row is under the data key again")
	s.NotEqual(old, resealed.BridgePasswordSealed)
	again, err := s.anotherRouter(kms).InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal(password, again.Password)
}
