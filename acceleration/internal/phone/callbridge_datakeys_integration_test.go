//go:build integration

package phone

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// rotatedSealer is a router keyring whose current key, 2, is newer than another it still opens
// with, so a test sees which version a seal under the router's key wrote.
func (s *AppsSuite) rotatedSealer() *auth.Sealer {
	sealer, err := auth.NewSealerWithKeyring(2, map[int]string{1: "apps suite key one", 2: "apps suite key two"})
	s.Require().NoError(err)
	return sealer
}

// placeParked places a call on a vendor that fetches its call plan on answer, and returns the
// token of its fetch and the trunk password the plan must carry.
func (s *AppsSuite) placeParked(fetcher *planStub, service *Service, e164 string) (string, string) {
	_, err := service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, _ := s.deployment.TrunkAuth(trunks[0])
	s.Require().NotEmpty(password)
	return fetcher.dialed.AnswerURL[strings.LastIndex(fetcher.dialed.AnswerURL, "/")+1:], password
}

// parkedBridge is the bridge the suite's customer has parked.
func (s *AppsSuite) parkedBridge() store.CallBridge {
	var parked store.CallBridge
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&parked).Where("customer_id = ?", s.customer).Scan(s.ctx))
	s.Require().NotEmpty(parked.TrunkPasswordSealed)
	return parked
}

// parkedBridges counts the bridges the suite's customer has parked.
func (s *AppsSuite) parkedBridges() int {
	count, err := s.store.ShardDB().NewSelect().Model((*store.CallBridge)(nil)).Where("customer_id = ?", s.customer).
		Count(s.ctx)
	s.Require().NoError(err)
	return count
}

// bridgeAADAsTheBaseWroteIt is the AAD of a parked password under the router's key, written out
// so a test fails when a bridge parked before data keys stops opening.
func (s *AppsSuite) bridgeAADAsTheBaseWroteIt(token string) []byte {
	return []byte("call_bridges/" + s.customer + "/" + token)
}

// AC: a bridged call answers with the password sealed under the app's data key, on a router that
// holds no key yet, and claiming it removes the row.
func (s *AppsSuite) TestAParkedBridgeUnderTheAppsDataKeyAnswersWithItsPassword() {
	kms := appkeystest.NewKMS()
	sealer := s.rotatedSealer()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), sealer)
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customer, 1)
	service.dataKeys = s.dataKeys(kms, true)
	token, password := s.placeParked(plivo, service, e164)

	parked := s.parkedBridge()
	s.Require().NotNil(parked.TrunkDataKeyVersion, "the password is under the app's data key")
	s.Equal(1, *parked.TrunkDataKeyVersion)
	s.Zero(parked.TrunkKEKVersion)
	s.Equal(password, s.openUnder(key, parked.TrunkPasswordSealed,
		appkeystest.RecordAAD("call_bridge", s.customer, s.bridgeAADAsTheBaseWroteIt(token))))
	_, err := sealer.OpenWithAADVersion(parked.TrunkPasswordSealed, s.bridgeAADAsTheBaseWroteIt(token), 2)
	s.Error(err, "the router's key does not open it")
	opens, unwraps := appkeystest.KEKOpens(s.T(), callBridgeRecord), kms.Unwraps()

	service.dataKeys = s.dataKeys(kms, true)
	_, err = service.Answer(s.ctx, token)

	s.Require().NoError(err)
	s.Equal(password, plivo.rendered.Password)
	s.Equal(unwraps+1, kms.Unwraps(), "a router that holds no key unwraps it once")
	s.Equal(opens, appkeystest.KEKOpens(s.T(), callBridgeRecord))
	s.Zero(s.parkedBridges(), "a claimed bridge is gone")
}

// AC: a bridge parked before data keys opens under the router's key with sealing on, counts one
// open there, and is gone afterwards, so nothing is sealed again.
func (s *AppsSuite) TestAParkedBridgeFromBeforeDataKeysOpensCountsOneKEKOpenAndIsGone() {
	kms := appkeystest.NewKMS()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), s.rotatedSealer())
	token, password := s.placeParked(plivo, service, e164)
	s.Require().Nil(s.parkedBridge().TrunkDataKeyVersion)
	opens := appkeystest.KEKOpens(s.T(), callBridgeRecord)

	service.dataKeys = s.dataKeys(kms, true)
	_, err := service.Answer(s.ctx, token)

	s.Require().NoError(err)
	s.Equal(password, plivo.rendered.Password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), callBridgeRecord))
	s.Zero(s.parkedBridges(), "a claimed bridge is gone")
	s.Zero(kms.Wraps()+kms.Unwraps(), "KMS is not asked about a bridge under the router's key")
	_, err = service.Answer(s.ctx, token)
	s.Error(err, "the token is spent")
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), callBridgeRecord))
}

func (s *AppsSuite) TestAParkedBridgeWhileKMSIsDownIsSealedUnderTheRoutersCurrentKey() {
	kms := appkeystest.NewKMS()
	sealer := s.rotatedSealer()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), sealer)
	service.dataKeys = s.dataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))
	fallbacks := appkeystest.SealFallbacks(s.T(), callBridgeRecord)

	token, password := s.placeParked(plivo, service, e164)

	parked := s.parkedBridge()
	s.Nil(parked.TrunkDataKeyVersion)
	s.Equal(2, parked.TrunkKEKVersion)
	opened, err := sealer.OpenWithAADVersion(parked.TrunkPasswordSealed, s.bridgeAADAsTheBaseWroteIt(token), 2)
	s.Require().NoError(err)
	s.Equal(password, opened)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), callBridgeRecord))
	_, err = service.Answer(s.ctx, token)
	s.Require().NoError(err, "KMS is not asked to open a bridge under the router's key")
	s.Equal(password, plivo.rendered.Password)
}

// The expected values come from a probe on the base (0425f0a1e): key version 2 of a {1,2}
// keyring, a blob that opens under the router's key with the base AAD, the password in the plan
// and no row left.
func (s *AppsSuite) TestWithoutDataKeysAParkedBridgeIsSealedAsBefore() {
	s.parksAsBefore(nil)
}

func (s *AppsSuite) TestWithSealingOffAParkedBridgeIsSealedAsBefore() {
	kms := appkeystest.NewKMS()
	s.parksAsBefore(s.dataKeys(kms, false))
	s.Zero(kms.Wraps()+kms.Unwraps(), "KMS is not asked")
}

func (s *AppsSuite) parksAsBefore(keys *appkeys.Keys) {
	sealer := s.rotatedSealer()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), sealer)
	service.dataKeys = keys
	token, password := s.placeParked(plivo, service, e164)

	parked := s.parkedBridge()
	s.Nil(parked.TrunkDataKeyVersion)
	s.Equal(2, parked.TrunkKEKVersion)
	opened, err := sealer.OpenWithAADVersion(parked.TrunkPasswordSealed, s.bridgeAADAsTheBaseWroteIt(token), 2)
	s.Require().NoError(err)
	s.Equal(password, opened)
	_, err = service.Answer(s.ctx, token)
	s.Require().NoError(err)
	s.Equal(password, plivo.rendered.Password)
	s.Zero(s.parkedBridges())
}

// Turning sealing off again leaves a bridge parked under the data key answerable.
func (s *AppsSuite) TestAParkedBridgeUnderTheDataKeyStillAnswersWithSealingOff() {
	kms := appkeystest.NewKMS()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), s.rotatedSealer())
	service.dataKeys = s.dataKeys(kms, true)
	token, password := s.placeParked(plivo, service, e164)
	s.Require().NotNil(s.parkedBridge().TrunkDataKeyVersion)

	service.dataKeys = s.dataKeys(kms, false)
	_, err := service.Answer(s.ctx, token)

	s.Require().NoError(err)
	s.Equal(password, plivo.rendered.Password)
}

// A bridge under the data key never falls back to the router's key. The cause stays in the
// error, which the answer endpoint only logs (api answerPhoneCall).
func (s *AppsSuite) TestAnAnswerWhileKMSCannotUnwrapTheAppsDataKeyFails() {
	kms := appkeystest.NewKMS()
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), s.rotatedSealer())
	service.dataKeys = s.dataKeys(kms, true)
	token, _ := s.placeParked(plivo, service, e164)
	service.dataKeys = s.dataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))

	_, err := service.Answer(s.ctx, token)

	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.NotErrorIs(err, ErrNoSealer)
	s.Empty(plivo.rendered.Password)
}

// A row counts as under the data key only with a data key version and kek version 0. A row with
// a kek version opens under the router's key whatever its data key column holds.
func (s *AppsSuite) TestAParkedBridgeWithAKEKVersionOpensUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	sealer := s.rotatedSealer()
	plivo, service, _ := s.onAPlanVendor("plivo", s.config(), sealer)
	service.dataKeys = s.dataKeys(kms, true)
	token, version := s.store.MintToken(), 1
	blob, err := sealer.SealWithAAD("neutral-value", s.bridgeAADAsTheBaseWroteIt(token))
	s.Require().NoError(err)
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, &store.CallBridge{
		Token: token, CustomerID: s.customer, Vendor: "plivo", TrunkURI: "sip:trunk@sip.example.test",
		TrunkUsername: "trunk", TrunkPasswordSealed: blob, TrunkKEKVersion: 2, TrunkDataKeyVersion: &version,
		CallID: "call-" + token, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))

	_, err = service.Answer(s.ctx, token)

	s.Require().NoError(err)
	s.Equal("neutral-value", plivo.rendered.Password)
	s.Zero(kms.Unwraps())
}

// A data key the router cannot read says nothing about the call, so the call is not dialed and
// a later try can work. Only a failure of the router's own key is lasting.
func (s *AppsSuite) TestACallWhoseDataKeyCannotBeReadIsNotDialedAndMayBeTriedAgain() {
	closed, err := store.Open(testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_apps"))
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(closed, appkeystest.NewKMS(), options)
	s.Require().NoError(err)
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), s.rotatedSealer())
	service.dataKeys = keys

	_, err = service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111"})

	s.ErrorIs(err, ErrNotDialed)
	s.NotErrorIs(err, ErrCannotDial)
	s.Empty(plivo.dialed.To)
	s.Zero(s.parkedBridges())
}
