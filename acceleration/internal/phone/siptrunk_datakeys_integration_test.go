//go:build integration

package phone

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// sealingTrunks gives the suite's service data keys on kms, sealing new SIP trunk passwords under
// them when seal is true, and returns a new customer, since a data key made by one test's KMS
// does not unwrap with the next one's.
func (s *SIPTrunkSuite) sealingTrunks(kms *appkeystest.KMS, seal bool) string {
	s.service.dataKeys = s.dataKeys(kms, seal)
	return "acme-" + rand.Text()
}

// dataKeys is a fresh appkeys.Keys on kms, holding no key yet, as a router that just started has.
func (s *SIPTrunkSuite) dataKeys(kms *appkeystest.KMS, seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	return keys
}

// anotherRouter is the suite's service as another router of the deployment runs it: the same
// router key and database, and data keys on kms that hold no key yet and do not seal.
func (s *SIPTrunkSuite) anotherRouter(kms *appkeystest.KMS) *Service {
	service, err := NewService(ServiceOptions{Registry: s.service.registry, Store: s.store,
		Sealer: s.service.sealer, DataKeys: s.dataKeys(kms, false), SIPTrunks: s.trunks})
	s.Require().NoError(err)
	return service
}

// stored is the trunk's row as the database holds it now.
func (s *SIPTrunkSuite) stored(trunk store.SIPTrunk) store.SIPTrunk {
	row, err := s.store.SIPTrunks.Get(s.ctx, trunk.CustomerID, trunk.ID)
	s.Require().NoError(err)
	return row
}

// opensUnderTheRoutersKey is the trunk's password opened under the router's key, with the AAD
// written out as the base sealed it, so a test fails when a row the base wrote stops opening.
func (s *SIPTrunkSuite) opensUnderTheRoutersKey(trunk store.SIPTrunk, version int) (string, error) {
	return s.service.sealer.OpenWithAADVersion(trunk.PasswordSealed,
		[]byte("sip_trunks/"+trunk.CustomerID+"/"+trunk.ID), version)
}

// dial is the password a call through the trunk's number is dialled with on service.
func (s *SIPTrunkSuite) dial(service *Service, held store.PhoneNumber) (string, error) {
	_, opened, err := service.dialVendor(s.ctx, held, 0)
	if err != nil {
		return "", err
	}
	return opened.Password, nil
}

func (s *SIPTrunkSuite) TestADialOnATrunkSealedUnderTheAppsDataKeyAuthenticatesWithItsPassword() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)

	trunk := s.create(customer, customerTrunkPassword)

	row := s.stored(trunk)
	s.Equal(new(1), row.PasswordDataKeyVersion, "the password is under the app's data key")
	s.Zero(row.PasswordKEKVersion)
	s.True(row.HasPassword(), "the API says the trunk has a password")
	_, err := s.opensUnderTheRoutersKey(row, s.service.sealer.CurrentVersion())
	s.Error(err, "the router's key does not open it")
	held := s.number(customer, trunk.ID, "+15550000301")
	other := s.anotherRouter(kms)
	opens := appkeystest.KEKOpens(s.T(), sipTrunkRecord)
	password, err := s.dial(other, held)
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, password, "the call authenticates at the trunk with its password")
	s.Equal(1, kms.Unwraps(), "a router that holds no key unwraps it once")
	again, err := s.dial(other, held)
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, again)
	s.Equal(1, kms.Unwraps(), "the next call uses the key the router holds, with no KMS call")
	s.Equal(opens, appkeystest.KEKOpens(s.T(), sipTrunkRecord), "no open under the router's key is counted")
	_, err = s.dial(s.service, held)
	s.Require().NoError(err)
	s.Equal(row.PasswordSealed, s.stored(trunk).PasswordSealed,
		"a router that seals does not seal a password under the data key again")
}

func (s *SIPTrunkSuite) TestAPasswordSealedUnderTheDataKeyOpensOnlyOnItsOwnTrunk() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, false)
	key := appkeystest.AddKey(s.T(), s.store, kms, customer, 1)
	mine := s.create(customer, "x")
	theirs := s.create(customer, "x")
	// The record's kind, app and row AAD written out, so a change that rows sealed before it
	// cannot follow fails here.
	blob := appkeystest.Seal(s.T(), key, []byte(customerTrunkPassword),
		appkeystest.RecordAAD("sip_trunk", customer, []byte("sip_trunks/"+customer+"/"+mine.ID)))
	for _, trunk := range []store.SIPTrunk{mine, theirs} {
		_, err := s.store.GlobalDB().NewUpdate().Model((*store.SIPTrunk)(nil)).
			Set("password_sealed = ?", blob).Set("password_kek_version = 0").Set("password_data_key_version = 1").
			Where("id = ?", trunk.ID).Exec(s.ctx)
		s.Require().NoError(err)
	}

	password, err := s.dial(s.service, s.number(customer, mine.ID, "+15550000302"))
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, password)
	_, err = s.dial(s.service, s.number(customer, theirs.ID, "+15550000303"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen, "a blob copied onto another trunk of the app does not open there")
	s.ErrorIs(unsent(err), ErrCannotDial, "a password no key opens fails every later try too")
}

func (s *SIPTrunkSuite) TestADialMovesAPasswordUnderTheRoutersKeyToTheAppsDataKeyAndCountsItOnce() {
	before := s.create("acme-"+rand.Text(), customerTrunkPassword)
	s.Require().Equal(1, before.PasswordKEKVersion)
	kms := appkeystest.NewKMS()
	s.service.dataKeys = s.dataKeys(kms, true)
	held := s.number(before.CustomerID, before.ID, "+15550000304")
	stamp := s.stored(before).UpdatedAt
	opens := appkeystest.KEKOpens(s.T(), sipTrunkRecord)

	password, err := s.dial(s.service, held)

	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, password)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), sipTrunkRecord), "the open under the router's key is counted")
	row := s.stored(before)
	s.Equal(new(1), row.PasswordDataKeyVersion, "the dial moved the password to the app's data key")
	s.Zero(row.PasswordKEKVersion)
	s.NotEqual(before.PasswordSealed, row.PasswordSealed)
	_, err = s.opensUnderTheRoutersKey(row, 1)
	s.Error(err)
	s.Equal(stamp, row.UpdatedAt, "a reseal is not a change the customer made")
	again, err := s.dial(s.anotherRouter(kms), held)
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, again)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), sipTrunkRecord), "the next dial opens under the data key")
}

func (s *SIPTrunkSuite) TestADialWithSealingOffLeavesAPasswordUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, false)
	trunk := s.create(customer, customerTrunkPassword)
	held := s.number(customer, trunk.ID, "+15550000305")
	opens := appkeystest.KEKOpens(s.T(), sipTrunkRecord)

	for range 2 {
		password, err := s.dial(s.service, held)
		s.Require().NoError(err)
		s.Equal(customerTrunkPassword, password)
	}

	s.Equal(opens+2, appkeystest.KEKOpens(s.T(), sipTrunkRecord), "each open under the router's key is counted")
	s.Equal(trunk.PasswordSealed, s.stored(trunk).PasswordSealed)
	s.Nil(s.stored(trunk).PasswordDataKeyVersion)
	s.Zero(kms.Wraps())
}

func (s *SIPTrunkSuite) TestADialWhileKMSCannotSealConnectsAndLeavesTheRow() {
	trunk := s.create("acme-"+rand.Text(), customerTrunkPassword)
	kms := appkeystest.NewKMS()
	s.service.dataKeys = s.dataKeys(kms, true)
	kms.Fail(errors.New("kms is down"))
	var logged bytes.Buffer
	s.service.logger = slog.New(slog.NewTextHandler(&logged, nil))

	password, err := s.dial(s.service, s.number(trunk.CustomerID, trunk.ID, "+15550000306"))

	s.Require().NoError(err, "the password opened, so the call goes on")
	s.Equal(customerTrunkPassword, password)
	s.Contains(logged.String(), "left a sip trunk password under the router's key")
	s.Contains(logged.String(), "sip_trunk="+trunk.ID)
	s.NotContains(logged.String(), customerTrunkPassword)
	row := s.stored(trunk)
	s.Equal(trunk.PasswordSealed, row.PasswordSealed)
	s.Equal(1, row.PasswordKEKVersion)
	s.Nil(row.PasswordDataKeyVersion)
}

func (s *SIPTrunkSuite) TestAResealLosesToAPasswordSetAfterTheTrunkWasRead() {
	opened := s.create("acme-"+rand.Text(), customerTrunkPassword)
	replaced := "y"
	settings := SettingsOf(opened)
	settings.Password = &replaced
	_, err := s.service.UpdateSIPTrunk(s.ctx, opened.CustomerID, opened.ID, settings)
	s.Require().NoError(err)
	s.service.dataKeys = s.dataKeys(appkeystest.NewKMS(), true)

	s.service.resealSIPTrunkPassword(s.ctx, opened, []byte(customerTrunkPassword))

	password, err := s.dial(s.service, s.number(opened.CustomerID, opened.ID, "+15550000307"))
	s.Require().NoError(err)
	s.Equal(replaced, password, "the old password resealed did not overwrite the new one")
}

func (s *SIPTrunkSuite) TestAFallbackToTheRoutersKeyNamesTheTrunkInTheLog() {
	kms := appkeystest.NewKMS()
	kms.Fail(errors.New("kms is down"))
	var logged bytes.Buffer
	options := appkeystest.Options()
	options.Seal = true
	options.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	s.service.dataKeys = keys
	customer := "acme-" + rand.Text()

	trunk := s.create(customer, customerTrunkPassword)

	s.Contains(logged.String(), "sealed a secret under the router's key")
	s.Contains(logged.String(), "sip_trunk="+trunk.ID, "an operator can tell which trunk fell back")
	s.NotContains(logged.String(), customerTrunkPassword)
}

func (s *SIPTrunkSuite) TestATrunkMadeWhileKMSIsDownIsSealedUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)
	kms.Fail(errors.New("kms is down"))
	fallbacks := appkeystest.SealFallbacks(s.T(), sipTrunkRecord)

	trunk := s.create(customer, customerTrunkPassword)

	row := s.stored(trunk)
	s.Nil(row.PasswordDataKeyVersion, "a trunk is never lost to KMS being down")
	s.Equal(s.service.sealer.CurrentVersion(), row.PasswordKEKVersion)
	opened, err := s.opensUnderTheRoutersKey(row, row.PasswordKEKVersion)
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, opened)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), sipTrunkRecord))
}

func (s *SIPTrunkSuite) TestADialFailsWhileKMSCannotUnwrapTheAppsDataKeyAndCanBeTriedAgain() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)
	trunk := s.create(customer, customerTrunkPassword)
	held := s.number(customer, trunk.ID, "+15550000308")
	other := s.anotherRouter(kms)
	kms.Fail(errors.New("kms is down"))

	_, err := s.dial(other, held)

	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(unsent(err), ErrNotDialed, "KMS being down says nothing about the trunk, so a campaign tries again")
	s.NotErrorIs(unsent(err), ErrCannotDial)
	s.NotContains(err.Error(), customerTrunkPassword)
	kms.Fail(nil)
	password, err := s.dial(other, held)
	s.Require().NoError(err, "the next call connects once KMS answers")
	s.Equal(customerTrunkPassword, password)
}

func (s *SIPTrunkSuite) TestADialOnATrunkUnderTheDataKeyFailsForGoodOnARouterWithoutDataKeys() {
	customer := s.sealingTrunks(appkeystest.NewKMS(), true)
	trunk := s.create(customer, customerTrunkPassword)
	s.service.dataKeys = nil

	_, err := s.dial(s.service, s.number(customer, trunk.ID, "+15550000309"))

	s.ErrorIs(err, appkeys.ErrNoKeys)
	s.ErrorIs(unsent(err), ErrCannotDial, "the deployment lacks the setting, which a second try does not change")
}

func (s *SIPTrunkSuite) TestAPasswordTheRoutersKeyCannotOpenFailsEveryDial() {
	trunk := s.create("acme", customerTrunkPassword)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.SIPTrunk)(nil)).
		Set("password_sealed = ?", []byte("not a sealed password")).Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.dial(s.service, s.number("acme", trunk.ID, "+15550000310"))

	s.Require().Error(err)
	s.ErrorIs(unsent(err), ErrCannotDial, "as on the base")
}

func (s *SIPTrunkSuite) TestATrunkSealedUnderTheDataKeyStillOpensWithSealingOff() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)
	trunk := s.create(customer, customerTrunkPassword)
	s.service.dataKeys = s.dataKeys(kms, false)
	held := s.number(customer, trunk.ID, "+15550000311")

	password, err := s.dial(s.service, held)
	s.Require().NoError(err)
	s.Equal(customerTrunkPassword, password)
	s.Equal(trunk.PasswordSealed, s.stored(trunk).PasswordSealed, "the row is not rewritten")

	replaced := "y"
	settings := SettingsOf(trunk)
	settings.Password = &replaced
	_, err = s.service.UpdateSIPTrunk(s.ctx, customer, trunk.ID, settings)
	s.Require().NoError(err)
	row := s.stored(trunk)
	s.Nil(row.PasswordDataKeyVersion, "a new password with sealing off goes under the router's key")
	s.Equal(s.service.sealer.CurrentVersion(), row.PasswordKEKVersion)
	password, err = s.dial(s.service, held)
	s.Require().NoError(err)
	s.Equal(replaced, password)
}

func (s *SIPTrunkSuite) TestANewPasswordWithSealingOnReplacesOneUnderTheRoutersKey() {
	trunk := s.create("acme-"+rand.Text(), customerTrunkPassword)
	s.service.dataKeys = s.dataKeys(appkeystest.NewKMS(), true)
	replaced := "y"
	settings := SettingsOf(trunk)
	settings.Password = &replaced

	_, err := s.service.UpdateSIPTrunk(s.ctx, trunk.CustomerID, trunk.ID, settings)

	s.Require().NoError(err)
	row := s.stored(trunk)
	s.Equal(new(1), row.PasswordDataKeyVersion)
	s.Zero(row.PasswordKEKVersion)
	password, err := s.dial(s.service, s.number(trunk.CustomerID, trunk.ID, "+15550000312"))
	s.Require().NoError(err)
	s.Equal(replaced, password)
}

func (s *SIPTrunkSuite) TestATrunkABuildBeforeDataKeysSavedAgainOpensUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)
	trunk := s.create(customer, customerTrunkPassword)
	// A build from before data keys writes a router-key blob at its current version and leaves
	// the data key column as it was.
	blob, err := s.service.sealer.SealWithAAD("y", sipTrunkAAD(customer, trunk.ID))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Model((*store.SIPTrunk)(nil)).
		Set("password_sealed = ?", blob).Set("password_kek_version = 1").Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)
	other := s.anotherRouter(kms)

	password, err := s.dial(other, s.number(customer, trunk.ID, "+15550000313"))

	s.Require().NoError(err)
	s.Equal("y", password)
	s.Zero(kms.Unwraps(), "the router's key opened it, with no KMS call")
}

// The expected values come from a probe of CreateSIPTrunk, the row and dialVendor on the base
// (5373f2c97): kek version 1, which is the sealer's current one, no data key version, the blob
// opens under the router's key with the AAD sip_trunks/<app>/<trunk>, and a dial gets the
// password back.
func (s *SIPTrunkSuite) TestWithoutDataKeysOrWithSealingOffASIPTrunkPasswordIsSealedAsBefore() {
	for i, config := range []struct {
		name string
		keys *appkeys.Keys
	}{{"no data keys", nil}, {"sealing off", s.dataKeys(appkeystest.NewKMS(), false)}} {
		s.service.dataKeys = config.keys
		customer, name := "acme-"+rand.Text(), config.name

		trunk := s.create(customer, customerTrunkPassword)

		row := s.stored(trunk)
		s.Equal(1, row.PasswordKEKVersion, name)
		s.Nil(row.PasswordDataKeyVersion, name)
		s.True(row.HasPassword(), name)
		opened, err := s.opensUnderTheRoutersKey(row, 1)
		s.Require().NoError(err, name)
		s.Equal(customerTrunkPassword, opened, name)
		password, err := s.dial(s.service, s.number(customer, trunk.ID, fmt.Sprintf("+1555000031%d", 4+i)))
		s.Require().NoError(err, name)
		s.Equal(customerTrunkPassword, password, name)
		s.Equal(row.PasswordSealed, s.stored(trunk).PasswordSealed, name)
	}
}

// A password sealed under the router's key records the keyring's current version, whatever
// it is, with no data keys, sealing off, or KMS down.
func (s *SIPTrunkSuite) TestAPasswordUnderTheRoutersKeyRecordsTheCurrentKeyVersionAfterARotation() {
	sealer, err := auth.NewSealerWithKeyring(2, map[int]string{1: "sip trunk suite", 2: "sip trunk suite two"})
	s.Require().NoError(err)
	s.service.sealer = sealer
	down := appkeystest.NewKMS()
	down.Fail(errors.New("kms is down"))
	for i, keys := range []*appkeys.Keys{nil, s.dataKeys(appkeystest.NewKMS(), false), s.dataKeys(down, true)} {
		s.service.dataKeys = keys
		customer := "acme-" + rand.Text()

		trunk := s.create(customer, customerTrunkPassword)

		row := s.stored(trunk)
		s.Equal(2, row.PasswordKEKVersion, i)
		s.Nil(row.PasswordDataKeyVersion, i)
		password, err := s.dial(s.service, s.number(customer, trunk.ID, []string{"+15550000391", "+15550000392", "+15550000393"}[i]))
		s.Require().NoError(err, i)
		s.Equal(customerTrunkPassword, password, i)
	}
}

// A row an older build saved again (kek >= 1, stale data key version) is under the router's
// key, so a dial with sealing on moves it to the data key.
func (s *SIPTrunkSuite) TestADialMovesATrunkAnOlderBuildSavedAgainToTheDataKey() {
	kms := appkeystest.NewKMS()
	customer := s.sealingTrunks(kms, true)
	trunk := s.create(customer, customerTrunkPassword)
	blob, err := s.service.sealer.SealWithAAD("y", sipTrunkAAD(customer, trunk.ID))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Model((*store.SIPTrunk)(nil)).
		Set("password_sealed = ?", blob).Set("password_kek_version = 1").Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)

	password, err := s.dial(s.service, s.number(customer, trunk.ID, "+15550000394"))

	s.Require().NoError(err)
	s.Equal("y", password)
	row := s.stored(trunk)
	s.Zero(row.PasswordKEKVersion, "the dial moved the password to the data key")
	s.Equal(new(1), row.PasswordDataKeyVersion)
	s.NotEqual(blob, row.PasswordSealed)
}

// On such a row, a blob the router's key cannot open fails every later dial, as on the base.
func (s *SIPTrunkSuite) TestABadPasswordOnATrunkAnOlderBuildSavedAgainFailsEveryDial() {
	customer := s.sealingTrunks(appkeystest.NewKMS(), true)
	trunk := s.create(customer, customerTrunkPassword)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.SIPTrunk)(nil)).
		Set("password_sealed = ?", []byte("not a sealed password")).Set("password_kek_version = 1").
		Where("id = ?", trunk.ID).Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.dial(s.service, s.number(customer, trunk.ID, "+15550000395"))

	s.Require().Error(err)
	s.ErrorIs(unsent(err), ErrCannotDial)
}
