//go:build integration

package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// underDataKeys swaps the router's phone service for one that opens trunk passwords under the
// apps' data keys on kms, and holds the number at Twilio on a trunk of its own whose password is
// sealed under the app's data key, as a router with auth.app_keys_seal on stores it.
func (s *TwilioHookSuite) underDataKeys(kms *appkeystest.KMS, password string) {
	keys, err := appkeys.New(s.store, kms, appkeystest.Options())
	s.Require().NoError(err)
	sealer, err := auth.NewSealer(suiteTrunkKEK)
	s.Require().NoError(err)
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	logger := slog.New(slog.NewTextHandler(s.logged, &slog.HandlerOptions{ReplaceAttr: store.RedactLogAttr}))
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: vendors.Registry(config), Store: s.store, Gate: s.gate, Sealer: sealer, DataKeys: keys, Logger: logger,
	})
	s.Require().NoError(err)
	configured := s.router.phone
	s.router.phone = service
	s.T().Cleanup(func() { s.router.phone = configured })

	ctx := context.Background()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: s.e164, Vendor: "twilio", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	key := appkeystest.AddKey(s.T(), s.store, kms, s.customerID(), 1)
	aad := appkeystest.RecordAAD("stream_trunk", s.customerID(), []byte("phone_numbers/"+s.customerID()+"/"+s.e164))
	version := 1
	attachment := store.NumberAttachment{TrunkID: "trunk-" + s.utils.uuid(), CallType: "default"}
	group := store.TrunkGroup{CustomerID: s.customerID(), Vendor: "twilio"}
	s.Require().NoError(s.store.StreamTrunks.InGroup(ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		err := tx.SaveTrunk(store.StreamTrunk{
			ID: attachment.TrunkID, BridgeURI: "sip:" + s.e164 + "@y", BridgeUsername: "x", BridgePasswordSealed: appkeystest.Seal(s.T(), key, []byte(password), aad), BridgePasswordDataKeyVersion: &version,
			Own: true,
		})
		if err != nil {
			return err
		}
		return tx.Attach(s.e164, attachment)
	}))
}

func (s *TwilioHookSuite) TestACallToATrunkUnderTheAppsDataKeyIsSentThereWithItsPassword() {
	kms := appkeystest.NewKMS()
	s.underDataKeys(kms, "data-key-password")
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Require().Equal(http.StatusOK, status, body)
	s.Contains(body, `password="data-key-password"`)
	s.Contains(body, "sip:"+s.e164+"@y")
	s.Equal(1, kms.Unwraps())
}

// A KMS that does not unwrap the app's data key is the router's own fault. The vendor's webhook
// still gets a hang-up, as for any other trunk the router cannot open, and the next call connects
// once KMS answers again.
func (s *TwilioHookSuite) TestACallWhileKMSCannotUnwrapTheAppsDataKeyIsHungUp() {
	kms := appkeystest.NewKMS()
	s.underDataKeys(kms, "data-key-password")
	kms.Fail(errors.New("kms is down"))
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "data-key-password")
	s.True(s.loggedLine("hung up a twilio call: could not find its trunk", masked(s.e164)))
	row := s.countedOnce(activity.AreaPhoneSMS, store.AuditSourceAPI)
	s.Equal(int64(1), row.refused)
	s.Equal(internalError().Code, row.code)

	kms.Fail(nil)
	again := s.line.call(s.e164)
	status, _, body = s.line.deliver(again, s.line.sign(again))
	s.Require().Equal(http.StatusOK, status, body)
	s.Contains(body, `password="data-key-password"`, "the router is still up and the next call connects")
}
