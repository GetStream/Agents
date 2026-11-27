//go:build integration

package appkeys_test

import (
	"crypto/rand"
	"errors"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// routerKey stands in for the router's key. It seals at version 7 and opens what it sealed, and
// counts its calls, so a test sees which key a helper used.
type routerKey struct {
	calls int
	err   error
}

func (r *routerKey) seal() ([]byte, int, error) {
	r.calls++
	return []byte("under the router's key"), 7, r.err
}

func (r *routerKey) open() ([]byte, error) {
	r.calls++
	return []byte("secret"), r.err
}

// kindOfRecord is a record label of its own, so no other test moves its counters.
func kindOfRecord() string {
	return "test_record_" + rand.Text()
}

// TestASecondKindOfRecordSealsAndOpensThroughTheSharedCounters: a record other than connector
// credentials uses the same helpers, and its fallbacks and opens under the router's key are
// counted under its own label.
func (s *AppKeysSuite) TestASecondKindOfRecordSealsAndOpensThroughTheSharedCounters() {
	keys := s.newKeys(appkeys.Options{Seal: true})
	kind := kindOfRecord()
	record := appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text(), Attrs: []slog.Attr{slog.String("row", "row-1")}}
	router := &routerKey{}
	s.kms.Fail(errors.New("kms: permission denied on the key"))

	sealed, err := keys.SealRecord(s.T().Context(), record, []byte("secret"), []byte("row"), router.seal)
	s.Require().NoError(err)
	s.Equal(appkeys.Sealed{Blob: []byte("under the router's key"), KEKVersion: 7}, sealed)
	s.Equal(float64(1), appkeystest.SealFallbacks(s.T(), kind))
	s.Contains(s.logs.String(), `level=WARN msg="sealed a secret under the router's key, since the app's data key is `+
		`unavailable" record=`+kind+" app_id="+record.CustomerID+" row=row-1 reason=kms_error")
	opened, err := keys.OpenRecord(s.T().Context(), record, sealed.DataKeyVersion, sealed.Blob, []byte("row"), router.open)
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	s.Equal(float64(1), appkeystest.KEKOpens(s.T(), kind))

	s.kms.Fail(nil)
	s.advance(retryEvery)
	sealed, err = keys.SealRecord(s.T().Context(), record, []byte("secret"), []byte("row"), router.seal)
	s.Require().NoError(err)
	s.Equal(new(1), sealed.DataKeyVersion)
	s.Zero(sealed.KEKVersion)
	opened, err = keys.OpenRecord(s.T().Context(), record, sealed.DataKeyVersion, sealed.Blob, []byte("row"), router.open)
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	s.Equal(2, router.calls, "the router's key sealed and opened only while KMS failed")
	s.Equal(float64(1), appkeystest.SealFallbacks(s.T(), kind))
	s.Equal(float64(1), appkeystest.KEKOpens(s.T(), kind))
}

// TestWithoutDataKeysOrWithSealingOffARecordIsSealedUnderTheRoutersKey: neither is a fallback,
// so nothing asks KMS and nothing is counted as one.
func (s *AppKeysSuite) TestWithoutDataKeysOrWithSealingOffARecordIsSealedUnderTheRoutersKey() {
	kind := kindOfRecord()
	record := appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text()}
	for _, keys := range []*appkeys.Keys{nil, s.newKeys(appkeys.Options{})} {
		sealed, err := keys.SealRecord(s.T().Context(), record, []byte("secret"), []byte("row"), (&routerKey{}).seal)
		s.Require().NoError(err)
		s.Equal(appkeys.Sealed{Blob: []byte("under the router's key"), KEKVersion: 7}, sealed)
	}
	s.Zero(s.kms.Wraps())
	s.Zero(s.kms.Unwraps())
	s.Zero(appkeystest.SealFallbacks(s.T(), kind))
}

// TestARouterWithoutDataKeysOpensOnlyRecordsUnderItsOwnKey: a record under a data key is an
// error that names the missing setting, not a secret that does not open.
func (s *AppKeysSuite) TestARouterWithoutDataKeysOpensOnlyRecordsUnderItsOwnKey() {
	var keys *appkeys.Keys
	kind := kindOfRecord()
	record := appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text()}
	router := &routerKey{}

	opened, err := keys.OpenRecord(s.T().Context(), record, nil, []byte("blob"), []byte("row"), router.open)
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	s.Equal(float64(1), appkeystest.KEKOpens(s.T(), kind))
	_, err = keys.OpenRecord(s.T().Context(), record, new(1), []byte("blob"), []byte("row"), router.open)
	s.ErrorIs(err, appkeys.ErrNoKeys)
	s.ErrorContains(err, "auth.app_keys_kms_key is not set")
	s.Equal(1, router.calls)
}

// TestAFailureOfTheRoutersKeyIsReturnedAndNoOpenIsCounted covers both helpers, with and without a
// fallback from KMS.
func (s *AppKeysSuite) TestAFailureOfTheRoutersKeyIsReturnedAndNoOpenIsCounted() {
	kind := kindOfRecord()
	record := appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text()}
	broken := &routerKey{err: errors.New("auth: no key of the current version")}
	sealing := s.newKeys(appkeys.Options{Seal: true})
	s.kms.Fail(errors.New("kms: unavailable"))

	for _, keys := range []*appkeys.Keys{nil, sealing} {
		_, err := keys.SealRecord(s.T().Context(), record, []byte("secret"), []byte("row"), broken.seal)
		s.ErrorIs(err, broken.err)
	}
	_, err := sealing.OpenRecord(s.T().Context(), record, nil, []byte("blob"), []byte("row"), broken.open)
	s.ErrorIs(err, broken.err)
	s.Zero(appkeystest.KEKOpens(s.T(), kind))
}

// TestASealThatFailsForAnotherReasonThanKMSIsReturned: the store failing is no KMS outage, so
// the secret is not sealed under the router's key instead.
func (s *AppKeysSuite) TestASealThatFailsForAnotherReasonThanKMSIsReturned() {
	closed, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(closed, s.kms, options)
	s.Require().NoError(err)
	kind := kindOfRecord()
	router := &routerKey{}

	_, err = keys.SealRecord(s.T().Context(), appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text()},
		[]byte("secret"), []byte("row"), router.seal)
	s.Require().Error(err)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
	s.Zero(router.calls)
	s.Zero(appkeystest.SealFallbacks(s.T(), kind))
}

// TestAResealDuringAKMSOutageIsNoFallback: the secret is already sealed under the router's key,
// so a reseal that KMS cannot serve returns the error, and nothing is counted or logged as a
// fallback. With sealing off or no keys, a reseal asks nothing of KMS.
func (s *AppKeysSuite) TestAResealDuringAKMSOutageIsNoFallback() {
	kind := kindOfRecord()
	record := appkeys.Record{Kind: kind, CustomerID: "app-" + rand.Text()}
	for _, keys := range []*appkeys.Keys{nil, s.newKeys(appkeys.Options{})} {
		sealed, err := keys.ResealRecord(s.T().Context(), record, []byte("secret"), []byte("row"))
		s.Require().NoError(err)
		s.Equal(appkeys.Sealed{}, sealed)
	}
	s.Zero(s.kms.Wraps())

	keys := s.newKeys(appkeys.Options{Seal: true})
	s.kms.Fail(errors.New("kms: unavailable"))
	_, err := keys.ResealRecord(s.T().Context(), record, []byte("secret"), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.Zero(appkeystest.SealFallbacks(s.T(), kind))
	s.NotContains(s.logs.String(), "sealed a secret under the router's key")

	s.kms.Fail(nil)
	s.advance(retryEvery)
	sealed, err := keys.ResealRecord(s.T().Context(), record, []byte("secret"), []byte("row"))
	s.Require().NoError(err)
	s.Equal(new(1), sealed.DataKeyVersion)
	s.Zero(sealed.KEKVersion)
	opened, err := keys.OpenRecord(s.T().Context(), record, sealed.DataKeyVersion, sealed.Blob, []byte("row"),
		(&routerKey{}).open)
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
}

// TestARecordOpensOnlyAsTheKindAndAppItWasSealedFor: one data key seals every kind of record of
// an app, so a blob moved to a row of another kind with the same row AAD must not open.
func (s *AppKeysSuite) TestARecordOpensOnlyAsTheKindAndAppItWasSealedFor() {
	keys := s.newKeys(appkeys.Options{Seal: true})
	app := "app-" + rand.Text()
	channel := appkeys.Record{Kind: kindOfRecord(), CustomerID: app}
	model := appkeys.Record{Kind: kindOfRecord(), CustomerID: app}
	router := &routerKey{}

	sealed, err := keys.SealRecord(s.T().Context(), channel, []byte("secret"), []byte(app), router.seal)
	s.Require().NoError(err)
	opened, err := keys.OpenRecord(s.T().Context(), channel, sealed.DataKeyVersion, sealed.Blob, []byte(app), router.open)
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	_, err = keys.OpenRecord(s.T().Context(), model, sealed.DataKeyVersion, sealed.Blob, []byte(app), router.open)
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	_, err = keys.Open(s.T().Context(), app, *sealed.DataKeyVersion, sealed.Blob, []byte(app))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen, "the record's kind and app are not in the AAD")
	_, err = keys.Open(s.T().Context(), app, *sealed.DataKeyVersion, sealed.Blob,
		appkeystest.RecordAAD(channel.Kind, app, []byte(app)))
	s.NoError(err, "the AAD layout changed, so the rows already sealed no longer open")

	resealed, err := keys.ResealRecord(s.T().Context(), channel, []byte("secret"), []byte(app))
	s.Require().NoError(err)
	_, err = keys.OpenRecord(s.T().Context(), model, resealed.DataKeyVersion, resealed.Blob, []byte(app), router.open)
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.Zero(router.calls)
}

// TestARecordWithoutAKindAnAppOrARowAADIsRefused: such a record would open in any row of its
// app, so no key seals or opens it.
func (s *AppKeysSuite) TestARecordWithoutAKindAnAppOrARowAADIsRefused() {
	keys := s.newKeys(appkeys.Options{Seal: true})
	app := "app-" + rand.Text()
	router := &routerKey{}
	for _, bad := range []struct {
		record appkeys.Record
		aad    []byte
	}{
		{appkeys.Record{Kind: kindOfRecord(), CustomerID: app}, nil},
		{appkeys.Record{Kind: kindOfRecord(), CustomerID: app}, []byte{}},
		{appkeys.Record{CustomerID: app}, []byte("row")},
		{appkeys.Record{Kind: kindOfRecord()}, []byte("row")},
	} {
		for _, k := range []*appkeys.Keys{nil, keys} {
			_, err := k.SealRecord(s.T().Context(), bad.record, []byte("secret"), bad.aad, router.seal)
			s.Error(err)
			_, err = k.ResealRecord(s.T().Context(), bad.record, []byte("secret"), bad.aad)
			s.Error(err)
			_, err = k.OpenRecord(s.T().Context(), bad.record, nil, []byte("blob"), bad.aad, router.open)
			s.Error(err)
			_, err = k.OpenRecord(s.T().Context(), bad.record, new(1), []byte("blob"), bad.aad, router.open)
			s.Error(err)
		}
	}
	s.Zero(router.calls)
	s.Zero(s.kms.Wraps())
	s.Zero(s.kms.Unwraps())
}
