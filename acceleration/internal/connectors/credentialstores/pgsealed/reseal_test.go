//go:build integration

package pgsealed

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// sealedOnTheBase is acme-app's connection conn-sealed-on-base, holding the blob sealedOnBase
// that a router built before data keys sealed. It returns the blob too. acme-app's data keys
// go when the test ends, since TestCredentialsSealedOnTheBaseOpenAsBeforeAndNoDataKeyIsMade
// checks it has none.
func (s *PGSealedSuite) sealedOnTheBase() (core.ConnectionRef, []byte) {
	made := s.connected("acme-app", tokens{Access: "a", Refresh: "r"}, time.Time{})
	sealed, err := base64.StdEncoding.DecodeString(sealedOnBase)
	s.Require().NoError(err)
	_, err = s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET id = 'conn-sealed-on-base',"+
		" credentials_sealed = ? WHERE id = ?", sealed, made.ConnectionID)
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, err := s.db.GlobalDB().ExecContext(s.ctx, "DELETE FROM app_data_keys WHERE customer_id = 'acme-app'")
		s.Require().NoError(err)
	})
	return core.ConnectionRef{CustomerID: "acme-app", ConnectionID: "conn-sealed-on-base"}, sealed
}

// kekOpened is how many connector credentials have opened under the router's key so far.
func kekOpened() float64 {
	return testutil.ToFloat64(kekOpens.WithLabelValues(kekOpensRecord))
}

func (s *PGSealedSuite) TestWithSealingOnARowSealedOnTheBaseMovesToTheAppsDataKeyOnItsFirstUse() {
	ref, sealed := s.sealedOnTheBase()
	before := s.stored(ref)
	kms := appkeystest.NewKMS()
	sealing := s.sealing(kms)
	opens := kekOpened()

	s.Equal(tokens{Access: "base-access", Refresh: "base-refresh"}, opened(s.held(sealing, ref).Credentials))
	s.Equal(opens+1, kekOpened(), "the first use opened under the router's key")
	stored := s.stored(ref)
	s.Equal(new(1), stored.CredentialsDataKeyVersion)
	s.Zero(stored.CredentialsKEKVersion)
	s.Equal(before.Revision, stored.Revision, "a reseal is the same credentials at the same revision")
	s.Equal(store.ConnectionConnected, stored.Status)
	s.False(bytes.Equal(sealed, stored.CredentialsSealed))
	_, err := s.v1.OpenWithAADVersion(stored.CredentialsSealed, credentialsAAD(ref, stored.Revision), 1)
	s.Error(err, "the router's key no longer opens the row")

	s.Equal(tokens{Access: "base-access", Refresh: "base-refresh"}, opened(s.held(sealing, ref).Credentials))
	s.Equal(opens+1, kekOpened(), "the next use opened under the data key")
	s.True(bytes.Equal(stored.CredentialsSealed, s.stored(ref).CredentialsSealed), "a row under the data key is not sealed again")
	s.Equal(1, kms.Wraps())

	// A router with sealing off opens the row through the data key that was stored.
	s.Equal(tokens{Access: "base-access", Refresh: "base-refresh"}, opened(s.held(s.withDataKeys(kms), ref).Credentials))
	s.Equal(1, kms.Unwraps())
}

// TestWithSealingOffEveryOpenUnderTheRoutersKeyCountsAndTheRowStays: nothing moves the row,
// so each use counts again. A row sealed on the base keeping its bytes is
// TestCredentialsSealedOnTheBaseOpenAsBeforeAndNoDataKeyIsMade.
func (s *PGSealedSuite) TestWithSealingOffEveryOpenUnderTheRoutersKeyCountsAndTheRowStays() {
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	before := s.stored(ref)
	kms := appkeystest.NewKMS()
	opens := kekOpened()

	s.held(s.credentialStore(s.v1), ref)
	s.held(s.withDataKeys(kms), ref)
	s.Equal(opens+2, kekOpened())
	stored := s.stored(ref)
	s.True(bytes.Equal(before.CredentialsSealed, stored.CredentialsSealed))
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal(before.UpdatedAt, stored.UpdatedAt, "a use that changed nothing writes nothing")
	s.Zero(kms.Wraps())
}

// TestWithSealingOnAKMSFailureLeavesTheRowUnderTheRoutersKeyAndTheUseSucceeds: the
// credentials opened, so the use goes on. Nothing is written, so it is no seal fallback. The
// uses in the next 30 s ask KMS nothing, so they do not wait out its deadline under the
// connection's lock, and the first use after that moves the row.
func (s *PGSealedSuite) TestWithSealingOnAKMSFailureLeavesTheRowUnderTheRoutersKeyAndTheUseSucceeds() {
	for _, failure := range []struct {
		name string
		fail func(*appkeystest.KMS)
	}{
		{"refused", func(kms *appkeystest.KMS) { kms.Fail(errors.New("kms: permission denied on the key")) }},
		{"no answer", func(kms *appkeystest.KMS) { kms.Hold() }},
	} {
		s.Run(failure.name, func() {
			logs := s.captureLogs()
			kms := appkeystest.NewKMS()
			defer kms.Release()
			clock := newClock()
			options := appkeystest.Options()
			options.Seal, options.KMSTimeout, options.Now = true, time.Second, clock.now
			sealing := s.dataKeyStore(kms, options)
			ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
			before := s.stored(ref)
			fallbacks := testutil.ToFloat64(sealFallbacks)
			failure.fail(kms)

			s.Equal(tokens{Access: "a", Refresh: "r"}, opened(s.held(sealing, ref).Credentials))
			stored := s.stored(ref)
			s.True(bytes.Equal(before.CredentialsSealed, stored.CredentialsSealed))
			s.Equal(1, stored.CredentialsKEKVersion)
			s.Nil(stored.CredentialsDataKeyVersion)
			s.Equal(before.UpdatedAt, stored.UpdatedAt, "the row is not written")
			s.Equal(fallbacks, testutil.ToFloat64(sealFallbacks))
			s.Equal(1, strings.Count(logs.String(), `level=WARN msg="left connector credentials under the router's key`))
			s.Contains(logs.String(), "app_id="+ref.CustomerID+" connection="+ref.ConnectionID)
			s.Equal(1, kms.Wraps())

			for _, wait := range []time.Duration{0, 30*time.Second - time.Nanosecond} {
				clock.advance(wait)
				started := time.Now()
				s.Equal(tokens{Access: "a", Refresh: "r"}, opened(s.held(sealing, ref).Credentials))
				s.Less(time.Since(started), options.KMSTimeout/2, "the use waited on KMS")
			}
			s.Equal(1, kms.Wraps(), "a use within 30 s of the failure asked KMS")
			s.Nil(s.stored(ref).CredentialsDataKeyVersion)

			kms.Fail(nil)
			kms.Release()
			clock.advance(time.Nanosecond)
			s.held(sealing, ref)
			s.Equal(new(1), s.stored(ref).CredentialsDataKeyVersion, "a use after KMS is back moves the row")
			s.Equal(2, kms.Wraps())
		})
	}
}

// TestWithSealingOnNewCredentialsWrittenWhileKMSFailsAskKMSOnceIn30Seconds: each write falls
// back to the router's key, and only the first one waits on KMS.
func (s *PGSealedSuite) TestWithSealingOnNewCredentialsWrittenWhileKMSFailsAskKMSOnceIn30Seconds() {
	kms := appkeystest.NewKMS()
	defer kms.Release()
	clock := newClock()
	options := appkeystest.Options()
	options.Seal, options.KMSTimeout, options.Now = true, time.Second, clock.now
	sealing := s.dataKeyStore(kms, options)
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	other := s.connected("app-"+rand.Text(), tokens{Access: "o", Refresh: "p"}, time.Time{})
	fallbacks := testutil.ToFloat64(sealFallbacks)
	kms.Hold()
	s.Require().NoError(s.write(sealing, ref, credentials(tokens{Access: "b", Refresh: "s"})))
	s.Equal(1, kms.Wraps())

	for i, wait := range []time.Duration{0, 30*time.Second - time.Nanosecond} {
		clock.advance(wait)
		started := time.Now()
		held := tokens{Access: fmt.Sprintf("c%d", i), Refresh: fmt.Sprintf("t%d", i)}
		s.Require().NoError(s.write(sealing, ref, credentials(held)))
		s.Less(time.Since(started), options.KMSTimeout/2, "the write waited on KMS")
		stored := s.stored(ref)
		s.Equal(1, stored.CredentialsKEKVersion)
		s.Nil(stored.CredentialsDataKeyVersion)
		s.Equal(held, opened(s.held(s.credentialStore(s.v1), ref).Credentials))
	}
	s.Equal(1, kms.Wraps(), "a write within 30 s of the failure asked KMS")
	s.Equal(fallbacks+3, testutil.ToFloat64(sealFallbacks))

	kms.Release()
	s.Require().NoError(s.write(sealing, other, credentials(tokens{Access: "x", Refresh: "y"})))
	s.Equal(new(1), s.stored(other).CredentialsDataKeyVersion, "another app's write waited for the failed one's gap")
	clock.advance(time.Nanosecond)
	s.Require().NoError(s.write(sealing, ref, credentials(tokens{Access: "d", Refresh: "u"})))
	s.Equal(new(1), s.stored(ref).CredentialsDataKeyVersion)
	s.Equal(3, kms.Wraps())
}

// clock is a time that a test moves by hand, for appkeys.Options.Now.
type clock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *clock {
	return &clock{at: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// TestAResealThatLosesTheRaceLeavesTheNewerRow: another writer saves the row while the use
// runs, without the lock, as a delete does. The reseal then matches no row, and the use
// still succeeds.
func (s *PGSealedSuite) TestAResealThatLosesTheRaceLeavesTheNewerRow() {
	logs := s.captureLogs()
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	before := s.stored(ref)

	err := s.sealing(kms).Update(s.ctx, ref, func(*core.CredentialState, func() error) (bool, error) {
		_, err := s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET revision = revision + 1,"+
			" last_error = 'saved by another writer' WHERE id = ?", ref.ConnectionID)
		return false, err
	})
	s.Require().NoError(err)
	stored := s.stored(ref)
	s.Equal(before.Revision+1, stored.Revision)
	s.Equal("saved by another writer", stored.LastError)
	s.True(bytes.Equal(before.CredentialsSealed, stored.CredentialsSealed))
	s.Equal(1, stored.CredentialsKEKVersion)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal(1, kms.Wraps(), "the reseal was tried")
	s.Equal(1, strings.Count(logs.String(), `level=WARN msg="left connector credentials under the router's key`),
		"the lost race is not logged")
}

// TestWithSealingOnAndKMSDownARowUnderAnOlderKeyIsRewrappedUnderTheCurrentOne: the rewrap to the
// router's current key runs first, and its seal falls back to that key while KMS is down, so the
// row leaves the older key version even though it cannot reach the app's data key yet.
func (s *PGSealedSuite) TestWithSealingOnAndKMSDownARowUnderAnOlderKeyIsRewrappedUnderTheCurrentOne() {
	kms := appkeystest.NewKMS()
	ref := s.connected("app-"+rand.Text(), tokens{Access: "a", Refresh: "r"}, time.Time{})
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(s.db, kms, options)
	s.Require().NoError(err)
	sealing, err := New(s.db, rotated, WithDataKeys(keys))
	s.Require().NoError(err)
	kms.Fail(errors.New("kms: unavailable"))
	fallbacks := testutil.ToFloat64(sealFallbacks)

	s.Equal(tokens{Access: "a", Refresh: "r"}, opened(s.held(sealing, ref).Credentials))
	stored := s.stored(ref)
	s.Equal(2, stored.CredentialsKEKVersion, "the row stayed under the older key version")
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal(fallbacks+1, testutil.ToFloat64(sealFallbacks))
}

func (s *PGSealedSuite) TestWithSealingOnAnUnreadableRowIsNotResealed() {
	theirs := s.connected("acme-app", tokens{Access: "theirs", Refresh: "theirs"}, time.Time{})
	mine := s.connected("acme-app", tokens{Access: "mine", Refresh: "mine"}, time.Time{})
	_, err := s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET credentials_sealed = ? WHERE id = ?",
		s.stored(theirs).CredentialsSealed, mine.ConnectionID)
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, err := s.db.GlobalDB().ExecContext(s.ctx, "DELETE FROM app_data_keys WHERE customer_id = 'acme-app'")
		s.Require().NoError(err)
	})

	s.assertUnreadableBy(s.sealing(appkeystest.NewKMS()), mine)
}

// TestTwoRoutersResealingRowsOfOneAppAtOnceShareItsFirstDataKey: the app has no data key yet,
// so both reseals make one, and the store keeps the first. Each router has a pool of its own.
func (s *PGSealedSuite) TestTwoRoutersResealingRowsOfOneAppAtOnceShareItsFirstDataKey() {
	kms := appkeystest.NewKMS()
	app := "app-" + rand.Text()
	refs := []core.ConnectionRef{
		s.connected(app, tokens{Access: "a", Refresh: "r"}, time.Time{}),
		s.connected(app, tokens{Access: "b", Refresh: "s"}, time.Time{}),
	}
	routers := []*CredentialStore{s.sealingRouter(kms), s.sealingRouter(kms)}

	var wg sync.WaitGroup
	errs := make([]error, len(refs))
	for i, ref := range refs {
		wg.Go(func() {
			errs[i] = routers[i].Update(s.ctx, ref, func(*core.CredentialState, func() error) (bool, error) { return false, nil })
		})
	}
	wg.Wait()
	s.Require().NoError(errors.Join(errs...))

	var keys int
	s.Require().NoError(s.db.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM app_data_keys WHERE customer_id = ?", app).Scan(&keys))
	s.Equal(1, keys)
	reader := s.withDataKeys(kms)
	for i, held := range []tokens{{Access: "a", Refresh: "r"}, {Access: "b", Refresh: "s"}} {
		s.Equal(new(1), s.stored(refs[i]).CredentialsDataKeyVersion)
		s.Equal(held, opened(s.held(reader, refs[i]).Credentials))
	}
}

// sealingRouter is another router that seals under the app's data key through kms: a pool and
// data keys of its own, connected before the test starts.
func (s *PGSealedSuite) sealingRouter(kms *appkeystest.KMS) *CredentialStore {
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { db.Close() })
	s.Require().NoError(db.Ping(s.ctx))
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(db, kms, options)
	s.Require().NoError(err)
	credentialStore, err := New(db, s.v1, WithDataKeys(keys))
	s.Require().NoError(err)
	return credentialStore
}
