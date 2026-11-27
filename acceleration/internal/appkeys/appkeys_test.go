//go:build integration

package appkeys_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// The cache settings of AppKeysSuite's Keys.
const (
	ttl        = 10 * time.Minute
	staleLimit = time.Hour
	// retryEvery is appkeys' retryEvery, written out so a change to it fails here.
	retryEvery = 30 * time.Second
)

// AppKeysSuite opens secrets under data keys stored in Postgres and wrapped by a fake KMS. Each
// test uses apps of its own, so nothing is emptied between tests.
type AppKeysSuite struct {
	suite.Suite
	dsn  string
	db   *store.Store
	kms  *appkeystest.KMS
	keys *appkeys.Keys
	// now is the clock of keys, moved by hand.
	mu   sync.Mutex
	now  time.Time
	logs *bytes.Buffer
}

func TestAppKeysSuite(t *testing.T) {
	suite.Run(t, new(AppKeysSuite))
}

func (s *AppKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.dsn = testenv.Database(dsn, "appkeys")
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.T().Context()))
	s.db = db
}

func (s *AppKeysSuite) SetupTest() {
	s.kms = appkeystest.NewKMS()
	s.T().Cleanup(s.kms.Release)
	s.now = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	s.logs = &bytes.Buffer{}
	s.keys = s.newKeys(appkeys.Options{})
}

// newKeys returns a Keys on the suite's KMS, clock and log, with the suite's time limits unless
// options names others.
func (s *AppKeysSuite) newKeys(options appkeys.Options) *appkeys.Keys {
	options.TTL, options.StaleLimit = ttl, staleLimit
	if options.KMSTimeout == 0 {
		options.KMSTimeout = 5 * time.Second
	}
	options.Now = s.clock
	options.Logger = slog.New(slog.NewTextHandler(s.logs, nil))
	keys, err := appkeys.New(s.db, s.kms, options)
	s.Require().NoError(err)
	return keys
}

func (s *AppKeysSuite) clock() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

func (s *AppKeysSuite) advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

func (s *AppKeysSuite) TestASecretSealedUnderTheAppsKeyOpensWithOneUnwrap() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))

	opened, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
	s.Equal(1, s.kms.Unwraps())
}

func (s *AppKeysSuite) TestASecretUnderAnotherAADDoesNotOpen() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))

	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("another row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestABlobShorterThanANonceDoesNotOpen() {
	app := "app-" + rand.Text()
	appkeystest.AddKey(s.T(), s.db, s.kms, app, 1)

	_, err := s.keys.Open(s.T().Context(), app, 1, []byte("short"), []byte("row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
}

// TestASecretOfAnAppWithoutThatKeyDoesNotOpen: neither the app's other version nor another
// app's key stands in for the one missing.
func (s *AppKeysSuite) TestASecretOfAnAppWithoutThatKeyDoesNotOpen() {
	app, other := "app-"+rand.Text(), "app-"+rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))
	appkeystest.AddKey(s.T(), s.db, s.kms, other, 2)

	for _, missing := range []struct {
		app     string
		version int
	}{{app, 2}, {"app-" + rand.Text(), 1}} {
		_, err := s.keys.Open(s.T().Context(), missing.app, missing.version, sealed, []byte("row"))
		s.ErrorIs(err, appkeys.ErrDoesNotOpen)
		s.ErrorIs(err, store.ErrNoAppDataKey)
	}
	s.Zero(s.kms.Unwraps(), "no key, no KMS call")
}

func (s *AppKeysSuite) TestADatabaseFailureIsNeitherAnUnreadableSecretNorAKMSFault() {
	closed, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	keys, err := appkeys.New(closed, s.kms, appkeystest.Options())
	s.Require().NoError(err)

	_, err = keys.Open(s.T().Context(), "app-"+rand.Text(), 1, make([]byte, 40), []byte("row"))
	s.Require().Error(err)
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

// TestAWrappedKeyCopiedToAnotherAppDoesNotUnwrap: the wrap AAD names the app, so KMS refuses a
// key moved to another app's row, and nothing opens with it there.
func (s *AppKeysSuite) TestAWrappedKeyCopiedToAnotherAppDoesNotUnwrap() {
	mine, theirs := "app-"+rand.Text(), "app-"+rand.Text()
	key := appkeystest.AddKey(s.T(), s.db, s.kms, theirs, 1)
	appkeystest.AddKey(s.T(), s.db, s.kms, mine, 1)
	s.copyWrappedKey(theirs, 1, mine, 1)

	_, err := s.keys.Open(s.T().Context(), mine, 1, appkeystest.Seal(s.T(), key, []byte("secret"), []byte("row")), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
}

// TestAWrappedKeyCopiedToAnotherVersionDoesNotUnwrap: the wrap AAD names the version too.
func (s *AppKeysSuite) TestAWrappedKeyCopiedToAnotherVersionDoesNotUnwrap() {
	app := "app-" + rand.Text()
	key := appkeystest.AddKey(s.T(), s.db, s.kms, app, 1)
	appkeystest.AddKey(s.T(), s.db, s.kms, app, 2)
	s.copyWrappedKey(app, 1, app, 2)

	_, err := s.keys.Open(s.T().Context(), app, 2, appkeystest.Seal(s.T(), key, []byte("secret"), []byte("row")), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestAKMSFailureIsUnavailableAndKeepsTheKMSError() {
	app := "app-" + rand.Text()
	sealed := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))
	failure := errors.New("kms: deadline exceeded")
	s.kms.Fail(failure)

	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, failure, "the KMS error is kept for the log")
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
}

// TestAWrappedValueThatIsNoAESKeyIsAFaultOfTheKeys: what KMS unwrapped is not a key, so the
// fault is in the keys, and a caller must not take the secret for unreadable.
func (s *AppKeysSuite) TestAWrappedValueThatIsNoAESKeyIsAFaultOfTheKeys() {
	app := "app-" + rand.Text()
	s.Require().NoError(s.db.AppDataKeys.Insert(s.T().Context(), &store.AppDataKey{
		CustomerID: app, Version: 1, WrappedKey: s.kms.Wrap([]byte("seven b"), appkeystest.WrapAAD(app, 1)),
	}))

	_, err := s.keys.Open(s.T().Context(), app, 1, make([]byte, 40), []byte("row"))
	s.Require().Error(err)
	s.NotErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, appkeys.ErrUnavailable)
}

func (s *AppKeysSuite) TestNewNeedsAStoreAndAKMS() {
	_, err := appkeys.New(nil, s.kms, appkeystest.Options())
	s.Error(err)
	_, err = appkeys.New(s.db, nil, appkeystest.Options())
	s.Error(err)
}

func (s *AppKeysSuite) TestASecondOpenWithinTheTimeLimitMakesNoUnwrapAndOneAfterItDoes() {
	app, sealed := s.sealedSecret()

	s.opens(s.keys, app, sealed)
	s.advance(ttl - time.Nanosecond)
	s.opens(s.keys, app, sealed)
	s.Equal(1, s.kms.Unwraps())
	s.advance(time.Nanosecond)
	s.opens(s.keys, app, sealed)
	s.Equal(2, s.kms.Unwraps())
}

// TestWhileKMSFailsAHeldKeyOpensUntilTheStaleLimit: KMS is asked again once per retryEvery, and
// each failed ask writes one warning, not each open.
func (s *AppKeysSuite) TestWhileKMSFailsAHeldKeyOpensUntilTheStaleLimit() {
	app, sealed := s.sealedSecret()
	s.opens(s.keys, app, sealed)
	failure := errors.New("kms: unavailable")
	s.kms.Fail(failure)

	s.advance(ttl)
	s.opens(s.keys, app, sealed)
	s.opens(s.keys, app, sealed)
	s.Equal(2, s.kms.Unwraps(), "the second open uses the held key without asking KMS")
	s.Equal(1, s.logLines("could not unwrap an app's data key again"))
	s.advance(retryEvery - time.Nanosecond)
	s.opens(s.keys, app, sealed)
	s.Equal(2, s.kms.Unwraps())
	s.advance(time.Nanosecond)
	s.opens(s.keys, app, sealed)
	s.Equal(3, s.kms.Unwraps())
	s.Equal(2, s.logLines("could not unwrap an app's data key again"))

	s.advance(staleLimit - ttl - retryEvery - time.Nanosecond)
	s.opens(s.keys, app, sealed)
	s.Equal(4, s.kms.Unwraps())
	s.advance(time.Nanosecond)
	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, failure)

	s.kms.Fail(nil)
	s.opens(s.keys, app, sealed)
}

func (s *AppKeysSuite) TestAFailedUnwrapIsNotKept() {
	app, sealed := s.sealedSecret()
	s.kms.Fail(errors.New("kms: unavailable"))
	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.Require().ErrorIs(err, appkeys.ErrUnavailable)

	s.kms.Fail(nil)
	s.opens(s.keys, app, sealed)
	s.Equal(2, s.kms.Unwraps())
}

// TestAKMSThatDoesNotAnswerFailsWithinTheKMSDeadline: with no key held the open fails, so the
// connection's lock is held for the deadline, not for Cloud KMS's own 60 s.
func (s *AppKeysSuite) TestAKMSThatDoesNotAnswerFailsWithinTheKMSDeadline() {
	keys := s.newKeys(appkeys.Options{KMSTimeout: 20 * time.Millisecond})
	app, sealed := s.sealedSecret()
	s.kms.Hold()

	started := time.Now()
	_, err := keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Less(time.Since(started), time.Second)
}

// TestAnOpenWhoseKeyReadWaitsOnTheDatabaseFailsWithinTheKMSDeadline: the deadline covers the
// read of the wrapped key too, so a database with no free connection does not hold the open.
func (s *AppKeysSuite) TestAnOpenWhoseKeyReadWaitsOnTheDatabaseFailsWithinTheKMSDeadline() {
	app, sealed := s.sealedSecret()
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	db.GlobalDB().DB.SetMaxOpenConns(1)
	conn, err := db.GlobalDB().DB.Conn(s.T().Context())
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })
	keys, err := appkeys.New(db, s.kms, appkeys.Options{TTL: ttl, StaleLimit: staleLimit, KMSTimeout: 50 * time.Millisecond})
	s.Require().NoError(err)

	started := time.Now()
	_, err = keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Less(time.Since(started), time.Second)
}

func (s *AppKeysSuite) TestAKMSThatDoesNotAnswerLeavesTheHeldKeyInUseWithinTheKMSDeadline() {
	keys := s.newKeys(appkeys.Options{KMSTimeout: 20 * time.Millisecond})
	app, sealed := s.sealedSecret()
	s.opens(keys, app, sealed)
	s.advance(ttl)
	s.kms.Hold()

	started := time.Now()
	s.opens(keys, app, sealed)
	s.Less(time.Since(started), time.Second)
	s.Equal(1, s.logLines("could not unwrap an app's data key again"))
}

// TestWhileOneOpenUnwrapsAKeyAgainTheOthersUseTheHeldOne: only the open that asks KMS waits.
func (s *AppKeysSuite) TestWhileOneOpenUnwrapsAKeyAgainTheOthersUseTheHeldOne() {
	app, sealed := s.sealedSecret()
	s.opens(s.keys, app, sealed)
	s.advance(ttl)
	s.kms.Hold()
	asking := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		asking <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 2 }, 5*time.Second, time.Millisecond)

	started := time.Now()
	s.opens(s.keys, app, sealed)
	s.Less(time.Since(started), time.Second)
	s.kms.Release()
	s.NoError(<-asking)
	s.Equal(2, s.kms.Unwraps())
}

// TestOpensThatUseTheHeldKeyDoNotRaceTheUnwrapThatReplacesIt fails under -race when an open
// reads the held key after it let go of the lock, while the unwrap writes the new key.
func (s *AppKeysSuite) TestOpensThatUseTheHeldKeyDoNotRaceTheUnwrapThatReplacesIt() {
	app, sealed := s.sealedSecret()
	s.opens(s.keys, app, sealed)
	s.advance(ttl)
	s.kms.Hold()
	asking := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		asking <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 2 }, 5*time.Second, time.Millisecond)

	var stop atomic.Bool
	var opened atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
				s.NoError(err)
				opened.Add(1)
			}
		})
	}
	s.Require().Eventually(func() bool { return opened.Load() > 100 }, 5*time.Second, time.Millisecond)
	s.kms.Release()
	s.NoError(<-asking)
	s.Require().Eventually(func() bool { return opened.Load() > 1000 }, 5*time.Second, time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

func (s *AppKeysSuite) TestConcurrentOpensOfAnUncachedAppMakeOneUnwrap() {
	app, sealed := s.sealedSecret()
	s.kms.Hold()

	var wg sync.WaitGroup
	opened := make([][]byte, 16)
	failed := make([]error, len(opened))
	for i := range opened {
		wg.Go(func() { opened[i], failed[i] = s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row")) })
	}
	s.Require().Eventually(func() bool { return s.kms.Unwraps() >= 1 }, 5*time.Second, time.Millisecond)
	s.kms.Release()
	wg.Wait()
	for i := range opened {
		s.Require().NoError(failed[i])
		s.Equal("secret", string(opened[i]))
	}
	s.Equal(1, s.kms.Unwraps())
}

// TestAnOpenThatStopsWaitingLeavesTheSharedUnwrapToFinish: the first open's caller gives up, and
// the unwrap it started still holds the key for the next open.
func (s *AppKeysSuite) TestAnOpenThatStopsWaitingLeavesTheSharedUnwrapToFinish() {
	app, sealed := s.sealedSecret()
	s.kms.Hold()
	ctx, cancel := context.WithCancel(s.T().Context())
	first := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(ctx, app, 1, sealed, []byte("row"))
		first <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 1 }, 5*time.Second, time.Millisecond)

	waiting, stopWaiting := context.WithCancel(s.T().Context())
	second := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(waiting, app, 1, sealed, []byte("row"))
		second <- err
	}()
	stopWaiting()
	select {
	case err := <-second:
		s.ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.Fail("an open whose context ended kept waiting on KMS")
	}
	cancel()
	s.kms.Release()
	s.NoError(<-first)
	s.opens(s.keys, app, sealed)
	s.Equal(1, s.kms.Unwraps())
}

// TestOneAppsSlowUnwrapDoesNotHoldUpAnotherApp: apps share Keys but not each other's unwrap.
func (s *AppKeysSuite) TestOneAppsSlowUnwrapDoesNotHoldUpAnotherApp() {
	held, heldSealed := s.sealedSecret()
	s.opens(s.keys, held, heldSealed)
	slow, slowSealed := s.sealedSecret()
	s.kms.Hold()
	done := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(s.T().Context(), slow, 1, slowSealed, []byte("row"))
		done <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 2 }, 5*time.Second, time.Millisecond)

	s.opens(s.keys, held, heldSealed)
	s.kms.Release()
	s.NoError(<-done)
}

func (s *AppKeysSuite) TestAnotherAppsHeldKeyNeverStandsInForAnAppsOwn() {
	held, heldSealed := s.sealedSecret()
	s.opens(s.keys, held, heldSealed)
	app, sealed := s.sealedSecret()
	s.kms.Fail(errors.New("kms: unavailable"))

	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.opens(s.keys, held, heldSealed)
}

// TestPastMaxKeysTheLeastRecentlyUsedKeyIsUnwrappedAgain: a held key opened since is kept,
// and the one opened longest ago is dropped.
func (s *AppKeysSuite) TestTwoVersionsOfOneAppsKeyAreHeldApart() {
	app := "app-" + rand.Text()
	v1 := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret one"), []byte("row"))
	v2 := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 2), []byte("secret two"), []byte("row"))

	opened, err := s.keys.Open(s.T().Context(), app, 1, v1, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret one", string(opened))
	opened, err = s.keys.Open(s.T().Context(), app, 2, v2, []byte("row"))
	s.Require().NoError(err, "version 2 opens under version 2's key, not the held version 1")
	s.Equal("secret two", string(opened))
	s.Equal(2, s.kms.Unwraps())
}

func (s *AppKeysSuite) TestPastMaxKeysTheLeastRecentlyUsedKeyIsUnwrappedAgain() {
	keys := s.newKeys(appkeys.Options{MaxKeys: 2})
	first, firstSealed := s.sealedSecret()
	second, secondSealed := s.sealedSecret()
	third, thirdSealed := s.sealedSecret()
	s.opens(keys, first, firstSealed)
	s.opens(keys, second, secondSealed)
	s.opens(keys, first, firstSealed)
	s.opens(keys, third, thirdSealed)
	s.Equal(3, s.kms.Unwraps())

	s.opens(keys, first, firstSealed)
	s.Equal(3, s.kms.Unwraps())
	s.opens(keys, second, secondSealed)
	s.Equal(4, s.kms.Unwraps())
}

// TestEachUnwrapLogsOneLineThatNamesTheAppAndIsCountedWithoutIt: the counters carry no label,
// so no app becomes a series of its own.
// TestFailedOpensDoNotPushAHeldKeyOut: an app with no data key is not held, so opens of such
// apps do not push other apps' keys out of the bound.
func (s *AppKeysSuite) TestFailedOpensDoNotPushAHeldKeyOut() {
	keys := s.newKeys(appkeys.Options{MaxKeys: 2})
	app, sealed := s.sealedSecret()
	s.opens(keys, app, sealed)
	for range 2 {
		_, err := keys.Open(s.T().Context(), "app-"+rand.Text(), 1, sealed, []byte("row"))
		s.Require().ErrorIs(err, appkeys.ErrDoesNotOpen)
	}
	s.opens(keys, app, sealed)
	s.Equal(1, s.kms.Unwraps())
}

// TestAFailedUnwrapOfAKeyPushedOutLeavesTheKeyHeldAgain: a key pushed out of the bound
// while its unwrap runs is held again by a later open. When the first unwrap then fails, it
// must not drop the key the later open holds.
func (s *AppKeysSuite) TestAFailedUnwrapOfAKeyPushedOutLeavesTheKeyHeldAgain() {
	keys := s.newKeys(appkeys.Options{MaxKeys: 1})
	app, sealed := s.sealedSecret()
	s.kms.Hold()
	s.kms.Fail(errors.New("kms: unavailable"))
	first := make(chan error, 1)
	go func() {
		_, err := keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		first <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 1 }, 5*time.Second, time.Millisecond)
	s.kms.Fail(nil)
	_, err := keys.Open(s.T().Context(), "app-"+rand.Text(), 1, sealed, []byte("row"))
	s.Require().ErrorIs(err, appkeys.ErrDoesNotOpen)
	second := make(chan error, 1)
	go func() {
		_, err := keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		second <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 2 }, 5*time.Second, time.Millisecond)
	s.kms.Release()
	s.ErrorIs(<-first, appkeys.ErrUnavailable)
	s.NoError(<-second)
	s.opens(keys, app, sealed)
	s.Equal(2, s.kms.Unwraps())
}

// TestOpensWaitingOnAFailedUnwrapAreUnavailable: the opens that wait on an unwrap get its
// error, so the API answers them with its 503 like the open that asked KMS.
func (s *AppKeysSuite) TestOpensWaitingOnAFailedUnwrapAreUnavailable() {
	app, sealed := s.sealedSecret()
	s.kms.Hold()
	s.kms.Fail(errors.New("kms: unavailable"))
	var wg sync.WaitGroup
	failed := make([]error, 16)
	for i := range failed {
		wg.Go(func() { _, failed[i] = s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row")) })
	}
	s.Require().Eventually(func() bool { return s.kms.Unwraps() >= 1 }, 5*time.Second, time.Millisecond)
	s.kms.Release()
	wg.Wait()
	for _, err := range failed {
		s.ErrorIs(err, appkeys.ErrUnavailable)
	}
}

func (s *AppKeysSuite) TestEachUnwrapLogsOneLineThatNamesTheAppAndIsCountedWithoutIt() {
	app, sealed := s.sealedSecret()
	unwraps, kmsErrors := s.counter("router_app_key_unwraps_total"), s.counter("router_app_key_kms_errors_total")

	s.opens(s.keys, app, sealed)
	s.opens(s.keys, app, sealed)
	s.advance(ttl)
	s.opens(s.keys, app, sealed)
	s.Equal(2, s.logLines("unwrapped an app's data key"))
	s.Equal(2, strings.Count(s.logs.String(), "app_id="+app))
	s.Equal(unwraps+2, s.counter("router_app_key_unwraps_total"))
	s.Equal(kmsErrors, s.counter("router_app_key_kms_errors_total"))

	s.kms.Fail(errors.New("kms: unavailable"))
	other, otherSealed := s.sealedSecret()
	_, err := s.keys.Open(s.T().Context(), other, 1, otherSealed, []byte("row"))
	s.Require().ErrorIs(err, appkeys.ErrUnavailable)
	s.Equal(unwraps+3, s.counter("router_app_key_unwraps_total"))
	s.Equal(kmsErrors+1, s.counter("router_app_key_kms_errors_total"))
	s.NotContains(s.logs.String(), "app_id="+other, "a failure with nothing held is the caller's to log")
}

// sealedSecret makes an app with data key v1 and returns it with "secret" sealed under that
// key with the AAD "row".
func (s *AppKeysSuite) sealedSecret() (string, []byte) {
	app := "app-" + rand.Text()
	return app, appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, s.kms, app, 1), []byte("secret"), []byte("row"))
}

// opens checks that keys opens sealed, a blob from sealedSecret, to "secret".
func (s *AppKeysSuite) opens(keys *appkeys.Keys, app string, sealed []byte) {
	s.T().Helper()
	opened, err := keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
}

func (s *AppKeysSuite) logLines(message string) int {
	return strings.Count(s.logs.String(), `msg="`+message)
}

// counter reads one of appkeys' counters, and fails if it carries a label.
func (s *AppKeysSuite) counter(name string) float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	s.Require().NoError(err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		s.Require().Len(family.GetMetric(), 1)
		s.Empty(family.GetMetric()[0].GetLabel())
		return family.GetMetric()[0].GetCounter().GetValue()
	}
	s.Require().Failf("no such counter", "%s is not registered", name)
	return 0
}

// copyWrappedKey puts one stored wrapped key in place of another, as somebody with write
// access to the table could.
func (s *AppKeysSuite) copyWrappedKey(fromApp string, fromVersion int, toApp string, toVersion int) {
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(), "UPDATE app_data_keys SET wrapped_key ="+
		" (SELECT wrapped_key FROM app_data_keys WHERE customer_id = ? AND version = ?) WHERE customer_id = ? AND version = ?",
		fromApp, fromVersion, toApp, toVersion)
	s.Require().NoError(err)
}
