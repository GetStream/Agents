//go:build integration

package appkeys_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
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
	// logger writes to logs. Every Keys of a test shares it, since its handler is what keeps two
	// writes to logs apart.
	logger *slog.Logger
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
	s.logger = slog.New(slog.NewTextHandler(s.logs, nil))
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
	options.Logger = s.logger
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
	wrapped, err := s.kms.Wrap(s.T().Context(), []byte("seven b"), appkeystest.WrapAAD(app, 1))
	s.Require().NoError(err)
	_, err = s.db.AppDataKeys.InsertIfAbsent(s.T().Context(), &store.AppDataKey{CustomerID: app, Version: 1, WrappedKey: wrapped})
	s.Require().NoError(err)

	_, err = s.keys.Open(s.T().Context(), app, 1, make([]byte, 40), []byte("row"))
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
	s.Zero(s.logLines("KMS could not unwrap an app's data key"), "while the held key is used, the warning says so")
	s.advance(time.Nanosecond)
	_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, failure)
	s.Equal(1, s.logLines("KMS could not unwrap an app's data key"))

	s.kms.Fail(nil)
	s.opens(s.keys, app, sealed)
}

// TestAHeldKeyWhoseVersionTheStoreNoLongerHasIsNotUsedPastTheTTL: the store's answer that the
// version does not exist is not an outage, so the held key is not used up to the stale limit.
func (s *AppKeysSuite) TestAHeldKeyWhoseVersionTheStoreNoLongerHasIsNotUsedPastTheTTL() {
	app, sealed := s.sealedSecret()
	s.opens(s.keys, app, sealed)
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(), "DELETE FROM app_data_keys WHERE customer_id = ?", app)
	s.Require().NoError(err)

	s.advance(ttl)
	_, err = s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	_, err = s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	s.ErrorIs(err, appkeys.ErrDoesNotOpen, "the held key was dropped, not kept for the next open")
}

// TestAHeldKeyIsNotUsedWhenTheStaleLimitPassesDuringTheUnwrap: the stale limit is checked when
// the unwrap fails, not when it started.
func (s *AppKeysSuite) TestAHeldKeyIsNotUsedWhenTheStaleLimitPassesDuringTheUnwrap() {
	app, sealed := s.sealedSecret()
	s.opens(s.keys, app, sealed)
	s.advance(ttl)
	s.kms.Hold()
	s.kms.Fail(errors.New("kms: unavailable"))
	done := make(chan error, 1)
	go func() {
		_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		done <- err
	}()
	s.Require().Eventually(func() bool { return s.kms.Unwraps() == 2 }, 5*time.Second, time.Millisecond)
	s.advance(staleLimit)
	s.kms.Release()
	s.ErrorIs(<-done, appkeys.ErrUnavailable)
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

// TestAnUnwrapThatPanicsEndsForTheOpensThatWaitOnIt: the opens waiting on an unwrap that
// panicked fail, and the next open of that key unwraps it again.
func (s *AppKeysSuite) TestAnUnwrapThatPanicsEndsForTheOpensThatWaitOnIt() {
	app, sealed := s.sealedSecret()
	kms := &panickingKMS{KMS: s.kms, asked: make(chan struct{}), release: make(chan struct{})}
	keys, err := appkeys.New(s.db, kms, appkeys.Options{TTL: ttl, StaleLimit: staleLimit, KMSTimeout: 5 * time.Second})
	s.Require().NoError(err)
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_, _ = keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
	}()
	<-kms.asked

	waiter := &waitingContext{Context: s.T().Context(), waiting: make(chan struct{})}
	waited := make(chan error, 1)
	go func() {
		_, err := keys.Open(waiter, app, 1, sealed, []byte("row"))
		waited <- err
	}()
	<-waiter.waiting
	close(kms.release)
	s.NotNil(<-panicked, "the panic still reaches the open that asked KMS")
	s.Require().Eventually(func() bool { return len(waited) == 1 }, 5*time.Second, time.Millisecond)
	s.ErrorIs(<-waited, appkeys.ErrUnavailable)

	ctx, cancel := context.WithTimeout(s.T().Context(), time.Second)
	defer cancel()
	opened, err := keys.Open(ctx, app, 1, sealed, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
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

// TestPastMaxKeysTheLeastRecentlyUsedKeyIsUnwrappedAgain: a held key opened since is kept,
// and the one opened longest ago is dropped.
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

// TestASlowLogDoesNotHoldUpOpensOfAnotherApp: the unwrap writes its line after it lets go of
// the lock that every open takes.
func (s *AppKeysSuite) TestASlowLogDoesNotHoldUpOpensOfAnotherApp() {
	slow, slowSealed := s.sealedSecret()
	other, otherSealed := s.sealedSecret()
	log := &slowLog{app: slow, writing: make(chan struct{}), release: make(chan struct{})}
	s.T().Cleanup(func() { close(log.release) })
	keys, err := appkeys.New(s.db, s.kms, appkeys.Options{TTL: ttl, StaleLimit: staleLimit, KMSTimeout: 5 * time.Second,
		Now: s.clock, Logger: slog.New(log)})
	s.Require().NoError(err)
	s.opens(keys, other, otherSealed)

	go func() { _, _ = keys.Open(s.T().Context(), slow, 1, slowSealed, []byte("row")) }()
	<-log.writing
	opened := make(chan error, 1)
	go func() {
		_, err := keys.Open(s.T().Context(), other, 1, otherSealed, []byte("row"))
		opened <- err
	}()
	select {
	case err := <-opened:
		s.NoError(err)
	case <-time.After(time.Second):
		s.Fail("the open of another app waited on the slow log line")
	}
}

// TestEachUnwrapLogsOneLineThatNamesTheAppAndIsCountedWithoutIt: the counters carry no label,
// so no app becomes a series of its own.
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
}

// TestEachFailedUnwrapWithNothingHeldLogsOneErrorWithItsCause: the caller answers with text of
// its own, so the log line is the only place the KMS error shows. Each attempt writes one
// line, and the key itself is never in it.
func (s *AppKeysSuite) TestEachFailedUnwrapWithNothingHeldLogsOneErrorWithItsCause() {
	app := "app-" + rand.Text()
	key := appkeystest.AddKey(s.T(), s.db, s.kms, app, 1)
	sealed := appkeystest.Seal(s.T(), key, []byte("secret"), []byte("row"))
	s.kms.Fail(errors.New("kms: permission denied on the key"))

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := s.keys.Open(s.T().Context(), app, 1, sealed, []byte("row"))
		s.Require().ErrorIs(err, appkeys.ErrUnavailable)
		s.Equal(attempt, s.logLines("KMS could not unwrap an app's data key"))
	}
	s.Equal(2, strings.Count(s.logs.String(), `level=ERROR msg="KMS could not unwrap an app's data key" app_id=`+app+" version=1"))
	s.Contains(s.logs.String(), "kms: permission denied on the key")
	s.NotContains(s.logs.String(), base64.StdEncoding.EncodeToString(key))
	s.NotContains(s.logs.String(), hex.EncodeToString(key))

	_, err := s.keys.Open(s.T().Context(), app, 2, sealed, []byte("row"))
	s.Require().ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.Equal(2, s.logLines("KMS could not unwrap an app's data key"), "a key the store does not have is no KMS failure")
}

// TestTheFirstSealMakesTheAppsKeyAndAnOpenAfterItAsksKMSNothing: KMS wraps one key, and the Keys
// that made it holds it. Another replica unwraps it from the store.
func (s *AppKeysSuite) TestTheFirstSealMakesTheAppsKeyAndAnOpenAfterItAsksKMSNothing() {
	ctx, app := s.T().Context(), "app-"+rand.Text()
	first, version, err := s.keys.Seal(ctx, app, []byte("first"), []byte("row"))
	s.Require().NoError(err)
	s.Equal(1, version)
	second, _, err := s.keys.Seal(ctx, app, []byte("second"), []byte("row"))
	s.Require().NoError(err)
	s.Equal(1, s.kms.Wraps(), "the second seal uses the key the first one made")
	s.NotEqual(first[:12], second[:12], "every seal draws a nonce of its own")

	opened, err := s.keys.Open(ctx, app, 1, first, []byte("row"))
	s.Require().NoError(err)
	s.Equal("first", string(opened))
	s.Zero(s.kms.Unwraps())
	opened, err = s.newKeys(appkeys.Options{}).Open(ctx, app, 1, second, []byte("row"))
	s.Require().NoError(err)
	s.Equal("second", string(opened))
	s.Equal(1, s.kms.Unwraps())
	stored, err := s.db.AppDataKeys.Get(ctx, app, 1)
	s.Require().NoError(err)
	key, err := s.kms.Unwrap(ctx, stored.WrappedKey, appkeystest.WrapAAD(app, 1))
	s.Require().NoError(err)
	s.Len(key, 32, "AES-256")
	s.NotEqual(make([]byte, 32), key)
}

// TestFirstSealsRacingOnTwoReplicasEndWithOneKey: every writer wraps a key of its own before
// any is stored, the store keeps one, and every secret opens under it.
func (s *AppKeysSuite) TestFirstSealsRacingOnTwoReplicasEndWithOneKey() {
	ctx, app := s.T().Context(), "app-"+rand.Text()
	replicas := []*appkeys.Keys{s.keys, s.newKeys(appkeys.Options{})}
	const writers = 6
	sealed, failures := make([][]byte, writers), make([]error, writers)
	s.kms.Hold()
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			sealed[i], _, failures[i] = replicas[i%2].Seal(ctx, app, fmt.Appendf(nil, "secret %d", i), []byte("row"))
		})
	}
	s.Require().Eventually(func() bool { return s.kms.Wraps() == writers }, 5*time.Second, time.Millisecond)
	s.kms.Release()
	wg.Wait()

	var keys int
	s.Require().NoError(s.db.GlobalDB().QueryRowContext(ctx, "SELECT count(*) FROM app_data_keys WHERE customer_id = ?", app).Scan(&keys))
	s.Equal(1, keys)
	other := s.newKeys(appkeys.Options{})
	for i := range writers {
		s.Require().NoError(failures[i])
		opened, err := other.Open(ctx, app, 1, sealed[i], []byte("row"))
		s.Require().NoError(err)
		s.Equal(fmt.Sprintf("secret %d", i), string(opened))
	}
}

// TestASealThatKMSDoesNotWrapForFailsWithinTheDeadlineAndStoresNoKey: the app gets its key on
// a later seal, once KMS answers.
func (s *AppKeysSuite) TestASealThatKMSDoesNotWrapForFailsWithinTheDeadlineAndStoresNoKey() {
	ctx, app := s.T().Context(), "app-"+rand.Text()
	keys := s.newKeys(appkeys.Options{KMSTimeout: 50 * time.Millisecond})
	kmsErrors := s.counter("router_app_key_kms_errors_total")
	s.kms.Hold()
	start := time.Now()
	_, _, err := keys.Seal(ctx, app, []byte("secret"), []byte("row"))
	s.Less(time.Since(start), 5*time.Second)
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.kms.Release()
	s.kms.Fail(errors.New("kms: permission denied"))
	_, _, err = keys.Seal(ctx, app, []byte("secret"), []byte("row"))
	s.ErrorIs(err, appkeys.ErrUnavailable)
	s.Equal(kmsErrors+2, s.counter("router_app_key_kms_errors_total"))
	s.Equal(2, strings.Count(s.logs.String(), `level=ERROR msg="KMS could not wrap a new data key for an app" app_id=`+app+" version=1"))
	s.Contains(s.logs.String(), "kms: permission denied")
	_, err = s.db.AppDataKeys.Get(ctx, app, 1)
	s.ErrorIs(err, store.ErrNoAppDataKey)

	s.kms.Fail(nil)
	sealed, _, err := keys.Seal(ctx, app, []byte("secret"), []byte("row"))
	s.Require().NoError(err)
	opened, err := keys.Open(ctx, app, 1, sealed, []byte("row"))
	s.Require().NoError(err)
	s.Equal("secret", string(opened))
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

// slowLog is a log sink that holds the lines naming app until release is closed.
type slowLog struct {
	app     string
	writing chan struct{}
	release chan struct{}
}

func (l *slowLog) Enabled(context.Context, slog.Level) bool { return true }

func (l *slowLog) Handle(_ context.Context, record slog.Record) error {
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "app_id" && attr.Value.String() == l.app {
			close(l.writing)
			<-l.release
			return false
		}
		return true
	})
	return nil
}

func (l *slowLog) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *slowLog) WithGroup(string) slog.Handler { return l }

// panickingKMS panics in its first Unwrap once release is closed, as a KMS client with a bug
// would. Later Unwraps go to the KMS it wraps.
type panickingKMS struct {
	*appkeystest.KMS
	asked   chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (k *panickingKMS) Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error) {
	if k.calls.Add(1) == 1 {
		close(k.asked)
		<-k.release
		panic("appkeys_test: the KMS client panicked")
	}
	return k.KMS.Unwrap(ctx, ciphertext, aad)
}

// waitingContext closes waiting when an open first asks for Done, which an open does only
// when it waits on another open's unwrap.
type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
