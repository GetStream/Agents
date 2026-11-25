//go:build integration

package store

import (
	"errors"
	"strconv"
	"sync"
	"time"
)

// attempt stores an open consent attempt for a connection under state.
func (s *StoreSuite) attempt(connection ConnectorConnection, state string) ConnectorAuthorizationAttempt {
	attempt := &ConnectorAuthorizationAttempt{
		ID:            newID(),
		CustomerID:    connection.CustomerID,
		ConnectionID:  connection.ID,
		Kind:          AttemptConsent,
		StateHash:     AuthorizationStateHash(state),
		AttemptSealed: []byte("sealed attempt"),
		KEKVersion:    1,
		ExpiresAt:     time.Now().Add(10 * time.Minute),
	}
	s.Require().NoError(s.store.ConnectorAuthorizationAttempts.Insert(s.ctx, attempt))
	return *attempt
}

// expire moves an attempt's expiry into the past, which no caller can ask for.
func (s *StoreSuite) expire(id string) {
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE connector_authorization_attempts SET expires_at = now() - interval '1 second' WHERE id = ?", id)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestADeletedConnectionsAttemptCannotBeReadOrConsumed() {
	connection := s.connection("acme-app", nil)
	attempt := s.attempt(connection, "state-of-a-deleted-connection")
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	_, err := s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "state-of-a-deleted-connection")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
	_, err = s.store.ConnectorAuthorizationAttempts.Get(s.ctx, attempt.ID)
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
	_, err = s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "state-of-a-deleted-connection")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestADeletedConnectionTakesNoNewAttempt() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	err := s.store.ConnectorAuthorizationAttempts.Insert(s.ctx, &ConnectorAuthorizationAttempt{
		ID: newID(), CustomerID: "acme-app", ConnectionID: connection.ID, Kind: AttemptReconnect,
		StateHash: AuthorizationStateHash("too-late"), AttemptSealed: []byte("sealed"), KEKVersion: 1,
		ExpiresAt: time.Now().Add(time.Minute),
	})

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestAnAttemptIsFoundByItsStateAndByItsID() {
	connection := s.connection("acme-app", nil)
	created := s.attempt(connection, "the-state")

	byState, err := s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "the-state")
	s.Require().NoError(err)
	s.Equal(created.ID, byState.ID)
	s.Equal(AttemptConsent, byState.Kind)
	s.Equal([]byte("sealed attempt"), byState.AttemptSealed)
	s.Equal(1, byState.KEKVersion)
	s.Equal(connection.ID, byState.ConnectionID)
	s.Nil(byState.ConsumedAt)

	byID, err := s.store.ConnectorAuthorizationAttempts.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(created.ID, byID.ID)
}

func (s *StoreSuite) TestAnAttemptIsNotFoundByAnotherState() {
	s.attempt(s.connection("acme-app", nil), "the-state")

	_, err := s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "a-guessed-state")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestAConsumedAttemptCannotBeConsumedOrReadAgain() {
	created := s.attempt(s.connection("acme-app", nil), "the-state")

	consumed, err := s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "the-state")
	s.Require().NoError(err)
	s.Equal(created.ID, consumed.ID)
	s.NotNil(consumed.ConsumedAt)

	_, err = s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "the-state")
	s.ErrorIs(err, ErrNoAuthorizationAttempt, "a replayed callback is refused")
	_, err = s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "the-state")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
	_, err = s.store.ConnectorAuthorizationAttempts.Get(s.ctx, created.ID)
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestAnExpiredAttemptCannotBeReadOrConsumed() {
	created := s.attempt(s.connection("acme-app", nil), "the-state")
	s.expire(created.ID)

	_, err := s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "the-state")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
	_, err = s.store.ConnectorAuthorizationAttempts.Get(s.ctx, created.ID)
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
	_, err = s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "the-state")
	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestCreatingAnAttemptDeletesOnlyTheCustomersExpiredOnes() {
	mine := s.connection("acme-app", nil)
	theirs := s.connection("other-app", nil)
	expired := s.attempt(mine, "expired")
	open := s.attempt(mine, "open")
	theirsExpired := s.attempt(theirs, "theirs")
	s.expire(expired.ID)
	s.expire(theirsExpired.ID)

	fresh := s.attempt(mine, "new")

	var left []string
	s.Require().NoError(s.store.GlobalDB().NewSelect().Table("connector_authorization_attempts").
		Column("id").Scan(s.ctx, &left))
	s.ElementsMatch([]string{open.ID, theirsExpired.ID, fresh.ID}, left,
		"another customer's expired attempt waits for that customer's next one")
}

func (s *StoreSuite) TestAnAttemptForAnotherCustomersConnectionIsRefused() {
	theirs := s.connection("other-app", nil)

	err := s.store.ConnectorAuthorizationAttempts.Insert(s.ctx, &ConnectorAuthorizationAttempt{
		ID: newID(), CustomerID: "acme-app", ConnectionID: theirs.ID, Kind: AttemptConsent,
		StateHash: AuthorizationStateHash("mine"), AttemptSealed: []byte("sealed"), KEKVersion: 1,
		ExpiresAt: time.Now().Add(time.Minute),
	})

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestCallbacksRacingWithOneStateConsumeItExactlyOnce() {
	s.attempt(s.connection("acme-app", nil), "raced-state")

	// Each callback is a router of its own with a pool of its own, connected before they
	// all start at once, so the consumes overlap in Postgres rather than queue on one pool.
	const callbacks = 16
	routers := make([]*Store, callbacks)
	for i := range routers {
		router, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { router.Close() })
		s.Require().NoError(router.Ping(s.ctx))
		routers[i] = router
	}
	errs := make([]error, callbacks)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = router.ConnectorAuthorizationAttempts.Consume(s.ctx, "raced-state")
		}()
	}
	close(start)
	wg.Wait()

	won := 0
	for _, err := range errs {
		if err == nil {
			won++
			continue
		}
		s.True(errors.Is(err, ErrNoAuthorizationAttempt), "a callback that loses is told the state is used: %v", err)
	}
	s.Equal(1, won)
}

func (s *StoreSuite) TestAHandoffReplacesTheBlobItWasReadWithOnce() {
	created := s.attempt(s.connection("acme-app", nil), "the-state")

	s.Require().NoError(s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("handed off"), 2))

	handedOff, err := s.store.ConnectorAuthorizationAttempts.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal([]byte("handed off"), handedOff.AttemptSealed)
	s.Equal(2, handedOff.KEKVersion)
	s.Nil(handedOff.ConsumedAt, "the callback still consumes it")
	err = s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("again"), 2)
	s.ErrorIs(err, ErrNoAuthorizationAttempt, "the blob it was read with is gone")
}

func (s *StoreSuite) TestAnExpiredAttemptCannotBeHandedOff() {
	created := s.attempt(s.connection("acme-app", nil), "the-state")
	s.expire(created.ID)

	err := s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("handed off"), 1)

	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestAConsumedAttemptCannotBeHandedOff() {
	created := s.attempt(s.connection("acme-app", nil), "the-state")
	_, err := s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "the-state")
	s.Require().NoError(err)

	err = s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("handed off"), 1)

	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestADeletedConnectionsAttemptCannotBeHandedOff() {
	connection := s.connection("acme-app", nil)
	created := s.attempt(connection, "the-state")
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	err := s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("handed off"), 1)

	s.ErrorIs(err, ErrNoAuthorizationAttempt)
}

func (s *StoreSuite) TestHandoffsRacingFromOneReadReplaceTheBlobExactlyOnce() {
	created := s.attempt(s.connection("acme-app", nil), "raced-state")

	// As TestCallbacksRacingWithOneStateConsumeItExactlyOnce: a router and a pool per
	// handoff, so the updates overlap in Postgres.
	const handoffs = 16
	routers := make([]*Store, handoffs)
	for i := range routers {
		router, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { router.Close() })
		s.Require().NoError(router.Ping(s.ctx))
		routers[i] = router
	}
	errs := make([]error, handoffs)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = router.ConnectorAuthorizationAttempts.HandOff(s.ctx, created.ID, created.AttemptSealed, []byte("handed off "+newID()), 1)
		}()
	}
	close(start)
	wg.Wait()

	won := 0
	for _, err := range errs {
		if err == nil {
			won++
			continue
		}
		s.True(errors.Is(err, ErrNoAuthorizationAttempt), "a handoff that loses is told the attempt is handed off: %v", err)
	}
	s.Equal(1, won)
}

func (s *StoreSuite) TestConnectorAuthorizationAttemptQueryBudgets() {
	connection := s.connection("acme-app", nil)
	for i := range 30 {
		s.expire(s.attempt(connection, "expired-"+strconv.Itoa(i)).ID)
	}

	s.Equal(3, s.countQueries(func() { s.attempt(connection, "consume") }), "Insert")
	handed := s.attempt(connection, "hand-off")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorAuthorizationAttempts.GetByState(s.ctx, "hand-off")
		s.Require().NoError(err)
	}), "GetByState")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorAuthorizationAttempts.Get(s.ctx, handed.ID)
		s.Require().NoError(err)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectorAuthorizationAttempts.HandOff(s.ctx, handed.ID, handed.AttemptSealed, []byte("next"), 1))
	}), "HandOff")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectorAuthorizationAttempts.Consume(s.ctx, "consume")
		s.Require().NoError(err)
	}), "Consume")
}
