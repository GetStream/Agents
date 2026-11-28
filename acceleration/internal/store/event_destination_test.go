//go:build integration

package store

import (
	"strconv"
	"strings"
	"time"
)

// eventDestination stores a destination of customer's for connector, forwarding forward.
func (s *StoreSuite) eventDestination(customerID, connectorID, id, forward string) EventDestination {
	destination := EventDestination{
		ID: id, CustomerID: customerID, ConnectorID: connectorID, URL: "https://hooks.example.com/" + id,
		Forward: forward, SecretSealed: []byte("sealed-" + id), KEKVersion: 1,
	}
	s.Require().NoError(s.store.EventDestinations.Insert(s.ctx, &destination))
	return destination
}

// queued are the destinations a delivery id is queued for.
func (s *StoreSuite) queued(id string) []string {
	var destinations []string
	rows, err := s.store.GlobalDB().QueryContext(s.ctx, "SELECT destination_id FROM connector_event_deliveries WHERE id = ? ORDER BY destination_id", id)
	s.Require().NoError(err)
	defer rows.Close() //nolint:errcheck // read to the end below
	for rows.Next() {
		var destination string
		s.Require().NoError(rows.Scan(&destination))
		destinations = append(destinations, destination)
	}
	s.Require().NoError(rows.Err())
	return destinations
}

func (s *StoreSuite) TestAFourthEventDestinationOfAConnectorIsRefused() {
	for _, id := range []string{"one", "two", "three"} {
		s.eventDestination("acme-app", "slack_bot", id, ForwardAll)
	}
	s.eventDestination("acme-app", "linear", "another-connector", ForwardAll)

	fourth := EventDestination{
		ID: "four", CustomerID: "acme-app", ConnectorID: "slack_bot", URL: "https://hooks.example.com/four",
		Forward: ForwardAll, SecretSealed: []byte("sealed"), KEKVersion: 1,
	}
	s.ErrorIs(s.store.EventDestinations.Insert(s.ctx, &fourth), ErrEventDestinationsFull)
}

func (s *StoreSuite) TestAnEventDestinationIsOnlyItsOwnCustomers() {
	s.eventDestination("acme-app", "slack_bot", "acme-hook", ForwardAll)

	listed, err := s.store.EventDestinations.Select(s.ctx, "other-app", EventDestinationFilter{ConnectorID: "slack_bot", Limit: 0, After: nil})
	s.Require().NoError(err)
	s.Empty(listed)
	s.ErrorIs(s.store.EventDestinations.Delete(s.ctx, "other-app", "slack_bot", "acme-hook"), ErrNoEventDestination)
	_, err = s.store.EventDestinations.RotateSecret(s.ctx, "other-app", "slack_bot", "acme-hook", []byte("theirs"), 1, nil, s.base)
	s.ErrorIs(err, ErrNoEventDestination)
}

func (s *StoreSuite) TestAHandledDeliveryIsQueuedOnlyForDestinationsOfEveryEvent() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	s.eventDestination("acme-app", "slack_bot", "unhandled", ForwardUnhandled)
	s.eventDestination("other-app", "slack_bot", "theirs", ForwardAll)

	queued, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll}, EventDelivery{ID: "msg_handled", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)
	s.Equal(1, queued)
	s.Equal([]string{"every"}, s.queued("msg_handled"))

	queued, err = s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, EventDelivery{ID: "msg_unhandled", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)
	s.Equal(2, queued)
	s.Equal([]string{"every", "unhandled"}, s.queued("msg_unhandled"))
}

func (s *StoreSuite) TestADeliveryQueuedTwiceUnderOneIDIsOneRow() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	delivery := EventDelivery{ID: "msg_same", Headers: map[string]string{"X-Slack-Signature": "v0=abc"}, Body: []byte("{}"), NextAttemptAt: s.base}

	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, delivery)
	s.Require().NoError(err)
	queued, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, delivery)

	s.Require().NoError(err)
	s.Zero(queued)
	s.Len(s.queued("msg_same"), 1)
}

func (s *StoreSuite) TestAClaimedDeliveryIsNotClaimedAgainUntilItsLeaseRunsOut() {
	destination := s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, EventDelivery{
		ID: "msg_one", Headers: map[string]string{"Content-Type": "application/json"}, Body: []byte(`{"a":1}`), NextAttemptAt: s.base,
	})

	s.Require().NoError(err)

	claimed, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(destination.URL, claimed[0].URL)
	s.Equal([]byte(`{"a":1}`), claimed[0].Body)
	s.Equal(map[string]string{"Content-Type": "application/json"}, claimed[0].Headers)
	s.Equal(destination.SecretSealed, claimed[0].SecretSealed)
	s.InDelta(time.Minute, s.leftOnTheDatabasesClock(claimed[0].NextAttemptAt), float64(time.Second), "a minute on Postgres's clock")

	s.timePasses("connector_event_deliveries", 55*time.Second)
	again, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, 2*time.Minute)
	s.Require().NoError(err)
	s.Empty(again, "another worker leaves it while the lease runs")
	s.timePasses("connector_event_deliveries", 5*time.Second)
	after, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, 2*time.Minute)
	s.Require().NoError(err)
	s.Len(after, 1, "a stopped worker's delivery is taken once its lease ran out")
}

// AI-924: a destination already sending one of its two takes one more, and another
// destination's delivery is taken past the rest of its own.
func (s *StoreSuite) TestAClaimTakesAtMostPerDestinationLessWhatIsBeingSent() {
	s.eventDestination("acme-app", "slack_bot", "slow", ForwardAll)
	s.eventDestination("globex-app", "slack_bot", "other", ForwardAll)
	for _, id := range []string{"msg_1", "msg_2", "msg_3"} {
		_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll}, EventDelivery{ID: id, Body: []byte("{}"), NextAttemptAt: s.base})
		s.Require().NoError(err)
	}
	_, err := s.store.EventDeliveries.Queue(s.ctx, "globex-app", "slack_bot", []string{ForwardAll},
		EventDelivery{ID: "msg_other", Body: []byte("{}"), NextAttemptAt: s.base.Add(time.Second)})

	s.Require().NoError(err)

	claimed, err := s.store.EventDeliveries.Claim(s.ctx, 10, 2, map[string]int{"slow": 1}, time.Minute)

	s.Require().NoError(err)
	taken := map[string]int{}
	for _, delivery := range claimed {
		taken[delivery.DestinationID]++
	}
	s.Equal(map[string]int{"slow": 1, "other": 1}, taken)
}

// Two routers claim one backlog at once. Router A's claim pauses after the rows it ranks were
// read, while router B's claim takes them and commits: A must then skip what B leased, not
// lease it again (the review of #778). The pause is pg_sleep in A's statement, so B's commit
// lands inside A's statement every run.
func (s *StoreSuite) TestTwoRoutersClaimingAtOnceTakeEachDeliveryOnce() {
	for _, id := range []string{"one", "two", "three"} {
		s.eventDestination("acme-app", "slack_bot", id, ForwardAll)
	}
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll}, EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)
	routerA, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(routerA.Close()) })
	ranked := "    WHERE cedl.next_attempt_at <= now()\n), due AS ("
	s.Require().Contains(claimEventDeliveriesQuery, ranked)
	paused := strings.Replace(claimEventDeliveriesQuery, ranked,
		"    WHERE cedl.next_attempt_at <= now() AND (SELECT 1 FROM pg_sleep(0.5)) = 1\n), due AS (", 1)

	type claim struct {
		claimed []ClaimedEventDelivery
		err     error
	}
	byA := make(chan claim, 1)
	go func() {
		claimed, err := routerA.EventDeliveries.claim(s.ctx, paused, 10, 2, nil, time.Minute)
		byA <- claim{claimed, err}
	}()
	s.Require().Eventually(func() bool {
		var sleeping int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND datname = current_database()").Scan(&sleeping))
		return sleeping == 1
	}, 5*time.Second, 10*time.Millisecond, "router A's claim is in its pause")
	byB, err := s.store.EventDeliveries.Claim(s.ctx, 10, 2, nil, time.Minute)
	s.Require().NoError(err)
	a := <-byA
	s.Require().NoError(a.err)

	s.Len(byB, 3)
	s.Empty(a.claimed, "router A takes none of what router B leased")
}

func (s *StoreSuite) TestTheNextDeliveryIsTheOneDueFirstAndNoneWhenNothingIsQueued() {
	_, queued, err := s.store.EventDeliveries.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(queued)
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	for i, id := range []string{"msg_late", "msg_early"} {
		_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll},
			EventDelivery{ID: id, Body: []byte("{}"), NextAttemptAt: s.base.Add(time.Duration(1-i) * time.Minute)})

		s.Require().NoError(err)
	}

	next, queued, err := s.store.EventDeliveries.NextDue(s.ctx)

	s.Require().NoError(err)
	s.True(queued)
	s.True(next.Equal(s.base), "%s is not %s", next, s.base)
}

// AI-924: the store keeps until when the provider's own headers verify, for the worker to read.
func (s *StoreSuite) TestAClaimedDeliveryKeepsUntilWhenItsProviderHeadersVerify() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	until := s.base.Add(5 * time.Minute)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll},
		EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base, ProviderHeadersUntil: &until})

	s.Require().NoError(err)

	claimed, err := s.store.EventDeliveries.Claim(s.ctx, 10, 2, nil, time.Minute)

	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Require().NotNil(claimed[0].ProviderHeadersUntil)
	s.True(claimed[0].ProviderHeadersUntil.Equal(until))
}

func (s *StoreSuite) TestARetriedDeliveryIsDueAtItsNextAttempt() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)

	s.Require().NoError(s.store.EventDeliveries.Retry(s.ctx, "every", "msg_one", time.Now().Add(time.Hour)))

	early, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Minute)
	s.Require().NoError(err)
	s.Empty(early)
	s.timePasses("connector_event_deliveries", time.Hour+time.Minute)
	due, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(due, 1)
	s.Equal(1, due[0].Attempts, "the claim that took it, as Retry counts none")
}

// A forwarder that dies between its claim and its send records nothing else, so the claim is
// what counts the attempt; a release, of a claim that sent nothing, takes it back.
func (s *StoreSuite) TestAClaimCountsItsAttemptAndAReleaseTakesItBack() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll}, EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)

	first, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, 0)
	s.Require().NoError(err)
	s.Require().Len(first, 1)
	second, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Hour)
	s.Require().NoError(err)
	s.Require().Len(second, 1)
	s.Equal([]int{1, 2}, []int{first[0].Attempts, second[0].Attempts})

	s.Require().NoError(s.store.EventDeliveries.Release(s.ctx, second[0], 0))

	again, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Hour)
	s.Require().NoError(err)
	s.Require().Len(again, 1, "due again after the release's wait, though the last claim's lease runs an hour")
	s.Equal(2, again[0].Attempts, "the released claim is not counted")
}

// A release names the claim it gives back: one whose lease ran out, and whose row another
// router claimed since, leaves the newer claim alone.
func (s *StoreSuite) TestAReleaseOfAClaimTakenOverChangesNothing() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll}, EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)
	stale, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, 0)
	s.Require().NoError(err)
	s.Require().Len(stale, 1)
	current, err := s.store.EventDeliveries.Claim(s.ctx, 10, 10, nil, time.Hour)
	s.Require().NoError(err)
	s.Require().Len(current, 1)

	s.Require().NoError(s.store.EventDeliveries.Release(s.ctx, stale[0], 0))

	var held EventDelivery
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&held).Where("id = ?", "msg_one").Scan(s.ctx))
	s.Equal(2, held.Attempts, "the current claim's count")
	s.True(held.NextAttemptAt.Equal(current[0].NextAttemptAt), "the current claim's lease: %s", held.NextAttemptAt)
}

func (s *StoreSuite) TestDeletingADestinationDropsItsPendingDeliveries() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)
	_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "slack_bot", []string{ForwardAll, ForwardUnhandled}, EventDelivery{ID: "msg_one", Body: []byte("{}"), NextAttemptAt: s.base})
	s.Require().NoError(err)

	s.Require().NoError(s.store.EventDestinations.Delete(s.ctx, "acme-app", "slack_bot", "every"))

	s.Empty(s.queued("msg_one"))
}

func (s *StoreSuite) TestARotationKeepsTheReplacedSecretUntilItsTime() {
	s.eventDestination("acme-app", "slack_bot", "every", ForwardAll)

	rotated, err := s.store.EventDestinations.RotateSecret(s.ctx, "acme-app", "slack_bot", "every", []byte("sealed-new"), 2, nil, s.base)

	s.Require().NoError(err)
	s.Equal([]byte("sealed-new"), rotated.SecretSealed)
	s.Equal(2, rotated.KEKVersion)
	s.Equal([]byte("sealed-every"), rotated.PreviousSecretSealed)
	s.Equal(1, rotated.PreviousKEKVersion)
	s.Require().NotNil(rotated.PreviousUntil)
	s.True(rotated.PreviousUntil.Equal(s.base))
}

func (s *StoreSuite) TestEventDestinationsArePagedNewestFirst() {
	s.eventDestination("acme-app", "slack_bot", "older", ForwardAll)
	s.eventDestination("acme-app", "slack_bot", "newer", ForwardAll)

	first, err := s.store.EventDestinations.Select(s.ctx, "acme-app", EventDestinationFilter{ConnectorID: "slack_bot", Limit: 1, After: nil})
	s.Require().NoError(err)
	s.Require().Len(first, 2, "one more than the page, to tell there is another")
	s.Equal("newer", first[0].ID)
	next, err := s.store.EventDestinations.Select(s.ctx, "acme-app", EventDestinationFilter{ConnectorID: "slack_bot", Limit: 1, After: &EventDestinationPosition{CreatedAt: first[0].CreatedAt, ID: first[0].ID}})
	s.Require().NoError(err)
	s.Require().Len(next, 1)
	s.Equal("older", next[0].ID)
}

func (s *StoreSuite) TestEventDestinationQueryBudgets() {
	for i := range MaxEventDestinations - 1 {
		s.eventDestination("acme-app", "github", "destination-"+strconv.Itoa(i), ForwardAll)
	}

	s.Equal(5, s.countQueries(func() { s.eventDestination("acme-app", "github", "last", ForwardAll) }), "Insert")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.EventDestinations.Select(s.ctx, "acme-app", EventDestinationFilter{ConnectorID: "github", Limit: 2})
		s.Require().NoError(err)
		s.Len(listed, 3)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.EventDestinations.RotateSecret(s.ctx, "acme-app", "github", "last", []byte("rotated"), 1, nil, s.base.Add(time.Hour))
		s.Require().NoError(err)
	}), "RotateSecret")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.EventDestinations.Delete(s.ctx, "acme-app", "github", "last")) }), "Delete")
}

func (s *StoreSuite) TestEventDeliveryQueryBudgets() {
	for i := range MaxEventDestinations {
		s.eventDestination("acme-app", "github", "destination-"+strconv.Itoa(i), ForwardAll)
	}
	delivery := func(id string) EventDelivery {
		return EventDelivery{ID: id, Headers: map[string]string{}, Body: []byte(`{}`), NextAttemptAt: s.base}
	}
	for i := range 10 {
		_, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "github", []string{ForwardAll}, delivery("msg-"+strconv.Itoa(i)))
		s.Require().NoError(err)
	}

	s.Equal(1, s.countQueries(func() {
		queued, err := s.store.EventDeliveries.Queue(s.ctx, "acme-app", "github", []string{ForwardAll}, delivery("msg-last"))
		s.Require().NoError(err)
		s.Equal(MaxEventDestinations, queued)
	}), "Queue")
	var claimed []ClaimedEventDelivery
	s.Equal(1, s.countQueries(func() {
		var err error
		claimed, err = s.store.EventDeliveries.Claim(s.ctx, 20, 2, map[string]int{}, time.Minute)
		s.Require().NoError(err)
		s.Len(claimed, 2*MaxEventDestinations, "two of each destination")
	}), "Claim")
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.EventDeliveries.NextDue(s.ctx)
		s.Require().NoError(err)
	}), "NextDue")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.EventDeliveries.Retry(s.ctx, claimed[0].DestinationID, claimed[0].ID, s.base.Add(time.Hour)))
	}), "Retry")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.EventDeliveries.Finish(s.ctx, claimed[1].DestinationID, claimed[1].ID))
	}), "Finish")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.EventDeliveries.Release(s.ctx, claimed[2], time.Minute))
	}), "Release")
}

// sealedAs stores a destination of customerID whose current and previous secrets both hold
// blob, under key version 1.
func (s *StoreSuite) sealedAs(customerID, id string, blob []byte) {
	destination := EventDestination{
		ID: id, CustomerID: customerID, ConnectorID: "slack_bot", URL: "https://hooks.example.com/" + id,
		Forward: ForwardAll, SecretSealed: blob, KEKVersion: 1,
	}
	s.Require().NoError(s.store.EventDestinations.Insert(s.ctx, &destination))
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE connector_event_destinations SET previous_secret_sealed = ?, previous_kek_version = 1 WHERE id = ?", blob, id)
	s.Require().NoError(err)
}

// storedDestination reads a destination as the table holds it.
func (s *StoreSuite) storedDestination(id string) EventDestination {
	var destination EventDestination
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&destination).Where("id = ?", id).Scan(s.ctx))
	return destination
}

// A reseal writes only the destination it names, and only while that destination still holds
// the secret it read. The other rows hold the same blob, one of the same app and one of
// another, so a reseal that matched on the blob or the app alone would change them too.
func (s *StoreSuite) TestAResealChangesOnlyTheSecretItRead() {
	blob := []byte("sealed-under-the-routers-key")
	s.sealedAs("acme-app", "target", blob)
	s.sealedAs("acme-app", "sibling", blob)
	s.sealedAs("other-app", "theirs", blob)
	reseals := map[string]struct {
		reseal                     func(id string, was, sealed []byte) (bool, error)
		secret                     func(EventDestination) []byte
		kekVersion, dataKeyVersion func(EventDestination) any
		other                      func(EventDestination) []byte
	}{
		"current": {
			reseal: func(id string, was, sealed []byte) (bool, error) {
				return s.store.EventDestinations.ResealSecret(s.ctx, id, was, sealed, 2, 1)
			},
			secret:         func(d EventDestination) []byte { return d.SecretSealed },
			kekVersion:     func(d EventDestination) any { return d.KEKVersion },
			dataKeyVersion: func(d EventDestination) any { return d.SecretDataKeyVersion },
			other:          func(d EventDestination) []byte { return d.PreviousSecretSealed },
		},
		"previous": {
			reseal: func(id string, was, sealed []byte) (bool, error) {
				return s.store.EventDestinations.ResealPreviousSecret(s.ctx, id, was, sealed, 2, 1)
			},
			secret:         func(d EventDestination) []byte { return d.PreviousSecretSealed },
			kekVersion:     func(d EventDestination) any { return d.PreviousKEKVersion },
			dataKeyVersion: func(d EventDestination) any { return d.PreviousSecretDataKeyVersion },
			other:          func(d EventDestination) []byte { return d.SecretSealed },
		},
	}
	for name, column := range reseals {
		s.Run(name, func() {
			resealed := []byte("sealed-under-the-data-key-" + name)
			applied, err := column.reseal("target", blob, resealed)
			s.Require().NoError(err)
			s.True(applied)

			target := s.storedDestination("target")
			s.Equal(resealed, column.secret(target))
			s.Equal(2, column.kekVersion(target))
			s.Equal(new(1), column.dataKeyVersion(target))
			s.Equal(blob, column.other(s.storedDestination("sibling")), "the app's other destination keeps its secret")
			for _, untouched := range []string{"sibling", "theirs"} {
				other := s.storedDestination(untouched)
				s.Equal(blob, column.secret(other), untouched)
				s.Equal(1, column.kekVersion(other), untouched)
				s.Nil(column.dataKeyVersion(other), untouched)
			}

			applied, err = column.reseal("target", blob, []byte("a later reseal"))
			s.Require().NoError(err)
			s.False(applied, "the secret it read was replaced meanwhile")
			s.Equal(resealed, column.secret(s.storedDestination("target")))
		})
	}
	s.Equal(blob, s.storedDestination("sibling").SecretSealed)

	closed, err := Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	_, err = closed.EventDestinations.ResealSecret(s.ctx, "sibling", blob, []byte("never written"), 2, 1)
	s.Error(err, "a write that failed is not reported as a secret replaced meanwhile")
}

// A rotation moves the data key version with the secret, so the previous secret still opens
// under the key that sealed it.
func (s *StoreSuite) TestARotationMovesTheDataKeyVersionWithTheSecret() {
	destination := EventDestination{
		ID: "keyed", CustomerID: "acme-app", ConnectorID: "slack_bot", URL: "https://hooks.example.com/keyed",
		Forward: ForwardAll, SecretSealed: []byte("sealed-under-the-data-key"), KEKVersion: 1, SecretDataKeyVersion: new(1),
	}
	s.Require().NoError(s.store.EventDestinations.Insert(s.ctx, &destination))

	rotated, err := s.store.EventDestinations.RotateSecret(s.ctx, "acme-app", "slack_bot", "keyed", []byte("sealed-under-the-routers-key"),
		1, nil, s.base)
	s.Require().NoError(err)
	s.Nil(rotated.SecretDataKeyVersion)
	s.Equal(new(1), rotated.PreviousSecretDataKeyVersion)

	rotated, err = s.store.EventDestinations.RotateSecret(s.ctx, "acme-app", "slack_bot", "keyed", []byte("sealed-under-the-data-key-again"),
		1, new(1), s.base)
	s.Require().NoError(err)
	s.Equal(new(1), rotated.SecretDataKeyVersion)
	s.Nil(rotated.PreviousSecretDataKeyVersion)
	s.Equal(rotated, s.storedDestination("keyed"))
}
