//go:build integration

package eventforward

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/webhook"
)

// secretKind is the record label of the signing secrets, written out so a test fails when a
// rename would leave a dashboard without its series.
const secretKind = "connector_event_destination_secrets"

// errKMSDown is what the test KMS answers while it is down.
var errKMSDown = errors.New("kms: unavailable")

// dataKeys returns data keys wrapped by kms, with sealing on or off.
func (s *ForwarderSuite) dataKeys(kms *appkeystest.KMS, seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	return keys
}

// madeBy stores a destination of a new app whose secret router made, as a create through the
// API does, and returns it with the plain secret.
func (s *ForwarderSuite) madeBy(router *Forwarder, url string) (store.EventDestination, string) {
	destination := store.EventDestination{
		ID: uuid.NewString(), CustomerID: "customer-" + uuid.NewString(), ConnectorID: "slack_bot", URL: url, Forward: store.ForwardAll,
	}
	secret, err := router.NewSecret(s.ctx, destination.CustomerID, destination.ConnectorID, destination.ID)
	s.Require().NoError(err)
	destination.SecretSealed, destination.KEKVersion, destination.SecretDataKeyVersion = secret.Sealed, secret.Version, secret.DataKeyVersion
	s.Require().NoError(s.store.EventDestinations.Insert(s.ctx, &destination))
	return destination, secret.Plain
}

// rotatedBy rotates the destination to a secret router makes, as a rotation through the API
// does, and returns the new plain secret.
func (s *ForwarderSuite) rotatedBy(router *Forwarder, destination store.EventDestination) string {
	secret, err := router.NewSecret(s.ctx, destination.CustomerID, destination.ConnectorID, destination.ID)
	s.Require().NoError(err)
	_, err = s.store.EventDestinations.RotateSecret(s.ctx, destination.CustomerID, destination.ConnectorID, destination.ID,
		secret.Sealed, secret.Version, secret.DataKeyVersion, time.Now().Add(time.Hour))
	s.Require().NoError(err)
	return secret.Plain
}

// stored reads a destination as the table holds it.
func (s *ForwarderSuite) stored(id string) store.EventDestination {
	var destination store.EventDestination
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&destination).Where("id = ?", id).Scan(s.ctx))
	return destination
}

// forwardTo queues one forward to the app's destinations.
func (s *ForwarderSuite) forwardTo(router *Forwarder, customerID, body string) {
	s.Require().NoError(router.Forward(s.ctx, Event{CustomerID: customerID, ConnectorID: "slack_bot", Body: []byte(body)}))
}

// verified asserts the request carries one signature for each secret and verifies under each.
func (s *ForwarderSuite) verified(request posted, secrets ...string) {
	s.Len(strings.Fields(request.header.Get(headerSignature)), len(secrets))
	for _, secret := range secrets {
		s.Require().NoError(webhook.Verify(secret, request.header, request.body, time.Now()))
	}
}

// With sealing on, a new secret goes under the app's data key, bound to its kind, app and
// destination, and the router's key does not open it. A receiver verifies a forward signed
// with it, and with the secret a rotation made beside it.
func (s *ForwarderSuite) TestANewSecretIsSealedUnderTheAppsDataKeyAndSignsTheForward() {
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	target, arrived := s.receiver()
	router := s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: keys})
	opened := appkeystest.KEKOpens(s.T(), secretKind)

	destination, first := s.madeBy(router, target.URL)

	s.Equal(new(1), destination.SecretDataKeyVersion)
	s.Equal(1, destination.KEKVersion, "the router's current key version, which the table's CHECK asks for")
	aad := secretAAD(destination.CustomerID, destination.ConnectorID, destination.ID)
	_, err := s.sealer.OpenWithAADVersion(destination.SecretSealed, aad, 1)
	s.Require().Error(err, "the router's key does not open it")
	plain, err := keys.Open(s.ctx, destination.CustomerID, 1, destination.SecretSealed, appkeystest.RecordAAD(secretKind, destination.CustomerID, aad))
	s.Require().NoError(err)
	s.Equal(first, string(plain))
	s.forwardTo(router, destination.CustomerID, `{"n":1}`)
	s.verified(s.arrival(arrived), first)

	second := s.rotatedBy(router, destination)
	s.forwardTo(router, destination.CustomerID, `{"n":2}`)
	s.verified(s.arrival(arrived), second, first)

	s.Require().Eventually(func() bool { return s.pending(destination.CustomerID) == 0 }, 5*time.Second, 10*time.Millisecond)
	rotated := s.stored(destination.ID)
	s.Equal(new(1), rotated.SecretDataKeyVersion)
	s.Equal(new(1), rotated.PreviousSecretDataKeyVersion)
	s.Equal(destination.SecretSealed, rotated.PreviousSecretSealed)
	s.Equal(opened, appkeystest.KEKOpens(s.T(), secretKind), "no secret opened under the router's key")
}

// A forward whose secret is under the app's data key, which this router cannot open now, is put
// back in the queue with its attempt given back. It is sent once the key opens again.
func (s *ForwarderSuite) TestAForwardWhoseDataKeyIsUnavailableIsPutBackWithItsAttemptUnspent() {
	kms := appkeystest.NewKMS()
	maker, err := New(Options{Store: s.store, Secrets: s.sealer, DataKeys: s.dataKeys(kms, true)})
	s.Require().NoError(err)
	for name, unavailable := range map[string]func() *appkeys.Keys{
		"KMS is down": func() *appkeys.Keys {
			kms.Fail(errKMSDown)
			// Keys of its own, so no key unwrapped before is held.
			return s.dataKeys(kms, true)
		},
		"the router has no data keys": func() *appkeys.Keys { return nil },
	} {
		s.Run(name, func() {
			target, arrived := s.receiver()
			destination, secret := s.madeBy(maker, target.URL)
			logs := &logLines{}
			// A lease of 2 s keeps the forward out of the next claim while the test reads it.
			router := s.routerOn(s.sealer, target, logs, Options{DataKeys: unavailable(), Lease: 2 * time.Second,
				Retries: []time.Duration{time.Minute}})
			s.forwardTo(router, destination.CustomerID, `{"n":1}`)

			s.Require().Eventually(func() bool {
				return logs.about(rotatedDestination{id: destination.ID}, "cannot open an event destination's signing secret") == 1
			}, 5*time.Second, 10*time.Millisecond)
			router.Close()
			var attempts int
			s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
				"SELECT attempts FROM connector_event_deliveries WHERE destination_id = ?", destination.ID).Scan(&attempts))
			s.Zero(attempts, "the claim's attempt was given back")
			s.Empty(arrived)
			s.NotContains(logs.String(), "gave up")

			kms.Fail(nil)
			_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE connector_event_deliveries SET next_attempt_at = now() WHERE destination_id = ?",
				destination.ID)
			s.Require().NoError(err)
			s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: s.dataKeys(kms, true)})
			s.verified(s.arrival(arrived), secret)
		})
	}
}

// A secret under the app's data key that does not open there is given up, as a router-key
// secret that does not open is. It is never tried under the router's key, even when the blob
// is one the router's key would open, as a build from before data keys could write it.
func (s *ForwarderSuite) TestADataKeySecretThatDoesNotOpenIsGivenUpAndNeverTriedUnderTheRoutersKey() {
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	for name, blob := range map[string]func(destination store.EventDestination) []byte{
		"it is bound to another destination": func(destination store.EventDestination) []byte {
			sealed, _, err := keys.Seal(s.ctx, destination.CustomerID, []byte("whsec_AAAA"), appkeystest.RecordAAD(secretKind,
				destination.CustomerID, secretAAD(destination.CustomerID, destination.ConnectorID, "destination-other")))
			s.Require().NoError(err)
			return sealed
		},
		"it is under the router's key": func(destination store.EventDestination) []byte {
			sealed, _ := s.under(s.sealer)("whsec_AAAA", secretAAD(destination.CustomerID, destination.ConnectorID, destination.ID))
			return sealed
		},
	} {
		s.Run(name, func() {
			target, arrived := s.receiver()
			router := s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: keys})
			destination, _ := s.madeBy(router, target.URL)
			s.Require().NotNil(destination.SecretDataKeyVersion)
			_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE connector_event_destinations SET secret_sealed = ? WHERE id = ?",
				blob(destination), destination.ID)
			s.Require().NoError(err)

			s.forwardTo(router, destination.CustomerID, `{"n":1}`)

			s.Require().Eventually(func() bool { return s.pending(destination.CustomerID) == 0 }, 5*time.Second, 10*time.Millisecond,
				"a minute's lease would hold it far longer if it were released")
			s.Empty(arrived)
		})
	}
}

// A secret still under the router's key moves to the app's data key on its first send, the
// previous one too, each under the router's current key version. Each counts one open under
// the router's key, and the next send counts none.
func (s *ForwarderSuite) TestARouterKeySecretMovesToTheDataKeyOnItsFirstSendAndCountsOnce() {
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM connector_event_deliveries")
	s.Require().NoError(err)
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	kms := appkeystest.NewKMS()
	keys := s.dataKeys(kms, true)
	target, arrived := s.receiver()
	router := s.routerOn(routerTwo, target, &logLines{}, Options{DataKeys: keys})
	destination := s.rotated(target.URL, s.under(s.sealer), s.under(s.sealer), time.Now().Add(time.Hour))
	opened := appkeystest.KEKOpens(s.T(), secretKind)

	s.Require().NoError(s.send(router, destination, `{"n":1}`))
	s.verified(s.arrival(arrived), destination.current, destination.previous)
	s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond)

	moved := s.stored(destination.id)
	s.Equal(new(1), moved.SecretDataKeyVersion)
	s.Equal(new(1), moved.PreviousSecretDataKeyVersion)
	s.Equal(2, moved.KEKVersion, "the router's current key version, not the one the secret had")
	s.Equal(2, moved.PreviousKEKVersion)
	aad := appkeystest.RecordAAD(secretKind, destination.customer, secretAAD(destination.customer, "slack_bot", destination.id))
	current, err := keys.Open(s.ctx, destination.customer, 1, moved.SecretSealed, aad)
	s.Require().NoError(err)
	s.Equal(destination.current, string(current))
	previous, err := keys.Open(s.ctx, destination.customer, 1, moved.PreviousSecretSealed, aad)
	s.Require().NoError(err)
	s.Equal(destination.previous, string(previous))
	s.Equal(opened+2, appkeystest.KEKOpens(s.T(), secretKind), "the current and the previous secret, once each")

	s.Require().NoError(s.send(router, destination, `{"n":2}`))
	s.verified(s.arrival(arrived), destination.current, destination.previous)
	s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond)
	s.Equal(opened+2, appkeystest.KEKOpens(s.T(), secretKind), "both now open under the data key")
}

// With sealing on and KMS down, a new secret goes under the router's key and a secret under it
// stays there. Forwards are still sent, and the warning that it did not move is written once a
// lease.
func (s *ForwarderSuite) TestWhileKMSIsDownSecretsStayUnderTheRoutersKeyAndForwardsAreSent() {
	kms := appkeystest.NewKMS()
	kms.Fail(errKMSDown)
	target, arrived := s.receiver()
	logs := &logLines{}
	router := s.routerOn(s.sealer, target, logs, Options{DataKeys: s.dataKeys(kms, true)})
	fallbacks := appkeystest.SealFallbacks(s.T(), secretKind)

	destination, secret := s.madeBy(router, target.URL)
	s.Nil(destination.SecretDataKeyVersion)
	s.Equal(1, destination.KEKVersion)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), secretKind))
	for i := range 2 {
		s.forwardTo(router, destination.CustomerID, `{"n":`+strconv.Itoa(i)+`}`)
		s.verified(s.arrival(arrived), secret)
	}

	s.Require().Eventually(func() bool { return s.pending(destination.CustomerID) == 0 }, 5*time.Second, 10*time.Millisecond)
	s.Equal(destination.SecretSealed, s.stored(destination.ID).SecretSealed)
	s.Nil(s.stored(destination.ID).SecretDataKeyVersion)
	s.Equal(1, logs.about(rotatedDestination{id: destination.ID}, "could not move an event destination's signing secret"))
}

// After sealing is turned off, a secret under the app's data key still signs, a new one goes
// under the router's key, and a send moves nothing.
func (s *ForwarderSuite) TestWithSealingOffADataKeySecretStillSignsAndNothingMoves() {
	kms := appkeystest.NewKMS()
	sealing, err := New(Options{Store: s.store, Secrets: s.sealer, DataKeys: s.dataKeys(kms, true)})
	s.Require().NoError(err)
	target, arrived := s.receiver()
	router := s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: s.dataKeys(kms, false)})

	keyed, keyedSecret := s.madeBy(sealing, target.URL)
	s.Require().NotNil(keyed.SecretDataKeyVersion)
	plain, plainSecret := s.madeBy(router, target.URL)
	s.Nil(plain.SecretDataKeyVersion, "sealing off: the router's key")
	s.Equal(1, plain.KEKVersion)

	s.forwardTo(router, keyed.CustomerID, `{"n":1}`)
	s.verified(s.arrival(arrived), keyedSecret)
	s.forwardTo(router, plain.CustomerID, `{"n":2}`)
	s.verified(s.arrival(arrived), plainSecret)

	s.Require().Eventually(func() bool { return s.pending(plain.CustomerID) == 0 }, 5*time.Second, 10*time.Millisecond)
	s.Equal(plain.SecretSealed, s.stored(plain.ID).SecretSealed)
	s.Nil(s.stored(plain.ID).SecretDataKeyVersion)
}

// Without data keys, secrets are sealed and sent as on the base. A probe of 5373f2c97 gave
// these values: key version 1, the secret opens under the router's key with the destination's
// AAD, one signature, and a send rewrites nothing, also on a keyring whose current version is
// newer than the secret's.
func (s *ForwarderSuite) TestWithoutDataKeysSecretsAreSealedAndSentAsOnTheBase() {
	target, arrived := s.receiver()
	router := s.routerOn(s.sealer, target, &logLines{}, Options{})

	destination, secret := s.madeBy(router, target.URL)
	s.Equal(1, destination.KEKVersion)
	s.Nil(destination.SecretDataKeyVersion)
	opened, err := s.sealer.OpenWithAADVersion(destination.SecretSealed, secretAAD(destination.CustomerID, destination.ConnectorID, destination.ID), 1)
	s.Require().NoError(err)
	s.Equal(secret, opened)
	s.forwardTo(router, destination.CustomerID, `{"n":1}`)
	s.verified(s.arrival(arrived), secret)
	s.Require().Eventually(func() bool { return s.pending(destination.CustomerID) == 0 }, 5*time.Second, 10*time.Millisecond)
	s.Equal(destination.SecretSealed, s.stored(destination.ID).SecretSealed)
	s.Equal(1, s.stored(destination.ID).KEKVersion)

	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	newer, arrivedNewer := s.receiver()
	rotated := s.rotated(newer.URL, s.under(s.sealer), s.under(s.sealer), time.Now().Add(time.Hour))
	s.Require().NoError(s.send(s.routerOn(routerTwo, newer, &logLines{}, Options{}), rotated, `{"n":2}`))
	s.verified(s.arrival(arrivedNewer), rotated.current, rotated.previous)
	s.Require().Eventually(func() bool { return s.pending(rotated.customer) == 0 }, 5*time.Second, 10*time.Millisecond)
	kept := s.stored(rotated.id)
	s.Equal(rotated.sealed[1], kept.SecretSealed)
	s.Equal(rotated.sealed[0], kept.PreviousSecretSealed)
	s.Equal(1, kept.KEKVersion)
	s.Equal(1, kept.PreviousKEKVersion)
	s.Nil(kept.SecretDataKeyVersion)
	s.Nil(kept.PreviousSecretDataKeyVersion)
}

// Both series of the record are served from the start, before any secret counted.
func (s *ForwarderSuite) TestTheSigningSecretsAreCountedFromTheStart() {
	s.True(appkeystest.Served(s.T(), secretKind))
}

// A destination made before data keys, rotated once sealing is on, holds a current secret under
// the data key and a previous one under the router's key. Each opens under its own slot's key,
// so both sign, and the previous one moves to the data key.
func (s *ForwarderSuite) TestARotationAcrossTheRoutersKeyAndTheDataKeySignsWithBothSecrets() {
	keys := s.dataKeys(appkeystest.NewKMS(), true)
	target, arrived := s.receiver()
	before, err := New(Options{Store: s.store, Secrets: s.sealer})
	s.Require().NoError(err)
	destination, first := s.madeBy(before, target.URL)
	s.Require().Nil(destination.SecretDataKeyVersion)
	router := s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: keys})
	second := s.rotatedBy(router, destination)
	rotated := s.stored(destination.ID)
	s.Require().Equal(new(1), rotated.SecretDataKeyVersion)
	s.Require().Nil(rotated.PreviousSecretDataKeyVersion)

	s.forwardTo(router, destination.CustomerID, `{"n":1}`)
	s.verified(s.arrival(arrived), second, first)
	s.Require().Eventually(func() bool { return s.stored(destination.ID).PreviousSecretDataKeyVersion != nil }, 5*time.Second, 10*time.Millisecond)

	// And the other way round: a current secret a rotation sealed under the router's key while
	// KMS was down, beside a previous one under the data key.
	kms := appkeystest.NewKMS()
	keys = s.dataKeys(kms, true)
	router = s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: keys})
	destination, first = s.madeBy(router, target.URL)
	s.Require().NotNil(destination.SecretDataKeyVersion)
	kms.Fail(errKMSDown)
	down := s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: s.dataKeys(kms, true)})
	second = s.rotatedBy(down, destination)
	s.Require().Nil(s.stored(destination.ID).SecretDataKeyVersion)
	kms.Fail(nil)
	s.forwardTo(router, destination.CustomerID, `{"n":2}`)
	s.verified(s.arrival(arrived), second, first)
}

// A failed read of the app's data key, here a deadline that has passed before the database
// answers, is a key fault and not a secret that does not open. The forward goes back with its
// attempt unspent, nothing is given up, and it is signed once the read works again.
func (s *ForwarderSuite) TestAFailedReadOfTheDataKeyPutsTheForwardBackAndSendsItLater() {
	kms := appkeystest.NewKMS()
	maker, err := New(Options{Store: s.store, Secrets: s.sealer, DataKeys: s.dataKeys(kms, true)})
	s.Require().NoError(err)
	target, arrived := s.receiver()
	destination, secret := s.madeBy(maker, target.URL)
	options := appkeystest.Options()
	options.Seal = true
	options.KMSTimeout = time.Nanosecond
	slow, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	logs := &logLines{}
	router := s.routerOn(s.sealer, target, logs, Options{DataKeys: slow, Lease: 2 * time.Second, Retries: []time.Duration{time.Minute}})
	s.forwardTo(router, destination.CustomerID, `{"n":1}`)

	s.Require().Eventually(func() bool {
		return logs.about(rotatedDestination{id: destination.ID}, "cannot open an event destination's signing secret") == 1
	}, 5*time.Second, 10*time.Millisecond, "%s", logs.String())
	router.Close()
	var attempts int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT attempts FROM connector_event_deliveries WHERE destination_id = ?", destination.ID).Scan(&attempts))
	s.Zero(attempts)
	s.Empty(arrived)
	s.NotContains(logs.String(), "gave up")

	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE connector_event_deliveries SET next_attempt_at = now() WHERE destination_id = ?",
		destination.ID)
	s.Require().NoError(err)
	s.routerOn(s.sealer, target, &logLines{}, Options{DataKeys: s.dataKeys(kms, true)})
	s.verified(s.arrival(arrived), secret)
}

// A key fault that outlasts the retries and a lease gives the forward up, as a key version no
// router holds does, so a router with no data keys at all does not hold it for ever.
func (s *ForwarderSuite) TestAForwardWhoseDataKeyStaysUnavailableIsGivenUpAfterItsWait() {
	maker, err := New(Options{Store: s.store, Secrets: s.sealer, DataKeys: s.dataKeys(appkeystest.NewKMS(), true)})
	s.Require().NoError(err)
	target, arrived := s.receiver()
	destination, _ := s.madeBy(maker, target.URL)
	logs := &logLines{}
	router := s.routerOn(s.sealer, target, logs, Options{Lease: 300 * time.Millisecond, Retries: []time.Duration{time.Millisecond}})
	s.forwardTo(router, destination.CustomerID, `{"n":1}`)

	s.Require().Eventually(func() bool { return s.pending(destination.CustomerID) == 0 }, 8*time.Second, 10*time.Millisecond, "%s", logs.String())
	s.Contains(logs.String(), "gave up an event forward no router could sign")
	s.Empty(arrived)
}
