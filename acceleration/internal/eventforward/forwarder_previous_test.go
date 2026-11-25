//go:build integration

package eventforward

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/webhook"
)

// seal is how a test seals a destination's signing secret: the bytes the database holds and the
// key version they name.
type seal func(plain string, aad []byte) ([]byte, int)

// posted is one request a destination received.
type posted struct {
	header http.Header
	body   []byte
}

// logLines is a log handler's output, safe to read while the forwarder writes.
type logLines struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// about counts the lines that name the destination and hold text: a router also sends the
// other tests' forwards left queued in the suite's database.
func (l *logLines) about(destination rotatedDestination, text string) int {
	count := 0
	for _, line := range strings.Split(l.String(), "\n") {
		if strings.Contains(line, destination.id) && strings.Contains(line, text) {
			count++
		}
	}
	return count
}

// previousWarning is the warning a send writes when the previous secret does not open.
const previousWarning = "previous signing secret"

// A forward whose previous secret does not open is sent with the current secret alone, whatever
// the reason it does not open: the current secret is a valid signature on its own, and the
// previous one is only a grace window for a receiver not yet on the new one.
func (s *ForwarderSuite) TestAForwardWhosePreviousSecretDoesNotOpenIsSignedWithTheCurrentOneAlone() {
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	cases := map[string]seal{
		"its key version is not held by this router": s.under(routerTwo),
		"it is bound to another destination":         s.boundToAnother(),
		"its ciphertext is corrupt":                  s.corrupt(),
		"it is shorter than a nonce":                 s.truncated(),
		"it is empty":                                s.empty(),
	}
	for name, previous := range cases {
		s.Run(name, func() {
			target, arrived := s.receiver()
			logs := &logLines{}
			router := s.routerOn(s.sealer, target, logs, Options{})
			destination := s.rotated(target.URL, previous, s.under(s.sealer), time.Now().Add(time.Hour))

			// Three forwards inside one lease: each is sent, the warning is written once.
			for i := range 3 {
				s.Require().NoError(s.send(router, destination, `{"n":`+strconv.Itoa(i)+`}`))
			}

			for range 3 {
				request := s.arrival(arrived)
				signatures := strings.Fields(request.header.Get(headerSignature))
				s.Len(signatures, 1, "the current secret's signature alone")
				s.Require().NoError(webhook.Verify(destination.current, request.header, request.body, time.Now()))
				s.ErrorIs(webhook.Verify(destination.previous, request.header, request.body, time.Now()), webhook.ErrBadSignature,
					"no signature of the previous secret")
			}
			s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond, "finished, not released")
			s.Equal(1, logs.about(destination, previousWarning), "the warning is written once a lease for the destination")
			s.NotContains(logs.String(), destination.current, "and never a secret")
			s.NotContains(logs.String(), destination.previous)
			s.holdsNoCiphertext(logs, destination.sealed...)
		})
	}
}

// The warning is once a lease for each destination: a second forward of one destination inside
// the lease writes none, another destination's has a line of its own, and the first one's is
// written again once the lease has run out. The lease is the default minute, so a slow machine
// cannot end it inside the test; the test ages the mark the throttle holds to end it.
func (s *ForwarderSuite) TestThePreviousSecretWarningIsOnceALeasePerDestination() {
	target, arrived := s.receiver()
	logs := &logLines{}
	router := s.routerOn(s.sealer, target, logs, Options{})
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	one := s.rotated(target.URL, s.under(routerTwo), s.under(s.sealer), time.Now().Add(time.Hour))
	other := s.rotated(target.URL, s.under(routerTwo), s.under(s.sealer), time.Now().Add(time.Hour))

	markOf := func(destination rotatedDestination) time.Time {
		router.mu.Lock()
		defer router.mu.Unlock()
		return router.unopenedAt["previous/"+destination.id]
	}
	var written time.Time
	for i, destination := range []rotatedDestination{one, one, other} {
		s.Require().NoError(s.send(router, destination, `{"n":"`+uuid.NewString()+`"}`))
		s.arrival(arrived)
		if i == 0 {
			written = markOf(one)
		}
	}
	s.False(written.IsZero(), "the line marks the destination")
	s.Equal(written, markOf(one), "a forward inside the lease leaves the mark where it was: the lease does not slide")
	s.Equal(1, logs.about(one, previousWarning), "one line for a destination inside its lease")
	s.Equal(1, logs.about(other, previousWarning), "and one for another")

	router.mu.Lock()
	router.unopenedAt["previous/"+one.id] = time.Now().Add(-router.lease)
	router.mu.Unlock()
	s.Require().NoError(s.send(router, one, `{"n":"`+uuid.NewString()+`"}`))
	s.arrival(arrived)
	s.Equal(2, logs.about(one, previousWarning), "written again a lease later")
	s.Equal(1, logs.about(other, previousWarning), "and the other destination's mark is its own")
}

// The unopened line and the previous-secret warning are one destination's two lines, and neither
// hides the other: a customer whose current secret did not open and who then rotates, and one
// whose previous secret did not open and who rotates onto a key this router lacks, get both lines.
func (s *ForwarderSuite) TestTheUnopenedLineAndThePreviousSecretWarningDoNotHideEachOther() {
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	const unopened = "cannot open an event destination's signing secret"
	rotate := func(destination rotatedDestination, to seal) {
		secret, err := webhook.NewSecret()
		s.Require().NoError(err)
		sealed, version := to(secret, secretAAD(destination.customer, "slack_bot", destination.id))
		_, err = s.store.EventDestinations.RotateSecret(s.ctx, destination.customer, "slack_bot", destination.id, sealed, version, time.Now().Add(time.Hour))
		s.Require().NoError(err)
	}
	s.Run("an unopened line, then a rotation: the previous-secret warning is written", func() {
		target, arrived := s.receiver()
		logs := &logLines{}
		router := s.routerOn(s.sealer, target, logs, Options{Retries: []time.Duration{time.Minute}})
		destination := s.rotated(target.URL, s.under(s.sealer), s.under(routerTwo), time.Now().Add(-time.Minute))
		s.Require().NoError(s.send(router, destination, `{"n":"before"}`))
		s.Require().Eventually(func() bool { return logs.about(destination, unopened) == 1 }, 5*time.Second, 10*time.Millisecond)

		rotate(destination, s.under(s.sealer))
		// The rotation moved the unopenable secret to previous.
		s.Require().NoError(s.send(router, destination, `{"n":"after"}`))

		s.arrival(arrived)
		s.Equal(1, logs.about(destination, previousWarning), "the previous-secret warning after an unopened line")
		s.Equal(1, logs.about(destination, unopened), "the unopened line stays one")
	})
	s.Run("a previous-secret warning, then a rotation onto a key this router lacks: the unopened line is written", func() {
		target, arrived := s.receiver()
		logs := &logLines{}
		router := s.routerOn(s.sealer, target, logs, Options{Retries: []time.Duration{time.Minute}})
		destination := s.rotated(target.URL, s.under(routerTwo), s.under(s.sealer), time.Now().Add(time.Hour))
		s.Require().NoError(s.send(router, destination, `{"n":"before"}`))
		s.arrival(arrived)
		s.Require().Equal(1, logs.about(destination, previousWarning))

		rotate(destination, s.under(routerTwo))
		s.Require().NoError(s.send(router, destination, `{"n":"after"}`))

		s.Require().Eventually(func() bool { return logs.about(destination, unopened) == 1 }, 5*time.Second, 10*time.Millisecond,
			"the unopened line after a previous-secret warning")
		s.Empty(arrived)
	})
}

// Controls: what is as it was. Both secrets open within PreviousUntil: both sign. Past it: the
// current alone, and a previous secret that would not open is not even tried, so no warning.
func (s *ForwarderSuite) TestAPreviousSecretSignsOnlyWhileItIsKept() {
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	cases := []struct {
		name     string
		previous seal
		until    time.Time
		signs    int
	}{
		{"both open within the overlap", s.under(s.sealer), time.Now().Add(time.Hour), 2},
		{"an openable previous past the overlap", s.under(s.sealer), time.Now().Add(-time.Minute), 1},
		{"an unopenable previous past the overlap", s.under(routerTwo), time.Now().Add(-time.Minute), 1},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			target, arrived := s.receiver()
			logs := &logLines{}
			router := s.routerOn(s.sealer, target, logs, Options{})
			destination := s.rotated(target.URL, c.previous, s.under(s.sealer), c.until)

			s.Require().NoError(s.send(router, destination, `{"n":1}`))

			request := s.arrival(arrived)
			s.Len(strings.Fields(request.header.Get(headerSignature)), c.signs)
			s.Require().NoError(webhook.Verify(destination.current, request.header, request.body, time.Now()))
			if c.signs == 2 {
				s.Require().NoError(webhook.Verify(destination.previous, request.header, request.body, time.Now()))
			} else {
				s.ErrorIs(webhook.Verify(destination.previous, request.header, request.body, time.Now()), webhook.ErrBadSignature)
			}
			s.Zero(logs.about(destination, previousWarning))
		})
	}
}

// Controls: a current secret that does not open is not made up for by the previous one. A key
// version this router lacks leaves the forward to another router, a lease at a time, and gives
// it up once it has waited out its retries; any other reason gives it up at once.
func (s *ForwarderSuite) TestACurrentSecretThatDoesNotOpenIsLeftToAnotherRouterThenGivenUp() {
	routerTwo, err := auth.NewSealerWithKeyring(2, map[int]string{1: "test key one", 2: "test key two"})
	s.Require().NoError(err)
	s.Run("left to a router that holds its key version", func() {
		target, arrived := s.receiver()
		router := s.routerOn(s.sealer, target, &logLines{}, Options{Retries: []time.Duration{time.Minute}, Lease: 200 * time.Millisecond})
		destination := s.rotated(target.URL, s.under(s.sealer), s.under(routerTwo), time.Now().Add(time.Hour))

		s.Require().NoError(s.send(router, destination, `{"n":1}`))

		s.Never(func() bool { return s.pending(destination.customer) == 0 || len(arrived) > 0 }, 500*time.Millisecond, 10*time.Millisecond,
			"neither sent with the previous secret nor dropped")
	})
	s.Run("its line is written once a lease for the destination", func() {
		target, _ := s.receiver()
		logs := &logLines{}
		router := s.routerOn(s.sealer, target, logs, Options{Retries: []time.Duration{time.Minute}, Lease: time.Minute})
		destination := s.rotated(target.URL, s.under(s.sealer), s.under(routerTwo), time.Now().Add(time.Hour))
		other := s.rotated(target.URL, s.under(s.sealer), s.under(routerTwo), time.Now().Add(time.Hour))

		for i := range 3 {
			s.Require().NoError(s.send(router, destination, `{"n":`+strconv.Itoa(i)+`}`))
			s.Require().NoError(s.send(router, other, `{"n":`+strconv.Itoa(i)+`}`))
		}

		const line = "cannot open an event destination's signing secret"
		s.Require().Eventually(func() bool {
			return logs.about(destination, line) >= 1 && logs.about(other, line) >= 1
		}, 5*time.Second, 10*time.Millisecond)
		s.Never(func() bool { return logs.about(destination, line) > 1 || logs.about(other, line) > 1 }, 500*time.Millisecond, 10*time.Millisecond,
			"three forwards fail alike: one line says it, for each destination")
	})
	s.Run("given up once it has waited out its retries", func() {
		target, arrived := s.receiver()
		router := s.routerOn(s.sealer, target, &logLines{}, Options{Retries: []time.Duration{time.Millisecond}, Lease: 500 * time.Millisecond})
		destination := s.rotated(target.URL, s.under(s.sealer), s.under(routerTwo), time.Now().Add(time.Hour))

		s.Require().NoError(s.send(router, destination, `{"n":1}`))

		s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond)
		s.Empty(arrived)
	})
	s.Run("given up at once when it is empty", func() {
		target, arrived := s.receiver()
		router := s.routerOn(s.sealer, target, &logLines{}, Options{})
		destination := s.rotated(target.URL, s.under(s.sealer), s.empty(), time.Now().Add(time.Hour))

		s.Require().NoError(s.send(router, destination, `{"n":1}`))

		s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond)
		s.Empty(arrived)
	})
	s.Run("given up at once when it does not open for another reason", func() {
		target, arrived := s.receiver()
		router := s.routerOn(s.sealer, target, &logLines{}, Options{})
		destination := s.rotated(target.URL, s.under(s.sealer), s.corrupt(), time.Now().Add(time.Hour))

		s.Require().NoError(s.send(router, destination, `{"n":1}`))

		s.Require().Eventually(func() bool { return s.pending(destination.customer) == 0 }, 5*time.Second, 10*time.Millisecond,
			"a minute's lease would hold it far longer if it were released")
		s.Empty(arrived)
	})
}

// rotatedDestination is a destination whose secret was rotated once.
type rotatedDestination struct {
	id, customer      string
	current, previous string
	// sealed is the previous secret's blob, then the current one's, as the database holds them.
	sealed [][]byte
}

// rotated stores a destination that is sealed with previous, then rotated to a secret sealed
// with current, the earlier one signing beside it until until.
func (s *ForwarderSuite) rotated(url string, previous, current seal, until time.Time) rotatedDestination {
	destination := store.EventDestination{
		ID: uuid.NewString(), CustomerID: "customer-" + uuid.NewString(), ConnectorID: "slack_bot", URL: url, Forward: store.ForwardAll,
	}
	aad := secretAAD(destination.CustomerID, destination.ConnectorID, destination.ID)
	made := rotatedDestination{id: destination.ID, customer: destination.CustomerID}
	var err error
	made.previous, err = webhook.NewSecret()
	s.Require().NoError(err)
	made.current, err = webhook.NewSecret()
	s.Require().NoError(err)
	destination.SecretSealed, destination.KEKVersion = previous(made.previous, aad)
	made.sealed = append(made.sealed, destination.SecretSealed)
	s.Require().NoError(s.store.EventDestinations.Insert(s.ctx, &destination))
	sealed, version := current(made.current, aad)
	made.sealed = append(made.sealed, sealed)
	_, err = s.store.EventDestinations.RotateSecret(s.ctx, destination.CustomerID, destination.ConnectorID, destination.ID, sealed, version, until)
	s.Require().NoError(err)
	return made
}

// holdsNoCiphertext asserts the log holds none of the sealed blobs, in the forms a log line
// writes bytes in: quoted and escaped, hex, and base64 both ways.
func (s *ForwarderSuite) holdsNoCiphertext(logs *logLines, blobs ...[]byte) {
	for _, sealed := range blobs {
		quoted := strconv.Quote(string(sealed))
		for _, form := range []string{quoted[1 : len(quoted)-1], hex.EncodeToString(sealed),
			base64.StdEncoding.EncodeToString(sealed), base64.RawURLEncoding.EncodeToString(sealed)} {
			s.NotContains(logs.String(), form, "a sealed secret is never logged")
		}
	}
}

// send queues a forward of body to the destination.
func (s *ForwarderSuite) send(router *Forwarder, destination rotatedDestination, body string) error {
	return router.Forward(s.ctx, Event{CustomerID: destination.customer, ConnectorID: "slack_bot", Body: []byte(body)})
}

// routerOn starts a forwarder that opens secrets with sealer, writes its log to logs and sends
// through target's client; options override its retries and lease.
func (s *ForwarderSuite) routerOn(sealer *auth.Sealer, target *httptest.Server, logs *logLines, options Options) *Forwarder {
	options.Store, options.Secrets, options.HTTP = s.store, sealer, target.Client()
	options.Logger = slog.New(slog.NewTextHandler(logs, nil))
	if options.Retries == nil {
		options.Retries = []time.Duration{time.Millisecond, time.Millisecond}
	}
	options.Poll = 10 * time.Millisecond
	forwarder, err := New(options)
	s.Require().NoError(err)
	forwarder.Start()
	s.T().Cleanup(forwarder.Close)
	return forwarder
}

// receiver is a destination that hands every request it gets to the channel.
func (s *ForwarderSuite) receiver() (*httptest.Server, chan posted) {
	arrived := make(chan posted, 8)
	server := s.destination(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		arrived <- posted{header: r.Header.Clone(), body: body}
	})
	return server, arrived
}

// arrival is the next request the destination got.
func (s *ForwarderSuite) arrival(arrived chan posted) posted {
	select {
	case request := <-arrived:
		return request
	case <-time.After(5 * time.Second):
		s.FailNow("the destination got nothing")
		return posted{}
	}
}

// under seals with sealer, under its current key version.
func (s *ForwarderSuite) under(sealer *auth.Sealer) seal {
	return func(plain string, aad []byte) ([]byte, int) {
		sealed, err := sealer.SealWithAAD(plain, aad)
		s.Require().NoError(err)
		return sealed, sealer.CurrentVersion()
	}
}

// boundToAnother seals with this suite's key under the AAD of another destination, so it
// names a key version the router holds and still does not open.
func (s *ForwarderSuite) boundToAnother() seal {
	return func(plain string, _ []byte) ([]byte, int) {
		return s.under(s.sealer)(plain, secretAAD("customer-other", "slack_bot", "destination-other"))
	}
}

// empty seals a secret that holds no key.
func (s *ForwarderSuite) empty() seal {
	return func(_ string, aad []byte) ([]byte, int) { return s.under(s.sealer)("whsec_", aad) }
}

// corrupt seals with this suite's key and flips a bit of the ciphertext.
func (s *ForwarderSuite) corrupt() seal {
	return func(plain string, aad []byte) ([]byte, int) {
		sealed, version := s.under(s.sealer)(plain, aad)
		sealed[len(sealed)-1] ^= 1
		return sealed, version
	}
}

// truncated seals with this suite's key and keeps fewer bytes than a nonce.
func (s *ForwarderSuite) truncated() seal {
	return func(plain string, aad []byte) ([]byte, int) {
		sealed, version := s.under(s.sealer)(plain, aad)
		return sealed[:4], version
	}
}
