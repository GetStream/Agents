package egress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// LeavesSuite runs Proxy.leaf for the handshakes of several sandboxes, each named by its proxy credential.
type LeavesSuite struct {
	suite.Suite
	now   time.Time
	clock sync.Mutex
}

func TestLeavesSuite(t *testing.T) {
	suite.Run(t, new(LeavesSuite))
}

func (s *LeavesSuite) SetupTest() {
	s.now = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
}

func (s *LeavesSuite) time() time.Time {
	s.clock.Lock()
	defer s.clock.Unlock()
	return s.now
}

func (s *LeavesSuite) advance(d time.Duration) {
	s.clock.Lock()
	defer s.clock.Unlock()
	s.now = s.now.Add(d)
}

// newProxy is a proxy that keeps at most max certificates (maxLeaves when 0), dated by the suite's clock.
func (s *LeavesSuite) newProxy(max int) *Proxy {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.Require().NoError(err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "leaves"},
		NotBefore: s.now.Add(-24 * time.Hour), NotAfter: s.now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	s.Require().NoError(err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	s.Require().NoError(err)
	proxy, err := New(Options{
		CACert:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		CAKey:     pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		Now:       s.time,
		MaxLeaves: max,
	})
	s.Require().NoError(err)
	return proxy
}

// certificate is the serial of the certificate a handshake of owner's for host gets.
func (s *LeavesSuite) certificate(proxy *Proxy, host, owner string) string {
	certificate, err := proxy.leaf(host, owner)
	s.Require().NoError(err)
	return serial(certificate)
}

// serial is certificate's serial number, or "unparsable".
func serial(certificate tls.Certificate) string {
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return "unparsable"
	}
	return leaf.SerialNumber.String()
}

// keep has owner's handshakes keep a certificate for each of hosts, a minute apart, oldest first.
func (s *LeavesSuite) keep(proxy *Proxy, owner string, hosts ...string) map[string]string {
	kept := map[string]string{}
	for _, host := range hosts {
		kept[host] = s.certificate(proxy, host, owner)
		s.advance(time.Minute)
	}
	return kept
}

// hosts are n names under domain.
func hosts(domain string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("h%d.%s", i, domain)
	}
	return names
}

// changed says which of kept a handshake of owner's gets another certificate for now.
func (s *LeavesSuite) changed(proxy *Proxy, owner string, kept map[string]string) []string {
	var changed []string
	for host, certificate := range kept {
		if s.certificate(proxy, host, owner) != certificate {
			changed = append(changed, host)
		}
	}
	return changed
}

// TestAProxyKeepsAtMost1024CertificatesByDefault: maxLeaves is the documented cap when no test sets one.
func (s *LeavesSuite) TestAProxyKeepsAtMost1024CertificatesByDefault() {
	s.Equal(1024, s.newProxy(0).leaves.max)
}

// TestASandboxThatKeepsFewerTakesTheRoomOfTheOldestOfTheOneThatKeepsTheMost: in a full cache,
// each new host of a sandbox's is kept in place of the oldest certificate of the sandbox that keeps
// the most.
func (s *LeavesSuite) TestASandboxThatKeepsFewerTakesTheRoomOfTheOldestOfTheOneThatKeepsTheMost() {
	// Sixteen minutes of a's, so none is due for renewal by the end; six of them give room, oldest first.
	proxy := s.newProxy(16)
	first := s.keep(proxy, "sandbox-a", hosts("a.test", 16)...)
	oldest := hosts("a.test", 6)
	for _, host := range oldest {
		delete(first, host)
	}

	other := s.keep(proxy, "sandbox-b", hosts("b.test", 6)...)

	s.Empty(s.changed(proxy, "sandbox-b", other), "the new hosts are kept")
	s.Empty(s.changed(proxy, "sandbox-a", first), "the ten newest of a's are kept")
	s.Equal(map[string]int{"sandbox-a": 10, "sandbox-b": 6}, proxy.leaves.owned)
	for _, host := range oldest {
		s.NotContains(proxy.leaves.byHost, host, "one of a's six oldest gave its room")
	}
}

// TestTheSandboxThatKeepsTheMostKeepsNoNewHostOnceTheCacheIsFull: its new host is signed for each
// handshake, as past the cap before, and what it keeps stays.
func (s *LeavesSuite) TestTheSandboxThatKeepsTheMostKeepsNoNewHostOnceTheCacheIsFull() {
	proxy := s.newProxy(4)
	kept := s.keep(proxy, "sandbox-a", hosts("a.test", 4)...)

	once := s.certificate(proxy, "new.a.test", "sandbox-a")

	s.NotEqual(once, s.certificate(proxy, "new.a.test", "sandbox-a"), "signed for each handshake")
	s.Empty(s.changed(proxy, "sandbox-a", kept))
}

// TestAnotherSandboxsHostsStayKeptWhileOneSandboxNamesNewHosts: a sandbox that names fifty hosts
// under a wildcard takes no room from another sandbox's two, and the other's new host is kept too.
func (s *LeavesSuite) TestAnotherSandboxsHostsStayKeptWhileOneSandboxNamesNewHosts() {
	proxy := s.newProxy(8)
	other := s.keep(proxy, "sandbox-b", "api.b.test", "uploads.b.test")

	// All within the minute, so none is due for renewal.
	for _, host := range hosts("a.test", 50) {
		s.certificate(proxy, host, "sandbox-a")
	}
	third := s.certificate(proxy, "third.b.test", "sandbox-b")

	s.Empty(s.changed(proxy, "sandbox-b", other))
	s.Equal(third, s.certificate(proxy, "third.b.test", "sandbox-b"), "b's new host is kept")
	s.Len(proxy.leaves.byHost, 8)
	s.Equal(map[string]int{"sandbox-a": 5, "sandbox-b": 3}, proxy.leaves.owned)
}

// TestSandboxesThatKeepAsManyTakeNoRoomFromEachOther: in a full cache, a sandbox that keeps as many
// as the one that keeps the most gets its new hosts signed for the handshake alone, whichever of
// the two the cache's order visits first.
func (s *LeavesSuite) TestSandboxesThatKeepAsManyTakeNoRoomFromEachOther() {
	proxy := s.newProxy(4)
	first := s.keep(proxy, "sandbox-a", hosts("a.test", 2)...)
	s.keep(proxy, "sandbox-b", hosts("b.test", 2)...)

	// Go's map order is random for each range, and a small map visits b before a about once in
	// eight, so a hundred new hosts try both orders.
	for _, host := range hosts("new.b.test", 100) {
		s.certificate(proxy, host, "sandbox-b")
	}

	s.Empty(s.changed(proxy, "sandbox-a", first))
	s.Equal(map[string]int{"sandbox-a": 2, "sandbox-b": 2}, proxy.leaves.owned)
}

// TestARenewedCertificateCountsToTheSandboxThatRenewedIt: a host renewed for another sandbox's
// handshake, in a cache with room, is no longer the first sandbox's.
func (s *LeavesSuite) TestARenewedCertificateCountsToTheSandboxThatRenewedIt() {
	proxy := s.newProxy(4)
	s.keep(proxy, "ended-run", "api.test")
	s.advance(halfOfLeafValidity)

	s.certificate(proxy, "api.test", "next-run")

	s.Equal(map[string]int{"next-run": 1}, proxy.leaves.owned)
	s.Equal("next-run", proxy.leaves.byHost["api.test"].owner)
}

// TestCertificatesDueForRenewalMakeRoomBeforeAnyOtherIsDropped: in a full cache, a new host takes
// the room of the certificates half of leafValidity old before that of a sandbox that keeps more.
func (s *LeavesSuite) TestCertificatesDueForRenewalMakeRoomBeforeAnyOtherIsDropped() {
	proxy := s.newProxy(5)
	s.keep(proxy, "sandbox-a", hosts("a.test", 2)...)
	s.advance(20 * time.Minute)
	live := s.keep(proxy, "sandbox-b", hosts("b.test", 3)...)
	// a's are due for renewal, b's are 7 to 9 minutes old.
	s.advance(halfOfLeafValidity - 24*time.Minute)

	other := s.certificate(proxy, "c.test", "sandbox-c")

	s.Equal(other, s.certificate(proxy, "c.test", "sandbox-c"), "c's host is kept")
	s.Empty(s.changed(proxy, "sandbox-b", live), "b keeps the most, and gives no room while a's are due")
	s.Equal(map[string]int{"sandbox-b": 3, "sandbox-c": 1}, proxy.leaves.owned)
}

// halfOfLeafValidity is when a kept certificate is due for renewal.
const halfOfLeafValidity = leafValidity / 2

// TestASandboxWhoseCertificatesAreGoneLeavesTheCount: a sandbox whose run ended is counted only
// while a certificate its handshake kept is: one renewed for another sandbox's handshake, one that
// gave its room, or one dropped as due for renewal is no longer the first sandbox's.
func (s *LeavesSuite) TestASandboxWhoseCertificatesAreGoneLeavesTheCount() {
	proxy := s.newProxy(3)
	s.keep(proxy, "ended-run", "api.test")
	s.keep(proxy, "evicted-run", "h0.e.test", "h1.e.test")
	// api.test is due for renewal, the two others are not.
	s.advance(halfOfLeafValidity - 3*time.Minute)

	s.certificate(proxy, "api.test", "next-run")
	renewed := maps.Clone(proxy.leaves.owned)
	s.certificate(proxy, "x.test", "another-run")
	evicted := maps.Clone(proxy.leaves.owned)
	// h1.e.test is due for renewal now, and the cache is full.
	s.advance(2 * time.Minute)
	s.certificate(proxy, "y.test", "another-run")

	s.Equal(map[string]int{"evicted-run": 2, "next-run": 1}, renewed)
	s.Equal(map[string]int{"evicted-run": 1, "next-run": 1, "another-run": 1}, evicted)
	s.Equal(map[string]int{"next-run": 1, "another-run": 2}, proxy.leaves.owned)
	s.Len(proxy.leaves.byHost, 3)
}

// TestConcurrentHandshakesOfTwoSandboxesKeepTheOthersCertificate is the -race probe: while one
// sandbox's handshakes name new hosts at once, the other's handshakes all get its kept certificate,
// and the cache keeps its cap and its count.
func (s *LeavesSuite) TestConcurrentHandshakesOfTwoSandboxesKeepTheOthersCertificate() {
	proxy := s.newProxy(16)
	kept := s.certificate(proxy, "api.b.test", "sandbox-b")
	var wg sync.WaitGroup
	served := make(chan string, 8*25)
	for worker := range 8 {
		wg.Go(func() {
			for _, host := range hosts(fmt.Sprintf("w%d.a.test", worker), 25) {
				_, err := proxy.leaf(host, "sandbox-a")
				s.NoError(err)
				// At -cpu 1 too, the handshakes take turns.
				runtime.Gosched()
			}
		})
		wg.Go(func() {
			for range 25 {
				// Not s.certificate: Require stops the test's own goroutine alone.
				certificate, err := proxy.leaf("api.b.test", "sandbox-b")
				if s.NoError(err) {
					served <- serial(certificate)
				}
				runtime.Gosched()
			}
		})
	}
	wg.Wait()
	close(served)

	for certificate := range served {
		s.Require().Equal(kept, certificate)
	}
	proxy.leaves.mu.Lock()
	defer proxy.leaves.mu.Unlock()
	s.Len(proxy.leaves.byHost, 16)
	s.Equal(map[string]int{"sandbox-a": 15, "sandbox-b": 1}, proxy.leaves.owned)
}
