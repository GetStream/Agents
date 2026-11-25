package egress

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ResendSuite struct {
	suite.Suite
}

func TestResendSuite(t *testing.T) {
	suite.Run(t, new(ResendSuite))
}

// post is a POST whose body has a declared length.
func (s *ResendSuite) post(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
}

// TestARequestWithNoBodyReservesNothing: an empty GET has nothing to keep.
func (s *ResendSuite) TestARequestWithNoBodyReservesNothing() {
	p := &Proxy{keptBudget: maxKeptBodies}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	release := p.resendable(r)
	s.Zero(p.keptBodies.Load())
	s.NotNil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestADeclaredBodyReservesItsLengthAndGivesItBack: the budget counts the declared length, not the kept maximum.
func (s *ResendSuite) TestADeclaredBodyReservesItsLengthAndGivesItBack() {
	p := &Proxy{keptBudget: maxKeptBodies}
	r := s.post("0123456789")
	release := p.resendable(r)
	s.EqualValues(10, p.keptBodies.Load())
	s.NotNil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestABodyWithNoLengthReservesTheMostKept: a chunked body may grow to maxResentBody before it is known.
func (s *ResendSuite) TestABodyWithNoLengthReservesTheMostKept() {
	p := &Proxy{keptBudget: maxKeptBodies}
	r := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader("abc")))
	r.ContentLength = -1
	release := p.resendable(r)
	s.EqualValues(maxResentBody, p.keptBodies.Load())
	s.NotNil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestABodyOverTheKeptSizeReservesNothingAndGetsNoGetBody: it streams as before.
func (s *ResendSuite) TestABodyOverTheKeptSizeReservesNothingAndGetsNoGetBody() {
	p := &Proxy{keptBudget: maxKeptBodies}
	r := s.post(strings.Repeat("x", maxResentBody+1))
	release := p.resendable(r)
	s.Zero(p.keptBodies.Load())
	s.Nil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestARequestACheckKeptReservesNothing: its body is kept already, a second reservation would be spent for nothing.
func (s *ResendSuite) TestARequestACheckKeptReservesNothing() {
	p := &Proxy{keptBudget: maxKeptBodies}
	r := s.post("abc")
	kept := func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("abc")), nil }
	r.GetBody = kept
	release := p.resendable(r)
	s.Zero(p.keptBodies.Load())
	s.NotNil(r.GetBody)
	again, err := r.GetBody()
	s.Require().NoError(err)
	got, err := io.ReadAll(again)
	s.Require().NoError(err)
	s.Equal("abc", string(got))
	release()
	s.Zero(p.keptBodies.Load())
}

// TestABodyTheBudgetHasNoRoomForStreamsAsBefore: one byte short of its length.
func (s *ResendSuite) TestABodyTheBudgetHasNoRoomForStreamsAsBefore() {
	p := &Proxy{keptBudget: 9}
	r := s.post("0123456789")
	release := p.resendable(r)
	s.Zero(p.keptBodies.Load())
	s.Nil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestABodyThatJustFitsTheBudgetIsKept: the budget's last byte is usable.
func (s *ResendSuite) TestABodyThatJustFitsTheBudgetIsKept() {
	p := &Proxy{keptBudget: 10}
	r := s.post("0123456789")
	release := p.resendable(r)
	s.EqualValues(10, p.keptBodies.Load())
	s.NotNil(r.GetBody)
	release()
	s.Zero(p.keptBodies.Load())
}

// TestConcurrentReservationsNeverPassTheBudget: 512 callers want one byte of a budget of 64, and exactly 64 get it.
func (s *ResendSuite) TestConcurrentReservationsNeverPassTheBudget() {
	const budget, callers = 64, 512
	p := &Proxy{keptBudget: budget}
	var wg sync.WaitGroup
	var granted atomic.Int64
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if p.reserveBody(1) {
				granted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	s.EqualValues(budget, granted.Load())
	s.EqualValues(budget, p.keptBodies.Load())
}

// TestTheSQLCheckKeepsTheBodyToSendAgain: onlyOneQuery reads the body, so the host gets it from GetBody if its first
// connection is lost.
func (s *ResendSuite) TestTheSQLCheckKeepsTheBodyToSendAgain() {
	const body = `{"query":"SELECT 1"}`
	r := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p/queries", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	_, err := onlyOneQuery(r)
	s.Require().NoError(err)
	s.Require().NotNil(r.GetBody)
	again, err := r.GetBody()
	s.Require().NoError(err)
	got, err := io.ReadAll(again)
	s.Require().NoError(err)
	s.Equal(body, string(got))
}
