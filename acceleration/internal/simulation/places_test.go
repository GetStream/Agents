package simulation

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

// PlacesSuite covers how many runs a runner has at once, of one customer and of all of them.
type PlacesSuite struct {
	suite.Suite
	runner *Runner
}

func TestPlacesSuite(t *testing.T) {
	suite.Run(t, new(PlacesSuite))
}

func (s *PlacesSuite) SetupTest() {
	s.runner = &Runner{running: map[string]context.CancelFunc{}, places: map[string]int{}}
}

// together asks for a place for each customer at once and returns how many each was given.
func (s *PlacesSuite) together(customers ...string) map[string]int {
	given := map[string]int{}
	var mu sync.Mutex
	var asked sync.WaitGroup
	for _, customer := range customers {
		asked.Go(func() {
			if s.runner.take(customer) == nil {
				mu.Lock()
				defer mu.Unlock()
				given[customer]++
			}
		})
	}
	asked.Wait()
	return given
}

func (s *PlacesSuite) TestACustomerIsGivenFourPlacesAndIsToldTheyAreItsOwn() {
	s.Equal(map[string]int{"a": 4}, s.together("a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a"))
	s.ErrorIs(s.runner.take("a"), ErrCustomerBusy)
	s.NoError(s.runner.take("b"))
}

func (s *PlacesSuite) TestARunnerGivesEightPlacesToAllItsCustomers() {
	var customers []string
	for i := range 20 {
		customers = append(customers, fmt.Sprint("customer-", i))
	}

	s.Len(s.together(customers...), 8)
	s.ErrorIs(s.runner.take("another"), ErrBusy)
}

func (s *PlacesSuite) TestACustomerWithAllItsPlacesIsToldSoOnAFullRunner() {
	s.together("a", "a", "a", "a", "b", "b", "b", "b")

	s.ErrorIs(s.runner.take("a"), ErrCustomerBusy)
}

func (s *PlacesSuite) TestAPlaceLeftIsGivenAgainAndACustomerWithNoneIsForgotten() {
	s.together("a", "a", "a", "a")
	s.runner.leave("a")

	s.NoError(s.runner.take("a"))
	s.ErrorIs(s.runner.take("a"), ErrCustomerBusy)
	for range 4 {
		s.runner.leave("a")
	}
	s.Empty(s.runner.places)
}

func (s *PlacesSuite) TestAClosedRunnerGivesNoPlace() {
	s.runner.closed = true

	s.ErrorIs(s.runner.take("a"), ErrClosed)
	s.Empty(s.runner.places)
}
