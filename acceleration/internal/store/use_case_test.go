//go:build integration

package store

import (
	"fmt"
)

// useCase creates a draft use case for a customer.
func (s *StoreSuite) useCase(customerID, name string) UseCase {
	useCase := UseCase{CustomerID: customerID, Name: name, Status: "draft", UseCaseType: "CUSTOMER_CARE"}
	s.Require().NoError(s.store.UseCases.Insert(s.ctx, &useCase))
	return useCase
}

// A number assigned to no use case still has to send as something.
func (s *StoreSuite) TestAnAppsFirstUseCaseIsItsDefaultUntilAnotherTakesOver() {
	customerID := newID()
	first := s.useCase(customerID, "support")
	second := s.useCase(customerID, "reminders")
	s.True(first.IsDefault)
	s.False(second.IsDefault)

	second.IsDefault = true
	s.Require().NoError(s.store.UseCases.Update(s.ctx, &second))

	first, err := s.store.UseCases.Get(s.ctx, customerID, first.ID)
	s.Require().NoError(err)
	s.False(first.IsDefault)
	_, err = s.store.UseCases.Get(s.ctx, newID(), second.ID)
	s.ErrorIs(err, ErrUnknownUseCase, "another app does not see it")
}

// Two reviewers acting on the same use case at once must not both win.
func (s *StoreSuite) TestMovingAUseCaseFromAStatusItLeftWritesNothing() {
	useCase := s.useCase(newID(), "support")

	useCase.Status = "submitted"
	s.Require().NoError(s.store.UseCases.Move(s.ctx, &useCase, "draft", &ReviewLog{Actor: "app"}))
	useCase.Status = "approved"
	err := s.store.UseCases.Move(s.ctx, &useCase, "draft", &ReviewLog{Actor: "staff"})
	s.ErrorIs(err, ErrUseCaseMoved)

	logs, err := s.store.ReviewLogs.Select(s.ctx, useCase.CustomerID, useCase.ID, 0, nil)
	s.Require().NoError(err)
	s.Require().Len(logs, 1)
	s.Equal("draft", logs[0].FromStatus)
	s.Equal("submitted", logs[0].ToStatus)
	held, err := s.store.UseCases.Get(s.ctx, useCase.CustomerID, useCase.ID)
	s.Require().NoError(err)
	s.Equal("submitted", held.Status)
}

func (s *StoreSuite) TestTheDefaultUseCaseSendsForEveryNumberNotAssignedElsewhere() {
	customerID := newID()
	fallback := s.useCase(customerID, "support")
	reminders := s.useCase(customerID, "reminders")
	var numbers []string
	for i := range 2 {
		number := PhoneNumber{CustomerID: customerID, E164: fmt.Sprintf("+15550001%03d", i), Vendor: "telnyx"}
		s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &number))
		numbers = append(numbers, number.E164)
	}

	s.Require().NoError(s.store.UseCases.AssignNumbers(s.ctx, customerID, reminders.ID, numbers[:1]))

	assigned, err := s.store.UseCases.Numbers(s.ctx, reminders)
	s.Require().NoError(err)
	rest, err := s.store.UseCases.Numbers(s.ctx, fallback)
	s.Require().NoError(err)
	s.Require().Len(assigned, 1)
	s.Require().Len(rest, 1)
	s.Equal(numbers[0], assigned[0].E164)
	s.Equal(numbers[1], rest[0].E164)
	s.Error(s.store.UseCases.AssignNumbers(s.ctx, customerID, reminders.ID, []string{"+15550000000"}),
		"a number the app does not hold")
}
