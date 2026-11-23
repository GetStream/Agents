//go:build integration

package store

import (
	"fmt"
	"time"
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

// Two creates at once for an app with no use case both find none. The one whose default lands
// second is the app's second use case, not a failed create.
func (s *StoreSuite) TestACreateRacingTheAppsFirstUseCaseIsItsSecond() {
	customerID := newID()
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer tx.Rollback() //nolint:errcheck // a no-op once committed
	first := UseCase{ID: newID(), CustomerID: customerID, Name: "support", IsDefault: true, Status: "draft",
		UseCaseType: "CUSTOMER_CARE", MessageSamples: []string{}, CreatedAt: s.base, UpdatedAt: s.base}
	_, err = tx.NewInsert().Model(&first).Exec(s.ctx)
	s.Require().NoError(err)
	second := UseCase{CustomerID: customerID, Name: "reminders", Status: "draft", UseCaseType: "CUSTOMER_CARE"}
	created := make(chan error, 1)
	go func() { created <- s.store.UseCases.Insert(s.ctx, &second) }()
	s.Require().Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database() AND query LIKE 'INSERT INTO \"dlc_use_cases\"%'").Scan(&waiting))
		return waiting == 1
	}, 5*time.Second, 10*time.Millisecond, "the second create found no use case, and waits on the first's default")

	s.Require().NoError(tx.Commit())

	select {
	case err := <-created:
		s.Require().NoError(err)
	case <-time.After(5 * time.Second):
		s.FailNow("the second create never returned")
	}
	s.False(second.IsDefault)
	held, err := s.store.UseCases.Select(s.ctx, customerID, 10, nil)
	s.Require().NoError(err)
	s.Len(held, 2)
	first, err = s.store.UseCases.Get(s.ctx, customerID, first.ID)
	s.Require().NoError(err)
	s.True(first.IsDefault)
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

func (s *StoreSuite) TestUseCaseQueryBudgets() {
	customerID := newID()
	for i := range 3 {
		s.useCase(customerID, fmt.Sprintf("use case %d", i))
		s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
			E164: fmt.Sprintf("+1512555010%d", i), Vendor: "telnyx", Country: "US", CustomerID: customerID, PurchasedAt: s.base,
		}))
	}
	numbers := []string{"+15125550100", "+15125550101", "+15125550102"}
	useCase := UseCase{CustomerID: customerID, Name: "reminders", Status: "draft", UseCaseType: "CUSTOMER_CARE"}
	s.Equal(2+2, s.countQueries(func() { s.Require().NoError(s.store.UseCases.Insert(s.ctx, &useCase)) }),
		"Insert asks whether the app has one, then inserts")
	useCase.IsDefault = true
	s.Equal(2+2, s.countQueries(func() { s.Require().NoError(s.store.UseCases.Update(s.ctx, &useCase)) }),
		"Update of a default clears the old default, then writes")
	s.Equal(2+2, s.countQueries(func() { s.Require().NoError(s.store.UseCases.AssignNumbers(s.ctx, customerID, useCase.ID, numbers)) }),
		"AssignNumbers clears the old ones, then assigns every number in one statement")
	s.Equal(1, s.countQueries(func() {
		assigned, err := s.store.UseCases.AssignedNumbers(s.ctx, customerID, useCase.ID)
		s.Require().NoError(err)
		s.Equal(numbers, assigned)
	}), "AssignedNumbers")
	s.Equal(1, s.countQueries(func() {
		sending, err := s.store.UseCases.Numbers(s.ctx, useCase)
		s.Require().NoError(err)
		s.Len(sending, 3)
	}), "Numbers")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.UseCases.Select(s.ctx, customerID, 10, nil)
		s.Require().NoError(err)
		s.Len(listed, 4)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.UseCases.Get(s.ctx, customerID, useCase.ID)
		s.Require().NoError(err)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		found, err := s.store.UseCases.GetForNumber(s.ctx, customerID, numbers[0])
		s.Require().NoError(err)
		s.Equal(useCase.ID, found.ID)
	}), "GetForNumber")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.UseCases.GetByID(s.ctx, useCase.ID)
		s.Require().NoError(err)
	}), "GetByID")
	s.Equal(1, s.countQueries(func() {
		requested, err := s.store.UseCases.RequestCampaign(s.ctx, useCase.ID, s.base)
		s.Require().NoError(err)
		s.True(requested)
	}), "RequestCampaign")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.UseCases.ForgetCampaignRequest(s.ctx, useCase.ID, s.base)) }),
		"ForgetCampaignRequest")
	useCase.Status, useCase.Vendor, useCase.VendorCampaignID = "submitted", "telnyx", "campaign-"+useCase.ID
	s.Equal(2+2, s.countQueries(func() {
		s.Require().NoError(s.store.UseCases.Move(s.ctx, &useCase, "draft", &ReviewLog{Actor: "vendor", Notes: "Registered campaign-" + useCase.ID + "."}))
	}), "Move writes the use case and its log row")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.UseCases.GetByCampaign(s.ctx, "telnyx", useCase.VendorCampaignID)
		s.Require().NoError(err)
	}), "GetByCampaign")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.UseCases.SelectInStatus(s.ctx, "submitted", 10, nil)
		s.Require().NoError(err)
		s.Len(listed, 1)
	}), "SelectInStatus")
	s.Equal(1, s.countQueries(func() {
		registered, err := s.store.UseCases.SelectWithCampaigns(s.ctx, "Registered ", "submitted")
		s.Require().NoError(err)
		s.Require().Len(registered, 1)
	}), "SelectWithCampaigns")
	s.Equal(1, s.countQueries(func() {
		held, err := s.store.UseCases.HasIn(s.ctx, customerID, "submitted")
		s.Require().NoError(err)
		s.True(held)
	}), "HasIn")
	s.Equal(2+2, s.countQueries(func() { s.Require().NoError(s.store.UseCases.Delete(s.ctx, customerID, useCase.ID)) }),
		"Delete removes the use case, then unassigns its numbers")
}
