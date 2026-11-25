//go:build integration

package store

import (
	"time"
)

// order keeps a purchase of e164 for a customer.
func (s *StoreSuite) order(customerID, e164 string) PhoneNumberOrder {
	order := PhoneNumberOrder{CustomerID: customerID, Vendor: "telnyx", E164: e164, Action: OrderBuy, CreatedAt: s.base,
		NextAttemptAt: s.base}
	s.Require().NoError(s.store.PhoneNumberOrders.Insert(s.ctx, &order))
	return order
}

// Two routers settling the same purchase record one number: the order goes with the first.
func (s *StoreSuite) TestAPurchaseSettledTwiceRecordsOneNumber() {
	customerID := newID()
	order := s.order(customerID, "+15125550100")
	first := PhoneNumber{CustomerID: customerID, Vendor: "telnyx", E164: order.E164, PurchasedAt: order.CreatedAt}
	second := first

	bought, err := s.store.PhoneNumberOrders.Bought(s.ctx, order.ID, &first)
	s.Require().NoError(err)
	s.True(bought)
	bought, err = s.store.PhoneNumberOrders.Bought(s.ctx, order.ID, &second)
	s.Require().NoError(err)
	s.False(bought, "the order was settled")

	held, err := s.store.PhoneNumbers.Select(s.ctx, customerID, PhoneNumberFilter{})
	s.Require().NoError(err)
	s.Len(held, 1)
	s.Empty(s.claimOrders())
}

// A number a row already holds is not recorded again, and its order is kept.
func (s *StoreSuite) TestAPurchaseOfANumberARowHoldsRecordsNothing() {
	customerID := newID()
	held := PhoneNumber{CustomerID: customerID, Vendor: "telnyx", E164: "+15125550100", PurchasedAt: s.base}
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &held))
	order := s.order(newID(), held.E164)

	_, err := s.store.PhoneNumberOrders.Bought(s.ctx, order.ID,
		&PhoneNumber{CustomerID: order.CustomerID, Vendor: "telnyx", E164: order.E164, PurchasedAt: order.CreatedAt})

	s.ErrorIs(err, ErrNumberRecorded)
	s.Len(s.claimOrders(), 1)
}

// An order one router claimed is not claimed by another until its lease ends, and is then
// claimed behind the orders due before it.
func (s *StoreSuite) TestAClaimedOrderWaitsForItsLeaseBehindTheOrdersDueFirst() {
	stuck := s.order(newID(), "+15125550100")

	s.Equal([]int64{stuck.ID}, s.orderIDs(s.claimOrders()))
	s.Empty(s.claimOrders(), "another router's claim skips it")
	newer := s.order(newID(), "+15125550101")
	newer.NextAttemptAt = s.base.Add(time.Minute)
	_, err := s.store.global.NewUpdate().Model(&newer).Column("next_attempt_at").WherePK().Exec(s.ctx)
	s.Require().NoError(err)

	first, err := s.store.PhoneNumberOrders.Claim(s.ctx, time.Hour, 1)
	s.Require().NoError(err)
	s.Equal([]int64{newer.ID}, s.orderIDs(first), "the order claimed before is due after the one made since")
}

// claimOrders claims what is due, leased for an hour.
func (s *StoreSuite) claimOrders() []PhoneNumberOrder {
	claimed, err := s.store.PhoneNumberOrders.Claim(s.ctx, time.Hour, 10)
	s.Require().NoError(err)
	return claimed
}

// orderIDs are the ids of orders, in the order a claim took them.
func (s *StoreSuite) orderIDs(orders []PhoneNumberOrder) []int64 {
	var ids []int64
	for _, order := range orders {
		ids = append(ids, order.ID)
	}
	return ids
}

func (s *StoreSuite) TestPhoneNumberOrderQueryBudgets() {
	customerID := newID()
	var order PhoneNumberOrder
	s.Equal(1, s.countQueries(func() { order = s.order(customerID, "+15125550100") }), "Insert")
	s.Equal(1, s.countQueries(func() { s.Len(s.claimOrders(), 1) }), "Claim")
	s.Equal(2+2, s.countQueries(func() {
		bought, err := s.store.PhoneNumberOrders.Bought(s.ctx, order.ID,
			&PhoneNumber{CustomerID: customerID, Vendor: "telnyx", E164: order.E164, PurchasedAt: order.CreatedAt})
		s.Require().NoError(err)
		s.True(bought)
	}), "Bought forgets the order, then records the number")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.PhoneNumberOrders.Delete(s.ctx, order.ID)) }), "Delete")
}
