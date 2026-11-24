package routing

import (
	"context"
	"sync/atomic"
)

// Spent adds up what the rows recorded for one request, or one socket, cost, so the answer can
// say so. Memory only; the rows are what is billed.
type Spent struct {
	micros    atomic.Int64
	rows      atomic.Int64
	estimated atomic.Bool
	// within is the Spent this one is a part of, which every row added here is added to too.
	within *Spent
}

type spentKey struct{}

// WithSpent has every row recorded for work started with the returned context add its cost to
// the returned Spent.
func WithSpent(ctx context.Context) (context.Context, *Spent) {
	spent := &Spent{}
	return context.WithValue(ctx, spentKey{}, spent), spent
}

// SpentFrom is the Spent WithSpent put in ctx, nil when nobody asked.
func SpentFrom(ctx context.Context) *Spent {
	spent, _ := ctx.Value(spentKey{}).(*Spent)
	return spent
}

// Part is a Spent of its own for part of the work, such as one session of a socket, whose
// rows add to s as well. A nil s gives a Spent that adds to nothing else.
func (s *Spent) Part() *Spent {
	return &Spent{within: s}
}

// add counts one row of the cost, estimated when its quantities were the router's own count,
// here and in every Spent this one is a part of.
func (s *Spent) add(micros int64, estimated bool) {
	for spent := s; spent != nil; spent = spent.within {
		spent.micros.Add(micros)
		if estimated {
			spent.estimated.Store(true)
		}
		spent.rows.Add(1)
	}
}

// Total is what every row recorded so far cost, how many there were, and whether any was an
// estimate.
func (s *Spent) Total() (micros, rows int64, estimated bool) {
	if s == nil {
		return 0, 0, false
	}
	return s.micros.Load(), s.rows.Load(), s.estimated.Load()
}
