// Package gatetest is a money gate that answers what a test says.
package gatetest

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// Answers is a gate that answers what a test says, for a test on a fake clock, which the real
// enforcer's reads of Postgres and Redis would stop.
type Answers struct {
	mu   sync.Mutex
	err  error
	held map[uuid.UUID]int64
	// admitted is how many times it was asked for an admission.
	admitted int
}

// Admitted is how many admissions it was asked for.
func (a *Answers) Admitted() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.admitted
}

// Refuse answers err from now on, and grants when it is nil.
func (a *Answers) Refuse(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.err = err
}

// Held is how much is reserved.
func (a *Answers) Held() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var total int64
	for _, micros := range a.held {
		total += micros
	}
	return total
}

// Writer is a writer its recorders reserve through, whose rows nothing is to record.
func (a *Answers) Writer() *routing.Writer {
	return routing.NewWriter(routing.WriterOptions{Store: &store.Store{}, Gate: a})
}

func (a *Answers) Admit(context.Context, routing.Owner) (routing.Admission, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.admitted++
	return routing.Admission{Spend: a.err}, nil
}

func (a *Answers) Reserve(_ routing.Owner, usageID uuid.UUID, micros int64, _ time.Duration) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	if a.held == nil {
		a.held = map[uuid.UUID]int64{}
	}
	a.held[usageID] = micros
	return nil
}

func (a *Answers) Release(usageID uuid.UUID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.held, usageID)
}

// Unchecked is a refusal of money that could not be checked.
type Unchecked struct{}

func (Unchecked) Error() string   { return "the money could not be checked" }
func (Unchecked) Unchecked() bool { return true }
