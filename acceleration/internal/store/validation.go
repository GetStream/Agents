package store

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/uptrace/bun/driver/pgdriver"
)

// ValidationError is a row the store refused to write because a field breaks the model's
// rules. Nothing was written.
type ValidationError struct {
	// Model is the model refused, such as "session".
	Model string
	// Field is the column at fault, such as "customer_id".
	Field string
	// Reason says what is wrong with it.
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("store: %s: %s %s", e.Model, e.Field, e.Reason)
}

// validation collects the first failure of a model's checks, so Validate reads as a list of
// rules.
type validation struct {
	model string
	err   *ValidationError
}

func validate(model string) *validation { return &validation{model: model} }

// check refuses field for reason unless ok.
func (v *validation) check(ok bool, field, reason string) *validation {
	if v.err == nil && !ok {
		v.err = &ValidationError{Model: v.model, Field: field, Reason: reason}
	}
	return v
}

// required refuses an empty or blank value.
func (v *validation) required(field, value string) *validation {
	return v.check(strings.TrimSpace(value) != "", field, "is required")
}

// maxLength refuses a value longer than limit characters.
func (v *validation) maxLength(field, value string, limit int) *validation {
	return v.check(utf8.RuneCountInString(value) <= limit, field, fmt.Sprintf("is longer than %d characters", limit))
}

// oneOf refuses a value not in allowed. Empty is allowed only when allowed lists it.
func (v *validation) oneOf(field, value string, allowed ...string) *validation {
	for _, candidate := range allowed {
		if value == candidate {
			return v
		}
	}
	return v.check(false, field, fmt.Sprintf("is %q, want one of %s", value, strings.Join(allowed, ", ")))
}

// nonNegative refuses a value below zero.
func (v *validation) nonNegative(field string, value float64) *validation {
	return v.check(value >= 0, field, "is negative")
}

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// phoneNumber refuses a non-empty value that is not an E.164 number.
func (v *validation) phoneNumber(field, value string) *validation {
	return v.check(value == "" || e164.MatchString(value), field, "is not an E.164 number")
}

func (v *validation) done() error {
	if v.err == nil {
		return nil
	}
	return v.err
}

// constraint is the name of the unique constraint an error violated, empty for anything
// else.
func constraint(err error) string {
	var failure pgdriver.Error
	if errors.As(err, &failure) && failure.Field('C') == "23505" {
		return failure.Field('n')
	}
	return ""
}
