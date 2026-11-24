package api

import (
	"errors"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// moneyError is the answer for a refusal about money, and false for any other error. A
// suspension is the organization's, for every request, so it is a 403; a spent limit on one
// end user stops that user alone, so it is a 429 like the daily quota's, with Retry-After; a
// router that cannot share what it holds with the others is a 503 to retry shortly; the rest
// refuse only work that costs money, and are a 402 the caller fixes by paying or waiting for a
// reset.
func moneyError(err error) (APIError, bool) {
	for _, refusal := range moneyRefusals {
		if found := refusalOf(err, refusal.kind); found != nil {
			return refusal.answer(strings.TrimPrefix(found.Error(), "policy: ")), true
		}
	}
	return APIError{}, false
}

// moneyRefusals are the refusals about money moneyError answers, each with its answer.
var moneyRefusals = []struct {
	kind   error
	answer func(message string) APIError
}{
	{policy.ErrSuspended, func(message string) APIError {
		return APIError{Type: ErrorTypePermission, Code: codeAccountSuspended, Message: message}
	}},
	{policy.ErrNoCredits, func(message string) APIError { return paymentRequired(codeInsufficientCredits, message) }},
	{policy.ErrTopUpRequired, func(message string) APIError { return paymentRequired(codeTopUpRequired, message) }},
	{policy.ErrSystemLimit, func(message string) APIError { return paymentRequired(codeSystemLimitReached, message) }},
	{policy.ErrBudgetSpent, func(message string) APIError { return paymentRequired(codeSpendLimitReached, message) }},
	{policy.ErrUserLimit, func(message string) APIError {
		return APIError{Type: ErrorTypeRateLimited, Code: codeUserLimitReached, Message: message}
	}},
	{policy.ErrSpendCheckUnavailable, func(message string) APIError {
		return APIError{Type: ErrorTypeUnavailable, Code: codeSpendCheckUnavailable, Message: message}
	}},
}

// moneyCode reports whether code is one moneyError answers a refusal about money with.
func moneyCode(code string) bool {
	for _, refusal := range moneyRefusals {
		if refusal.answer("").Code == code {
			return true
		}
	}
	return false
}

// spendCheckRetry is how soon a caller refused because spend could not be shared is told to
// try again, in seconds.
const spendCheckRetry = "5"

// moneyHeaders is what a refusal about money answers beside its body: Retry-After, in seconds
// until the window ends, for a user's spent limit, and spendCheckRetry when spend could not be
// shared; nothing for the rest.
func moneyHeaders(err error) http.Header {
	if errors.Is(err, policy.ErrSpendCheckUnavailable) {
		return http.Header{"Retry-After": {spendCheckRetry}}
	}
	if !errors.Is(err, policy.ErrUserLimit) {
		return nil
	}
	resets, ok := policy.ResetsAt(err)
	if !ok {
		return nil
	}
	seconds := int64(math.Ceil(time.Until(resets).Seconds()))
	return http.Header{"Retry-After": {strconv.FormatInt(max(seconds, 1), 10)}}
}

// moneyAnswer is a refusal about money as an operation returns it, with its headers.
type moneyAnswer struct {
	APIError
	headers http.Header
}

func (m moneyAnswer) GetHeaders() http.Header { return m.headers }

// refusedRequest is a request routing refused, as the caller is told it: a refusal about money
// as moneyError says, a mistake of the caller's own (callerMistakes) as theirs to change, and a
// feature the deployment lacks, its own Stream app's keys included (unkeptAnswer), as not
// configured. Anything else is ours to answer for, such as a database that failed, whose text
// names where the database is: the error goes on as is, so Huma logs it with its stack and
// tells the caller only that something went wrong.
func refusedRequest(err error) error {
	if answer, ok := moneyError(err); ok {
		if headers := moneyHeaders(err); headers != nil {
			return moneyAnswer{APIError: answer, headers: headers}
		}
		return answer
	}
	if answer, unkept := unkeptAnswer(err); unkept {
		return answer
	}
	switch {
	case errors.Is(err, session.ErrToolNotConfigured):
		return toolNotConfigured(err)
	case errors.Is(err, session.ErrNoMemory):
		return errNoMemory
	case errors.Is(err, session.ErrNotConfigured):
		return notConfigured(err.Error())
	case callerMistake(err):
		return invalidRequest(err.Error())
	}
	return err
}

// callerMistakes are the errors a session, a turn or routing refuses a request with that are
// the caller's to correct. Their messages say what to change and name nothing of the router's.
var callerMistakes = []error{
	session.ErrInvalid, conversation.ErrInvalid, agent.ErrInvalid, harness.ErrInvalid,
	routing.ErrRequest, routing.ErrModelNotAllowed,
	session.ErrNoStreamApp, session.ErrAppStreamKeysRejected, session.ErrNoAgentChannelType, session.ErrUnsafeAgentChannelType,
	session.ErrConversationReadOnly, session.ErrConversationElsewhere, session.ErrMemoryRegion,
	streamapp.ErrStreamAppMoved, streamapp.ErrReadOnly, agent.ErrNoVideo,
	conversation.ErrResponseRunning, conversation.ErrCommandNotFound,
}

// callerMistake reports whether err is one of callerMistakes, or a conversation the caller may
// not read.
func callerMistake(err error) bool {
	if _, refused := errors.AsType[conversation.RefusedRead](err); refused {
		return true
	}
	return slices.ContainsFunc(callerMistakes, func(mistake error) bool { return errors.Is(err, mistake) })
}

// refusalOf is the error in err's tree that is kind or wraps it directly, which reads as the
// policy wrote it rather than with whatever the errors around it added; nil when there is none.
func refusalOf(err, kind error) error {
	if err == nil || !errors.Is(err, kind) {
		return nil
	}
	if err == kind {
		return err
	}
	switch wrapper := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range wrapper.Unwrap() {
			if found := refusalOf(inner, kind); found != nil {
				return found
			}
		}
	case interface{ Unwrap() error }:
		inner := wrapper.Unwrap()
		if inner == kind {
			return err
		}
		return refusalOf(inner, kind)
	}
	return nil
}
