package agent

import "context"

// Requester is the person a tool call was made for: whoever spoke the turn the model was
// answering on a call, or sent the command it was answering in writing.
type Requester struct {
	UserID string
	Name   string
}

type requesterKey struct{}

// WithRequester says who the tool call ctx runs is for.
func WithRequester(ctx context.Context, requester Requester) context.Context {
	return context.WithValue(ctx, requesterKey{}, requester)
}

// RequesterFrom is who the tool call ctx runs is for, if the agent knew.
func RequesterFrom(ctx context.Context) (Requester, bool) {
	requester, ok := ctx.Value(requesterKey{}).(Requester)
	return requester, ok
}

type scheduledCallKey struct{}

// WithScheduledCall marks ctx as a tool call asked for in a turn a scheduled task fired, which
// nobody asked for just now. It is not routing.WithScheduledTurn: that one makes spend the
// customer's, and what the tool does after is still charged as it was.
func WithScheduledCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, scheduledCallKey{}, true)
}

// ScheduledCall reports whether the tool call ctx runs was asked for in a turn a scheduled task
// fired.
func ScheduledCall(ctx context.Context) bool {
	scheduled, _ := ctx.Value(scheduledCallKey{}).(bool)
	return scheduled
}

// requesterLocked is who the turn being answered came from. A participant heard on a call
// names their user; a message written in a channel of a type an agent config answers in names
// its writer (WithWriter); a typed command names its sender. The caller holds a.mu.
func (a *Agent) requesterLocked() Requester {
	requester := Requester{UserID: a.lastParticipant.UserID, Name: a.lastParticipant.Name}
	if requester.UserID == "" {
		requester = Requester{UserID: a.lastWriter.id, Name: a.lastWriter.name}
	}
	if requester.UserID == "" {
		requester.UserID = a.sender.UserID
	}
	if requester.UserID == "" {
		return Requester{}
	}
	return requester
}
