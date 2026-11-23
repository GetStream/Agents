package channels

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The words carriers expect a texting program to obey, whatever the agent would have said.
var (
	stopWords  = []string{"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "OPTOUT", "REVOKE"}
	startWords = []string{"START", "UNSTOP"}
	helpWords  = []string{"HELP", "INFO"}
)

// What a keyword is answered with when the number's use case says nothing.
const (
	stoppedReply = "You are unsubscribed and will receive no more messages. Reply START to resubscribe."
	startedReply = "You are subscribed again. Reply STOP to unsubscribe."
	helpReply    = "Reply STOP to unsubscribe."
)

// keyword obeys STOP, START and HELP before any agent sees the message, and answers what the
// message is confirmed with, or "" when it is none. It runs before the delivery is answered,
// so a keyword the store could not record is the provider's to deliver again.
func (s *Service) keyword(ctx context.Context, account store.ChannelAccount, message Message) (string, error) {
	word := strings.ToUpper(strings.TrimSpace(message.Text))
	stop, start, help := slices.Contains(stopWords, word), slices.Contains(startWords, word), slices.Contains(helpWords, word)
	if !stop && !start && !help {
		return "", nil
	}

	useCase, err := s.store.UseCases.GetForNumber(ctx, account.CustomerID, account.E164)
	if err != nil && !errors.Is(err, store.ErrUnknownUseCase) {
		s.logger.Error("could not find what a line sends as", "number", account.E164, "error", err)
	}
	switch {
	case stop:
		err := s.store.OptOuts.Insert(ctx, &store.OptOut{
			CustomerID: account.CustomerID, Recipient: message.From, Channel: account.Kind, Source: "keyword",
		})
		return cmp.Or(useCase.OptOutMessage, stoppedReply), err
	case start:
		err := s.store.OptOuts.RevokeAll(ctx, account.CustomerID, message.From, account.Kind)
		return cmp.Or(useCase.OptInMessage, startedReply), err
	default:
		return cmp.Or(useCase.HelpMessage, helpReply), nil
	}
}

// confirm writes back what a keyword is answered with. It goes out past the gate: telling
// somebody they are unsubscribed is the one message they are still owed.
func (s *Service) confirm(account store.ChannelAccount, line Account, provider Provider, message Message, confirmation string) {
	ctx, cancel := context.WithTimeout(s.ctx, runTimeout)
	defer cancel()
	s.say(ctx, line, provider, message, Reply{Text: confirmation}, answering(account.CustomerID, ""))
}
