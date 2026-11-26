package channels

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

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

// keywordTimeout bounds recording a STOP or START, which goes on after a provider that hung
// up on its delivery.
const keywordTimeout = 10 * time.Second

// errKeywordNotRecorded fails a delivery whose STOP or START the store could not record, so
// the provider delivers it again.
var errKeywordNotRecorded = errors.New("the message could not be recorded; deliver it again")

// claim takes a message for its line, false when it was taken already, and bills a message
// taken. It records the STOP or START it is in the same transaction: a keyword the store
// could not record leaves the message to be delivered again, and a delivery of it racing this
// one waits for the outcome. Neither the write nor the bill is cut by the provider hanging up
// on a slow delivery.
func (s *Service) claim(ctx context.Context, account store.ChannelAccount, message Message) (bool, error) {
	word := keywordOf(message.Text)
	stop, start := slices.Contains(stopWords, word), slices.Contains(startWords, word)
	if !stop && !start {
		fresh, err := s.store.ChannelDeliveries.Claim(ctx, account.ID, message.ID, message.From)
		if fresh {
			s.received(ctx, account, message)
		}
		return fresh, err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keywordTimeout)
	defer cancel()
	// The sender's number in E.164, read in the line's own country when it came without one.
	recipient := store.OptOutRecipient(message.From, account.E164)
	var fresh bool
	var err error
	if stop {
		fresh, err = s.store.ChannelDeliveries.ClaimStop(ctx, account.ID, message.ID, message.From, &store.OptOut{
			CustomerID: account.CustomerID, Recipient: recipient, Channel: account.Kind, Source: "keyword",
		})
	} else {
		fresh, err = s.store.ChannelDeliveries.ClaimStart(ctx, account.ID, message.ID, message.From,
			account.CustomerID, recipient, account.Kind)
	}
	if err != nil {
		s.logger.Error("could not record a keyword", "keyword", word, "channel", account.Kind,
			"app_id", account.CustomerID, "error", err)
		return false, errKeywordNotRecorded
	}
	if fresh {
		s.received(ctx, account, message)
	}
	return fresh, nil
}

// keyword confirms STOP, START and HELP before any agent sees the message, and reports
// whether the message was one; claim recorded it. The confirmation goes out past the gate:
// telling somebody they are unsubscribed is the one message they are still owed.
func (s *Service) keyword(ctx context.Context, account store.ChannelAccount, line Account, provider Provider, message Message) bool {
	word := keywordOf(message.Text)
	stop, start, help := slices.Contains(stopWords, word), slices.Contains(startWords, word), slices.Contains(helpWords, word)
	if !stop && !start && !help {
		return false
	}

	useCase, err := s.store.UseCases.GetForNumber(ctx, account.CustomerID, account.E164)
	if err != nil && !errors.Is(err, store.ErrUnknownUseCase) {
		s.logger.Error("could not find what a line sends as", "number", account.E164, "error", err)
	}
	var reply string
	switch {
	case stop:
		reply = cmp.Or(useCase.OptOutMessage, stoppedReply)
	case start:
		reply = cmp.Or(useCase.OptInMessage, startedReply)
	default:
		reply = cmp.Or(useCase.HelpMessage, helpReply)
	}
	s.say(ctx, line, provider, message, Reply{Text: reply}, answering(account.CustomerID, ""))
	return true
}

// keywordOf is the word a message is, as the keyword lists spell it.
func keywordOf(text string) string {
	return strings.ToUpper(strings.TrimSpace(text))
}
