//go:build integration

package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/channelbridge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
	"github.com/GetStream/Vision-Agents/acceleration/internal/chatlog"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// TelnyxUnpaidSuite is a Telnyx line whose customer's organization has its money read from a
// real usage service, so the gate refuses its texts as a router refuses an organization that may
// not pay: suspended, or with no credit.
type TelnyxUnpaidSuite struct {
	telnyxLine
	// sharing is the Redis the router shares its holds through, behind a proxy a test can take
	// down, so its money cannot be checked.
	sharing *redisProxy
}

func TestTelnyxUnpaidSuite(t *testing.T) {
	runSuite(t, new(TelnyxUnpaidSuite))
}

// SetupSuite gives the suite a database of its own, as its tests start a scheduled-task worker.
func (s *TelnyxUnpaidSuite) SetupSuite() {
	s.database = "telnyx_unpaid"
	s.limited = true
	s.sharing, s.holds = newRedisProxy(s.T())
	s.telnyxLine.SetupSuite()
}

func (s *TelnyxUnpaidSuite) SetupTest() {
	s.sharing.down.Store(false)
	s.telnyxLine.SetupTest()
}

// refusedForMoney waits until the gate refuses the customer a text to person for money: an
// organization's money is read after its first admission, which it admits.
func (s *TelnyxUnpaidSuite) refusedForMoney(person string) {
	s.Require().Eventually(func() bool {
		return errors.Is(s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person), dlc.ErrUnpaid)
	}, 10*time.Second, 50*time.Millisecond, "the organization's money is read")
}

// creditedThread has person text the line while the organization has credit, so the thread is
// linked and answered, and returns the thread channel.
func (s *TelnyxUnpaidSuite) creditedThread(person string) string {
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	return s.smsTurn(person, "hi")
}

// suspendedThread is creditedThread, with the organization suspended afterwards.
func (s *TelnyxUnpaidSuite) suspendedThread(person string) string {
	channel := s.creditedThread(person)
	s.usage.Suspend(s.T(), s.RouterSuite.app.organization.ID, "fraud review")
	s.refusedForMoney(person)
	return channel
}

// threadTask is a task made in the thread channel, by the person who wrote in it.
func (s *TelnyxUnpaidSuite) threadTask(channel string) store.ScheduledTask {
	user := s.chat.Stored(channel)[0]["user_id"].(string)
	next := time.Now().Add(2 * time.Hour).UTC()
	task := store.ScheduledTask{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user,
		CallerKind: "server", APIKey: s.RouterSuite.app.key, ChannelThread: channel, Title: "Stretch",
		Instructions: "Remind them to stretch.", StartLocal: next, Timezone: "Europe/Amsterdam", NextRunAt: &next,
		Channels: []string{"conversation"}}
	s.Require().NoError(s.store.ScheduledTasks.Insert(context.Background(), &task, 0))
	return task
}

// settledByTheWorker makes task due, has a worker deliver it with its retries at once, and
// returns its settled run and how many attempts the worker made.
func (s *TelnyxUnpaidSuite) settledByTheWorker(task store.ScheduledTask) (store.ScheduledTaskRun, int32) {
	return s.settledByTheWorkerAfter(task, nil)
}

// settledByTheWorkerAfter is settledByTheWorker, with before run ahead of each attempt, given
// the attempt's number from 1.
func (s *TelnyxUnpaidSuite) settledByTheWorkerAfter(task store.ScheduledTask, before func(ctx context.Context, attempt int32)) (store.ScheduledTaskRun, int32) {
	s.retryAtOnce()
	var attempts atomic.Int32
	deliver := func(ctx context.Context, claimed store.ClaimedScheduledTask) (string, string, error) {
		attempt := attempts.Add(1)
		if before != nil {
			before(ctx, attempt)
		}
		return s.router.DeliverScheduledTask(ctx, claimed)
	}
	logger := slog.New(slog.NewTextHandler(s.logged, nil))
	worker := scheduling.NewScheduler(s.store, deliver, logger)
	worker.Start(context.Background())
	defer worker.Close()
	past := time.Now().Add(-time.Minute)
	task.NextRunAt = &past
	s.Require().NoError(s.store.ScheduledTasks.Update(context.Background(), &task))
	worker.Kick()
	var settled store.ScheduledTaskRun
	s.Require().Eventually(func() bool {
		runs, err := s.store.ScheduledTaskRuns.Select(context.Background(), task.CustomerID, store.ScheduledTaskRunFilter{TaskID: task.ID, Limit: 25})
		s.Require().NoError(err)
		if len(runs) == 0 || runs[0].State == store.ScheduledTaskRunRunning {
			return false
		}
		settled = runs[0]
		return true
	}, deliveryWait, 20*time.Millisecond, "the task's run never settled")
	return settled, attempts.Load()
}

// A task made in a thread of a suspended organization texts nothing and is not tried again: the
// run says the account is suspended, and the router warns, naming the customer.
func (s *TelnyxUnpaidSuite) TestAThreadTaskOfASuspendedOrganizationIsNotRetried() {
	person := "+13125550001"
	task := s.threadTask(s.suspendedThread(person))

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeAccountSuspended, *run.Error)
	s.Equal(int32(1), attempts, "a suspension outlasts every retry of the run")
	s.Len(s.telnyx().sent(), 1, "only the answer to hi")
	s.Contains(s.logged.String(), `level=WARN msg="held a channel message back: the organization may not pay for it" connector=telnyx customer=`+s.customerID())
}

// A thread task whose organization's money cannot be checked, as when the router cannot share
// what it holds with the others, is tried again: the check passes again on its own.
func (s *TelnyxUnpaidSuite) TestAThreadTaskWhoseMoneyCannotBeCheckedIsRetried() {
	person := "+13125550003"
	task := s.threadTask(s.creditedThread(person))
	s.sharing.stop()
	s.Require().Eventually(func() bool {
		// Each search the router admits holds its estimate, which cannot be shared.
		s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "again"})
		err := s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person)
		return errors.Is(err, dlc.ErrUnpaid) && policy.Transient(err)
	}, 10*time.Second, 20*time.Millisecond, "refused once a hold waited unshared")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendCheckUnavailable, *run.Error)
	s.Equal(int32(1+len(scheduling.DeliveryRetries)), attempts, "tried again after each wait")
}

// A reply the agent finished in a thread of a suspended organization is not sent, and is no
// failure to log: the gate's warning says why.
func (s *TelnyxUnpaidSuite) TestAReplyOfASuspendedOrganizationIsNotSentAndLogsNoError() {
	person := "+13125550002"
	channel := s.suspendedThread(person)

	bridge, built := s.bridge.(*channelbridge.Bridge)
	s.Require().True(built)

	bridge.Reply(conversation.FinishedReply{Customer: s.customerID(), CID: "agent:" + channel,
		MessageID: s.utils.uuid(), Text: "Time to stretch."})

	s.Eventually(func() bool { return bridge.Replying() == 0 }, settleFor, 10*time.Millisecond)
	s.Len(s.telnyx().sent(), 1, "only the answer to hi")
	s.Contains(s.logged.String(), `level=WARN msg="held a channel message back: the organization may not pay for it"`)
	s.NotContains(s.logged.String(), "could not send a reply to its external thread")
}

// spentAfterOpening is a user who texts the agent's SMS line, and a session they opened on the
// agent while the organization had credit; the credit is spent afterwards, so what costs money
// is refused.
func (s *TelnyxUnpaidSuite) spentAfterOpening() (*testClient, Session) {
	number, writer := "+1555"+digits(s.utils.uuid()), "+1347"+digits(s.utils.uuid())
	s.seedSMSLine(number)
	s.config.Channels.Lines = store.AgentChannels{SMS: &store.ChannelLine{Number: number}}
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	user := s.data.createUser()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(context.Background(), &store.ContactMapEntry{
		CustomerID: s.customerID(), ConfigID: s.config.ID, Kind: string(channels.SMS), Address: writer, UserID: user.userID,
	}))
	organization := s.RouterSuite.app.organization.ID
	s.usage.Credit(s.T(), organization, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	opened := user.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
	s.usage.Credit(s.T(), organization, -10_000_000)
	s.refusedForMoney(writer)
	return user, opened
}

// userTask is a task of user's on channels, following up in the session sessionID, or opening
// one on its first occurrence when sessionID is empty.
func (s *TelnyxUnpaidSuite) userTask(user *testClient, sessionID string, channels ...string) store.ScheduledTask {
	next := time.Now().Add(2 * time.Hour).UTC()
	task := store.ScheduledTask{CustomerID: s.customerID(), ConfigID: s.config.ID, SessionID: sessionID, UserID: user.userID,
		CallerKind: "authenticated", APIKey: s.RouterSuite.app.key, Title: "Stretch", Instructions: "Remind them to stretch.",
		StartLocal: next, Timezone: "Europe/Amsterdam", NextRunAt: &next, Channels: channels}
	s.Require().NoError(s.store.ScheduledTasks.Insert(context.Background(), &task, 0))
	return task
}

// A task delivered by SMS for an organization whose credit ran out is not tried again: the run
// says the credit is spent, and the router warns, naming the task.
func (s *TelnyxUnpaidSuite) TestAnSMSTaskOfAnOrganizationWithNoCreditIsNotRetried() {
	// The task follows up in a session opened while the organization had credit, so what its
	// spent credit refuses is the text.
	user, opened := s.spentAfterOpening()
	task := s.userTask(user, opened.Id, "sms")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeInsufficientCredits, *run.Error)
	s.Equal(int32(1), attempts, "spent credit outlasts every retry of the run")
	s.Contains(s.logged.String(), `level=WARN msg="a scheduled task failed to deliver" task=`+task.ID)
}

// A task whose organization is suspended while its turn runs passes Reachable, and the text
// Notify then sends is refused with dlc.ErrUnpaid: the run is not tried again.
func (s *TelnyxUnpaidSuite) TestAnSMSTaskWhoseOrganizationStopsPayingDuringItsTurnIsNotRetried() {
	number, writer := "+1555"+digits(s.utils.uuid()), "+1347"+digits(s.utils.uuid())
	s.seedSMSLine(number)
	s.config.Channels.Lines = store.AgentChannels{SMS: &store.ChannelLine{Number: number}}
	s.config.Models.LLM = "holding/holding-model"
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	user := s.data.createUser()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(context.Background(), &store.ContactMapEntry{
		CustomerID: s.customerID(), ConfigID: s.config.ID, Kind: string(channels.SMS), Address: writer, UserID: user.userID,
	}))
	organization := s.RouterSuite.app.organization.ID
	s.usage.Credit(s.T(), organization, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	opened := user.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
	next := time.Now().Add(2 * time.Hour).UTC()
	task := store.ScheduledTask{CustomerID: s.customerID(), ConfigID: s.config.ID, SessionID: opened.Id, UserID: user.userID,
		CallerKind: "authenticated", APIKey: s.RouterSuite.app.key, Title: "Stretch", Instructions: "Remind them to stretch.",
		StartLocal: next, Timezone: "Europe/Amsterdam", NextRunAt: &next, Channels: []string{"sms"}}
	s.Require().NoError(s.store.ScheduledTasks.Insert(context.Background(), &task, 0))

	model := s.holding
	model.mu.Lock()
	before := model.turns
	model.mu.Unlock()
	model.holds()
	defer model.answers()
	type result struct {
		run      store.ScheduledTaskRun
		attempts int32
	}
	done := make(chan result, 1)
	go func() {
		run, attempts := s.settledByTheWorker(task)
		done <- result{run, attempts}
	}()
	s.Require().Eventually(func() bool {
		model.mu.Lock()
		defer model.mu.Unlock()
		return model.turns > before
	}, deliveryWait, 10*time.Millisecond, "the task's turn never began")
	s.usage.Suspend(s.T(), organization, "fraud review")
	s.refusedForMoney(writer)
	model.answers()

	got := <-done
	s.Equal(store.ScheduledTaskRunFailed, got.run.State)
	s.Require().NotNil(got.run.Error)
	s.Equal(codeAccountSuspended, *got.run.Error)
	s.Equal(int32(1), got.attempts, "a suspension outlasts every retry of the run")
}

// A task whose first occurrence cannot open its session, the credit being spent, is not tried
// again: the run says the credit is spent.
func (s *TelnyxUnpaidSuite) TestATaskWhoseSessionCannotOpenForMoneyIsNotRetried() {
	user, _ := s.spentAfterOpening()
	task := s.userTask(user, "", "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeInsufficientCredits, *run.Error)
	s.Equal(int32(1), attempts, "spent credit outlasts every retry of the run")
}

// A task whose session ended and cannot be carried on, the credit being spent, is not tried
// again: the run says the credit is spent.
func (s *TelnyxUnpaidSuite) TestATaskWhoseEndedSessionCannotReopenForMoneyIsNotRetried() {
	user, opened := s.spentAfterOpening()
	status, body := user.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/stop", nil)
	s.Require().Equal(http.StatusNoContent, status, string(body))
	task := s.userTask(user, opened.Id, "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeInsufficientCredits, *run.Error)
	s.Equal(int32(1), attempts, "spent credit outlasts every retry of the run")
}

// A message to an ended session that cannot be carried on, the credit being spent, is refused for
// money as any request is: the scheduled delivery's own answer to the refusal is its alone.
func (s *TelnyxUnpaidSuite) TestAMessageToAnEndedSessionOfAnOrganizationWithNoCreditIsRefusedForMoney() {
	user, opened := s.spentAfterOpening()
	status, body := user.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/stop", nil)
	s.Require().Equal(http.StatusNoContent, status, string(body))
	request := s.utils.uuid()

	status, body = s.serverClient.actingFor(user).call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/respond",
		RespondRequest{Text: "still there?", RequestId: &request})

	s.Equal(http.StatusPaymentRequired, status, string(body))
	s.Contains(string(body), `"code":"insufficient_credits"`)
}

// A phone task whose call the gate refuses, the credit being spent, rings nobody and is not tried
// again: the run says the credit is spent.
func (s *TelnyxUnpaidSuite) TestAPhoneTaskOfAnOrganizationWithNoCreditIsNotRetried() {
	user, opened := s.spentAfterOpening()
	task := s.userTask(user, opened.Id, "phone")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeInsufficientCredits, *run.Error)
	s.Equal(int32(1), attempts, "spent credit outlasts every retry of the run")
}

// A task whose session cannot open because the organization's money cannot be checked is tried
// again: the check passes again on its own.
func (s *TelnyxUnpaidSuite) TestATaskWhoseSessionCannotOpenWhileMoneyCannotBeCheckedIsRetried() {
	person := "+1347" + digits(s.utils.uuid())
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	s.sharing.stop()
	s.Require().Eventually(func() bool {
		s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "again"})
		err := s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person)
		return errors.Is(err, dlc.ErrUnpaid) && policy.Transient(err)
	}, 10*time.Second, 20*time.Millisecond, "refused once a hold waited unshared")
	task := s.userTask(s.data.createUser(), "", "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendCheckUnavailable, *run.Error)
	s.Equal(int32(1+len(scheduling.DeliveryRetries)), attempts, "tried again after each wait")
}

// reservationRefused is an SMS the router refuses at its reservation, before Telnyx is asked, as
// Notify's send is refused when the money changes after the gate let it through, recorded through a
// writer the policies gate, as cmd/router's telephony is. Its meter is the one a text to a US number
// is sent under (channels.Service.textMeter): the reservation prices it with its direction and
// country, as its row is priced (routing.Meter.reserve).
func (s *TelnyxUnpaidSuite) reservationRefused() error {
	logger := slog.New(slog.DiscardHandler)
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Logger: logger, Gate: s.enforcer})
	defer writer.Close(context.Background())
	list, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), logger)
	s.Require().NoError(err)
	meter := routing.Meter{Recorder: routing.NewRecorderOn(routing.Phone, writer, list, logger),
		Owner:      routing.Owner{CustomerID: s.customerID(), Attribution: routing.Attribution{APIKey: s.RouterSuite.app.key}},
		Config:     routing.ProviderConfig{Provider: channels.Vendor(channels.SMS), Model: string(channels.SMS)},
		Attributes: map[string]string{phone.AttributeDirection: store.Outbound, phone.AttributeCountry: phone.Country("+13479018418")},
		Estimate:   pricing.Quantities{pricing.Messages: 1, pricing.MessageSegments: 1}}
	_, err = routing.Call(context.Background(), meter, func(context.Context) (channels.Sent, error) {
		s.Fail("a refused reservation asks no vendor")
		return channels.Sent{}, nil
	})
	s.Require().True(routing.ReservationRefused(err), "refused at the reservation: %v", err)
	s.Require().NotErrorIs(err, dlc.ErrUnpaid, "the gate did not refuse it")
	return err
}

// A text of a suspended organization refused at its reservation is recorded under the
// suspension, and not tried again.
func (s *TelnyxUnpaidSuite) TestATextRefusedAtItsReservationIsFinal() {
	s.suspendedThread("+1347" + digits(s.utils.uuid()))

	var failure *scheduling.Failure
	s.Require().ErrorAs(unpaid(s.reservationRefused()), &failure)
	s.Equal(codeAccountSuspended, failure.Reason)
	s.True(failure.Final, "a suspension outlasts every retry of the run")
}

// A text refused at its reservation because the organization's money cannot be checked is tried
// again.
func (s *TelnyxUnpaidSuite) TestATextRefusedAtItsReservationWhileMoneyCannotBeCheckedIsRetried() {
	person := "+1347" + digits(s.utils.uuid())
	s.creditedThread(person)
	s.sharing.stop()
	s.Require().Eventually(func() bool {
		s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "again"})
		return policy.Transient(s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person))
	}, 10*time.Second, 20*time.Millisecond, "refused once a hold waited unshared")

	var failure *scheduling.Failure
	s.Require().ErrorAs(unpaid(s.reservationRefused()), &failure)
	s.Equal(codeSpendCheckUnavailable, failure.Reason)
	s.False(failure.Final, "the check passes again on its own")
}

// refusedTurn is the follow-up of a task due in the session opened before the credit ran out,
// captured as a text channel captures it once the gate let the send through: the money changed
// after it did, so the turn is what the organization's money refuses.
func (s *TelnyxUnpaidSuite) refusedTurn(user *testClient, opened Session) error {
	task := s.userTask(user, opened.Id, "sms")
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: task.ID, RunID: "run", CommandID: "st_run", Text: "stretch"}
	_, _, err := s.router.captureReply(scheduledOwnerContext(context.Background(), delivery), found, delivery)
	return err
}

// A turn refused for money inside a session that is open is not tried again: the run records the
// money, not an agent that said nothing.
func (s *TelnyxUnpaidSuite) TestATurnRefusedForMoneyInAnOpenSessionIsFinal() {
	var failure *scheduling.Failure
	user, opened := s.spentAfterOpening()
	s.Require().ErrorAs(s.refusedTurn(user, opened), &failure)
	s.Equal(codeInsufficientCredits, failure.Reason)
	s.True(failure.Final, "spent credit outlasts every retry of the run")
}

// uncheckableAfterOpening is a user who texts the agent's SMS line, and a session they opened on
// the agent while the organization had credit; the Redis its holds are shared through goes down
// afterwards, so what costs money is refused because the money cannot be checked.
func (s *TelnyxUnpaidSuite) uncheckableAfterOpening() (*testClient, Session) {
	return s.uncheckableAfterOpeningOn(nil)
}

// uncheckableAfterOpeningOn is uncheckableAfterOpening with the session opened on node, or on the
// suite's router when node is nil.
func (s *TelnyxUnpaidSuite) uncheckableAfterOpeningOn(node *httptest.Server) (*testClient, Session) {
	number, writer := "+1555"+digits(s.utils.uuid()), "+1347"+digits(s.utils.uuid())
	s.seedSMSLine(number)
	s.config.Channels.Lines = store.AgentChannels{SMS: &store.ChannelLine{Number: number}}
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	user := s.data.createUser()
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	opener := user
	if node != nil {
		opener = user.on(node)
	}
	opened := opener.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
	s.sharing.stop()
	s.Require().Eventually(func() bool {
		s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "again"})
		return policy.Transient(s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer))
	}, 10*time.Second, 20*time.Millisecond, "refused once a hold waited unshared")
	return user, opened
}

// A turn refused because the organization's money cannot be checked is tried again.
func (s *TelnyxUnpaidSuite) TestATurnRefusedWhileMoneyCannotBeCheckedIsRetried() {
	user, opened := s.uncheckableAfterOpening()

	var failure *scheduling.Failure
	s.Require().ErrorAs(s.refusedTurn(user, opened), &failure)
	s.Equal(codeSpendCheckUnavailable, failure.Reason)
	s.False(failure.Final, "the check passes again on its own")
}

// conversationTurn is the follow-up of a task due in the session opened, delivered on the
// conversation channel, which carries the reply in the session itself.
func (s *TelnyxUnpaidSuite) conversationTurn(user *testClient, opened Session) error {
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: "task", RunID: "run", CommandID: "st_run", Text: "stretch"}
	_, _, err := s.router.deliverOn(scheduledOwnerContext(context.Background(), delivery), delivery, found,
		store.ScheduledTaskDeliveredConversation)
	return err
}

// A task delivered in the conversation of a session that is open, whose turn the model refuses for
// money, did not reach the user: its run fails under the refusal's code, as the API shows it, and is
// not tried again (AI-3183).
func (s *TelnyxUnpaidSuite) TestAConversationTaskWhoseTurnIsRefusedForMoneyIsNotRetried() {
	user, opened := s.spentAfterOpening()
	task := s.userTask(user, opened.Id, "conversation")

	_, attempts := s.settledByTheWorker(task)

	var page ScheduledTaskRunPage
	s.Require().Equal(http.StatusOK, user.do(http.MethodGet, "/v1/agents/scheduled-tasks/"+task.ID+"/runs", nil, &page))
	s.Require().Len(page.Items, 1)
	s.Equal(store.ScheduledTaskRunFailed, page.Items[0].State)
	s.Empty(page.Items[0].DeliveredTo)
	s.Require().NotNil(page.Items[0].Error)
	s.Equal(codeInsufficientCredits, *page.Items[0].Error)
	s.Equal(int32(1), attempts, "spent credit outlasts every retry of the run")
}

// A conversation turn refused because the organization's money cannot be checked is tried again:
// the check passes again on its own.
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedWhileMoneyCannotBeCheckedIsRetried() {
	user, opened := s.uncheckableAfterOpening()

	var failure *scheduling.Failure
	s.Require().ErrorAs(s.conversationTurn(user, opened), &failure)
	s.Equal(codeSpendCheckUnavailable, failure.Reason)
	s.False(failure.Final, "the check passes again on its own")
}

// checkedAgainOnRetry is a before for settledByTheWorkerAfter that holds the second attempt
// until restored is closed, and closes retrying once that attempt waits on it.
func (s *TelnyxUnpaidSuite) checkedAgainOnRetry() (before func(ctx context.Context, attempt int32), retrying <-chan struct{}, restored chan<- struct{}) {
	second, back := make(chan struct{}), make(chan struct{})
	return func(ctx context.Context, attempt int32) {
		if attempt != 2 {
			return
		}
		close(second)
		select {
		case <-back:
		case <-ctx.Done():
		}
	}, second, back
}

// sharedAgain brings back the Redis the router shares its holds through, and waits until what
// costs money is admitted again.
func (s *TelnyxUnpaidSuite) sharedAgain() {
	s.sharing.down.Store(false)
	s.Require().Eventually(func() bool {
		status, _ := s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "back"})
		return status == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "admitted again once a flush lands")
}

// A conversation task whose turn is refused because the money cannot be checked runs its turn
// again on the retry once the check passes: the user gets the reply the first turn never gave,
// and the run is delivered (AI-3183).
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedWhileMoneyCannotBeCheckedRunsAgainOnItsRetry() {
	user, opened := s.uncheckableAfterOpening()
	task := s.userTask(user, opened.Id, "conversation")
	before, retrying, restored := s.checkedAgainOnRetry()
	type result struct {
		run      store.ScheduledTaskRun
		attempts int32
	}
	done := make(chan result, 1)
	go func() {
		run, attempts := s.settledByTheWorkerAfter(task, before)
		done <- result{run, attempts}
	}()
	select {
	case <-retrying:
	case <-time.After(deliveryWait):
		s.FailNow("the task was never tried again")
	}
	s.sharedAgain()
	close(restored)

	got := <-done
	run, attempts := got.run, got.attempts
	s.Equal(store.ScheduledTaskRunDelivered, run.State)
	s.Equal(store.ScheduledTaskDeliveredConversation, run.DeliveredTo)
	s.Nil(run.Error)
	s.Equal(int32(2), attempts)
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	refused, _, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID)
	s.Equal("failed", refused, "the first turn was refused")
	state, said, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID + rerunSuffix)
	s.Equal("completed", state, "the retry ran the turn again")
	s.NotEmpty(said, "the user got a reply")
}

// A conversation task whose turn is refused because the money cannot be checked, and whose rerun
// on the retry is refused again, did not reach the user: the run fails under the money once its
// attempts are spent, as an SMS task's does, and is never delivered (AI-3183).
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedAgainOnItsRerunFailsTheRun() {
	user, opened := s.uncheckableAfterOpening()
	task := s.userTask(user, opened.Id, "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Empty(run.DeliveredTo)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendCheckUnavailable, *run.Error)
	s.Equal(int32(1+len(scheduling.DeliveryRetries)), attempts, "tried again after each wait")
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	rerun, _, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID + rerunSuffix)
	s.Equal("failed", rerun, "the retry ran the turn again, and it was refused again")
}

// A conversation task whose session another router holds is handed to that router on every
// attempt, with the reason the attempt before failed under: once a turn there is refused because
// the money cannot be checked, the retry runs it again there, and the user gets the reply
// (AI-3183).
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedOnAnotherRouterRunsAgainThereOnItsRetry() {
	other, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	user, opened := s.uncheckableAfterOpeningOn(other)
	task := s.userTask(user, opened.Id, "conversation")
	before, retrying, restored := s.checkedAgainOnRetry()
	type result struct {
		run      store.ScheduledTaskRun
		attempts int32
	}
	done := make(chan result, 1)
	go func() {
		run, attempts := s.settledByTheWorkerAfter(task, before)
		done <- result{run, attempts}
	}()
	select {
	case <-retrying:
	case <-time.After(deliveryWait):
		s.FailNow("the task was never tried again")
	}
	s.sharedAgain()
	close(restored)

	got := <-done
	run, attempts := got.run, got.attempts
	s.Equal(store.ScheduledTaskRunDelivered, run.State)
	s.Equal(store.ScheduledTaskDeliveredConversation, run.DeliveredTo)
	s.Equal(int32(2), attempts)
	found, running := sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running, "the session is the other router's")
	refused, _, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID)
	s.Equal("failed", refused, "the first turn was refused")
	state, said, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID + rerunSuffix)
	s.Equal("completed", state, "the retry ran the turn again on the router holding the session")
	s.NotEmpty(said, "the user got a reply")
}

// A thread task whose reply failed, retried after a refusal for money, runs its turn again in the
// thread under the rerun's command id, as the conversation and the text channels do.
func (s *TelnyxUnpaidSuite) TestAThreadRetryAfterARefusalForMoneyRunsTheTurnAgain() {
	channel := s.creditedThread("+1347" + digits(s.utils.uuid()))
	s.config.Models.LLM = "failing/failing-model"
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	found, running := s.router.sessions.ByConversationWhere(chatlog.ChannelType+":"+channel, func(string, int64) bool { return true })
	s.Require().True(running)
	task := s.threadTask(channel)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: task.UserID,
		CallerKind: "server", TaskID: task.ID, RunID: "run", CommandID: "st_run", Text: "stretch"}
	ctx := scheduledOwnerContext(context.Background(), delivery)
	_, _, err := s.router.deliverOnThread(ctx, delivery, channel)
	s.Require().Error(err)
	state, _, _ := found.FollowUpReply("st_run")
	s.Require().Equal("failed", state)

	// The retry carries the reason the attempt before it failed under, as the scheduler records it.
	delivery.Earlier = codeSpendCheckUnavailable
	_, _, err = s.router.deliverOnThread(ctx, delivery, channel)

	s.Equal(scheduling.ReasonReplyFailed, scheduling.ReasonOf(err), "the rerun failed on the model too")
	_, _, rerun := found.FollowUpReply("st_run" + rerunSuffix)
	s.True(rerun, "the retry after a refusal for money ran the turn again")
}

// A text channel's turn refused because the money cannot be checked runs again on the attempt
// after it, once the check passes, and that attempt carries what the agent said.
func (s *TelnyxUnpaidSuite) TestATextTurnRefusedWhileMoneyCannotBeCheckedRunsAgainOnItsRetry() {
	user, opened := s.uncheckableAfterOpening()
	task := s.userTask(user, opened.Id, "sms")
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: task.ID, RunID: "run", CommandID: "st_run", Text: "stretch"}
	ctx := scheduledOwnerContext(context.Background(), delivery)
	var failure *scheduling.Failure
	_, _, err := s.router.captureReply(ctx, found, delivery)
	s.Require().ErrorAs(err, &failure)
	s.Require().Equal(codeSpendCheckUnavailable, failure.Reason)
	s.sharedAgain()

	// The retry carries the reason the attempt before it failed under, as the scheduler records it.
	delivery.Earlier = failure.Reason
	said, _, err := s.router.captureReply(ctx, found, delivery)

	s.Require().NoError(err)
	s.NotEmpty(said, "the turn ran again and said something")
	state, _, _ := found.FollowUpReply("st_run" + rerunSuffix)
	s.Equal("completed", state)
}

// A thread task whose turn is refused for money, and whose rerun is refused too, ends the attempt
// after under the money, not as a reply that failed (AI-3183).
func (s *TelnyxUnpaidSuite) TestAThreadTurnRefusedTwiceFailsUnderTheMoney() {
	person := "+1347" + digits(s.utils.uuid())
	s.teamed()
	channel := s.creditedThread(person)
	s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	found, running := s.router.sessions.ByConversationWhere(chatlog.ChannelType+":"+channel, func(string, int64) bool { return true })
	s.Require().True(running)
	s.teamSpent(person)
	task := s.threadTask(channel)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: task.UserID,
		CallerKind: "server", TaskID: task.ID, RunID: "run", CommandID: "st_run", Text: "stretch"}
	ctx := scheduledOwnerContext(context.Background(), delivery)

	var reasons []string
	for range 3 {
		_, _, err := s.router.deliverOnThread(ctx, delivery, channel)
		s.Require().Error(err)
		reason := scheduling.ReasonOf(err)
		reasons = append(reasons, reason)
		delivery.Earlier = reason
	}

	s.Equal([]string{codeSpendLimitReached, codeSpendLimitReached, codeSpendLimitReached}, reasons)
	rerun, _, _ := found.FollowUpReply("st_run" + rerunSuffix)
	s.Equal("failed", rerun, "the retry ran the turn again, and it was refused again")
}

// A text channel's turn refused because the money cannot be checked, whose rerun is refused too,
// ends the attempt after under the money, not as a reply that failed (AI-3183).
func (s *TelnyxUnpaidSuite) TestATextTurnRefusedTwiceFailsUnderTheMoney() {
	user, opened := s.uncheckableAfterOpening()
	task := s.userTask(user, opened.Id, "sms")
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: task.ID, RunID: "run", CommandID: "st_run", Text: "stretch"}
	ctx := scheduledOwnerContext(context.Background(), delivery)

	var reasons []string
	for range 3 {
		_, _, err := s.router.captureReply(ctx, found, delivery)
		s.Require().Error(err)
		reason := scheduling.ReasonOf(err)
		reasons = append(reasons, reason)
		delivery.Earlier = reason
	}

	s.Equal([]string{codeSpendCheckUnavailable, codeSpendCheckUnavailable, codeSpendCheckUnavailable}, reasons)
}

// A text task whose turn fails for a reason that is not money reads that reply back on its
// retries, as it always has: the turn is not run again, and the run fails as a reply that failed.
func (s *TelnyxUnpaidSuite) TestATextTurnFailedNotForMoneyIsNotRunAgainOnItsRetry() {
	user, opened := s.creditedOpening("failing/failing-model")
	task := s.userTask(user, opened.Id, "sms")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(scheduling.ReasonReplyFailed, *run.Error)
	s.Equal(int32(1+len(scheduling.DeliveryRetries)), attempts)
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	state, _, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID)
	s.Equal("failed", state)
	_, _, rerun := found.FollowUpReply(conversation.ReadBackPrefix + run.ID + rerunSuffix)
	s.False(rerun, "a reply that failed not for money is not run again")
}

// creditedOpening is a user who texts the agent's SMS line, on model, and a session they opened on
// the agent while the organization has credit.
func (s *TelnyxUnpaidSuite) creditedOpening(model string) (*testClient, Session) {
	number, writer := "+1555"+digits(s.utils.uuid()), "+1347"+digits(s.utils.uuid())
	s.seedSMSLine(number)
	s.config.Channels.Lines = store.AgentChannels{SMS: &store.ChannelLine{Number: number}}
	s.config.Models.LLM = model
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	user := s.data.createUser()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(context.Background(), &store.ContactMapEntry{
		CustomerID: s.customerID(), ConfigID: s.config.ID, Kind: string(channels.SMS), Address: writer, UserID: user.userID,
	}))
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	return user, user.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
}

// A task delivered in the conversation of a session that is open, whose turn fails for a reason
// that is not money, such as the model's provider being down, settles as it always has: delivered,
// on the first attempt. Only a refusal for money fails the run.
func (s *TelnyxUnpaidSuite) TestAConversationTaskWhoseTurnFailsNotForMoneyIsDelivered() {
	user, opened := s.creditedOpening("failing/failing-model")
	task := s.userTask(user, opened.Id, "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunDelivered, run.State)
	s.Equal(store.ScheduledTaskDeliveredConversation, run.DeliveredTo)
	s.Nil(run.Error)
	s.Equal(int32(1), attempts)
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	s.Eventually(func() bool {
		state, _, _ := found.FollowUpReply(conversation.ReadBackPrefix + run.ID)
		return state == "failed"
	}, deliveryWait, 20*time.Millisecond, "the task's turn did fail")
}

// A task on SMS and then the conversation, whose turn fails for a reason that is not money: the
// SMS channel has nothing to carry, and the conversation, which holds the failed turn already,
// is delivered to at once, as it always was, rather than waiting for a turn that is over.
func (s *TelnyxUnpaidSuite) TestAConversationAfterATextChannelIsNotWaitedOnForTheSameTurn() {
	user, opened := s.creditedOpening("failing/failing-model")
	task := s.userTask(user, opened.Id, "sms", "conversation")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunDelivered, run.State)
	s.Equal(store.ScheduledTaskDeliveredConversation, run.DeliveredTo)
	s.Equal(int32(1), attempts)
}

// heldConversationTurn starts the conversation channel's delivery of a task due in a session on
// the holding model, under ctx, and returns once the turn holds, with what the delivery answers.
func (s *TelnyxUnpaidSuite) heldConversationTurn(ctx context.Context) (*session.Session, <-chan error) {
	user, opened := s.creditedOpening("holding/holding-model")
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: "task", RunID: "run", CommandID: "st_run", Text: "stretch"}
	model := s.holding
	model.mu.Lock()
	before := model.turns
	model.mu.Unlock()
	model.holds()
	s.T().Cleanup(model.answers)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.router.deliverOn(scheduledOwnerContext(ctx, delivery), delivery, found,
			store.ScheduledTaskDeliveredConversation)
		done <- err
	}()
	s.Require().Eventually(func() bool {
		model.mu.Lock()
		defer model.mu.Unlock()
		return model.turns > before
	}, deliveryWait, 10*time.Millisecond, "the turn never began")
	return found, done
}

// The conversation channel settles once its turn ends, not when the turn starts: a failure
// reported meanwhile that is not about money, such as a tool's, leaves the turn running, and the
// turn may still be refused.
func (s *TelnyxUnpaidSuite) TestAConversationTurnIsWaitedOutToItsEnd() {
	found, done := s.heldConversationTurn(context.Background())
	found.Report(errors.New("the weather tool timed out"), "tool")

	s.Never(func() bool { return len(done) > 0 }, time.Second, 20*time.Millisecond, "settled while the turn ran")
	s.holding.answers()
	select {
	case err := <-done:
		s.NoError(err)
	case <-time.After(deliveryWait):
		s.Fail("the delivery never settled once the turn ended")
	}
}

// A conversation turn still running when the reply window ends is delivered: the reply is the
// conversation's, and comes when it comes.
func (s *TelnyxUnpaidSuite) TestAConversationTurnStillRunningAtTheWindowIsDelivered() {
	window := scheduledReplyWindow
	scheduledReplyWindow = time.Second
	s.T().Cleanup(func() { scheduledReplyWindow = window })
	_, done := s.heldConversationTurn(context.Background())

	select {
	case err := <-done:
		s.NoError(err)
	case <-time.After(deliveryWait):
		s.Fail("the delivery waited past its window")
	}
}

// A conversation turn whose delivery is cut short, as a draining router cuts it, answers why: the
// run is left to its lease, not settled delivered while the turn may still be refused.
func (s *TelnyxUnpaidSuite) TestAConversationTurnCutShortIsNotDelivered() {
	ctx, cancel := context.WithCancel(context.Background())
	_, done := s.heldConversationTurn(ctx)
	cancel()

	select {
	case err := <-done:
		s.ErrorIs(err, context.Canceled)
	case <-time.After(deliveryWait):
		s.Fail("the delivery never stopped")
	}
}

// A conversation turn whose session ends while it runs is delivered, as it always was: nothing
// more comes from the session to say otherwise.
func (s *TelnyxUnpaidSuite) TestAConversationTurnWhoseSessionEndsIsDelivered() {
	found, done := s.heldConversationTurn(context.Background())
	_, err := s.router.sessions.Close(found.ID(), session.OwnerOf(found.Spec()))
	s.Require().NoError(err)

	select {
	case err := <-done:
		s.NoError(err)
	case <-time.After(deliveryWait):
		s.Fail("the delivery never settled once the session ended")
	}
}

// teamed tags the config with a team: a session opened on it after counts under the team.
func (s *TelnyxUnpaidSuite) teamed() {
	if s.config.Tags == nil {
		s.config.Tags = map[string]string{}
	}
	s.config.Tags[session.TeamTag] = "finance"
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
}

// teamSpent tags the config with a team and spends the team's weekly limit. The text gate admits
// a delivery with the attribution it carries, which has no team, so it lets the task through and
// only the session the config opens, or the turn it runs, is refused (spend_limit_reached).
func (s *TelnyxUnpaidSuite) teamSpent(person string) {
	s.teamed()
	status, payload := s.serverClient.call(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "team", "team": "finance", "interval": "weekly", "limit_micros": 1_000_000})
	s.Require().Equal(http.StatusCreated, status, string(payload))
	s.usage.Spend(s.T(), s.RouterSuite.app.organization.ID, 1_000_000, func(event *usage.Event) {
		event.AppID = s.customerID()
		event.UsageCategories = []string{"team:finance"}
	})
	teamed := routing.WithAttribution(context.Background(), routing.Attribution{APIKey: s.RouterSuite.app.key, UsageCategories: []string{"team:finance"}})
	s.Require().Eventually(func() bool {
		s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "again"})
		return s.gate.Allow(teamed, s.customerID(), dlc.SMS, person) != nil
	}, 10*time.Second, 50*time.Millisecond, "the team's limit is read")
	s.Require().NoError(s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person), "no team, no refusal")
}

// A task made in a thread whose conversation has no session running opens one on the task's
// config, which a spent team limit refuses though the text gate let the task through: the run
// records the limit and is not tried again.
func (s *TelnyxUnpaidSuite) TestAThreadTaskWhoseSessionCannotOpenForATeamLimitIsNotRetried() {
	person := "+1347" + digits(s.utils.uuid())
	channel := s.creditedThread(person)
	cid := chatlog.ChannelType + ":" + channel
	if found, running := s.router.sessions.ByConversationWhere(cid, func(string, int64) bool { return true }); running {
		_, err := s.router.sessions.Close(found.ID(), session.OwnerOf(found.Spec()))
		s.Require().NoError(err)
	}
	s.teamSpent(person)
	task := s.threadTask(channel)

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendLimitReached, *run.Error)
	s.Equal(int32(1), attempts, "a spent limit outlasts every retry of the run")
}

// A turn on which the session reports a failure that is not about money, such as a tool's error,
// still carries the reply the agent then gives: only a refusal for money ends the capture.
func (s *TelnyxUnpaidSuite) TestATurnThatReportsAFailureNotAboutMoneyStillCarriesItsReply() {
	s.config.Models.LLM = "holding/holding-model"
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	user := s.data.createUser()
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, "+13125550009") == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	opened := user.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	delivery := scheduledDelivery{CustomerID: s.customerID(), ConfigID: s.config.ID, UserID: user.userID,
		CallerKind: "authenticated", TaskID: "task", RunID: "run", CommandID: "st_run", Text: "stretch"}

	model := s.holding
	model.mu.Lock()
	before := model.turns
	model.mu.Unlock()
	model.holds()
	defer model.answers()
	type result struct {
		text string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		text, _, err := s.router.captureReply(scheduledOwnerContext(context.Background(), delivery), found, delivery)
		done <- result{text, err}
	}()
	s.Require().Eventually(func() bool {
		model.mu.Lock()
		defer model.mu.Unlock()
		return model.turns > before
	}, deliveryWait, 10*time.Millisecond, "the turn never began")
	found.Report(errors.New("the weather tool timed out"), "tool")
	model.answers()

	got := <-done
	s.Require().NoError(got.err)
	s.Equal("Held.", got.text)
}

// A task made in a thread, carried in the session the backend keeps on it, whose turn a spent
// team limit refuses though the text gate let the reply through: the run records the limit and is
// not tried again, as a text channel's turn does.
func (s *TelnyxUnpaidSuite) TestAThreadTaskWhoseTurnIsRefusedForATeamLimitIsNotRetried() {
	person := "+1347" + digits(s.utils.uuid())
	s.teamed()
	channel := s.creditedThread(person)
	s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	s.teamSpent(person)
	task := s.threadTask(channel)

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendLimitReached, *run.Error)
	s.Equal(int32(1), attempts, "a spent limit outlasts every retry of the run")
}

// A task on two text channels whose turn a spent team limit refuses, the text gate having let it
// through: the second channel does not read the refused turn back as a failed reply. The run
// records the limit and is not tried again.
func (s *TelnyxUnpaidSuite) TestATurnRefusedForATeamLimitIsFinalOnEveryTextChannel() {
	number, writer := "+1555"+digits(s.utils.uuid()), "+1347"+digits(s.utils.uuid())
	s.seedSMSLine(number)
	s.config.Channels.Lines = store.AgentChannels{SMS: &store.ChannelLine{Number: number}}
	s.teamed()
	user := s.data.createUser()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(context.Background(), &store.ContactMapEntry{
		CustomerID: s.customerID(), ConfigID: s.config.ID, Kind: string(channels.SMS), Address: writer, UserID: user.userID,
	}))
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		return s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, writer) == nil
	}, 10*time.Second, 50*time.Millisecond, "the credit is read")
	opened := user.createSession(CreateSessionRequest{ConfigId: &s.config.ID})
	s.teamSpent(writer)
	task := s.userTask(user, opened.Id, "sms", "sms")

	run, attempts := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(codeSpendLimitReached, *run.Error)
	s.Equal(int32(1), attempts, "a spent limit outlasts every retry of the run")
}

// A task made in a thread, carried in the session the backend keeps on it, whose session ends
// while its turn runs: the delivery stops reading the ended session's events and the run
// settles, here delivered by the retry, which opens a session of its own.
func (s *TelnyxUnpaidSuite) TestAThreadTaskWhoseSessionEndsMidTurnSettles() {
	person := "+1347" + digits(s.utils.uuid())
	channel := s.creditedThread(person)
	s.config.Models.LLM = "holding/holding-model"
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	kept := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	found, running := s.router.sessions.ByConversationWhere(chatlog.ChannelType+":"+channel, func(string, int64) bool { return true })
	s.Require().True(running)
	s.Require().Equal(kept.Id, found.ID(), "the delivery takes the session kept on the thread")
	task := s.threadTask(channel)
	model := s.holding
	model.mu.Lock()
	before := model.turns
	model.mu.Unlock()
	model.holds()
	defer model.answers()

	s.retryAtOnce()
	worker := scheduling.NewScheduler(s.store, s.router.DeliverScheduledTask, slog.New(slog.NewTextHandler(s.logged, nil)))
	worker.Start(context.Background())
	defer func() {
		// A delivery that never returns would hold Close: the test fails on its own deadline.
		closed := make(chan struct{})
		go func() { worker.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(deliveryWait):
			s.Fail("the worker's delivery never returned")
		}
	}()
	past := time.Now().Add(-time.Minute)
	task.NextRunAt = &past
	s.Require().NoError(s.store.ScheduledTasks.Update(context.Background(), &task))
	worker.Kick()
	s.Require().Eventually(func() bool {
		model.mu.Lock()
		defer model.mu.Unlock()
		return model.turns > before
	}, deliveryWait, 10*time.Millisecond, "the turn never began")
	_, err := s.router.sessions.Close(found.ID(), session.OwnerOf(found.Spec()))
	s.Require().NoError(err)
	model.answers()

	var settled store.ScheduledTaskRun
	s.Require().Eventually(func() bool {
		runs, err := s.store.ScheduledTaskRuns.Select(context.Background(), task.CustomerID, store.ScheduledTaskRunFilter{TaskID: task.ID, Limit: 25})
		s.Require().NoError(err)
		if len(runs) == 0 || runs[0].State == store.ScheduledTaskRunRunning {
			return false
		}
		settled = runs[0]
		return true
	}, deliveryWait, 20*time.Millisecond, "the task's run never settled")
	s.Equal(store.ScheduledTaskRunDelivered, settled.State)
}

// A task made in a thread its organization's tier no longer lets it text fails as a tier limit
// reached, not as a channel that cannot reach the person, and texts nothing.
func (s *TelnyxUnpaidSuite) TestAThreadTaskOverTheTiersLimitsIsATierLimitReached() {
	person := "+13125550001"
	task := s.threadTask(s.creditedThread(person))
	s.usage.SetLimits(s.T(), s.RouterSuite.app.organization.ID, usage.Limits{Channels: map[string]usage.ChannelLimits{
		dlc.SMS: {Countries: []string{"CA"}},
	}})
	s.Require().Eventually(func() bool {
		return errors.Is(s.gate.Allow(context.Background(), s.customerID(), dlc.SMS, person), dlc.ErrRefused)
	}, 10*time.Second, 50*time.Millisecond, "the organization's limits are read")

	run, _ := s.settledByTheWorker(task)

	s.Equal(store.ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal("tier_limit_reached", *run.Error)
	s.Len(s.telnyx().sent(), 1, "only the answer to hi")
}

// A conversation turn the user stops while it runs is delivered, as it always was: the turn is
// over, and nothing in it was a refusal for money.
func (s *TelnyxUnpaidSuite) TestAConversationTurnTheUserStopsIsDelivered() {
	found, done := s.heldConversationTurn(context.Background())
	found.Interrupt()

	select {
	case err := <-done:
		s.NoError(err)
	case <-time.After(deliveryWait):
		s.Fail("the delivery never settled once the turn was stopped")
	}
}
