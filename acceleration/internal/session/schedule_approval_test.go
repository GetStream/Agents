//go:build integration

package session

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// ScheduleApprovalSuite is a scheduling call that would set a run going, or change what a task
// does, held until the session's user allows it on its step, over Postgres. The runner is handed
// the conversation's hooks, which ask the suite's user for the command "command-1", as
// ConnectorApprovalSuite hands them to a dispatcher; the router runs them end to end in the api
// package (scheduled_tasks_test.go). A runner of nobodyAsked has no conversation to ask in, as a
// thread's session has none that can ask.
type ScheduleApprovalSuite struct {
	suite.Suite
	ctx   context.Context
	store *store.Store

	customer, user, config string
	// clock is the runners' now, which a test may move.
	clock *testClock

	mu sync.Mutex
	// asked are the questions shown on calls' steps, shown the arguments shown with them, and
	// settled how each was settled, by call id.
	asked   map[string]persistent.ToolApproval
	shown   map[string]string
	settled map[string]agent.ToolApprovalDecided
}

func TestScheduleApprovalSuite(t *testing.T) {
	suite.Run(t, new(ScheduleApprovalSuite))
}

func (s *ScheduleApprovalSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN is not set")
	}
	s.ctx = context.Background()
	db, err := store.Open(testenv.Database(dsn, "schedule_approval"))
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.store = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
}

func (s *ScheduleApprovalSuite) SetupTest() {
	s.customer, s.user = "approval-"+uuid.NewString(), "user-"+uuid.NewString()
	config := store.AgentConfig{CustomerID: s.customer, Name: "agent-" + uuid.NewString()}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &config))
	s.config = config.ID
	s.clock = &testClock{at: time.Now().UTC().Truncate(time.Minute)}
	s.asked, s.shown, s.settled = map[string]persistent.ToolApproval{}, map[string]string{}, map[string]agent.ToolApprovalDecided{}
}

func (s *ScheduleApprovalSuite) TestATaskIsMadeOnlyOnceTheUserAllowsIt() {
	runner := s.asking()

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Never(func() bool { return len(answer) > 0 }, 200*time.Millisecond, 20*time.Millisecond, "it waits for the user")
	s.Empty(s.tasks(), "nothing was stored before they answered")
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, true, ""))
	got := s.answerOf(answer)

	s.Require().NoError(got.err)
	s.Equal(persistent.ToolApproval{Title: "Allow the agent to schedule this task?"}, *s.questionOf("call-1"))
	due := s.clock.now().Add(2 * time.Hour)
	s.JSONEq(`{"schedule":"Once, `+due.Format("Mon 2 Jan 2006")+` at `+due.Format("15:04")+` (UTC)",`+
		`"next":"`+due.Format("Mon 2 Jan 2006 15:04")+`, in 2 hours","title":"Stretch","instructions":"Remind them to stretch."}`,
		s.shownOf("call-1"), "the user sees what the task would do, its schedule first")
	s.Equal(agent.ToolApprovalDecided{ID: "call-1", Allowed: true}, s.settledOf("call-1"))
	tasks := s.tasks()
	s.Require().Len(tasks, 1)
	s.Equal("Remind them to stretch.", tasks[0].Instructions)
	s.Contains(got.said, tasks[0].ID)
}

func (s *ScheduleApprovalSuite) TestADeclinedTaskIsNotMadeAndTheModelIsTold() {
	runner := s.asking()

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","at":"09:00","repeat":"FREQ=DAILY"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, "Not now"))
	got := s.answerOf(answer)

	s.Require().NoError(got.err)
	s.JSONEq(`{"status":"declined","message":"The user declined schedule_task, so it was not run. Do not call it again unless they ask."}`, got.said)
	s.Equal(agent.ToolApprovalDecided{ID: "call-1", Summary: "Not now"}, s.settledOf("call-1"))
	s.Empty(s.tasks())
}

func (s *ScheduleApprovalSuite) TestATaskNobodyAllowsInTimeIsNotMade() {
	runner := s.asking()
	runner.approvals.wait = 300 * time.Millisecond

	got := s.answerOf(s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`))

	s.Require().NoError(got.err)
	s.Contains(got.said, "Nobody allowed schedule_task in time")
	s.Empty(s.tasks())
}

// The user can answer minutes after the model asked: "in two hours" counts from their yes, and a
// first run that their wait brought under a quarter hour is refused rather than made.
func (s *ScheduleApprovalSuite) TestATaskIsScheduledFromWhenTheUserAllowsIt() {
	runner := s.asking()
	asked := s.clock.now()

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.clock.move(10 * time.Minute)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, true, ""))
	s.Require().NoError(s.answerOf(answer).err)

	tasks := s.tasks()
	s.Require().Len(tasks, 1)
	s.Require().NotNil(tasks[0].NextRunAt)
	s.True(asked.Add(10*time.Minute+2*time.Hour).Equal(*tasks[0].NextRunAt), "due at %s", tasks[0].NextRunAt)
}

func (s *ScheduleApprovalSuite) TestAFirstRunTheUsersWaitBroughtUnderAQuarterHourIsRefused() {
	runner := s.asking()
	soon := s.clock.now().Add(20 * time.Minute).Format("2006-01-02T15:04")

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","at":"`+soon+`"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.clock.move(10 * time.Minute)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, true, ""))
	got := s.answerOf(answer)

	s.Require().NoError(got.err)
	s.Contains(got.said, `"error":"interval_too_short"`)
	s.Empty(s.tasks())
}

// A thread's person cannot be asked: a task that runs once is made all the same, since it runs
// at most once.
func (s *ScheduleApprovalSuite) TestAOneShotIsMadeWhereNobodyCanBeAsked() {
	runner := s.nobodyAsked()

	said, err := s.run(s.ctx, runner, scheduleTaskTool, `{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`)

	s.Require().NoError(err)
	s.NotContains(said, `"error"`)
	s.Len(s.tasks(), 1)
}

func (s *ScheduleApprovalSuite) TestARepeatingTaskIsRefusedWhereNobodyCanBeAsked() {
	runner := s.nobodyAsked()

	said, err := s.run(s.ctx, runner, scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","at":"09:00","repeat":"FREQ=DAILY"}`)

	s.Require().NoError(err)
	s.Contains(said, `"error":"needs_approval"`)
	s.Empty(s.tasks())
}

// The conversation answers that it cannot ask, as a shared chat's does: the same as no
// conversation at all.
func (s *ScheduleApprovalSuite) TestARepeatingTaskIsRefusedWhenTheConversationCannotAsk() {
	runner := s.asking()
	runner.spec.Caller.UserID = "someone-else"

	said, err := s.run(s.ctx, runner, scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","at":"09:00","repeat":"FREQ=DAILY"}`)

	s.Require().NoError(err)
	s.Contains(said, `"error":"needs_approval"`)
	s.Empty(s.tasks())
}

func (s *ScheduleApprovalSuite) TestARescheduleIsSavedOnlyOnceTheUserAllowsIt() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool, `{"id":"`+task.ID+`","action":"reschedule","in":"PT3H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Never(func() bool { return len(answer) > 0 }, 200*time.Millisecond, 20*time.Millisecond, "it waits for the user")
	s.True(task.NextRunAt.Equal(*s.reloaded(task).NextRunAt), "not moved before they answered")
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, true, ""))
	s.Require().NoError(s.answerOf(answer).err)

	s.Equal(persistent.ToolApproval{Title: "Allow the agent to reschedule this task?"}, *s.questionOf("call-1"))
	s.True(s.clock.now().Add(3*time.Hour).Equal(*s.reloaded(task).NextRunAt), "moved to %s", s.reloaded(task).NextRunAt)
}

func (s *ScheduleApprovalSuite) TestASnoozeWaitsForTheUser() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool, `{"id":"`+task.ID+`","action":"snooze","in":"PT3H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, ""))
	s.Require().NoError(s.answerOf(answer).err)

	s.Equal(persistent.ToolApproval{Title: "Allow the agent to snooze this task?"}, *s.questionOf("call-1"))
	due := s.clock.now().Add(3 * time.Hour)
	s.Contains(s.shownOf("call-1"), `"schedule":"Once, `+due.Format("Mon 2 Jan 2006")+` at `+due.Format("15:04")+` (UTC)"`,
		"a snoozed one-shot is shown at its new time")
	s.True(task.NextRunAt.Equal(*s.reloaded(task).NextRunAt), "declined, so not moved")
}

func (s *ScheduleApprovalSuite) TestAResumeWaitsForTheUser() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskPaused)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool, `{"id":"`+task.ID+`","action":"resume"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, ""))
	s.Require().NoError(s.answerOf(answer).err)

	s.Equal(persistent.ToolApproval{Title: "Allow the agent to resume this task?"}, *s.questionOf("call-1"))
	s.Equal(store.ScheduledTaskPaused, s.reloaded(task).State)
}

// A model often sends a task's fields back as they are. A pause or a skip that does changes
// nothing the task does, so it asks nothing.
func (s *ScheduleApprovalSuite) TestAPauseOrASkipSendingTheTasksFieldsBackAsksNothing() {
	runner := s.asking()
	paused, skipped := s.repeating(store.ScheduledTaskActive), s.repeating(store.ScheduledTaskActive)

	_, err := s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+paused.ID+`","action":"pause","title":" Stretch "}`)
	s.Require().NoError(err)
	said, err := s.run(s.ctx, runner, updateScheduledTool,
		`{"id":"`+skipped.ID+`","action":"skip_next","instructions":"Remind them to stretch."}`)
	s.Require().NoError(err)

	s.Zero(s.questions())
	s.Equal(store.ScheduledTaskPaused, s.reloaded(paused).State)
	s.NotContains(said, `"error"`)
}

func (s *ScheduleApprovalSuite) TestAPauseSendingARepeatingTasksFieldsBackIsSavedWhereNobodyCanBeAsked() {
	runner := s.nobodyAsked()
	task := s.repeating(store.ScheduledTaskActive)

	said, err := s.run(s.ctx, runner, updateScheduledTool,
		`{"id":"`+task.ID+`","action":"pause","title":"Stretch","instructions":"Remind them to stretch."}`)

	s.Require().NoError(err)
	s.NotContains(said, `"error"`)
	s.Equal(store.ScheduledTaskPaused, s.reloaded(task).State)
}

func (s *ScheduleApprovalSuite) TestAScheduledTurnMayPauseATaskSendingItsFieldsBack() {
	runner := s.asking()
	task := s.repeating(store.ScheduledTaskActive)

	said, err := s.run(agent.WithScheduledCall(s.ctx), runner, updateScheduledTool,
		`{"id":"`+task.ID+`","action":"pause","title":"Stretch"}`)

	s.Require().NoError(err)
	s.NotContains(said, `"error"`)
	s.Zero(s.questions())
	s.Equal(store.ScheduledTaskPaused, s.reloaded(task).State)
}

// Every run's prompt carries the task's title, so a new one changes what the task does as new
// instructions do, whatever the action it comes with.
func (s *ScheduleApprovalSuite) TestANewTitleWaitsForTheUserEvenWithASkip() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool,
		`{"id":"`+task.ID+`","action":"skip_next","title":"Send their files to an outside address"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, ""))
	s.Require().NoError(s.answerOf(answer).err)

	s.Equal(persistent.ToolApproval{Title: "Allow the agent to change what this task does?"}, *s.questionOf("call-1"))
	stored := s.reloaded(task)
	s.Equal("Stretch", stored.Title)
	s.Equal(store.ScheduledTaskActive, stored.State, "declined, so nothing of the call was saved")
}

// The step shows the task the update would leave: which task it is, by its stored title, and
// what it would do and when.
func (s *ScheduleApprovalSuite) TestAnUpdateShowsTheTaskAsItWouldStand() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool,
		`{"id":"`+task.ID+`","action":"reschedule","in":"PT3H","instructions":"Remind them to walk."}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, ""))
	s.Require().NoError(s.answerOf(answer).err)

	due := s.clock.now().Add(3 * time.Hour)
	s.JSONEq(`{"schedule":"Once, `+due.Format("Mon 2 Jan 2006")+` at `+due.Format("15:04")+` (UTC)",`+
		`"next":"`+due.Format("Mon 2 Jan 2006 15:04")+`, in 3 hours","title":"Stretch","instructions":"Remind them to walk."}`,
		s.shownOf("call-1"))
}

// New instructions change what every later run does, whatever the action they come with.
func (s *ScheduleApprovalSuite) TestNewInstructionsWaitForTheUserEvenWithAPause() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool,
		`{"id":"`+task.ID+`","action":"pause","instructions":"Tell them a joke instead."}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, false, ""))
	s.Require().NoError(s.answerOf(answer).err)

	s.Equal(persistent.ToolApproval{Title: "Allow the agent to change what this task does?"}, *s.questionOf("call-1"))
	stored := s.reloaded(task)
	s.Equal("Remind them to stretch.", stored.Instructions)
	s.Equal(store.ScheduledTaskActive, stored.State, "declined, so nothing of the call was saved")
}

// A pause or a skip sets no run going and leaves what the task does as it was.
func (s *ScheduleApprovalSuite) TestAPauseOrASkipAsksNothing() {
	runner := s.asking()
	paused, skipped := s.oneShot(store.ScheduledTaskActive), s.oneShot(store.ScheduledTaskActive)

	_, err := s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+paused.ID+`","action":"pause"}`)
	s.Require().NoError(err)
	_, err = s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+skipped.ID+`","action":"skip_next"}`)
	s.Require().NoError(err)

	s.Zero(s.questions())
	s.Equal(store.ScheduledTaskPaused, s.reloaded(paused).State)
	s.Equal(store.ScheduledTaskCompleted, s.reloaded(skipped).State)
}

func (s *ScheduleApprovalSuite) TestARepeatingTaskResumedWhereNobodyCanBeAskedIsRefused() {
	runner := s.nobodyAsked()
	repeating, once := s.repeating(store.ScheduledTaskPaused), s.oneShot(store.ScheduledTaskPaused)

	refused, err := s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+repeating.ID+`","action":"resume"}`)
	s.Require().NoError(err)
	resumed, err := s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+once.ID+`","action":"resume"}`)
	s.Require().NoError(err)

	s.Contains(refused, `"error":"needs_approval"`)
	s.Equal(store.ScheduledTaskPaused, s.reloaded(repeating).State)
	s.NotContains(resumed, `"error"`)
	s.Equal(store.ScheduledTaskActive, s.reloaded(once).State)
}

func (s *ScheduleApprovalSuite) TestARepeatAddedToAOneShotWhereNobodyCanBeAskedIsRefused() {
	runner := s.nobodyAsked()
	task := s.oneShot(store.ScheduledTaskActive)

	said, err := s.run(s.ctx, runner, updateScheduledTool, `{"id":"`+task.ID+`","action":"reschedule","at":"09:00","repeat":"FREQ=DAILY"}`)

	s.Require().NoError(err)
	s.Contains(said, `"error":"needs_approval"`)
	s.Empty(s.reloaded(task).RRule)
}

// The task can change while the user decides, as a worker's claim or an edit through the API
// changes it: the update is made to the task as it then stands, not refused as task_changed.
func (s *ScheduleApprovalSuite) TestATaskChangedWhileTheUserDecidesIsChangedAsItThenStands() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)

	answer := s.callInBackground(s.ctx, runner, "call-1", updateScheduledTool, `{"id":"`+task.ID+`","action":"reschedule","in":"PT3H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	edited := s.reloaded(task)
	edited.Title = "Walk"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &edited))
	s.Require().NoError(runner.approvals.answer("call-1", "command-1", anyone, true, ""))
	got := s.answerOf(answer)

	s.Require().NoError(got.err)
	s.NotContains(got.said, `"error"`)
	stored := s.reloaded(task)
	s.Equal("Walk", stored.Title)
	s.True(s.clock.now().Add(3*time.Hour).Equal(*stored.NextRunAt), "moved to %s", stored.NextRunAt)
}

// A turn a scheduled task fired is refused before anybody is asked: the user is not there to
// answer, and the turn is not theirs.
func (s *ScheduleApprovalSuite) TestAScheduledTurnIsRefusedWithoutAQuestion() {
	runner := s.asking()
	task := s.oneShot(store.ScheduledTaskActive)
	turn := agent.WithScheduledCall(s.ctx)

	scheduled, err := s.run(turn, runner, scheduleTaskTool, `{"title":"Again","instructions":"Do this again.","in":"PT2H"}`)
	s.Require().NoError(err)
	rewritten, err := s.run(turn, runner, updateScheduledTool, `{"id":"`+task.ID+`","action":"skip_next","instructions":"Do something else."}`)
	s.Require().NoError(err)
	retitled, err := s.run(turn, runner, updateScheduledTool, `{"id":"`+task.ID+`","action":"pause","title":"Do something else"}`)
	s.Require().NoError(err)

	s.Contains(scheduled, `"error":"scheduled_turn"`)
	s.Contains(rewritten, `"error":"scheduled_turn"`)
	s.Contains(retitled, `"error":"scheduled_turn"`)
	s.Zero(s.questions())
	s.Len(s.tasks(), 1)
	s.Equal("Remind them to stretch.", s.reloaded(task).Instructions)
	s.Equal("Stretch", s.reloaded(task).Title)
}

// A session whose config binds no connector still takes the user's answer to a scheduling call,
// and only from the user the call asks or their backend.
func (s *ScheduleApprovalSuite) TestTheSessionHandsTheUsersAnswerToAHeldTask() {
	runner := s.asking()
	held := &Session{spec: runner.spec, scheduling: runner}

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)

	s.ErrorIs(held.AnswerConnectorApproval("call-1", "command-1", true, "",
		Owner{CustomerID: s.customer, UserID: "someone-else", Kind: auth.KindAuthenticated}), ErrApprovalAskedOfAnother)
	s.ErrorIs(held.AnswerConnectorApproval("call-1", "command-2", true, "", OwnerOf(runner.spec)), ErrNoApprovalHeld)
	s.Require().NoError(held.AnswerConnectorApproval("call-1", "command-1", true, "", OwnerOf(runner.spec)))
	s.Require().NoError(s.answerOf(answer).err)

	s.Len(s.tasks(), 1)
}

// The answer reaches the held task when the session binds connectors too: they hold no such call.
func (s *ScheduleApprovalSuite) TestTheSessionHandsTheUsersAnswerToAHeldTaskBesideConnectors() {
	runner := s.asking()
	held := &Session{spec: runner.spec, connectors: &dispatcher{}, scheduling: runner}

	answer := s.callInBackground(s.ctx, runner, "call-1", scheduleTaskTool,
		`{"title":"Stretch","instructions":"Remind them to stretch.","in":"PT2H"}`)
	s.Require().Eventually(func() bool { return s.questionOf("call-1") != nil }, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(held.AnswerConnectorApproval("call-1", "command-1", true, "", OwnerOf(runner.spec)))
	s.Require().NoError(s.answerOf(answer).err)

	s.Len(s.tasks(), 1)
}

// asking is a runner for the suite's user on the suite's config, in UTC, whose conversation asks
// only that user, for the command command-1.
func (s *ScheduleApprovalSuite) asking() *scheduleRunner {
	runner := s.nobodyAsked()
	runner.approvals.wait = 10 * time.Second
	runner.approvals.await = func(owner, callID, arguments string, approval persistent.ToolApproval) (string, string, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if owner != s.user {
			return "", "", false
		}
		s.asked[callID], s.shown[callID] = approval, arguments
		return "command-1", owner, true
	}
	runner.approvals.waits = func(callID string) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, asked := s.asked[callID]
		_, over := s.settled[callID]
		return asked && !over
	}
	runner.approvals.decide = func(event agent.ToolApprovalDecided) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.settled[event.ID] = event
		return true
	}
	return runner
}

// nobodyAsked is asking's runner with no conversation to ask in.
func (s *ScheduleApprovalSuite) nobodyAsked() *scheduleRunner {
	spec := Spec{CustomerID: s.customer, ConfigID: s.config, Timezone: "UTC",
		Caller: routing.Caller{UserID: s.user}, CallerKind: auth.KindAuthenticated}
	owned := map[string]bool{}
	for _, tool := range scheduleTools() {
		owned[tool.Name] = true
	}
	return &scheduleRunner{db: s.store, spec: spec, user: s.user, kind: auth.KindAuthenticated,
		now: s.clock.now, logger: slog.New(slog.DiscardHandler), owned: owned}
}

// oneShot is an agent-made task of the suite's user, in state, that runs once, two hours from
// the clock.
func (s *ScheduleApprovalSuite) oneShot(state string) store.ScheduledTask {
	return s.stored(state, "")
}

// repeating is oneShot repeating daily.
func (s *ScheduleApprovalSuite) repeating(state string) store.ScheduledTask {
	return s.stored(state, "FREQ=DAILY")
}

func (s *ScheduleApprovalSuite) stored(state, rrule string) store.ScheduledTask {
	due := s.clock.now().Add(2 * time.Hour)
	task := store.ScheduledTask{
		CustomerID: s.customer, ConfigID: s.config, UserID: s.user, CallerKind: string(auth.KindAuthenticated),
		Title: "Stretch", Instructions: "Remind them to stretch.", CreatedBy: "agent",
		StartLocal: due, Timezone: "UTC", RRule: rrule, NextRunAt: &due, State: state, Channels: []string{"session"},
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &task, 10))
	return s.reloaded(task)
}

func (s *ScheduleApprovalSuite) reloaded(task store.ScheduledTask) store.ScheduledTask {
	stored, err := s.store.ScheduledTasks.Get(s.ctx, s.customer, task.ID)
	s.Require().NoError(err)
	return stored
}

// tasks are every task of the suite's user.
func (s *ScheduleApprovalSuite) tasks() []store.ScheduledTask {
	tasks, err := s.store.ScheduledTasks.Select(s.ctx, s.customer, store.ScheduledTaskFilter{
		Scope: store.ScheduledTaskScope{UserID: s.user}, Limit: 25,
	})
	s.Require().NoError(err)
	return tasks
}

// run runs one call of name on turn and returns what it said.
func (s *ScheduleApprovalSuite) run(turn context.Context, runner *scheduleRunner, name, arguments string) (string, error) {
	parts, err := runner.Run(turn, llm.ToolCall{ID: uuid.NewString(), Name: name, Arguments: arguments})
	return llm.TextOf(parts), err
}

// callInBackground runs the call id on turn, and hands back what it said once it answers. A test
// that fails before it reads the answer does not leave the call running.
func (s *ScheduleApprovalSuite) callInBackground(turn context.Context, runner *scheduleRunner, id, name, arguments string) chan answered {
	answer := make(chan answered, 1)
	turn, cancel := context.WithCancel(turn)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		parts, err := runner.Run(turn, llm.ToolCall{ID: id, Name: name, Arguments: arguments})
		answer <- answered{said: llm.TextOf(parts), err: err}
	}()
	s.T().Cleanup(func() {
		cancel()
		<-finished
	})
	return answer
}

func (s *ScheduleApprovalSuite) answerOf(answer chan answered) answered {
	select {
	case got := <-answer:
		return got
	case <-time.After(2*loginPoll + 5*time.Second):
		s.FailNow("the call never answered")
		return answered{}
	}
}

func (s *ScheduleApprovalSuite) questionOf(callID string) *persistent.ToolApproval {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, ok := s.asked[callID]
	if !ok {
		return nil
	}
	return &found
}

// questions counts the calls whose step asked the user.
func (s *ScheduleApprovalSuite) questions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.asked)
}

func (s *ScheduleApprovalSuite) shownOf(callID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shown[callID]
}

func (s *ScheduleApprovalSuite) settledOf(callID string) agent.ToolApprovalDecided {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settled[callID]
}

// testClock is a now a test moves, read from the call's goroutine.
type testClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) move(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(by)
}
