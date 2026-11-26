//go:build integration

package session

import (
	"context"
	"encoding/json"
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

// ScheduleApprovalSuite tests the code-enforced approval gate on repeating
// schedule_task and update_scheduled_task calls, and verifies that one-shot
// operations and non-mutating actions stay frictionless.
type ScheduleApprovalSuite struct {
	suite.Suite
	ctx   context.Context
	store *store.Store
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

type approvalHarness struct {
	mu      sync.Mutex
	asked   map[string]persistent.ToolApproval
	settled map[string]agent.ToolApprovalDecided
}

func (s *ScheduleApprovalSuite) interactiveRunner() (*scheduleRunner, *Session, *approvalHarness, string, string) {
	customer, user := "sched-appr-"+uuid.NewString(), "user-"+uuid.NewString()
	config := store.AgentConfig{CustomerID: customer, Name: "agent-" + uuid.NewString()}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &config))
	spec := Spec{
		CustomerID: customer,
		ConfigID:   config.ID,
		Timezone:   "UTC",
		Caller:     routing.Caller{UserID: user},
		CallerKind: auth.KindAuthenticated,
	}
	h := &approvalHarness{
		asked:   map[string]persistent.ToolApproval{},
		settled: map[string]agent.ToolApprovalDecided{},
	}
	runner := &scheduleRunner{
		db:     s.store,
		spec:   spec,
		user:   user,
		kind:   auth.KindAuthenticated,
		now:    func() time.Time { return time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC) },
		logger: slog.New(slog.DiscardHandler),
		owned: map[string]bool{
			scheduleTaskTool:    true,
			listScheduledTool:   true,
			updateScheduledTool: true,
			cancelScheduledTool: true,
		},
		approvals: approvals{
			await: func(owner, callID, _ string, approval persistent.ToolApproval) (string, string, bool) {
				if owner == "" {
					return "", "", false
				}
				h.mu.Lock()
				h.asked[callID] = approval
				h.mu.Unlock()
				return "cmd-1", owner, true
			},
			waits: func(string) bool { return true },
			decide: func(d agent.ToolApprovalDecided) bool {
				h.mu.Lock()
				h.settled[d.ID] = d
				h.mu.Unlock()
				return true
			},
		},
	}
	sess := &Session{spec: spec, scheduling: runner}
	return runner, sess, h, customer, config.ID
}

func (s *ScheduleApprovalSuite) awaitAsked(h *approvalHarness, callID string) persistent.ToolApproval {
	var got persistent.ToolApproval
	s.Require().Eventually(func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		a, ok := h.asked[callID]
		got = a
		return ok
	}, 2*time.Second, 5*time.Millisecond, "call %s never asked for approval", callID)
	return got
}

func (s *ScheduleApprovalSuite) insertTask(customer, configID, user, rrule, state string) store.ScheduledTask {
	due := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	task := store.ScheduledTask{
		CustomerID:   customer,
		ConfigID:     configID,
		UserID:       user,
		CallerKind:   string(auth.KindAuthenticated),
		Title:        "Stretch",
		Instructions: "Remind them to stretch.",
		StartLocal:   due,
		Timezone:     "UTC",
		RRule:        rrule,
		NextRunAt:    &due,
		State:        state,
		Channels:     []string{store.ScheduledTaskDeliveredConversation},
		CreatedBy:    "agent",
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &task, 10))
	return task
}

func (s *ScheduleApprovalSuite) TestRepeatingScheduleTaskHoldsForApprovalAndPersistsWhenAllowed() {
	runner, sess, h, customer, _ := s.interactiveRunner()
	callID := "call-repeat-allow"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      scheduleTaskTool,
			Arguments: `{"title":"Daily stretch","instructions":"Stretch.","in":"PT2H","repeat":"FREQ=DAILY"}`,
		})
		s.NoError(err)
		done <- parts
	}()

	asked := s.awaitAsked(h, callID)
	s.Equal(`Allow repeating schedule "Daily stretch"?`, asked.Title)

	tasks, err := s.store.ScheduledTasks.Select(s.ctx, customer, store.ScheduledTaskFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(tasks, "task must not be persisted before approval")

	// Another user is refused with ErrApprovalAskedOfAnother.
	s.ErrorIs(sess.AnswerConnectorApproval(callID, "cmd-1", true, "",
		Owner{CustomerID: customer, UserID: "other-user", Kind: auth.KindAuthenticated}), ErrApprovalAskedOfAnother)
	// Another customer is refused with ErrNoApprovalHeld.
	s.ErrorIs(sess.AnswerConnectorApproval(callID, "cmd-1", true, "",
		Owner{CustomerID: "other-customer", UserID: runner.user, Kind: auth.KindAuthenticated}), ErrNoApprovalHeld)

	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", true, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Require().Len(parts, 1)
	s.Contains(parts[0].Text, `"title":"Daily stretch"`)
	tasks, err = s.store.ScheduledTasks.Select(s.ctx, customer, store.ScheduledTaskFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(tasks, 1)
	s.Equal("FREQ=DAILY", tasks[0].RRule)
}

func (s *ScheduleApprovalSuite) TestRepeatingScheduleTaskDeclinedDoesNotPersist() {
	runner, sess, h, customer, _ := s.interactiveRunner()
	callID := "call-repeat-decline"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      scheduleTaskTool,
			Arguments: `{"title":"Daily stretch","instructions":"Stretch.","in":"PT2H","repeat":"FREQ=DAILY"}`,
		})
		s.NoError(err)
		done <- parts
	}()

	s.awaitAsked(h, callID)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "No thanks",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Require().Len(parts, 1)
	var out map[string]string
	s.Require().NoError(json.Unmarshal([]byte(parts[0].Text), &out))
	s.Equal("declined", out["status"])

	tasks, err := s.store.ScheduledTasks.Select(s.ctx, customer, store.ScheduledTaskFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(tasks)
}

func (s *ScheduleApprovalSuite) TestRepeatingScheduleTaskOnNonInteractiveSessionIsRefusedWithoutPersisting() {
	runner, _, _, customer, _ := s.interactiveRunner()
	runner.approvals.await = nil

	parts, err := runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-noninteractive",
		Name:      scheduleTaskTool,
		Arguments: `{"title":"Daily stretch","instructions":"Stretch.","in":"PT2H","repeat":"FREQ=DAILY"}`,
	})

	s.Require().NoError(err)
	s.Require().Len(parts, 1)
	var out map[string]string
	s.Require().NoError(json.Unmarshal([]byte(parts[0].Text), &out))
	s.Equal("unavailable", out["status"])
	s.Contains(out["message"], "interactive approval")

	tasks, err := s.store.ScheduledTasks.Select(s.ctx, customer, store.ScheduledTaskFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(tasks)
}

func (s *ScheduleApprovalSuite) TestOneShotScheduleTaskPersistsWithoutApproval() {
	runner, _, h, customer, _ := s.interactiveRunner()

	parts, err := runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-oneshot",
		Name:      scheduleTaskTool,
		Arguments: `{"title":"Stretch","instructions":"Stretch.","in":"PT2H"}`,
	})

	s.Require().NoError(err)
	s.Require().Len(parts, 1)
	s.Contains(parts[0].Text, `"title":"Stretch"`)
	s.Empty(h.asked, "one-shot schedule_task must not ask for approval")
	tasks, err := s.store.ScheduledTasks.Select(s.ctx, customer, store.ScheduledTaskFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(tasks, 1)
	s.Empty(tasks[0].RRule)
}

func (s *ScheduleApprovalSuite) TestUpdateAddingRepeatHoldsForApproval() {
	runner, sess, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "", store.ScheduledTaskActive)
	callID := "call-add-repeat"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      updateScheduledTool,
			Arguments: `{"id":"` + task.ID + `","action":"reschedule","repeat":"FREQ=DAILY"}`,
		})
		s.NoError(err)
		done <- parts
	}()

	asked := s.awaitAsked(h, callID)
	s.Equal(`Allow updating repeating schedule "Stretch"?`, asked.Title)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Contains(parts[0].Text, `"status":"declined"`)
	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Empty(stored.RRule)
}

func (s *ScheduleApprovalSuite) TestUpdateSnoozeOnRepeatingTaskHoldsForApproval() {
	runner, sess, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)
	callID := "call-snooze-repeat"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      updateScheduledTool,
			Arguments: `{"id":"` + task.ID + `","action":"snooze","in":"PT3H"}`,
		})
		s.NoError(err)
		done <- parts
	}()

	s.awaitAsked(h, callID)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Contains(parts[0].Text, `"status":"declined"`)
	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.True(task.NextRunAt.Equal(*stored.NextRunAt))
}

func (s *ScheduleApprovalSuite) TestUpdateResumeOnRepeatingTaskHoldsForApproval() {
	runner, sess, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskPaused)
	callID := "call-resume-repeat"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      updateScheduledTool,
			Arguments: `{"id":"` + task.ID + `","action":"resume"}`,
		})
		s.NoError(err)
		done <- parts
	}()

	s.awaitAsked(h, callID)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Contains(parts[0].Text, `"status":"declined"`)
	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal(store.ScheduledTaskPaused, stored.State)
}

func (s *ScheduleApprovalSuite) TestUpdatePauseWithInstructionsOnRepeatingTaskHoldsForApproval() {
	runner, sess, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)
	callID := "call-pause-instructions"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      updateScheduledTool,
			Arguments: `{"id":"` + task.ID + `","action":"pause","instructions":"New instructions."}`,
		})
		s.NoError(err)
		done <- parts
	}()

	s.awaitAsked(h, callID)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Contains(parts[0].Text, `"status":"declined"`)
	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal("Remind them to stretch.", stored.Instructions)
	s.Equal(store.ScheduledTaskActive, stored.State)
}

func (s *ScheduleApprovalSuite) TestUpdateSkipNextWithInstructionsOnRepeatingTaskHoldsForApproval() {
	runner, sess, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)
	callID := "call-skip-instructions"
	done := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := runner.Run(s.ctx, llm.ToolCall{
			ID:        callID,
			Name:      updateScheduledTool,
			Arguments: `{"id":"` + task.ID + `","action":"skip_next","instructions":"New instructions."}`,
		})
		s.NoError(err)
		done <- parts
	}()

	s.awaitAsked(h, callID)
	s.Require().NoError(sess.AnswerConnectorApproval(callID, "cmd-1", false, "",
		Owner{CustomerID: customer, UserID: runner.user, Kind: auth.KindAuthenticated}))

	parts := <-done
	s.Contains(parts[0].Text, `"status":"declined"`)
	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal("Remind them to stretch.", stored.Instructions)
}

func (s *ScheduleApprovalSuite) TestUpdatePauseAndSkipNextWithoutInstructionsNeedNoApproval() {
	runner, _, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)

	parts, err := runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-skip-only",
		Name:      updateScheduledTool,
		Arguments: `{"id":"` + task.ID + `","action":"skip_next"}`,
	})
	s.Require().NoError(err)
	s.NotContains(parts[0].Text, `"error"`)

	parts, err = runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-pause-only",
		Name:      updateScheduledTool,
		Arguments: `{"id":"` + task.ID + `","action":"pause"}`,
	})
	s.Require().NoError(err)
	s.NotContains(parts[0].Text, `"error"`)
	s.Empty(h.asked, "pause and skip_next without instructions must not ask for approval")

	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal(store.ScheduledTaskPaused, stored.State)
}

func (s *ScheduleApprovalSuite) TestUpdateRemovingRepeatDowngradesWithoutApproval() {
	runner, _, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)

	parts, err := runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-downgrade",
		Name:      updateScheduledTool,
		Arguments: `{"id":"` + task.ID + `","action":"reschedule","repeat":""}`,
	})
	s.Require().NoError(err)
	s.NotContains(parts[0].Text, `"error"`)
	s.Empty(h.asked, "removing repeat to make a task one-shot needs no approval")

	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Empty(stored.RRule)
}

func (s *ScheduleApprovalSuite) TestUpdateOnOneShotNeedsNoApproval() {
	runner, _, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "", store.ScheduledTaskActive)

	parts, err := runner.Run(s.ctx, llm.ToolCall{
		ID:        "call-oneshot-update",
		Name:      updateScheduledTool,
		Arguments: `{"id":"` + task.ID + `","action":"reschedule","in":"PT3H","instructions":"Updated one-shot."}`,
	})
	s.Require().NoError(err)
	s.NotContains(parts[0].Text, `"error"`)
	s.Empty(h.asked)

	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal("Updated one-shot.", stored.Instructions)
}

func (s *ScheduleApprovalSuite) TestScheduledCallCannotRewriteRepeatingInstructionsOnPause() {
	runner, _, h, customer, configID := s.interactiveRunner()
	task := s.insertTask(customer, configID, runner.user, "FREQ=DAILY", store.ScheduledTaskActive)

	parts, err := runner.Run(agent.WithScheduledCall(s.ctx), llm.ToolCall{
		ID:        "call-scheduled-pause-edit",
		Name:      updateScheduledTool,
		Arguments: `{"id":"` + task.ID + `","action":"pause","instructions":"Rewritten during scheduled turn."}`,
	})
	s.Require().NoError(err)
	s.Contains(parts[0].Text, `"error":"scheduled_turn"`)
	s.Empty(h.asked, "scheduled calls must be refused without pausing for approval")

	stored, err := s.store.ScheduledTasks.Get(s.ctx, customer, task.ID)
	s.Require().NoError(err)
	s.Equal("Remind them to stretch.", stored.Instructions)
}
