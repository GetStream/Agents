package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The scheduling tools' names, as the model asks for them.
const (
	scheduleTaskTool      = "schedule_task"
	listScheduledTool     = "list_scheduled_tasks"
	updateScheduledTool   = "update_scheduled_task"
	cancelScheduledTool   = "cancel_scheduled_task"
	scheduledTaskNotFound = "scheduled_task_not_found"
	tooManyTasks          = "too_many_tasks"
	conflictCode          = "conflict"
	taskChanged           = "task_changed"
	serverSideOnly        = "server_side_only"
	needsApproval         = "needs_approval"
	unknownConfig         = "unknown_config"
	scheduledTurnCode     = "scheduled_turn"
)

// armedByTask is what a call asked for in a turn a scheduled task fired is told when it
// would set a run going, or change what a task does. That turn is the customer's spend, never
// the user's, so a task that could arm another would run on, uncounted, without the user ever
// asking again; and nobody is there to allow it (scheduleRunner.approve).
const armedByTask = "a scheduled task you are carrying out cannot schedule a task, nor reschedule, " +
	"snooze or resume one, nor change what one does; the user can ask for that in a message of their own"

// unapprovedRepeat is what a call that would leave a task repeating is told where nobody can
// be asked to allow it (scheduleRunner.approve).
const unapprovedRepeat = "a task that repeats is set up or changed only once the user allows it, and they " +
	"cannot be asked to here, so nothing was done; offer a task that runs once instead, or tell them to " +
	"set the repeat up in the app"

// scheduleTools are the scheduling tools a session may offer. The four names
// are the router's own: a caller that declared one of them keeps it, and the
// router's is simply not offered (see Manager.schedulingRunner).
func scheduleTools() []harness.Tool {
	return []harness.Tool{
		{
			Name: scheduleTaskTool,
			Description: "Schedule something for you to do for the user later: a reminder, a check-in, " +
				"or a task to carry out once or on a repeating schedule. When it is due, you are woken in " +
				"this conversation with the instructions and act on them. Give the time with at, or the " +
				"span with in; the first run is 15 minutes from now at the soonest. Agree the day and " +
				"time with the user first, in their own zone unless they name another, then read back " +
				"the schedule this returns.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title":        map[string]any{"type": "string", "maxLength": 80, "description": "A short name the user will recognise, e.g. \"Stretch\"."},
					"instructions": map[string]any{"type": "string", "maxLength": 2000, "description": "What to do when it is due, written to yourself, e.g. \"Remind Sam to stretch and ask how his back is.\""},
					"at":           map[string]any{"type": "string", "description": "The user's local date and time, 2026-10-20T09:00, or a time of day, 09:00, meaning the next one. Give at or in."},
					"in":           map[string]any{"type": "string", "description": "How long from now, as an ISO 8601 duration: PT20M, PT2H, P1D. Days are calendar days: P1D is the same time tomorrow."},
					"repeat":       map[string]any{"type": "string", "description": "Omit for once. Otherwise an RRULE without DTSTART or UNTIL: FREQ=DAILY, FREQ=WEEKLY;BYDAY=MO,WE,FR, FREQ=MONTHLY;BYMONTHDAY=1. FREQ=MONTHLY started on the 29th to 31st skips the months without that day; FREQ=MONTHLY;BYMONTHDAY=-1 is the last day of every month."},
					"until":        map[string]any{"type": "string", "description": "The last local date it repeats on, 2026-12-31."},
					"timezone":     map[string]any{"type": "string", "description": "An IANA zone such as Europe/Amsterdam, only when the user names one. Otherwise theirs is used."},
				},
				"required": []string{"title", "instructions"},
			},
		},
		{
			Name: listScheduledTool,
			Description: "List the tasks you have scheduled for this user with this agent, each with when it runs next. " +
				"Running and paused tasks come first; more says when there are others not shown.",
			Parameters: map[string]any{"type": "object", "properties": map[string]any{}},
		},
		{
			Name: updateScheduledTool,
			Description: "Change a task you scheduled, by the id list_scheduled_tasks gave it: reschedule it, " +
				"snooze its next run to a new at or in, skip its next run, pause it or resume it. With " +
				"reschedule, fields left out keep what the task has. Returns the task as it now stands.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id":           map[string]any{"type": "string", "description": "The task's id."},
					"action":       map[string]any{"type": "string", "enum": []string{"reschedule", "snooze", "skip_next", "pause", "resume"}, "description": "What to do to the task."},
					"title":        map[string]any{"type": "string", "maxLength": 80},
					"instructions": map[string]any{"type": "string", "maxLength": 2000},
					"at":           map[string]any{"type": "string", "description": "The user's local date and time, 2026-10-20T09:00, or a time of day, 09:00."},
					"in":           map[string]any{"type": "string", "description": "How long from now, as an ISO 8601 duration: PT20M, PT2H, P1D. Days are calendar days: P1D is the same time tomorrow."},
					"repeat":       map[string]any{"type": "string", "description": "An RRULE without DTSTART or UNTIL. An empty string makes the task run once. FREQ=MONTHLY started on the 29th to 31st skips the months without that day; BYMONTHDAY=-1 is the last day."},
					"until":        map[string]any{"type": "string", "description": "The last local date it repeats on, 2026-12-31. An empty string lifts the end."},
					"timezone":     map[string]any{"type": "string", "description": "An IANA zone such as Europe/Amsterdam."},
				},
				"required": []string{"id", "action"},
			},
		},
		{
			Name:        cancelScheduledTool,
			Description: "Cancel a task you scheduled, by the id list_scheduled_tasks gave it. It will not run again.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id": map[string]any{"type": "string", "description": "The task's id."},
				},
				"required": []string{"id"},
			},
		},
	}
}

// zoneUser is the user whose stored zone a session runs in, the one its scheduled tasks
// belong to: a verified end user or a backend acting for one, else the person of a thread
// on a line of one person. Empty for an incognito session, which speaks for nobody, and for
// a guest or an anonymous caller, whose id the app has not vouched for.
func zoneUser(spec Spec) string {
	switch {
	case spec.Incognito:
		return ""
	case verifiedCaller(spec):
		return spec.Caller.UserID
	}
	return spec.ThreadPerson
}

// schedulingRunner builds the schedule runner for a session, and the tools it
// adds to what the session offers, or nil when this session may not schedule:
// the config has not enabled it, there is no store to hold tasks, the session
// is incognito, or the caller is not a verified end user or a backend acting
// for one — guests and anonymous callers have ids too and must not get the
// tools — nor the person of a thread on a line of one person (Spec.ThreadPerson).
// A caller that declared a tool with one of these names keeps theirs.
func (m *Manager) schedulingRunner(spec Spec, sessionID string, offered []harness.Tool) (*scheduleRunner, []harness.Tool) {
	user, kind, thread := spec.Caller.UserID, spec.CallerKind, ""
	if !verifiedCaller(spec) {
		// The Router answers the thread's person, as a backend acts for its user.
		user, kind, thread = spec.ThreadPerson, auth.KindServer, spec.ThreadChannel
	}
	if !spec.Scheduling.Enabled || m.options.Store == nil || spec.Incognito ||
		spec.ConfigID == "" || user == "" {
		return nil, nil
	}
	taken := map[string]bool{}
	for _, tool := range offered {
		taken[tool.Name] = true
	}
	m.mu.Lock()
	kick := m.kickScheduler
	m.mu.Unlock()
	runner := &scheduleRunner{
		db: m.options.Store, spec: spec, sessionID: sessionID, user: user, kind: kind, thread: thread,
		now: time.Now, logger: m.logger, owned: map[string]bool{}, kick: kick,
		approvals: approvals{wait: m.options.Connectors.ApprovalWait},
	}
	var added []harness.Tool
	for _, tool := range scheduleTools() {
		if taken[tool.Name] {
			continue
		}
		runner.owned[tool.Name] = true
		added = append(added, tool)
	}
	if len(added) == 0 {
		return nil, nil
	}
	return runner, added
}

// scheduleRunner runs the scheduling tools against the store. A task belongs
// to the session's user on this session's agent config, whatever conversation
// of theirs created it.
type scheduleRunner struct {
	db        *store.Store
	spec      Spec
	sessionID string
	now       func() time.Time
	logger    *slog.Logger
	owned     map[string]bool
	next      agent.ToolRunner
	// kick wakes the scheduled-task worker after a task is made or changed;
	// nil where no worker runs.
	kick func()
	// user and kind are who the tasks belong to: the caller, or a thread's
	// person, whose thread channel is thread.
	user   string
	kind   auth.Kind
	thread string
	// approvals hold a call that would set a run going until the user allows it
	// (approve). Without a conversation to ask in, nobody is asked.
	approvals approvals
}

func (r *scheduleRunner) Run(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	if !r.owned[call.Name] {
		return r.next.Run(ctx, call)
	}
	switch call.Name {
	case scheduleTaskTool:
		return r.schedule(ctx, call)
	case listScheduledTool:
		return r.list(ctx)
	case updateScheduledTool:
		return r.update(ctx, call)
	default:
		return r.cancel(ctx, call)
	}
}

type scheduleArgs struct {
	Title        string `json:"title"`
	Instructions string `json:"instructions"`
	At           string `json:"at"`
	In           string `json:"in"`
	Repeat       string `json:"repeat"`
	Until        string `json:"until"`
	Timezone     string `json:"timezone"`
}

func (r *scheduleRunner) schedule(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	if agent.ScheduledCall(ctx) {
		return refusal(scheduledTurnCode, armedByTask), nil
	}
	var args scheduleArgs
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		return refusal(scheduling.CodeInvalidSchedule, "the arguments are not JSON I can read"), nil
	}
	if refusal := checkText(args.Title, args.Instructions); refusal != nil {
		return refusal, nil
	}
	now := r.now()
	zone, err := r.zone(ctx, args.Timezone)
	if err != nil {
		if parts, refused := refusalOf(err); refused {
			return parts, nil
		}
		return nil, err
	}
	parsed, err := scheduling.Parse(scheduling.Input{
		At: args.At, In: args.In, Repeat: args.Repeat, Until: args.Until, Timezone: zone,
	}, now)
	if err != nil {
		parts, _ := refusalOf(err)
		return parts, nil
	}
	// A repeat whose start has passed starts at its next occurrence, its
	// COUNT counted from there.
	next, ok := parsed.FromNow(now)
	if !ok {
		return refusal(scheduling.CodeScheduleInPast, "that schedule has no occurrence left; its last one has passed"), nil
	}
	if err := scheduling.CheckFirst(next, now); err != nil {
		parts, _ := refusalOf(err)
		return parts, nil
	}
	if !agent.Approved(ctx) {
		view := shownTask(parsed, &next, strings.TrimSpace(args.Title), args.Instructions, now)
		asked, parts, err := r.approve(ctx, call, "Allow the agent to schedule this task?", view, parsed.Repeat != "")
		if parts != nil || err != nil {
			return parts, err
		}
		if asked {
			// The user can answer minutes later, so the schedule is read again from then.
			return r.schedule(agent.WithApproval(ctx), call)
		}
	}
	// A task made in a thread is delivered on that thread's channel.
	deliverTo := store.ScheduledTaskDeliveredConversation
	if r.thread != "" {
		deliverTo = r.spec.ThreadDelivery
	}
	task := store.ScheduledTask{
		CustomerID:      r.spec.CustomerID,
		ConfigID:        r.spec.ConfigID,
		UserID:          r.user,
		CallerKind:      string(r.kind),
		APIKey:          r.spec.Attribution.APIKey,
		UsageCategories: r.spec.Attribution.UsageCategories,
		OriginSessionID: r.sessionID,
		SessionID:       r.sessionID,
		ConversationID:  r.spec.ConversationID,
		ChannelThread:   r.thread,
		Title:           strings.TrimSpace(args.Title),
		Instructions:    args.Instructions,
		StartLocal:      parsed.StartLocal,
		Timezone:        parsed.Timezone,
		RRule:           parsed.Repeat,
		NextRunAt:       &next,
		State:           store.ScheduledTaskActive,
		Channels:        []string{deliverTo},
		CreatedBy:       "agent",
	}
	if !parsed.Until.IsZero() {
		until := parsed.Until
		task.UntilDate = &until
	}
	maxActive := r.maxActive()
	if err := r.db.ScheduledTasks.Insert(ctx, &task, maxActive); err != nil {
		if errors.Is(err, store.ErrTooManyScheduledTasks) {
			return refusal(tooManyTasks, fmt.Sprintf("the user already has %d tasks running, the most this agent allows; cancel one first, or pause one until it is wanted", maxActive)), nil
		}
		if errors.Is(err, store.ErrNoAgentConfig) {
			return refusal(unknownConfig, "this agent has been deleted, so it cannot schedule a task"), nil
		}
		return nil, err
	}
	r.kickScheduled()
	// A zone the user named for this task is theirs from now on.
	if args.Timezone != "" {
		kind := store.UserKindAuthenticated
		if r.kind == auth.KindGuest {
			kind = store.UserKindGuest
		}
		if err := r.db.Users.SetTimezone(ctx, r.spec.CustomerID, r.user, kind, parsed.Timezone); err != nil {
			r.logger.Error("could not store the user's timezone", "user", r.user, "error", err)
		}
	}
	return result(r.taskResult(&task, parsed, now)), nil
}

func (r *scheduleRunner) list(ctx context.Context) ([]llm.ContentPart, error) {
	tasks, more, err := r.listed(ctx)
	if err != nil {
		return nil, err
	}
	now := r.now()
	items := make([]map[string]any, 0, len(tasks))
	var display *time.Location
	for i := range tasks {
		task := &tasks[i]
		item := map[string]any{"id": task.ID, "title": task.Title, "state": task.State}
		if schedule, err := scheduling.FromStored(*task); err == nil {
			item["schedule"] = schedule.Describe()
			if display == nil {
				display = schedule.Location()
			}
			if task.NextRunAt != nil {
				item["next"] = localNext(*task.NextRunAt, schedule.Location(), now)
			}
		}
		items = append(items, item)
	}
	if display == nil {
		display = r.displayZone()
	}
	listed := map[string]any{
		"now":   localNow(now, display),
		"tasks": items,
	}
	if more {
		listed["more"] = "the user has more tasks than these; the running and paused ones are listed first"
	}
	return result(listed), nil
}

// listedStates is the order list_scheduled_tasks gives tasks in: the ones that
// will still run, then the paused, which can be resumed, before the finished.
var listedStates = []string{store.ScheduledTaskActive, store.ScheduledTaskPaused,
	store.ScheduledTaskFailed, store.ScheduledTaskCompleted}

// listed is the user's tasks on this agent as list_scheduled_tasks shows them,
// state by state in listedStates, at most as many as the user may have running
// so every running one fits, and whether there are more than that.
func (r *scheduleRunner) listed(ctx context.Context) ([]store.ScheduledTask, bool, error) {
	limit := max(scheduling.MaxActivePerUser, r.maxActive())
	filter := store.ScheduledTaskFilter{
		Scope: store.ScheduledTaskScope{UserID: r.user,
			Kinds: []string{string(r.kind), string(auth.KindServer)}},
		ConfigID: r.spec.ConfigID,
		Limit:    limit,
	}
	var tasks []store.ScheduledTask
	for _, state := range listedStates {
		filter.State = state
		// The store answers one more than the limit, so a state that overflows it says so.
		page, err := r.db.ScheduledTasks.Select(ctx, r.spec.CustomerID, filter)
		if err != nil {
			return nil, false, err
		}
		tasks = append(tasks, page...)
		if len(tasks) > limit {
			return tasks[:limit], true, nil
		}
	}
	return tasks, false, nil
}

type updateArgs struct {
	ID           string  `json:"id"`
	Action       string  `json:"action"`
	Title        *string `json:"title"`
	Instructions *string `json:"instructions"`
	At           *string `json:"at"`
	In           *string `json:"in"`
	Repeat       *string `json:"repeat"`
	Until        *string `json:"until"`
	Timezone     *string `json:"timezone"`
}

func (r *scheduleRunner) update(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	var args updateArgs
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		return refusal(scheduling.CodeInvalidSchedule, "the arguments are not JSON I can read"), nil
	}
	// A pause or a skip sets no run going, so a task may still do either.
	if agent.ScheduledCall(ctx) && updateQuestion(args) != "" {
		return refusal(scheduledTurnCode, armedByTask), nil
	}
	task, parts, err := r.ownTask(ctx, args.ID)
	if parts != nil || err != nil {
		return parts, err
	}
	schedule, err := scheduling.FromStored(task)
	if err != nil {
		return nil, err
	}
	now := r.now()
	var slept *store.ScheduledTaskRun
	if args.Title != nil {
		if refusal := checkTitle(*args.Title); refusal != nil {
			return refusal, nil
		}
		task.Title = strings.TrimSpace(*args.Title)
	}
	if args.Instructions != nil {
		// What a backend told the agent to do is the backend's to change, as over
		// the API, where a device may not rewrite it either.
		if task.CreatedBy != "agent" {
			return refusal(serverSideOnly, "the instructions of a task the app scheduled are the app's to change; you may retitle or reschedule it"), nil
		}
		if refusal := checkInstructions(*args.Instructions); refusal != nil {
			return refusal, nil
		}
		task.Instructions = *args.Instructions
	}
	switch args.Action {
	case "pause":
		if task.State == store.ScheduledTaskCompleted {
			return refusal(conflictCode, "that task has already finished; there is nothing to pause"), nil
		}
		// Paused by the user's say, so a data move's cut-over leaves it.
		task.State, task.MovePaused = store.ScheduledTaskPaused, false
	case "resume":
		if task.State == store.ScheduledTaskCompleted {
			return refusal(conflictCode, "that task has finished; schedule a new one instead"), nil
		}
		next, ok := schedule.Next(now)
		missed := scheduling.SleptThrough(task)
		if !ok && missed != nil {
			task.State, task.NextRunAt = store.ScheduledTaskCompleted, nil
			slept = missed
			break
		}
		if !ok {
			return refusal(scheduling.CodeScheduleInPast, "that task has no occurrence left; reschedule it to a new time instead"), nil
		}
		task.State = store.ScheduledTaskActive
		task.NextRunAt = &next
		task.ConsecutiveFailures = 0
		task.MovePaused = false
	case "reschedule":
		parsed, parts := r.rescheduled(&task, args, now)
		if parts != nil {
			return parts, nil
		}
		schedule = parsed
		if task.State != store.ScheduledTaskPaused {
			task.State = store.ScheduledTaskActive
		}
	case "snooze":
		target, parts := r.snoozeTarget(task, args, now)
		if parts != nil {
			return parts, nil
		}
		task.NextRunAt = &target
		if task.RRule == "" {
			// A task that runs once is moved whole: its one time is the new one,
			// held the way StartLocal always is, as the wall clock read in UTC.
			local := target.In(schedule.Location())
			task.StartLocal = time.Date(local.Year(), local.Month(), local.Day(),
				local.Hour(), local.Minute(), local.Second(), 0, time.UTC)
		}
	case "skip_next":
		if task.NextRunAt == nil {
			return refusal(conflictCode, "that task has no next run to skip"), nil
		}
		if next, ok := schedule.Next(*task.NextRunAt); ok {
			task.NextRunAt = &next
		} else {
			task.State = store.ScheduledTaskCompleted
			task.NextRunAt = nil
		}
	default:
		return refusal(scheduling.CodeInvalidSchedule, "action must be reschedule, snooze, skip_next, pause or resume"), nil
	}
	if question := updateQuestion(args); question != "" && !agent.Approved(ctx) {
		view := shownTask(schedule, task.NextRunAt, task.Title, task.Instructions, now)
		asked, parts, err := r.approve(ctx, call, question, view, task.RRule != "")
		if parts != nil || err != nil {
			return parts, err
		}
		if asked {
			// The user can answer minutes later, and the task can change or come due meanwhile,
			// so it is read and changed again from then.
			return r.update(agent.WithApproval(ctx), call)
		}
	}
	if slept != nil {
		err = r.db.ScheduledTasks.Close(ctx, &task, slept)
	} else {
		err = r.db.ScheduledTasks.Update(ctx, &task)
	}
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNoAgentConfig):
			return refusal(unknownConfig, "this agent has been deleted, so the task cannot run again"), nil
		case errors.Is(err, store.ErrTooManyScheduledTasks):
			return refusal(tooManyTasks, "the user already has the most tasks running that this agent allows; cancel one first, or pause one until it is wanted"), nil
		case errors.Is(err, store.ErrScheduledTaskChanged):
			return refusal(taskChanged, "that task changed while you were changing it; read it again with list_scheduled_tasks and retry"), nil
		}
		return nil, err
	}
	r.kickScheduled()
	// Read the schedule back off the task, so what is reported is what now stands.
	if fresh, err := scheduling.FromStored(task); err == nil {
		schedule = fresh
	}
	out := r.taskResult(&task, schedule, now)
	if slept != nil {
		out["missed"] = slept.ScheduledFor.In(schedule.Location()).Format(time.RFC3339) +
			" passed while the task was paused, so it did not run"
	}
	return result(out), nil
}

// updateQuestion is what the user is asked before an update is saved: one that sets a run
// going, as armedByTask counts them, or changes what the task does. "" for a pause or a skip,
// with or without a new title, which ask nothing.
func updateQuestion(args updateArgs) string {
	switch args.Action {
	case "reschedule":
		return "Allow the agent to reschedule this task?"
	case "snooze":
		return "Allow the agent to snooze this task?"
	case "resume":
		return "Allow the agent to resume this task?"
	}
	if args.Instructions != nil {
		return "Allow the agent to change what this task does?"
	}
	return ""
}

// approve holds call until the user allows it on its step in the conversation, with question
// as what they are asked and view as the task the call would leave (shownTask), and reports
// whether anybody was asked; parts is what the model reads
// instead when the call may not go on. The model does what any text it reads says, a web page
// or a tool's result among them, so only the user's own answer lets a call set a run going.
// Where nobody can be asked, as on an SMS, WhatsApp or iMessage thread, a call that leaves its
// task running once goes on unasked, since it runs at most once more; one that leaves it
// repeating is refused (unapprovedRepeat).
func (r *scheduleRunner) approve(ctx context.Context, call llm.ToolCall, question, view string, repeats bool) (bool, []llm.ContentPart, error) {
	answer, asked := r.approvals.ask(r.spec.Caller.UserID, r.spec.askedAs, question, call, view)
	switch {
	case asked:
		if allowed, parts, _, err := r.approvals.waitForAnswer(ctx, call, answer); !allowed {
			return true, parts, err
		}
		return true, nil, nil
	case repeats:
		return false, refusal(needsApproval, unapprovedRepeat), nil
	}
	return false, nil, nil
}

// shownTask is what the step asking the user shows of the task a call would leave, in place of
// the call's own arguments: its schedule in words and its next run first, then its title and
// instructions. A step shows only so much of what it is given (conversation.AwaitApproval), and
// the model picks the order of its arguments, so long instructions would otherwise push the
// repeat out of sight. For an update it is the task as it would then stand, so the user sees
// which task it is and what it would do.
func shownTask(schedule *scheduling.Schedule, next *time.Time, title, instructions string, now time.Time) string {
	view := struct {
		Schedule     string `json:"schedule"`
		Next         string `json:"next,omitempty"`
		Title        string `json:"title"`
		Instructions string `json:"instructions"`
	}{Schedule: schedule.Describe(), Title: title, Instructions: instructions}
	if next != nil {
		view.Next = localNext(*next, schedule.Location(), now)
	}
	// A struct of strings always marshals.
	raw, _ := json.Marshal(view)
	return string(raw)
}

// kickScheduled wakes the scheduled-task worker, when this process runs one.
func (r *scheduleRunner) kickScheduled() {
	if r.kick != nil {
		r.kick()
	}
}

func (r *scheduleRunner) cancel(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	var args struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		return refusal(scheduling.CodeInvalidSchedule, "the arguments are not JSON I can read"), nil
	}
	task, parts, err := r.ownTask(ctx, args.ID)
	if parts != nil || err != nil {
		return parts, err
	}
	if err := r.db.ScheduledTasks.Delete(ctx, r.spec.CustomerID, task.ID); err != nil {
		return nil, err
	}
	return result(map[string]any{"cancelled": task.Title}), nil
}

// ownTask reads one of the session user's tasks on this agent config. Another
// user's, or another agent's, is reported as not there: the tools reach only
// what list_scheduled_tasks would have shown.
func (r *scheduleRunner) ownTask(ctx context.Context, id string) (store.ScheduledTask, []llm.ContentPart, error) {
	task, err := r.db.ScheduledTasks.Get(ctx, r.spec.CustomerID, id)
	if errors.Is(err, store.ErrNoScheduledTask) {
		return task, refusal(scheduledTaskNotFound, "there is no task with that id among this user's tasks on this agent; list_scheduled_tasks shows them"), nil
	}
	if err != nil {
		return task, nil, err
	}
	if task.UserID != r.user || task.ConfigID != r.spec.ConfigID {
		return task, refusal(scheduledTaskNotFound, "there is no task with that id among this user's tasks on this agent; list_scheduled_tasks shows them"), nil
	}
	return task, nil, nil
}

// rescheduled works out the schedule an update asks for, keeping what it left
// out, and writes it onto the task.
func (r *scheduleRunner) rescheduled(task *store.ScheduledTask, args updateArgs, now time.Time) (*scheduling.Schedule, []llm.ContentPart) {
	input := scheduling.Input{Timezone: task.Timezone, Repeat: task.RRule}
	if task.UntilDate != nil {
		input.Until = task.UntilDate.Format("2006-01-02")
	}
	if args.Timezone != nil {
		input.Timezone = *args.Timezone
	}
	if args.Repeat != nil {
		input.Repeat = *args.Repeat
	}
	if args.Until != nil {
		input.Until = *args.Until
	}
	switch {
	case args.At != nil:
		input.At = *args.At
		if input.Repeat != "" {
			input.At = scheduling.StoredClock(input.At, task.StartLocal)
		}
	case args.In != nil:
		input.In = *args.In
	default:
		input.At = task.StartLocal.Format("2006-01-02T15:04")
	}
	parsed, err := scheduling.Parse(input, now)
	if err != nil {
		parts, _ := refusalOf(err)
		return nil, parts
	}
	// A reschedule that names a new time moves the run: a start that has
	// passed goes to the next occurrence, its COUNT counted from there, as a
	// create does. One that does not, or names the stored start again,
	// re-parses it, a change of title, instructions or repeat, and keeps
	// counting from it. A new zone, repeat or until can still bring the run
	// sooner, so it is held to the floor as a new time is; a change of title or
	// instructions alone leaves the run where it is.
	moved := args.At != nil || args.In != nil || args.Timezone != nil || args.Repeat != nil || args.Until != nil
	next, ok := parsed.Next(now)
	if parsed.MovesStartFrom(task.StartLocal) {
		next, ok = parsed.FromNow(now)
	}
	if !ok {
		return nil, refusal(scheduling.CodeScheduleInPast, "that schedule has no occurrence left; its last one has passed")
	}
	if moved {
		if err := scheduling.CheckMove(next, task.NextRunAt, now); err != nil {
			parts, _ := refusalOf(err)
			return nil, parts
		}
	}
	task.StartLocal = parsed.StartLocal
	task.Timezone = parsed.Timezone
	task.RRule = parsed.Repeat
	task.NextRunAt = &next
	if parsed.Until.IsZero() {
		task.UntilDate = nil
	} else {
		until := parsed.Until
		task.UntilDate = &until
	}
	return parsed, nil
}

// snoozeTarget is the instant a snooze moves the next run to.
func (r *scheduleRunner) snoozeTarget(task store.ScheduledTask, args updateArgs, now time.Time) (time.Time, []llm.ContentPart) {
	input := scheduling.Input{Timezone: task.Timezone}
	switch {
	case args.At != nil:
		input.At = *args.At
	case args.In != nil:
		input.In = *args.In
	default:
		return time.Time{}, refusal(scheduling.CodeInvalidSchedule, "snooze needs at or in: when it should run instead")
	}
	parsed, err := scheduling.Parse(input, now)
	if err != nil {
		parts, _ := refusalOf(err)
		return time.Time{}, parts
	}
	target, ok := parsed.First()
	if !ok {
		return time.Time{}, refusal(scheduling.CodeScheduleInPast, "that time has passed; snooze to a time in the future")
	}
	if err := scheduling.CheckMove(target, task.NextRunAt, now); err != nil {
		parts, _ := refusalOf(err)
		return time.Time{}, parts
	}
	return target, nil
}

// zone picks the zone for a new task: what was named for it, the session's,
// the user's stored one, then the config's default.
func (r *scheduleRunner) zone(ctx context.Context, argument string) (string, error) {
	stored, err := r.db.Users.GetTimezone(ctx, r.spec.CustomerID, r.user)
	if err != nil {
		return "", err
	}
	return scheduling.ResolveZone(argument, r.spec.Timezone, stored, r.spec.Scheduling.DefaultTimezone)
}

func (r *scheduleRunner) maxActive() int {
	return r.spec.Scheduling.MaxActive()
}

// displayZone is where the current time is told from when no task says: the
// session's zone, the config's default, or UTC.
func (r *scheduleRunner) displayZone() *time.Location {
	for _, candidate := range []string{r.spec.Timezone, r.spec.Scheduling.DefaultTimezone} {
		if candidate == "" {
			continue
		}
		if loc, err := scheduling.LoadZone(candidate); err == nil {
			return loc
		}
	}
	return time.UTC
}

// taskResult is what a tool answers with for a task that now stands as stored.
func (r *scheduleRunner) taskResult(task *store.ScheduledTask, schedule *scheduling.Schedule, now time.Time) map[string]any {
	out := map[string]any{
		"id":       task.ID,
		"title":    task.Title,
		"schedule": schedule.Describe(),
		"now":      localNow(now, schedule.Location()),
	}
	if task.NextRunAt != nil {
		out["next"] = localNext(*task.NextRunAt, schedule.Location(), now)
	}
	if task.State != store.ScheduledTaskActive {
		out["state"] = task.State
	}
	return out
}

// checkText refuses a task without a title or instructions, or with more of
// either than a task may hold.
func checkText(title, instructions string) []llm.ContentPart {
	if parts := checkTitle(title); parts != nil {
		return parts
	}
	return checkInstructions(instructions)
}

func checkTitle(title string) []llm.ContentPart {
	if strings.TrimSpace(title) == "" {
		return refusal(scheduling.CodeInvalidSchedule, "a task needs a title: a short name the user will recognise")
	}
	if len([]rune(title)) > 80 {
		return refusal(scheduling.CodeInvalidSchedule, "the title is over 80 characters; shorten it")
	}
	return nil
}

func checkInstructions(instructions string) []llm.ContentPart {
	if strings.TrimSpace(instructions) == "" {
		return refusal(scheduling.CodeInvalidSchedule, "a task needs instructions: what to do when it is due, written to yourself")
	}
	if len([]rune(instructions)) > 2000 {
		return refusal(scheduling.CodeInvalidSchedule, "the instructions are over 2000 characters; shorten them")
	}
	return nil
}

// result is a tool result carrying payload as JSON.
func result(payload map[string]any) []llm.ContentPart {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return llm.TextParts("{}")
	}
	return llm.TextParts(string(encoded))
}

// refusal is a tool result saying why not, in a code the model can act on. A
// refusal is a result, not a failed call: the model reads it and does better.
func refusal(code, message string) []llm.ContentPart {
	return result(map[string]any{"error": code, "message": message})
}

// refusalOf turns a scheduling refusal into a tool result. Anything else is
// not the caller's to fix, and is reported as not refused.
func refusalOf(err error) ([]llm.ContentPart, bool) {
	var failure *scheduling.Error
	if errors.As(err, &failure) {
		return refusal(failure.Code, failure.Message), true
	}
	return nil, false
}

// localNow is an instant as the user's clock reads it, zone named.
func localNow(instant time.Time, loc *time.Location) string {
	return instant.In(loc).Format("Mon 2 Jan 2006 15:04") + " " + loc.String()
}

// localNext is an occurrence as the user's clock reads it, with how far away.
func localNext(instant time.Time, loc *time.Location, now time.Time) string {
	return instant.In(loc).Format("Mon 2 Jan 2006 15:04") + ", " + fromNow(now, instant)
}

// fromNow says how long after now an instant is, in the units people count.
func fromNow(now, instant time.Time) string {
	away := instant.Sub(now)
	if away < time.Minute {
		return "in under a minute"
	}
	days := int(away / (24 * time.Hour))
	hours := int(away % (24 * time.Hour) / time.Hour)
	minutes := int(away % time.Hour / time.Minute)
	parts := make([]string, 0, 3)
	if days > 0 {
		parts = append(parts, counted(days, "day"))
	}
	if hours > 0 {
		parts = append(parts, counted(hours, "hour"))
	}
	if minutes > 0 && days == 0 {
		parts = append(parts, counted(minutes, "minute"))
	}
	if len(parts) == 0 {
		parts = append(parts, counted(minutes, "minute"))
	}
	return "in " + strings.Join(parts, " ")
}

func counted(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}
