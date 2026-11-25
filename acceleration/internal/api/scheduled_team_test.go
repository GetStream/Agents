//go:build integration

package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A task of an agent config is that team's agent's, in an app that keeps each team's configs
// to its own users: an end user who left the team neither reaches the tasks they made on it
// nor hears from it when one comes due.

func (s *ScheduledTasksSuite) TestATaskOfATeamTheUserLeftIsAnsweredAsATaskThatDoesNotExist() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})

	for _, operation := range s.checkedTaskOperations() {
		status, refused := s.taskFailure(anya, operation.method, "/v1/agents/scheduled-tasks/"+task.ID+operation.path, operation.body)
		unknownStatus, unknown := s.taskFailure(anya, operation.method, "/v1/agents/scheduled-tasks/"+s.utils.uuid()+operation.path, operation.body)

		s.Equal(http.StatusNotFound, status, "%s %s", operation.method, operation.path)
		s.Equal(unknownStatus, status, "%s %s", operation.method, operation.path)
		s.Equal(unknown, refused, "%s %s: the same answer as a task that does not exist", operation.method, operation.path)
	}
	s.Equal("Stretch", s.taskReloaded(task).Title, "the refused change was not made")
}

// Stopping a task runs nothing of its agent, so a user who left its team still pauses and
// deletes a task of theirs that would otherwise go on firing for them.
func (s *ScheduledTasksSuite) TestAUserWhoLeftATasksTeamStillPausesAndDeletesIt() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})

	s.Equal(http.StatusOK, anya.do(http.MethodPost, "/v1/agents/scheduled-tasks/"+task.ID+"/pause", nil, nil))
	s.Equal(store.ScheduledTaskPaused, s.taskReloaded(task).State)
	s.Equal(http.StatusNoContent, anya.do(http.MethodDelete, "/v1/agents/scheduled-tasks/"+task.ID, nil, nil))
	_, err := s.store.ScheduledTasks.Get(context.Background(), s.customerID(), task.ID)
	s.ErrorIs(err, store.ErrNoScheduledTask)
}

func (s *ScheduledTasksSuite) TestAUserStillInATasksTeamReachesItByEveryOperation() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)

	for _, operation := range s.taskOperations() {
		status, payload := anya.call(operation.method, "/v1/agents/scheduled-tasks/"+task.ID+operation.path, operation.body)

		s.Less(status, http.StatusBadRequest, "%s %s: %s", operation.method, operation.path, payload)
	}
}

func (s *ScheduledTasksSuite) TestABackendReachesATaskOfATeamItsUserLeftWithoutAskingStream() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	asked := s.usersRead(chat)

	s.Equal(http.StatusOK, s.serverClient.actingFor(anya).do(http.MethodGet, "/v1/agents/scheduled-tasks/"+task.ID, nil, nil))
	s.Equal(asked, s.usersRead(chat))
}

// The list leaves out the tasks on a config of another team in the query, so a page is cut
// from the tasks the user may see: full, saying whether there are more, and its cursor going
// on from the last one it holds. A task on no config has no team to leave it out by; one on a
// deleted config has none to let it through, as a read of it says.
func (s *ScheduledTasksSuite) TestTheListLeavesOutTasksOfAnotherTeamAndStillFillsEachPage() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	red, blue, deleted := s.agentOfTeam("red"), s.agentOfTeam("blue"), s.agentOfTeam("red")
	var shown []string
	for _, config := range []string{red, blue, "", deleted, blue, red, blue} {
		task := s.agentsTask(anya, config, nil)
		if config != blue && config != deleted {
			shown = append([]string{task.ID}, shown...)
		}
	}
	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/configs/"+deleted, nil, nil))

	var first, second ScheduledTaskPage
	s.Require().Equal(http.StatusOK, anya.do(http.MethodGet, "/v1/agents/scheduled-tasks?limit=2", nil, &first))
	s.Require().NotNil(first.NextCursor)
	s.Require().Equal(http.StatusOK, anya.do(http.MethodGet, "/v1/agents/scheduled-tasks?limit=2&cursor="+*first.NextCursor, nil, &second))

	s.Equal(shown[:2], idsOf(first.Items))
	s.True(first.HasMore)
	s.Equal(shown[2:], idsOf(second.Items))
	s.False(second.HasMore)
}

func (s *ScheduledTasksSuite) TestATaskOfATeamIsReachedByNoUserWhileTheirTeamsCannotBeRead() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)
	chat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { chat.Heal(http.MethodGet, "/users") })

	listed, listing := s.taskFailure(anya, http.MethodGet, "/v1/agents/scheduled-tasks", nil)
	read, reading := s.taskFailure(anya, http.MethodGet, "/v1/agents/scheduled-tasks/"+task.ID, nil)

	s.Equal(http.StatusServiceUnavailable, listed)
	s.Equal(errTeamsUnreadable.Message, listing.Error.Message)
	s.Equal(http.StatusServiceUnavailable, read)
	s.Equal(errTeamsUnreadable.Message, reading.Error.Message)
}

// The occurrence of a user who left the team fails for good under its own reason, and the
// agent is not run for it: no session is opened, and a one-shot is over, failed.
func (s *ScheduledTasksSuite) TestATaskOfAUserWhoLeftItsAgentsTeamFailsWithoutRunningTheAgent() {
	s.retryAtOnce()
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), func(task *store.ScheduledTask) {
		due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
		task.StartLocal, task.NextRunAt = due, &due
	})
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	// Due before the worker starts, so its first look claims it.
	s.runWorker()

	run := s.failedRun(task)

	s.Equal(scheduling.ReasonNotInTeam, *run.Error)
	s.Equal(int32(1), s.attempts.Load(), "a user out of the team is not asked about again")
	ended := s.taskReloaded(task)
	s.Equal(store.ScheduledTaskFailed, ended.State)
	s.Empty(ended.SessionID, "no session was opened for the agent")
}

// The same holds for a task whose conversation is still running on this node, which the
// occurrence would otherwise follow up in.
func (s *ScheduledTasksSuite) TestATaskOfAUserWhoLeftItsAgentsTeamIsNotFollowedUpInTheirRunningChat() {
	s.retryAtOnce()
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	config := s.agentOfTeam("red")
	opened := s.openScheduling(anya, config, nil)
	running, ok := s.manager.Get(opened.Id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(ok)
	task := s.agentsTask(anya, config, func(task *store.ScheduledTask) {
		task.SessionID, task.OriginSessionID = opened.Id, opened.Id
		task.ConversationID = running.Spec().ConversationID
	})
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)
	s.makeDue(task)
	s.runWorker()

	run := s.failedRun(task)

	s.Equal(scheduling.ReasonNotInTeam, *run.Error)
	s.Equal(session.Live, running.State(), "the occurrence was refused while the chat ran")
	s.False(s.model().heard(task.Instructions), "the agent was not told the task")
}

// Nor is it handed to the node holding their chat, which takes this node's word for it.
func (s *ScheduledTasksSuite) TestATaskOfAUserWhoLeftItsAgentsTeamIsNotHandedToTheNodeHoldingTheirChat() {
	s.retryAtOnce()
	chat := s.teamsApp()
	other, _ := s.otherRouter(nil)
	anya := s.inTeam(chat, "red")
	config := s.agentOfTeam("red")
	opened := anya.on(other).createSession(CreateSessionRequest{ConfigId: &config})
	task := s.agentsTask(anya, config, func(task *store.ScheduledTask) {
		task.SessionID, task.OriginSessionID = opened.Id, opened.Id
	})
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.makeDue(task)
	s.runWorker()

	run := s.failedRun(task)

	s.Equal(scheduling.ReasonNotInTeam, *run.Error)
	s.False(s.model().heard(task.Instructions), "the node holding the chat was not told the task")
}

// The agent stores a task with the kind of whoever opened the chat, so one it made for a user in
// a chat their backend opened is the backend's by kind. It still speaks to that user for the
// team's agent, and fails once they left the team as one they made does.
func (s *ScheduledTasksSuite) TestATaskTheAgentMadeInAChatTheBackendOpenedFailsOnceItsUserLeftTheTeam() {
	s.retryAtOnce()
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), func(task *store.ScheduledTask) {
		task.CallerKind = string(auth.KindServer)
		due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
		task.StartLocal, task.NextRunAt = due, &due
	})
	chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.runWorker()

	run := s.failedRun(task)

	s.Equal(scheduling.ReasonNotInTeam, *run.Error)
	s.Empty(s.taskReloaded(task).SessionID, "no session was opened for the agent")
}

// A task the agent made on an SMS or WhatsApp thread is answered by the Router's own session on
// that thread, which no team limits: its person's teams are not asked, and it goes on to its
// thread, which this test never made.
func (s *ScheduledTasksSuite) TestATaskTheAgentMadeOnAThreadIsNotCheckedAgainstItsPersonsTeams() {
	chat := s.teamsApp()
	person := s.inTeam(chat, "blue")
	task := s.agentsTask(person, s.agentOfTeam("red"), func(task *store.ScheduledTask) {
		task.CallerKind, task.ChannelThread = string(auth.KindServer), "thread-"+s.utils.uuid()
	})
	asked := s.usersRead(chat)

	_, _, err := s.deliverTask(task)

	s.failedAs(err, scheduling.ReasonThreadGone, true)
	s.Equal(asked, s.usersRead(chat), "Stream was not asked about the thread's person")
}

// A task in a chat opened on no config has no team to be kept to: it is delivered to its user,
// who is in a team, and Stream is not asked about them.
func (s *ScheduledTasksSuite) TestATaskTheAgentMadeInAChatOnNoConfigIsDeliveredWithoutAskingStream() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	opened := anya.createSession(textSession(nil))
	running, ok := s.manager.Get(opened.Id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(ok)
	task := s.agentsTask(anya, "", func(task *store.ScheduledTask) {
		task.SessionID, task.OriginSessionID = opened.Id, opened.Id
		task.ConversationID = running.Spec().ConversationID
	})
	asked := s.usersRead(chat)
	s.makeDue(task)
	s.runWorker()

	s.Equal(store.ScheduledTaskDeliveredConversation, s.delivered(task).DeliveredTo)
	s.Equal(asked, s.usersRead(chat), "Stream was not asked about the user's teams")
}

func (s *ScheduledTasksSuite) TestATaskOfAUserStillInItsAgentsTeamIsDelivered() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), nil)
	s.makeDue(task)
	s.runWorker()

	s.Equal(store.ScheduledTaskDeliveredConversation, s.delivered(task).DeliveredTo)
}

// A backend is not limited by teams, so a task it made for a user who is in none of its
// config's runs as any of its tasks does.
func (s *ScheduledTasksSuite) TestATaskItsBackendMadeForAUserOutsideTheTeamIsDelivered() {
	chat := s.teamsApp()
	anya := s.inTeam(chat, "blue")
	task := s.agentsTask(anya, s.agentOfTeam("red"), func(task *store.ScheduledTask) {
		task.CallerKind, task.CreatedBy = string(auth.KindServer), "api"
	})
	s.makeDue(task)
	s.runWorker()

	s.Equal(store.ScheduledTaskDeliveredConversation, s.delivered(task).DeliveredTo)
}

// Teams Stream could not tell fail the occurrence's attempts, and it is tried again; the task
// goes on to its next occurrence, which may find them.
func (s *ScheduledTasksSuite) TestAnOccurrenceWhoseTeamsCannotBeReadFailsAndTheTaskGoesOn() {
	s.retryAtOnce()
	chat := s.teamsApp()
	anya := s.inTeam(chat, "red")
	task := s.agentsTask(anya, s.agentOfTeam("red"), func(task *store.ScheduledTask) { task.RRule = "FREQ=DAILY" })
	chat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { chat.Heal(http.MethodGet, "/users") })
	s.makeDue(task)
	s.runWorker()

	run := s.failedRun(task)

	s.Equal(scheduling.ReasonFailed, *run.Error)
	s.Equal(int32(3), s.attempts.Load(), "each retry asked again")
	going := s.taskReloaded(task)
	s.Equal(store.ScheduledTaskActive, going.State)
	s.Empty(going.SessionID, "no session was opened for the agent")
}

// teamsApp moves the test to an app that keeps each team's agent configs to its own users, and
// returns the Stream app its users' teams are read from.
func (s *RouterSuite) teamsApp() *chattest.Server {
	s.useApp(s.data.createAppAdmitting(store.AppSettings{AgentTeams: new(true)}))
	return s.giveApp(s.customerID(), 4242, teamsAppKey)
}

// inTeam is a new end user of the app whose Stream user in chat is in team.
func (s *RouterSuite) inTeam(chat *chattest.Server, team string) *testClient {
	user := s.data.createUser()
	chat.PutUser(map[string]any{"id": user.userID, "teams": []any{team}})
	return user
}

// agentOfTeam is an agent config with scheduling on, of team.
func (s *ScheduledTasksSuite) agentOfTeam(team string) string {
	return s.agentWithScheduling(func(request *AgentConfigRequest) {
		request.Tags = &map[string]string{session.TeamTag: team}
	})
}

// agentsTask is a one-shot a day out that the agent scheduled for user on config, in a session
// the user opened, as its scheduling tool stores one, changed as change says.
func (s *ScheduledTasksSuite) agentsTask(user *testClient, config string, change func(*store.ScheduledTask)) store.ScheduledTask {
	next := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Minute)
	task := store.ScheduledTask{
		CustomerID: s.customerID(), ConfigID: config, UserID: user.userID, APIKey: s.app.key,
		CallerKind: string(auth.KindAuthenticated), CreatedBy: "agent",
		Title: "Stretch", Instructions: "Remind them to stretch, " + s.utils.uuid(),
		StartLocal: next, Timezone: "UTC", NextRunAt: &next,
	}
	if change != nil {
		change(&task)
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(context.Background(), &task, 0))
	return task
}

// taskOperation is one of the operations a device may call on a task, by its path under it.
type taskOperation struct {
	method, path string
	body         any
}

// taskOperations are every operation a device may call on one task, in an order that leaves
// the task there for the next until the delete.
func (s *ScheduledTasksSuite) taskOperations() []taskOperation {
	return []taskOperation{
		{http.MethodGet, "", nil},
		{http.MethodGet, "/runs", nil},
		{http.MethodPatch, "", UpdateScheduledTaskRequest{Title: pointerTo("Walk")}},
		{http.MethodPost, "/pause", nil},
		{http.MethodPost, "/resume", nil},
		{http.MethodDelete, "", nil},
	}
}

// checkedTaskOperations are the operations a device reaches a task by only while its user is in
// the team of its config: all of them but those that only stop it.
func (s *ScheduledTasksSuite) checkedTaskOperations() []taskOperation {
	return []taskOperation{
		{http.MethodGet, "", nil},
		{http.MethodGet, "/runs", nil},
		{http.MethodPatch, "", UpdateScheduledTaskRequest{Title: pointerTo("Walk")}},
		{http.MethodPost, "/resume", nil},
	}
}

// usersRead is how many user queries chat has been sent, a teams read among them.
func (s *ScheduledTasksSuite) usersRead(chat *chattest.Server) int {
	read := 0
	for _, request := range chat.Requests(teamsAppKey) {
		if request.Method == http.MethodGet && strings.HasSuffix(request.Path, "/users") {
			read++
		}
	}
	return read
}

// idsOf are the ids of tasks, in order.
func idsOf(tasks []ScheduledTask) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.Id)
	}
	return ids
}
