//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/campaign"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// countingSettings is a router that runs the work it counts for an app on its own: the
// forwarder (it needs connectors, so a keyring), and the scheduler, the campaign dialer and the
// simulation runner (they need sessions, so an llm, which the mock providers give).
func (s *RouterStartSuite) countingSettings() config.Config {
	setDevKEK(s.T(), "activity-wiring-key")
	settings := s.settings
	settings.Addr = freeAddress(s.T())
	settings.Connectors.Enabled = true
	settings.MockProviders = true
	return settings
}

// counted is what the activity record holds for app under area, the client the router's own
// work is counted under. The record writes when the router stops, so call it after stop.
func (s *RouterStartSuite) counted(app, area string) (requests, refused int64) {
	s.Require().NoError(s.db.GlobalDB().QueryRowContext(s.ctx,
		"SELECT COALESCE(sum(requests), 0), COALESCE(sum(refused), 0) FROM customer_activity WHERE app_id = ? AND area = ? AND client = ?",
		app, area, activity.ClientRouter).Scan(&requests, &refused))
	return requests, refused
}

// settled waits until query, a boolean about app's work, answers true.
func (s *RouterStartSuite) settled(what, query, app string) {
	s.Require().Eventually(func() bool {
		var done bool
		s.Require().NoError(s.db.GlobalDB().QueryRowContext(s.ctx, query, app).Scan(&done))
		return done
	}, 30*time.Second, 50*time.Millisecond, what)
}

// Defect: the scheduler built without the activity record, so the occurrences it settles for an
// app are not counted in its usage. The occurrence is over a day old, so the scheduler settles
// it as missed without delivering anything, and counts it refused.
func (s *RouterStartSuite) TestARunningRouterCountsTheScheduledTaskOccurrencesItSettles() {
	due := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	task := store.ScheduledTask{
		CustomerID: "customer-" + uuid.NewString(), ConfigID: "config-1", UserID: "user-1", CallerKind: store.UserKindAuthenticated,
		Title: "Stretch", Instructions: "Remind them to stretch.", StartLocal: due, Timezone: "UTC", NextRunAt: &due,
	}
	s.Require().NoError(s.db.ScheduledTasks.Insert(s.ctx, &task, 25))
	s.T().Cleanup(func() { s.NoError(s.db.ScheduledTasks.Delete(s.ctx, task.CustomerID, task.ID)) })
	stop := s.start(s.countingSettings())

	s.settled("the task was not settled", "SELECT EXISTS (SELECT 1 FROM scheduled_tasks WHERE customer_id = ? AND state = 'completed')", task.CustomerID)
	s.Require().NoError(stop())

	requests, refused := s.counted(task.CustomerID, activity.AreaScheduling)
	s.Equal([]int64{1, 1}, []int64{requests, refused})
}

// Defect: the forwarder built without the activity record, so the forwards it sends or gives up
// for an app are not counted. The destination's signing secret does not open (nobody sealed it),
// so the forward is given up as not sent before any request is made: no network, no retry wait.
func (s *RouterStartSuite) TestARunningRouterCountsTheEventForwardsItEnds() {
	destination := store.EventDestination{
		ID: uuid.NewString(), CustomerID: "customer-" + uuid.NewString(), ConnectorID: "slack_bot",
		URL: "https://example.com/hook", Forward: store.ForwardAll, SecretSealed: []byte("sealed by nobody"), KEKVersion: 1,
	}
	s.Require().NoError(s.db.EventDestinations.Insert(s.ctx, &destination))
	s.T().Cleanup(func() {
		_, err := s.db.GlobalDB().ExecContext(s.ctx, "DELETE FROM connector_event_destinations WHERE id = ?", destination.ID)
		s.NoError(err)
	})
	_, err := s.db.EventDeliveries.Queue(s.ctx, destination.CustomerID, destination.ConnectorID, []string{store.ForwardAll},
		store.EventDelivery{ID: "delivery-" + uuid.NewString(), Body: []byte(`{}`), NextAttemptAt: time.Now()})
	s.Require().NoError(err)
	stop := s.start(s.countingSettings())

	s.settled("the forward was not ended", "SELECT NOT EXISTS (SELECT 1 FROM connector_event_deliveries d JOIN connector_event_destinations ced ON ced.id = d.destination_id WHERE ced.customer_id = ?)", destination.CustomerID)
	s.Require().NoError(stop())

	requests, refused := s.counted(destination.CustomerID, activity.AreaWebhooksOut)
	s.Equal([]int64{1, 1}, []int64{requests, refused})
}

// Defect: the campaign dialer built without the activity record, so the dials it makes for an
// app are not counted. The campaign's agent does not exist, so the dial pauses the campaign
// without ringing anybody, and is counted refused.
func (s *RouterStartSuite) TestARunningRouterCountsTheDialsOfItsCampaigns() {
	sequence, err := json.Marshal(campaign.LegacySequence("config-1"))
	s.Require().NoError(err)
	hours, err := json.Marshal(campaign.LegacyCallingHours())
	s.Require().NoError(err)
	customer := "customer-" + uuid.NewString()
	row := store.Campaign{CustomerID: customer, Name: "renewals", ConfigID: "config-1", FromNumber: "+15550100000", Concurrency: 1, Sequence: sequence, CallingHours: hours}
	s.Require().NoError(s.db.Campaigns.Insert(s.ctx, &row))
	// Its contacts and attempts go with it (ON DELETE CASCADE). Delete refuses a paused campaign.
	s.T().Cleanup(func() {
		_, err := s.db.GlobalDB().ExecContext(context.Background(), "DELETE FROM campaigns WHERE id = ?", row.ID)
		s.NoError(err)
	})
	_, _, err = s.db.CampaignContacts.InsertMany(s.ctx, row.ID, []store.Contact{{ToNumber: "+15550000001", TimeZone: "UTC"}})
	s.Require().NoError(err)
	_, err = s.db.Campaigns.Launch(s.ctx, customer, row.ID, store.Launch{
		StartAt: time.Now().Add(-time.Minute), Now: time.Now(), APIKey: "key-a", UserID: "user-1", UsageCategories: []string{"sales"},
	})
	s.Require().NoError(err)
	stop := s.start(s.countingSettings())

	s.settled("the dial was not made", "SELECT EXISTS (SELECT 1 FROM campaign_attempts ca JOIN campaigns c ON c.id = ca.campaign_id WHERE c.customer_id = ? AND ca.ended_at IS NOT NULL)", customer)
	s.Require().NoError(stop())

	requests, refused := s.counted(customer, activity.AreaPhoneSMS)
	s.Equal([]int64{1, 1}, []int64{requests, refused})
}

// Defect: the simulation runner built without the activity record, so the runs it settles for an
// app are not counted. The agent, the caller and the judge are pinned to the mock model, so the
// run needs no network and no provider key, whatever the checkout's .env holds. It errors (the mock
// does not play a caller), and an errored run is counted refused.
func (s *RouterStartSuite) TestARunningRouterCountsTheSimulationRunsItSettles() {
	customer := "customer-" + uuid.NewString()
	agent := store.AgentConfig{CustomerID: customer, Name: "order taker", Mode: store.AgentModeText, Instructions: "Take orders.", Models: store.AgentModels{LLM: "mock/mock-llm"}}
	s.Require().NoError(s.db.AgentConfigs.Insert(s.ctx, &agent))
	simulation := store.Simulation{
		CustomerID: customer, Name: "orders", Mode: store.SimulationText, ConfigID: agent.ID,
		Scenario: "Order a pizza.", Assertion: "Was a pizza ordered?", Variations: 1,
		CallerTarget: "mock/mock-llm", JudgeTarget: "mock/mock-llm",
	}
	s.Require().NoError(s.db.Simulations.Insert(s.ctx, &simulation))
	// Its runs and cases go with it (ON DELETE CASCADE). Simulations.Delete and AgentConfigs.Delete
	// only mark a row as gone.
	s.T().Cleanup(func() {
		_, err := s.db.GlobalDB().ExecContext(context.Background(), "DELETE FROM simulations WHERE id = ?", simulation.ID)
		s.NoError(err)
		_, err = s.db.GlobalDB().ExecContext(context.Background(), "DELETE FROM agent_configs WHERE id = ?", agent.ID)
		s.NoError(err)
	})
	settings := s.countingSettings()
	stop := s.start(settings)

	request, err := http.NewRequest(http.MethodPost, "http://"+settings.Addr+"/v1/agents/simulations/"+simulation.ID+"/run", strings.NewReader("{}"))
	s.Require().NoError(err)
	request.Header.Set("X-Customer-Id", customer)
	// The request is counted too, under the cli, so it is not one of the rows the run's end makes.
	request.Header.Set("X-Stream-Client", store.AuditSourceCLI)
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	s.Require().Equal(http.StatusAccepted, response.StatusCode)
	s.settled("the run was not settled", "SELECT EXISTS (SELECT 1 FROM simulation_runs WHERE customer_id = ? AND finished_at IS NOT NULL)", customer)
	s.Require().NoError(stop())

	requests, refused := s.counted(customer, activity.AreaEvals)
	s.Equal([]int64{1, 1}, []int64{requests, refused})
}
