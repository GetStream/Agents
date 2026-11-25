//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"
)

// SimulationsSuite covers writing down a conversation to put an agent through, running it,
// and reading what it came to.
type SimulationsSuite struct {
	RouterSuite
}

func TestSimulationsSuite(t *testing.T) {
	runSuite(t, new(SimulationsSuite))
}

// SimulationsShutDownSuite covers a run asked of a router whose simulation runner has
// stopped. It is a suite of its own because the runner stays stopped.
type SimulationsShutDownSuite struct {
	RouterSuite
}

func TestSimulationsShutDownSuite(t *testing.T) {
	runSuite(t, new(SimulationsShutDownSuite))
}

func (s *SimulationsShutDownSuite) TestARunAskedOfARouterThatIsShuttingDownIsUnavailable() {
	s.useApp(s.data.createApp())
	var created Simulation
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/simulations", SimulationRequest{
		Name: "sim-" + s.utils.uuid(), ConfigId: s.data.createAgentConfig().Id, Scenario: "ask about the weather",
		Assertion: "did the agent answer?",
	}, &created))
	s.router.simulations.Close()

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/simulations/"+created.Id+"/run", nil)

	s.Equal(http.StatusServiceUnavailable, status, string(body))
	s.Contains(string(body), `"type":"unavailable"`)
	s.NotContains(string(body), "simulation:")
}

func (s *SimulationsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *SimulationsSuite) TestASimulationSurvivesBeingStoredAndReadBack() {
	agent := s.data.createAgentConfig()

	created := s.createSimulation(SimulationRequest{
		Name: "orders", ConfigId: agent.Id,
		Scenario:  "order a pizza and then change your mind",
		Assertion: "did the agent cancel the order?", Variations: pointerTo(3),
		MaxTurns: pointerTo(4), Tags: &map[string]string{"project": "support"},
	})

	s.Require().NotEmpty(created.Id)
	read := s.simulation(created.Id)
	s.Equal("orders", read.Name)
	s.Equal(agent.Id, read.ConfigId)
	s.Equal("order a pizza and then change your mind", read.Scenario)
	s.Equal("did the agent cancel the order?", read.Assertion)
	s.Equal(3, read.Variations)
	s.Equal(4, read.MaxTurns)
	s.Equal(SimulationMode("text"), read.Mode, "text is what a simulation is unless it says otherwise")
}

func (s *SimulationsSuite) TestTheAppsSimulationsAreListedBack() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))

	var listed []Simulation
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/simulations", nil, &listed))

	named := make([]string, 0, len(listed))
	for _, simulation := range listed {
		named = append(named, simulation.Id)
	}
	s.Contains(named, created.Id)
}

func (s *SimulationsSuite) TestASimulationIsEditedWithoutBeingRewritten() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))

	edited := s.against(AgentConfig{Id: created.ConfigId})
	edited.Name = "renamed"
	edited.Scenario = "ask for a refund"
	var updated Simulation
	s.Require().Equal(http.StatusOK, s.serverClient.do(
		http.MethodPut, "/v1/agents/simulations/"+created.Id, edited, &updated))

	s.Equal("renamed", updated.Name)
	s.Equal("ask for a refund", s.simulation(created.Id).Scenario)
}

func (s *SimulationsSuite) TestADeletedSimulationIsGone() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))

	s.Require().Equal(http.StatusNoContent,
		s.serverClient.do(http.MethodDelete, "/v1/agents/simulations/"+created.Id, nil, nil))

	status, _ := s.serverClient.call(http.MethodGet, "/v1/agents/simulations/"+created.Id, nil)
	s.Equal(http.StatusNotFound, status)
}

func (s *SimulationsSuite) TestASimulationWithNothingToCheckIsRefused() {
	agent := s.data.createAgentConfig()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/simulations",
		SimulationRequest{Name: "orders", ConfigId: agent.Id, Scenario: "order a pizza"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "something to check")
}

func (s *SimulationsSuite) TestASimulationAgainstAnAgentThatIsNotThereIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/simulations",
		SimulationRequest{Name: "orders", ConfigId: s.utils.uuid(),
			Scenario: "order a pizza", Assertion: "was one ordered?"})

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, errUnknownConfig.Message)
}

func (s *SimulationsSuite) TestRunningASimulationStartsTheConversationsItAsksFor() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))

	var run SimulationRun
	s.Require().Equal(http.StatusAccepted, s.serverClient.do(
		http.MethodPost, "/v1/agents/simulations/"+created.Id+"/run", nil, &run))

	s.Equal(created.Id, run.SimulationId)
	s.Equal(1, run.Cases, "one way of asking, as the simulation was written")
	s.NotEmpty(run.State)
}

func (s *SimulationsSuite) TestARunIsReadBackWithTheConversationsItHad() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))
	started := s.run(created.Id)

	var read SimulationRun
	s.Require().Equal(http.StatusOK, s.serverClient.do(
		http.MethodGet, "/v1/agents/simulation-runs/"+started.Id, nil, &read))

	s.Equal(started.Id, read.Id)
	s.NotNil(read.Conversations, "one run asked for by itself carries its transcripts")
}

func (s *SimulationsSuite) TestTheLogOfRunsIsNotOneOfTheRuns() {
	// A run named "runs" would otherwise be the log, and the log would never be reachable.
	created := s.createSimulation(s.against(s.data.createAgentConfig()))
	started := s.run(created.Id)

	var log []SimulationRun
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/simulation-runs?simulation_id="+created.Id, nil, &log))

	ran := make([]string, 0, len(log))
	for _, run := range log {
		ran = append(ran, run.Id)
	}
	s.Contains(ran, started.Id)
	s.Nil(log[0].Conversations, "a log of fifty runs is not fifty transcripts")
}

func (s *SimulationsSuite) TestAListOfRunsIsAskedForUpToFiveHundred() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))
	s.run(created.Id)
	newest := s.run(created.Id)

	for limit, held := range map[string]int{"1": 1, "500": 2} {
		var log []SimulationRun
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
			"/v1/agents/simulation-runs?simulation_id="+created.Id+"&limit="+limit, nil, &log))
		s.Require().Len(log, held, limit)
		s.Equal(newest.Id, log[0].Id, "newest first")
	}
	for _, limit := range []string{"0", "-1", "501", "1000000000"} {
		status, answered := s.serverClient.failure(http.MethodGet, "/v1/agents/simulation-runs?limit="+limit, nil)
		s.Equal(http.StatusBadRequest, status, limit)
		s.Contains(answered, "limit", limit)
	}
}

func (s *SimulationsSuite) TestARunNobodyStartedIsNotFound() {
	status, _ := s.serverClient.call(http.MethodGet, "/v1/agents/simulation-runs/"+s.utils.uuid(), nil)

	s.Equal(http.StatusNotFound, status)
}

func (s *SimulationsSuite) TestAnotherAppsSimulationIsNotFound() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/simulations/"+created.Id, nil, nil)
	})
}

func (s *SimulationsSuite) TestOnlyTheAppsOwnBackendMayWriteASimulation() {
	agent := s.data.createAgentConfig()

	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodPost, "/v1/agents/simulations", s.against(agent))
		return status
	})
}

func (s *SimulationsSuite) TestASimulationTheDatabaseCannotStoreIsAnsweredAsInternalWithNothingOfTheDatabase() {
	request := s.against(s.data.createAgentConfig())
	s.failQueries(`INSERT INTO "simulations"`)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/simulations", request)

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "simulations")
	s.NotContains(string(body), "deadline")
}

func (s *SimulationsSuite) TestASimulationTheDatabaseCannotEditIsAnsweredAsInternalWithNothingOfTheDatabase() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))
	s.failQueries(`UPDATE "simulations"`)

	status, body := s.serverClient.call(http.MethodPut, "/v1/agents/simulations/"+created.Id,
		s.against(AgentConfig{Id: created.ConfigId}))

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "simulations")
	s.NotContains(string(body), "deadline")
}

func (s *SimulationsSuite) TestARunTheDatabaseCannotStartIsAnsweredAsInternalWithNothingOfTheDatabase() {
	created := s.createSimulation(s.against(s.data.createAgentConfig()))
	s.failQueries(`INSERT INTO "simulation_runs"`)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/simulations/"+created.Id+"/run", nil)

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "simulation_runs")
	s.NotContains(string(body), "deadline")
}

func (s *SimulationsSuite) TestAnAppHasFourRunsAtOnceWhenItAsksForManyTogether() {
	ids := make([]string, 10)
	for i := range ids {
		ids[i] = s.stuckSimulation()
	}
	answers := make([]*http.Response, len(ids))
	var asked sync.WaitGroup
	for i, id := range ids {
		asked.Go(func() {
			answers[i] = s.serverClient.raw(http.MethodPost, "/v1/agents/simulations/"+id+"/run", nil)
		})
	}
	asked.Wait()

	going := 0
	for _, answer := range answers {
		if answer.StatusCode == http.StatusAccepted {
			var run SimulationRun
			s.Require().NoError(json.NewDecoder(answer.Body).Decode(&run))
			s.cancelled(run.Id)
			going++
			continue
		}
		s.Equal(http.StatusTooManyRequests, answer.StatusCode)
		s.Equal("30", answer.Header.Get("Retry-After"))
		var failed ErrorResponse
		s.Require().NoError(json.NewDecoder(answer.Body).Decode(&failed))
		s.Equal("concurrency_limited", failed.Error.Code)
		s.NotContains(failed.Error.Message, "simulation:")
	}
	s.Equal(4, going)
}

func (s *SimulationsSuite) TestAnAppsRunsLeaveRoomForAnotherApps() {
	for range 4 {
		s.cancelled(s.run(s.stuckSimulation()).Id)
	}

	s.useApp(s.data.createApp())

	s.cancelled(s.run(s.stuckSimulation()).Id)
}

func (s *SimulationsSuite) TestACancelledRunFreesItsPlaceOnceItsConversationsEnd() {
	var last string
	for range 4 {
		last = s.run(s.stuckSimulation()).Id
		s.cancelled(last)
	}
	next := s.stuckSimulation()
	status, _ := s.serverClient.call(http.MethodPost, "/v1/agents/simulations/"+next+"/run", nil)
	s.Require().Equal(http.StatusTooManyRequests, status)

	s.Require().Equal(http.StatusOK, s.serverClient.do(
		http.MethodPost, "/v1/agents/simulation-runs/"+last+"/cancel", nil, nil))

	var run SimulationRun
	s.Require().Eventually(func() bool {
		return s.serverClient.do(http.MethodPost, "/v1/agents/simulations/"+next+"/run", nil, &run) == http.StatusAccepted
	}, 30*time.Second, 100*time.Millisecond)
	s.cancelled(run.Id)
}

func (s *SimulationsSuite) TestARunThatCouldNotBeWrittenHoldsNoPlace() {
	ids := []string{s.stuckSimulation(), s.stuckSimulation(), s.stuckSimulation(), s.stuckSimulation()}
	s.failQueries(`INSERT INTO "simulation_runs"`)
	for _, id := range ids {
		status, _ := s.serverClient.call(http.MethodPost, "/v1/agents/simulations/"+id+"/run", nil)
		s.Require().Equal(http.StatusInternalServerError, status)
	}
	s.failing.value.Store(nil)

	s.cancelled(s.run(ids[0]).Id)
}

func (s *SimulationsSuite) TestARunBeyondTheRunsTheRouterHasAtOnceIsAskedToWait() {
	// Apps one after another, until the router has no place left for a new one.
	going := 0
	var answer *http.Response
	for range 5 {
		for range 4 {
			answer = s.serverClient.raw(http.MethodPost, "/v1/agents/simulations/"+s.stuckSimulation()+"/run", nil)
			if answer.StatusCode != http.StatusAccepted {
				break
			}
			var run SimulationRun
			s.Require().NoError(json.NewDecoder(answer.Body).Decode(&run))
			s.cancelled(run.Id)
			going++
		}
		if answer.StatusCode != http.StatusAccepted {
			break
		}
		s.useApp(s.data.createApp())
	}

	s.Equal(8, going)
	s.Equal(http.StatusTooManyRequests, answer.StatusCode)
	s.Equal("30", answer.Header.Get("Retry-After"))
	var failed ErrorResponse
	s.Require().NoError(json.NewDecoder(answer.Body).Decode(&failed))
	s.Equal("rate_limited", failed.Error.Code)
	s.NotContains(failed.Error.Message, "simulation:")
}

func (s *SimulationsSuite) TestMoreVariationsThanARunTriesIsRefusedSayingHowMany() {
	request := s.against(s.data.createAgentConfig())
	request.Variations = pointerTo(11)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/simulations", request)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "variations is not between 1 and 10")
}

// against is a simulation to put one agent through, named so that two tests' rows are not
// each other's.
func (s *SimulationsSuite) against(agent AgentConfig) SimulationRequest {
	return SimulationRequest{
		Name: "orders-" + s.utils.uuid(), ConfigId: agent.Id,
		Scenario: "order a pizza", Assertion: "was one ordered?",
	}
}

func (s *SimulationsSuite) createSimulation(request SimulationRequest) Simulation {
	var created Simulation
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/simulations", request, &created))
	return created
}

func (s *SimulationsSuite) simulation(id string) Simulation {
	var read Simulation
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/simulations/"+id, nil, &read))
	return read
}

func (s *SimulationsSuite) run(id string) SimulationRun {
	var started SimulationRun
	s.Require().Equal(http.StatusAccepted, s.serverClient.do(
		http.MethodPost, "/v1/agents/simulations/"+id+"/run", nil, &started))
	return started
}

// stuckSimulation is a simulation whose caller takes a minute over each thing it says, so a run
// of it is still going when the next one is asked for.
func (s *SimulationsSuite) stuckSimulation() string {
	request := s.against(s.data.createAgentConfig())
	request.CallerTarget = pointerTo("stuck/stuck-model")
	return s.createSimulation(request).Id
}

// cancelled stops a run of the app in use when the test ends, and waits for it to be over.
func (s *SimulationsSuite) cancelled(id string) {
	client := s.serverClient
	s.T().Cleanup(func() {
		client.do(http.MethodPost, "/v1/agents/simulation-runs/"+id+"/cancel", nil, nil)
		s.Require().Eventually(func() bool {
			var run SimulationRun
			client.do(http.MethodGet, "/v1/agents/simulation-runs/"+id, nil, &run)
			return run.State != "" && run.State != "running"
		}, 30*time.Second, 50*time.Millisecond)
	})
}
