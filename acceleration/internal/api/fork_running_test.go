//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// ForkRunningSuite covers what a fork of a running session takes of it: what it was opened
// with, and nothing of what it was as one conversation.
type ForkRunningSuite struct {
	RouterSuite
}

func TestForkRunningSuite(t *testing.T) {
	runSuite(t, new(ForkRunningSuite))
}

func (s *ForkRunningSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ForkRunningSuite) TestASessionCarriedOnIsForkedWhileItRuns() {
	for path, end := range s.ends() {
		for name, client := range s.callers() {
			for _, messages := range []*bool{nil, pointerTo(true), pointerTo(false)} {
				s.Run(path+" "+name, func() {
					parent := client.createSession(textSession(nil))
					s.carryOn(client, parent.Id, end)

					forked := s.fork(client, parent.Id, ForkSessionRequest{Messages: messages})

					s.Equal(parent.Id, value(forked.ForkedFrom))
					s.NotEqual(value(parent.ConversationId), value(forked.ConversationId))
					s.True(forked.CreatedAt.After(parent.CreatedAt), "it opens now, not when its parent first did")
					s.True(s.specOf(forked.Id).Reopened.IsZero())
					respondIn(&s.RouterSuite, client, forked.Id, "hello")
				})
			}
		}
	}
}

func (s *ForkRunningSuite) TestASessionCarriedOnIsForkedAtAMessage() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			owner := s.data.createUser()
			parent := owner.createSession(textSession(nil))
			before := s.answeredTurn(owner, parent.Id, "before")
			s.carryOn(owner, parent.Id, end)
			after := s.answeredTurn(owner, parent.Id, "after")

			s.Equal(1, s.recalled(s.fork(owner, parent.Id, ForkSessionRequest{ResponseId: &before}).Id))
			s.Equal(2, s.recalled(s.fork(owner, parent.Id, ForkSessionRequest{ResponseId: &after}).Id))
		})
	}
}

func (s *ForkRunningSuite) TestASessionCarriedOnIsForkedOntoAnotherConfig() {
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": "other-" + s.utils.uuid(), "mode": "text", "llm": "echo/echo-model"}, &config))
	owner := s.data.createUser()
	parent := owner.createSession(textSession(nil))
	s.carryOn(owner, parent.Id, s.ends()["stopped"])

	forked := s.fork(owner, parent.Id, ForkSessionRequest{ConfigId: &config.Id})

	s.Equal("echo/echo-model", value(forked.Llm))
}

func (s *ForkRunningSuite) TestASessionCarriedOnIsForkedOnAnotherNode() {
	owner := s.data.createUser()
	parent := owner.createSession(textSession(nil))
	s.carryOn(owner, parent.Id, s.ends()["stopped"])
	other, _ := s.otherRouter(nil)

	forked := s.fork(owner.on(other), parent.Id, ForkSessionRequest{})

	s.Equal(parent.Id, value(forked.ForkedFrom))
}

func (s *ForkRunningSuite) TestAForkOfAForkCarriedOnIsItsOwnSession() {
	owner := s.data.createUser()
	first := s.fork(owner, owner.createSession(textSession(nil)).Id, ForkSessionRequest{})
	s.carryOn(owner, first.Id, s.ends()["stopped"])

	again := s.fork(owner, first.Id, ForkSessionRequest{})

	s.Equal(first.Id, value(again.ForkedFrom))
}

func (s *ForkRunningSuite) TestAProjectLongerThanATagIsForkedUnderAnother() {
	owner := s.data.createUser()
	parent := owner.createSession(textSession(nil))
	s.ends()["stopped"](parent.Id)
	_, err := s.store.ShardDB().ExecContext(context.Background(),
		"UPDATE agent_sessions SET project = repeat('h', 300) WHERE id = ?", parent.Id)
	s.Require().NoError(err)
	s.carryOn(owner, parent.Id, func(string) {})

	forked := s.fork(owner, parent.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})

	s.Equal("beta", value(forked.ProjectId))
}

func (s *ForkRunningSuite) TestAForkIsNotThePlacedCallItsParentIs() {
	spec := s.backendSpec()
	spec.CampaignID, spec.ContactID, spec.Navigating = "campaign", "contact", true
	spec.Voicemail = &session.VoicemailPlan{Message: "call back"}

	forked := s.specOf(s.fork(s.serverClient, s.running(spec), ForkSessionRequest{Messages: pointerTo(false)}).Id)

	s.Empty(forked.CampaignID)
	s.Empty(forked.ContactID)
	s.False(forked.Navigating)
	s.Nil(forked.Voicemail)
}

func (s *ForkRunningSuite) TestAForkDoesNotAnswerItsParentsHandOut() {
	spec := s.backendSpec()
	spec.Dispatched, spec.HandedOutBy = true, "ann"

	forked := s.specOf(s.fork(s.serverClient, s.running(spec), ForkSessionRequest{Messages: pointerTo(false)}).Id)

	s.False(forked.Dispatched, "its questions are written into its own channel")
	s.Empty(forked.HandedOutBy)
}

// A running parent's usage categories are its fork's, as many as a request may name.
func (s *ForkRunningSuite) TestAForkOfASessionOfTenCategoriesIsMade() {
	parent := s.serverClient.with(auth.UsageCategoriesHeader, "a:1,a:2,a:3,a:4,a:5,a:6,a:7,a:8,a:9,team:legal").
		createSession(textSession(nil))
	want := []string{"a:1", "a:2", "a:3", "a:4", "a:5", "a:6", "a:7", "a:8", "a:9", "team:legal"}
	s.Require().Equal(want, s.specOf(parent.Id).Attribution.UsageCategories)
	for name, asking := range map[string]*testClient{
		"naming none": s.serverClient, "naming a project": s.serverClient.with(auth.UsageCategoriesHeader, "project:support"),
		"naming ten": s.serverClient.with(auth.UsageCategoriesHeader, "b:1,b:2,b:3,b:4,b:5,b:6,b:7,b:8,b:9,b:10"),
	} {
		s.Run(name, func() {
			s.Equal(want, s.specOf(s.fork(asking, parent.Id, ForkSessionRequest{}).Id).Attribution.UsageCategories)
		})
	}
	s.carryOn(s.serverClient, parent.Id, s.ends()["stopped"])
	s.Equal(want, s.specOf(s.fork(s.serverClient, parent.Id, ForkSessionRequest{}).Id).Attribution.UsageCategories)
}

func (s *ForkRunningSuite) TestAForkWithoutMessagesTakesNoneOfTheHistoryItsParentWasHanded() {
	request := textSession(nil)
	request.History = &[]HistoryMessage{{Role: HistoryRoleUser, Text: "Where is order 3825?"}}
	parent := s.serverClient.createSession(request)

	s.Empty(s.specOf(s.fork(s.serverClient, parent.Id, ForkSessionRequest{Messages: pointerTo(false)}).Id).History)
	s.Len(s.specOf(s.fork(s.serverClient, parent.Id, ForkSessionRequest{}).Id).History, 1)
	s.Len(s.specOf(s.fork(s.serverClient, parent.Id, ForkSessionRequest{Messages: pointerTo(true)}).Id).History, 1)
	var bare Session
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/fork", nil, &bare))
	s.Len(s.specOf(bare.Id).History, 1, "a fork with no body carries its messages")
}

// ends are the ways a session ends before it is carried on: stopped, so it is carried on from
// its row, or ended here, so it is from the spec it ran on.
func (s *ForkRunningSuite) ends() map[string]func(id string) {
	return map[string]func(id string){
		"stopped": func(id string) {
			s.Require().Eventually(func() bool {
				_, err := s.store.Sessions.Get(context.Background(), s.customerID(), id)
				return err == nil
			}, settleFor, 20*time.Millisecond)
			_, err := s.manager.Close(id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
			s.Require().NoError(err)
			s.Require().NoError(s.manager.FlushRecords(context.Background()))
		},
		"ended here": s.endHere,
	}
}

func (s *ForkRunningSuite) callers() map[string]*testClient {
	return map[string]*testClient{
		"user": s.client, "guest": s.guestClient, "anonymous": s.anonymousClient, "backend": s.serverClient,
		"backend for a user": s.serverClient.actingFor(s.data.createUser()),
	}
}

// carryOn ends the session id and has client carry it on, held running for the test.
func (s *ForkRunningSuite) carryOn(client *testClient, id string, end func(id string)) {
	end(id)
	s.Require().Equal(http.StatusOK, client.do(http.MethodPost, "/v1/agents/sessions/"+id+"/resume", ResumeSessionRequest{}, nil))
	client.opens("/v1/agents/sessions/" + id + "/events")
	s.Require().False(s.specOf(id).Reopened.IsZero(), "it runs, carried on")
}

func (s *ForkRunningSuite) fork(client *testClient, id string, request ForkSessionRequest) Session {
	status, body := client.call(http.MethodPost, "/v1/agents/sessions/"+id+"/fork", request)
	s.Require().Equal(http.StatusCreated, status, string(body))
	var forked Session
	s.Require().NoError(json.Unmarshal(body, &forked))
	return forked
}

// backendSpec is a text conversation the router opens for the app's backend.
func (s *ForkRunningSuite) backendSpec() session.Spec {
	return session.Spec{CustomerID: s.customerID(), Text: true, PersistConversation: true, CallerKind: auth.KindServer,
		Attribution: routing.Attribution{APIKey: s.app.key}}
}

func (s *ForkRunningSuite) running(spec session.Spec) string {
	created, err := s.manager.Create(context.Background(), spec)
	s.Require().NoError(err)
	return created.ID()
}

func (s *ForkRunningSuite) specOf(id string) session.Spec {
	held, ok := s.manager.Get(id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(ok, "session %s is held", id)
	s.Require().NotEqual(session.Ended, held.State())
	return held.Spec()
}

// answeredTurn asks a session a question and returns the id of its answer, once recorded.
func (s *ForkRunningSuite) answeredTurn(client *testClient, id, text string) string {
	watching := client.opens("/v1/agents/sessions/" + id + "/events")
	respondIn(&s.RouterSuite, client, id, text)
	s.await(watching, "responded")
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
	exchanges, err := s.store.Responses.Exchanges(context.Background(), s.customerID(), id, "")
	s.Require().NoError(err)
	s.Require().NotEmpty(exchanges)
	return exchanges[len(exchanges)-1].ResponseID
}

// recalled is how many questions a fork was handed of what its parent recorded.
func (s *ForkRunningSuite) recalled(id string) int {
	s.Require().NotNil(s.specOf(id).Recall)
	asked := 0
	for _, message := range s.specOf(id).Recall.Messages {
		if message.Role == llm.User {
			asked++
		}
	}
	return asked
}
