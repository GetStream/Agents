//go:build integration

package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ForkReopenSuite covers what a fork remembers of its parent once it has ended and is carried
// on: the parent's turns before the fork point, which live in no channel of the fork's own.
type ForkReopenSuite struct {
	RouterSuite
}

func TestForkReopenSuite(t *testing.T) {
	runSuite(t, new(ForkReopenSuite))
}

func (s *ForkReopenSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ForkReopenSuite) TestAForkAtAResponseIsReopenedWithItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
			first, _ := s.answered(parent.Id, "Is invoice 7 paid?")
			s.answered(parent.Id, "And invoice 8?")

			before, after := s.reopenedAnswers(parent.Id, ForkSessionRequest{ResponseId: &first}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turn before the fork point comes first")
			s.True(strings.HasPrefix(after, before+"\n"),
				"a reopened fork has the history it had before it ended\nbefore:\n%s\nafter:\n%s", before, after)
			s.NotContains(after, "And invoice 8?", "nothing the parent said after the fork point")
		})
	}
}

func (s *ForkReopenSuite) TestAWholeForkIsReopenedWithItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
			s.answered(parent.Id, "Is invoice 7 paid?")
			s.answered(parent.Id, "And invoice 8?")

			before, after := s.reopenedAnswers(parent.Id, ForkSessionRequest{}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turns come first")
			s.True(strings.HasPrefix(after, before+"\n"),
				"a reopened fork has the history it had before it ended\nbefore:\n%s\nafter:\n%s", before, after)
		})
	}
}

func (s *ForkReopenSuite) TestAForkWithoutMessagesIsReopenedWithoutItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
			s.answered(parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(parent.Id, ForkSessionRequest{Messages: pointerTo(false)})
			_, before := s.answered(forked.Id, "When was it paid?")
			s.Require().Equal("user: When was it paid?", before, "the fork starts with none of its parent's turns")

			end(forked.Id)
			_, after := s.answered(forked.Id, "Who paid it?")

			s.NotContains(after, "Is invoice 7 paid?", "a reopen hands it none of them either")
		})
	}
}

func (s *ForkReopenSuite) TestAForkWhoseParentWasDeletedIsReopenedWithoutItsParentsTurns() {
	parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
	s.answered(parent.Id, "Is invoice 7 paid?")
	forked := s.forkOf(parent.Id, ForkSessionRequest{})
	s.answered(forked.Id, "When was it paid?")
	s.serverClient.deleteSession(parent.Id)
	s.ends()["stopped"](forked.Id)

	_, after := s.answered(forked.Id, "Who paid it?")

	s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
}

func (s *ForkReopenSuite) TestAForkWhoseForkPointWasRewoundIsReopenedWithoutItsParentsTurns() {
	parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
	first, _ := s.answered(parent.Id, "Is invoice 7 paid?")
	forked := s.forkOf(parent.Id, ForkSessionRequest{ResponseId: &first})
	s.answered(forked.Id, "When was it paid?")
	s.ends()["stopped"](forked.Id)
	_, err := s.store.ShardDB().NewUpdate().Table("agent_responses").Set("rewound_at = now()").
		Where("id = ?", first).Exec(context.Background())
	s.Require().NoError(err)

	_, after := s.answered(forked.Id, "Who paid it?")

	s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
}

// A parent whose channel the fork can no longer read: its row is made to name another agent,
// whose channel this one is not, which the read refuses as it would any channel not the
// caller's to read.
func (s *ForkReopenSuite) TestAForkWhoseParentCannotBeReadIsReopenedWithoutItsParentsTurns() {
	parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
	s.answered(parent.Id, "Is invoice 7 paid?")
	forked := s.forkOf(parent.Id, ForkSessionRequest{})
	s.answered(forked.Id, "When was it paid?")
	s.ends()["stopped"](forked.Id)
	_, err := s.store.ShardDB().NewUpdate().Table("agent_sessions").Set("agent_id = ?", "agent-"+s.utils.uuid()).
		Where("id = ?", parent.Id).Exec(context.Background())
	s.Require().NoError(err)

	_, after := s.answered(forked.Id, "Who paid it?")

	s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
}

func (s *ForkReopenSuite) TestAForkMadeBeforeItsForkPointWasKeptIsReopenedAsBefore() {
	parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
	s.answered(parent.Id, "Is invoice 7 paid?")
	forked := s.forkOf(parent.Id, ForkSessionRequest{})
	s.answered(forked.Id, "When was it paid?")
	s.ends()["stopped"](forked.Id)
	_, err := s.store.ShardDB().NewUpdate().Table("agent_sessions").Set("fork_point = NULL").
		Where("id = ?", forked.Id).Exec(context.Background())
	s.Require().NoError(err)

	_, after := s.answered(forked.Id, "Who paid it?")

	s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
}

func (s *ForkReopenSuite) TestAForkKeepsWhereItsHistoryStops() {
	parent := s.serverClient.createSession(CreateSessionRequest{ConfigId: pointerTo(s.recitingConfig())})
	first, _ := s.answered(parent.Id, "Is invoice 7 paid?")

	for request, want := range map[*ForkSessionRequest]store.ForkPoint{
		{}:                           {Messages: true},
		{ResponseId: &first}:         {Messages: true, ResponseID: first},
		{Messages: pointerTo(false)}: {Messages: false},
		{Messages: pointerTo(true)}:  {Messages: true},
	} {
		forked := s.forkOf(parent.Id, *request)
		var row store.AgentSession
		s.Require().Eventually(func() bool {
			var err error
			row, err = s.store.Sessions.Get(context.Background(), s.customerID(), forked.Id)
			return err == nil
		}, settleFor, 20*time.Millisecond)
		s.Require().NotNil(row.ForkPoint)
		s.Equal(want, *row.ForkPoint)
	}
}

func (s *ForkReopenSuite) forkOf(parent string, request ForkSessionRequest) Session {
	var forked Session
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+parent+"/fork", request, &forked))
	return forked
}

// reopenedAnswers forks parent, asks the fork one question, ends it with end and asks it
// another, carrying it on. It returns both answers, each the history the fork was handed.
func (s *ForkReopenSuite) reopenedAnswers(parent string, request ForkSessionRequest, end func(id string)) (string, string) {
	forked := s.forkOf(parent, request)
	_, before := s.answered(forked.Id, "When was it paid?")
	s.Require().Equal("user: Is invoice 7 paid?", firstLine(before), "the fork starts from its parent's turns")

	end(forked.Id)
	_, after := s.answered(forked.Id, "Who paid it?")
	return before, after
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}

// recitingConfig is a text config on the recites model, which answers with every message it
// was handed, so an answer reads back the history the session had.
func (s *ForkReopenSuite) recitingConfig() string {
	agent := store.AgentConfig{
		CustomerID: s.customerID(), Name: "fork-reopen-" + s.utils.uuid(), Mode: store.AgentModeText,
		Models: store.AgentModels{LLM: "recites/recites-model"},
	}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &agent))
	return agent.ID
}

// answered asks a session a question, carrying it on if it has ended, and returns the id of the
// recorded answer and its text.
func (s *ForkReopenSuite) answered(id, question string) (string, string) {
	watching := s.serverClient.opens("/v1/agents/sessions/" + id + "/events")
	respondIn(&s.RouterSuite, s.serverClient, id, question)
	text, _ := s.await(watching, "responded")["text"].(string)
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
	exchanges, err := s.store.Responses.Exchanges(context.Background(), s.customerID(), id, "")
	s.Require().NoError(err)
	s.Require().NotEmpty(exchanges)
	return exchanges[len(exchanges)-1].ResponseID, text
}

// ends are the ways a fork ends before it is carried on: stopped, so it is carried on from its
// row, or ended here, so it is from the spec it ran on.
func (s *ForkReopenSuite) ends() map[string]func(id string) {
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
