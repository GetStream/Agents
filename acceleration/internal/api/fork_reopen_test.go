//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ForkReopenSuite covers what a fork remembers of its parent once it has ended and is carried
// on: the parent's turns before the fork point, which live in no channel of the fork's own.
//
// Every session runs on the recites model, which answers with every message it was handed, so
// an answer reads back the history the session had.
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
			parent := s.reciting(s.serverClient)
			first, _ := s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			s.answered(s.serverClient, parent.Id, "And invoice 8?")

			before, after := s.reopenedAnswers(s.serverClient, parent.Id, ForkSessionRequest{ResponseId: &first}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turn before the fork point comes first")
			s.keeps(before, after)
			s.NotContains(after, "And invoice 8?", "nothing the parent said after the fork point")
		})
	}
}

func (s *ForkReopenSuite) TestAWholeForkIsReopenedWithItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			s.answered(s.serverClient, parent.Id, "And invoice 8?")

			before, after := s.reopenedAnswers(s.serverClient, parent.Id, ForkSessionRequest{}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turns come first")
			s.keeps(before, after)
		})
	}
}

func (s *ForkReopenSuite) TestAWholeForkIsReopenedWithoutWhatItsParentSaidAfterIt() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
			_, before := s.answered(s.serverClient, forked.Id, "When was it paid?")
			s.answered(s.serverClient, parent.Id, "And invoice 8?")
			end(forked.Id)

			_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

			s.keeps(before, after)
			s.NotContains(after, "And invoice 8?", "what the parent said after the fork is not the fork's")
		})
	}
}

func (s *ForkReopenSuite) TestAForkIsReopenedWithItsParentsTurnsAfterTheParentRegeneratesTheForkPoint() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			first, _ := s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{ResponseId: &first})
			_, before := s.answered(s.serverClient, forked.Id, "When was it paid?")
			s.regenerated(parent.Id, first)
			end(forked.Id)

			_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turn as it was when the fork was made")
			s.keeps(before, after)
		})
	}
}

func (s *ForkReopenSuite) TestAForkWithoutMessagesIsReopenedWithoutItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{Messages: pointerTo(false)})
			_, before := s.answered(s.serverClient, forked.Id, "When was it paid?")
			s.Require().Equal("user: When was it paid?", before, "the fork starts with none of its parent's turns")
			end(forked.Id)

			_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

			s.NotContains(after, "Is invoice 7 paid?", "a reopen hands it none of them either")
		})
	}
}

func (s *ForkReopenSuite) TestAForkWhoseParentWasDeletedIsReopenedWithoutItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
			s.answered(s.serverClient, forked.Id, "When was it paid?")
			s.serverClient.deleteSession(parent.Id)
			end(forked.Id)

			_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

			s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
		})
	}
}

// The response a fork was cut at, gone from its parent: what was deleted is not handed to the
// model again, even by a fork that ended here and still holds what it was opened with.
func (s *ForkReopenSuite) TestAForkWhoseForkPointWasDeletedIsReopenedWithoutItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
			s.answered(s.serverClient, forked.Id, "When was it paid?")
			_, err := s.store.ShardDB().NewDelete().Table("agent_responses").
				Where("session_id = ?", parent.Id).Exec(context.Background())
			s.Require().NoError(err)
			end(forked.Id)

			_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

			s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
		})
	}
}

// A parent whose channel holds turns it never recorded as responses, as one from before
// responses were kept: a whole fork of it reads them from the channel and has no cut, so a fork
// that ended here goes on with what it was opened with.
func (s *ForkReopenSuite) TestAWholeForkOfAParentWithNoRecordedTurnKeepsWhatItWasOpenedWithHere() {
	parent := s.reciting(s.serverClient)
	s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
	_, err := s.store.ShardDB().NewDelete().Table("agent_responses").
		Where("session_id = ?", parent.Id).Exec(context.Background())
	s.Require().NoError(err)
	forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
	s.Require().Equal(store.ForkPoint{Messages: true}, s.forkPointOf(forked.Id), "the fork has no cut")
	_, before := s.answered(s.serverClient, forked.Id, "When was it paid?")
	s.Require().Equal("user: Is invoice 7 paid?", firstLine(before), "the fork starts from its parent's channel")
	s.endHere(forked.Id)

	_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

	s.keeps(before, after)
}

func (s *ForkReopenSuite) TestAForkMadeBeforeItsForkPointWasKeptIsReopenedAsBefore() {
	parent := s.reciting(s.serverClient)
	s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
	forked := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
	s.answered(s.serverClient, forked.Id, "When was it paid?")
	s.stopped(forked.Id)
	_, err := s.store.ShardDB().NewUpdate().Table("agent_sessions").Set("fork_point = NULL").
		Where("id = ?", forked.Id).Exec(context.Background())
	s.Require().NoError(err)

	_, after := s.answered(s.serverClient, forked.Id, "Who paid it?")

	s.Equal("user: When was it paid?", firstLine(after), "it is carried on from its own turns alone")
}

// A parent that kept no channel hands a fork what it recorded instead, at the fork and again
// when the fork is carried on.
func (s *ForkReopenSuite) TestAWholeForkOfAParentWithoutAChannelIsReopenedWithItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			spec := session.FromConfig(s.recitingConfigRow())
			spec.CustomerID, spec.Text, spec.CallerKind = s.customerID(), true, auth.KindServer
			spec.Attribution = routing.Attribution{APIKey: s.app.key}
			created, err := s.manager.Create(context.Background(), spec)
			s.Require().NoError(err)
			s.answered(s.serverClient, created.ID(), "Is invoice 7 paid?")

			before, after := s.reopenedAnswers(s.serverClient, created.ID(), ForkSessionRequest{}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's recorded turns come first")
			s.keeps(before, after)
		})
	}
}

// A fork of a fork is carried on with the history it was made with: the turns of the fork it
// was made from. Those are only that fork's own, since a whole fork is made from a read of its
// parent's channel, which holds none of the grandparent's turns; carrying those on as well is a
// change to how a fork is made, not to how it is carried on.
func (s *ForkReopenSuite) TestAForkOfAForkIsReopenedWithTheHistoryItWasMadeWith() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			parent := s.reciting(s.serverClient)
			s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
			first := s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{})
			s.answered(s.serverClient, first.Id, "When was it paid?")
			again := s.forkOf(s.serverClient, first.Id, ForkSessionRequest{})
			_, before := s.answered(s.serverClient, again.Id, "Who paid it?")
			s.Require().Equal("user: When was it paid?", firstLine(before), "it is made from its parent's own turns")
			end(again.Id)

			_, after := s.answered(s.serverClient, again.Id, "By card?")

			s.keeps(before, after)
		})
	}
}

// A backend forks one user's conversation at a response for another, who carries the fork on:
// the history it was made with is theirs, though the parent and its channel are not. (A whole
// fork of a channel the other user cannot read is refused when it is made.)
func (s *ForkReopenSuite) TestAForkCarriedOnByAnotherUserThanItsParentsKeepsItsParentsTurns() {
	for path, end := range s.ends() {
		s.Run(path, func() {
			owner, other := s.data.createUser(), s.serverClient.actingFor(s.data.createUser())
			parent := s.reciting(owner)
			first, _ := s.answered(owner, parent.Id, "Is invoice 7 paid?")

			before, after := s.reopenedAnswers(other, parent.Id, ForkSessionRequest{ResponseId: &first}, end)

			s.Equal("user: Is invoice 7 paid?", firstLine(after), "the parent's turns come first")
			s.keeps(before, after)
		})
	}
}

func (s *ForkReopenSuite) TestAWholeForkKeepsItsParentsLastResponseAsWhereItStops() {
	parent := s.reciting(s.serverClient)
	s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
	last, _ := s.answered(s.serverClient, parent.Id, "And invoice 8?")

	s.Equal(store.ForkPoint{Messages: true, ResponseID: last},
		s.forkPointOf(s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{}).Id))
}

func (s *ForkReopenSuite) TestAForkAtAResponseKeepsTheResponseItStopsAt() {
	parent := s.reciting(s.serverClient)
	first, _ := s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")
	s.answered(s.serverClient, parent.Id, "And invoice 8?")

	s.Equal(store.ForkPoint{Messages: true, ResponseID: first},
		s.forkPointOf(s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{ResponseId: &first}).Id))
}

func (s *ForkReopenSuite) TestAForkWithoutMessagesKeepsThatItCarriedNone() {
	parent := s.reciting(s.serverClient)
	s.answered(s.serverClient, parent.Id, "Is invoice 7 paid?")

	s.Equal(store.ForkPoint{Messages: false},
		s.forkPointOf(s.forkOf(s.serverClient, parent.Id, ForkSessionRequest{Messages: pointerTo(false)}).Id))
}

// reopenedAnswers has client fork parent, ask the fork one question, end it with end and ask it
// another, carrying it on. It returns both answers, each the history the fork was handed.
func (s *ForkReopenSuite) reopenedAnswers(client *testClient, parent string, request ForkSessionRequest, end func(id string)) (string, string) {
	forked := s.forkOf(client, parent, request)
	_, before := s.answered(client, forked.Id, "When was it paid?")
	s.Require().Contains(before, "user: Is invoice 7 paid?", "the fork starts from its parent's turns")

	end(forked.Id)
	_, after := s.answered(client, forked.Id, "Who paid it?")
	return before, after
}

// keeps checks that a fork carried on was handed the history it had before it ended, with its
// last answer and the new question after it.
func (s *ForkReopenSuite) keeps(before, after string) {
	s.True(strings.HasPrefix(after, before+"\n"),
		"a reopened fork has the history it had before it ended\nbefore:\n%s\nafter:\n%s", before, after)
}

func (s *ForkReopenSuite) reciting(client *testClient) Session {
	row := s.recitingConfigRow()
	return client.createSession(CreateSessionRequest{ConfigId: &row.ID})
}

// recitingConfigRow is a text config on the recites model.
func (s *ForkReopenSuite) recitingConfigRow() store.AgentConfig {
	agent := store.AgentConfig{
		CustomerID: s.customerID(), Name: "fork-reopen-" + s.utils.uuid(), Mode: store.AgentModeText,
		Models: store.AgentModels{LLM: "recites/recites-model"},
	}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &agent))
	return agent
}

// answered has client ask a session a question, carrying it on if it has ended, and returns
// the id of the recorded answer and its text.
func (s *ForkReopenSuite) answered(client *testClient, id, question string) (string, string) {
	watching := client.opens("/v1/agents/sessions/" + id + "/events")
	respondIn(&s.RouterSuite, client, id, question)
	text, _ := s.await(watching, "responded")["text"].(string)
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
	leaf, err := s.store.Responses.ActiveLeaf(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	s.Require().NotEmpty(leaf)
	return leaf, text
}

// regenerated asks the session for another answer to its response replaced, and waits for it.
func (s *ForkReopenSuite) regenerated(id, replaced string) {
	watching := s.serverClient.opens("/v1/agents/sessions/" + id + "/events")
	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/sessions/"+id+"/responses",
		CreateResponseRequest{RequestId: pointerTo(s.utils.uuid()), Replaces: &replaced})
	s.Require().Equal(http.StatusAccepted, status, string(body))
	s.await(watching, "responded")
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
	leaf, err := s.store.Responses.ActiveLeaf(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	s.Require().NotEqual(replaced, leaf, "the regenerated answer is the parent's now")
}

func (s *ForkReopenSuite) forkOf(client *testClient, parent string, request ForkSessionRequest) Session {
	var forked Session
	status, body := client.call(http.MethodPost, "/v1/agents/sessions/"+parent+"/fork", request)
	s.Require().Equal(http.StatusCreated, status, string(body))
	s.Require().NoError(json.Unmarshal(body, &forked))
	return forked
}

// forkPointOf is the fork point a fork's row keeps, once the row is written.
func (s *ForkReopenSuite) forkPointOf(id string) store.ForkPoint {
	var row store.AgentSession
	s.Require().Eventually(func() bool {
		var err error
		row, err = s.store.Sessions.Get(context.Background(), s.customerID(), id)
		return err == nil
	}, settleFor, 20*time.Millisecond)
	s.Require().NotNil(row.ForkPoint, "the fork's row keeps its fork point")
	return *row.ForkPoint
}

func (s *ForkReopenSuite) stopped(id string) {
	s.Require().Eventually(func() bool {
		_, err := s.store.Sessions.Get(context.Background(), s.customerID(), id)
		return err == nil
	}, settleFor, 20*time.Millisecond)
	_, err := s.manager.Close(id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().NoError(err)
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
}

// ends are the ways a fork ends before it is carried on: stopped, so it is carried on from its
// row, or ended here, so it is from the spec it ran on.
func (s *ForkReopenSuite) ends() map[string]func(id string) {
	return map[string]func(id string){"stopped": s.stopped, "ended here": s.endHere}
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}
