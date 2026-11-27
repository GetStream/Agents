//go:build integration

package store

import (
	"time"
)

// openedThere opens a session of customer in the other shard, as a router of it does.
func (s *ShardedStoreSuite) openedThere(id, customer string) {
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: customer, AgentName: "docs",
		UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}))
}

func (s *ShardedStoreSuite) TestASessionLivesInTheShardItOpenedIn() {
	s.opened("chat", "app", s.base, nil)

	home, err := s.other.SessionHomes.Of(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal("here", home)
	elsewhere, err := s.other.SessionHomes.Elsewhere(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal("here", elsewhere, "a router of the other shard hands it here")
	elsewhere, err = s.store.SessionHomes.Elsewhere(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Empty(elsewhere, "a router of its own shard answers it")
	home, err = s.other.SessionHomes.Of(s.ctx, "another-app", "chat")
	s.Require().NoError(err)
	s.Empty(home, "another customer's session of that id has no home")
}

func (s *ShardedStoreSuite) TestASessionKeepsTheShardItFirstOpenedIn() {
	s.opened("chat", "app", s.base, nil)
	// A late write from a router of the other shard, as one from before sessions stayed home.
	s.openedThere("chat", "app")

	home, err := s.store.SessionHomes.Of(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal("here", home)
}

func (s *ShardedStoreSuite) TestASessionOfAnotherShardIsNotReadHere() {
	s.openedThere("chat", "app")
	s.Require().NoError(s.other.Responses.Insert(s.ctx, &AgentResponse{ID: "turn", SessionID: "chat", CustomerID: "app", CreatedAt: s.base}))

	_, err := s.store.Sessions.Get(s.ctx, "app", "chat")
	s.ErrorIs(err, ErrNoSession, "its row is in the other shard alone")
	responses, err := s.store.Responses.Select(s.ctx, "app", "chat", ResponseFilter{})
	s.Require().NoError(err)
	s.Empty(responses)
	_, err = s.store.Responses.Get(s.ctx, "app", "chat", "turn")
	s.ErrorIs(err, ErrUnknownResponse)

	found, err := s.other.Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal("chat", found.ID)
}

func (s *ShardedStoreSuite) TestASessionIDAnotherShardHasIsTaken() {
	s.openedThere("chat", "another-app")

	taken, err := s.store.Sessions.Exists(s.ctx, "chat")
	s.Require().NoError(err)
	s.True(taken, "a session id is one session's whichever shard it lives in")
	taken, err = s.store.Sessions.Exists(s.ctx, "unused")
	s.Require().NoError(err)
	s.False(taken)
}

func (s *ShardedStoreSuite) TestADeletedSessionsIDIsFreeAgain() {
	s.opened("chat", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Delete(s.ctx, "app", "chat"))

	taken, err := s.other.Sessions.Exists(s.ctx, "chat")
	s.Require().NoError(err)
	s.False(taken)
	home, err := s.other.SessionHomes.Of(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Empty(home)
}

func (s *ShardedStoreSuite) TestShippingGivesTheSessionsFromBeforeHomesTheyKeep() {
	// Rows written before homes were, which no Upsert recorded.
	for _, id := range []string{"first", "second"} {
		_, err := s.store.shard.NewInsert().Model(&AgentSession{ID: id, CustomerID: "app", AgentName: "docs",
			UserID: "jlahey", CallerKind: "user", CreatedAt: s.base, UpdatedAt: s.base}).Exec(s.ctx)
		s.Require().NoError(err)
	}
	home, err := s.other.SessionHomes.Of(s.ctx, "app", "first")
	s.Require().NoError(err)
	s.Require().Empty(home)

	s.Require().NoError(s.store.ShipIndex(s.ctx, time.Now()))
	s.Require().NoError(s.other.ShipIndex(s.ctx, time.Now()), "the other shard ships its own sessions, none of these")

	for _, id := range []string{"first", "second"} {
		home, err := s.other.SessionHomes.Of(s.ctx, "app", id)
		s.Require().NoError(err)
		s.Equal("here", home, id)
	}
}

func (s *ShardedStoreSuite) TestATaskAboutASessionIsClaimedInTheShardItLivesInAlone() {
	s.openedThere("chat", "acme")
	due := s.base.Add(-time.Hour)
	s.task("about-chat", func(task *ScheduledTask) {
		task.SessionID = "chat"
		task.NextRunAt = &due
	})

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute, ShardClaim{Shard: "here"})
	s.Require().NoError(err)
	s.Empty(claimed, "overdue, but its session's rows are in the other shard")

	claimed, err = s.other.ScheduledTasks.Claim(s.ctx, 16, time.Minute, ShardClaim{Shard: "there"})
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal("about-chat", claimed[0].Task.ID)
}
