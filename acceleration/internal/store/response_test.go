//go:build integration

package store

import (
	"fmt"
	"time"
)

// branchAnswered starts a finished response of session "one" on into, after parent and answering
// question's question: empty question is a question of its own.
func (s *StoreSuite) branchAnswered(into *Store, id, parent, question string, at time.Time) {
	s.Require().NoError(into.Responses.Insert(s.ctx, &AgentResponse{
		ID: id, SessionID: "one", CustomerID: "app", Said: id, CreatedAt: at,
		ParentResponseID: parent, QuestionResponseID: question,
	}))
	s.Require().NoError(into.Responses.Finish(s.ctx, "app", id, ResponseCompleted, "", at.Add(time.Second)))
}

// conversation is the ids of session "one"'s responses still in its conversation, oldest first.
func (s *StoreSuite) conversation() []string {
	responses, err := s.store.Responses.Select(s.ctx, "app", "one", ResponseFilter{})
	s.Require().NoError(err)
	ids := make([]string, 0, len(responses))
	for _, response := range responses {
		ids = append(ids, response.ID)
	}
	return ids
}

func responseIDs(responses []AgentResponse) []string {
	found := make([]string, 0, len(responses))
	for _, response := range responses {
		found = append(found, response.ID)
	}
	return found
}

// branchedAfterFirst is session "one" with a first question, then a question asked after it
// answered twice, and an edit of that question answered twice:
//
//	first ─┬─ asked, asked-again          (question asked)
//	       └─ edited, edited-again        (question edited)
func (s *StoreSuite) branchedAfterFirst() {
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "asked", "first", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "asked-again", "first", "asked", s.base.Add(2*time.Minute))
	s.branchAnswered(s.store, "edited", "first", "", s.base.Add(3*time.Minute))
	s.branchAnswered(s.store, "edited-again", "first", "edited", s.base.Add(4*time.Minute))
}

func (s *StoreSuite) TestARegenerationIsAnotherReplyToTheSameQuestion() {
	s.branchedAfterFirst()

	replies, err := s.store.Responses.Siblings(s.ctx, "app", "one", "asked-again", SiblingsOfReply)
	s.Require().NoError(err)
	s.Equal([]string{"asked", "asked-again"}, responseIDs(replies))
	index, count := SiblingPosition(replies, replies[1], SiblingsOfReply)
	s.Equal([2]int{2, 2}, [2]int{index, count})

	replies, err = s.store.Responses.Siblings(s.ctx, "app", "one", "edited", SiblingsOfReply)
	s.Require().NoError(err)
	s.Equal([]string{"edited", "edited-again"}, responseIDs(replies))
}

func (s *StoreSuite) TestAnEditIsAnotherQuestionHoweverOftenEachWasAnswered() {
	s.branchedAfterFirst()

	questions, err := s.store.Responses.Siblings(s.ctx, "app", "one", "edited-again", SiblingsOfQuestion)
	s.Require().NoError(err)
	s.Equal([]string{"asked", "edited"}, responseIDs(questions))

	regenerated, err := s.store.Responses.Get(s.ctx, "app", "one", "asked-again")
	s.Require().NoError(err)
	index, count := SiblingPosition(questions, regenerated, SiblingsOfQuestion)
	s.Equal([2]int{1, 2}, [2]int{index, count}, "a regeneration stands where its question does")
	edit, err := s.store.Responses.Get(s.ctx, "app", "one", "edited-again")
	s.Require().NoError(err)
	index, count = SiblingPosition(questions, edit, SiblingsOfQuestion)
	s.Equal([2]int{2, 2}, [2]int{index, count})
}

func (s *StoreSuite) TestTheFirstQuestionOfASessionHasAlternativesToo() {
	s.opened("one", "app", s.base, nil)
	// A row from before branches were kept has no question, and is no alternative to anything.
	_, err := s.store.shard.NewInsert().Model(&AgentResponse{
		ID: "legacy", SessionID: "one", CustomerID: "app", Status: ResponseCompleted, CreatedAt: s.base,
	}).Exec(s.ctx)
	s.Require().NoError(err)
	s.branchAnswered(s.store, "opening", "", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "reworded", "", "", s.base.Add(2*time.Minute))

	questions, err := s.store.Responses.Siblings(s.ctx, "app", "one", "reworded", SiblingsOfQuestion)
	s.Require().NoError(err)
	s.Equal([]string{"opening", "reworded"}, responseIDs(questions))

	legacy, err := s.store.Responses.Siblings(s.ctx, "app", "one", "legacy", SiblingsOfQuestion)
	s.Require().NoError(err)
	s.Equal([]string{"legacy"}, responseIDs(legacy))
}

func (s *StoreSuite) TestSiblingsOfSomethingElseAreRefused() {
	s.branchedAfterFirst()

	_, err := s.store.Responses.Siblings(s.ctx, "app", "one", "asked", "thread")
	s.ErrorContains(err, "siblings of")
}

func (s *StoreSuite) TestTheTailOfABranchEndsAtItsNewestResponseWhereverItIs() {
	// fork ─┬─ old ── old-later      (old-later answered last)
	//       └─ newer
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "fork", "", "", s.base)
	s.branchAnswered(s.store, "old", "fork", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "newer", "fork", "old", s.base.Add(2*time.Minute))
	s.branchAnswered(s.store, "old-later", "old", "", s.base.Add(3*time.Minute))

	tail, err := s.store.Responses.BranchTail(s.ctx, "app", "one", "fork")
	s.Require().NoError(err)
	s.Equal([]string{"fork", "old", "old-later"}, responseIDs(tail))

	tail, err = s.store.Responses.BranchTail(s.ctx, "app", "one", "newer")
	s.Require().NoError(err)
	s.Equal([]string{"newer"}, responseIDs(tail))
}

func (s *StoreSuite) TestMovingThePathRewindsTheOldBranchAndBringsBackTheNew() {
	// first ── second ── third        (rewound by a regenerate of second)
	//      └── second-again           (the conversation)
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "third", "second", "", s.base.Add(2*time.Minute))
	s.Require().NoError(s.store.Responses.Rewind(s.ctx, "app", "one", "first", s.base.Add(3*time.Minute)))
	s.branchAnswered(s.store, "second-again", "first", "second", s.base.Add(4*time.Minute))
	s.Require().Equal([]string{"first", "second-again"}, s.conversation())

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "third", s.base.Add(5*time.Minute)))
	s.Equal([]string{"first", "second", "third"}, s.conversation())

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "third", s.base.Add(6*time.Minute)))
	s.Equal([]string{"first", "second", "third"}, s.conversation(), "moving to the same path again changes nothing")

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "second-again", s.base.Add(7*time.Minute)))
	s.Equal([]string{"first", "second-again"}, s.conversation())
}

func (s *StoreSuite) TestMovingToAResponseInTheConversationCutsItShortThere() {
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "first", s.base.Add(2*time.Minute)))

	s.Equal([]string{"first"}, s.conversation())
}

// A switch finished after the person asked on takes off the conversation only what it found
// there when it began.
func (s *StoreSuite) TestMovingThePathUntilATimeLeavesTheTurnsAskedSinceOnTheConversation() {
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "asked-since", "second", "", s.base.Add(3*time.Minute))

	s.Require().NoError(s.store.Responses.MoveActivePathUntil(s.ctx, "app", "one", "first", s.base.Add(4*time.Minute), s.base.Add(2*time.Minute)))

	s.Equal([]string{"first", "asked-since"}, s.conversation())
}

func (s *StoreSuite) TestMovingThePathKeepsTurnsFromBeforeBranchesWereKept() {
	s.opened("one", "app", s.base, nil)
	_, err := s.store.shard.NewInsert().Model(&AgentResponse{
		ID: "legacy", SessionID: "one", CustomerID: "app", Status: ResponseCompleted, CreatedAt: s.base,
	}).Exec(s.ctx)
	s.Require().NoError(err)
	s.branchAnswered(s.store, "first", "legacy", "", s.base.Add(time.Minute))
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(2*time.Minute))
	s.Require().NoError(s.store.Responses.Rewind(s.ctx, "app", "one", "legacy", s.base.Add(3*time.Minute)))
	s.branchAnswered(s.store, "first-again", "legacy", "first", s.base.Add(4*time.Minute))

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "second", s.base.Add(5*time.Minute)))

	s.Equal([]string{"legacy", "first", "second"}, s.conversation())
}

func (s *StoreSuite) TestMovingToNoResponseRewindsTheWholeConversation() {
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "", s.base.Add(2*time.Minute)))

	s.Empty(s.conversation())
	leaf, err := s.store.Responses.ActiveLeaf(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Empty(leaf)
}

func (s *StoreSuite) TestTheActiveLeafIsTheNewestResponseOnTheConversation() {
	// first ── second             (rewound by a regenerate of second)
	//      └── second-again       (the conversation)
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))
	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "first", s.base.Add(2*time.Minute)))
	s.branchAnswered(s.store, "second-again", "first", "second", s.base.Add(3*time.Minute))

	leaf, err := s.store.Responses.ActiveLeaf(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal("second-again", leaf)

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "second", s.base.Add(4*time.Minute)))
	leaf, err = s.store.Responses.ActiveLeaf(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal("second", leaf, "an older branch brought back is the conversation's end")
	response, err := s.store.Responses.Get(s.ctx, "app", "one", "second-again")
	s.Require().NoError(err)
	s.Equal("first", response.ParentResponseID)
	s.Equal("second", response.QuestionResponseID)
	s.NotNil(response.RewoundAt)
}

func (s *StoreSuite) TestASessionWithNoResponsesHasNoActiveLeaf() {
	s.opened("one", "app", s.base, nil)

	leaf, err := s.store.Responses.ActiveLeaf(s.ctx, "app", "one")

	s.Require().NoError(err)
	s.Empty(leaf)
}

func (s *StoreSuite) TestMovingToAResponseTheSessionDoesNotHaveFails() {
	s.opened("one", "app", s.base, nil)

	err := s.store.Responses.MoveActivePath(s.ctx, "app", "one", "nowhere", s.base)
	s.ErrorIs(err, ErrUnknownResponse)
}

func (s *ShardedStoreSuite) TestMovingThePathMovesItInEveryShard() {
	// first and second were answered here; the session was carried on in the other shard,
	// where second was regenerated and answered on.
	s.opened("one", "app", s.base, nil)
	s.branchAnswered(s.store, "first", "", "", s.base)
	s.branchAnswered(s.store, "second", "first", "", s.base.Add(time.Minute))
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "one", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}))
	s.Require().NoError(s.other.Responses.Rewind(s.ctx, "app", "one", "first", s.base.Add(2*time.Minute)))
	s.branchAnswered(s.other, "second-again", "first", "second", s.base.Add(3*time.Minute))
	s.branchAnswered(s.other, "third-again", "second-again", "", s.base.Add(4*time.Minute))
	s.Require().Equal([]string{"first", "second-again", "third-again"}, s.conversation())

	s.Require().NoError(s.store.Responses.MoveActivePath(s.ctx, "app", "one", "second", s.base.Add(5*time.Minute)))
	s.Equal([]string{"first", "second"}, s.conversation())

	tail, err := s.other.Responses.BranchTail(s.ctx, "app", "one", "second-again")
	s.Require().NoError(err)
	s.Equal([]string{"second-again", "third-again"}, responseIDs(tail))
	s.Require().NoError(s.other.Responses.MoveActivePath(s.ctx, "app", "one", "third-again", s.base.Add(6*time.Minute)))
	s.Equal([]string{"first", "second-again", "third-again"}, s.conversation())

	questions, err := s.store.Responses.Siblings(s.ctx, "app", "one", "second-again", SiblingsOfReply)
	s.Require().NoError(err)
	s.Equal([]string{"second", "second-again"}, responseIDs(questions))
}

func (s *StoreSuite) TestResponseQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.opened("conversed", "app", at, func(row *AgentSession) { row.ConversationID = "conversation" })
	parent := ""
	for i := range 30 {
		id := fmt.Sprintf("response-%02d", i)
		s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: id, SessionID: "conversed",
			CustomerID: "app", Said: "question " + id, CreatedAt: at.Add(time.Duration(i) * time.Second),
			ParentResponseID: parent, Status: ResponseCompleted}))
		parent = id
	}
	items := make([]AgentResponseItem, 0, 30)
	for i := range 30 {
		items = append(items, AgentResponseItem{ResponseID: "response-29", SessionID: "conversed", Ordinal: i,
			Kind: "message", Text: "part", At: at.Add(time.Minute)})
	}
	retry := &AgentResponse{ID: "retried", SessionID: "conversed", CustomerID: "app", Said: "again",
		CreatedAt: at.Add(time.Hour), ParentResponseID: "response-28"}
	s.Require().NoError(s.store.ResponseItems.InsertMany(s.ctx, items))
	s.withinQueryBudgets([]queryBudget{
		{"Insert", 1, 0, func() error { return s.store.Responses.Insert(s.ctx, retry) }},
		{"Finish", 2, 0, func() error {
			return s.store.Responses.Finish(s.ctx, retry.CustomerID, retry.ID, ResponseCompleted, "", at.Add(time.Hour))
		}},
		{"Select", 0, 1, func() error {
			listed, err := s.store.Responses.Select(s.ctx, "app", "conversed", ResponseFilter{Limit: 10})
			s.Len(listed, 11)
			return err
		}},
		{"Get", 1, 0, func() error { _, err := s.store.Responses.Get(s.ctx, "app", "conversed", retry.ID); return err }},
		{"Exchanges", 0, 2, func() error { _, err := s.store.Responses.Exchanges(s.ctx, "app", "conversed", ""); return err }},
		{"Replies", 0, 1, func() error {
			_, err := s.store.Responses.Replies(s.ctx, "app", "conversation", []string{"turn-1"})
			return err
		}},
		{"Opening", 0, 1, func() error { _, err := s.store.Responses.Opening(s.ctx, "app", "conversation"); return err }},
		{"Siblings", 1, 1, func() error {
			_, err := s.store.Responses.Siblings(s.ctx, "app", "conversed", retry.ID, SiblingsOfReply)
			return err
		}},
		{"QuestionsAt", 0, 1, func() error {
			_, err := s.store.Responses.QuestionsAt(s.ctx, "app", "conversed", "response-28")
			return err
		}},
		{"BranchTail", 0, 1, func() error {
			_, err := s.store.Responses.BranchTail(s.ctx, "app", "conversed", "response-10")
			return err
		}},
		{"ActiveChild", 0, 1, func() error {
			_, _, err := s.store.Responses.ActiveChild(s.ctx, "app", "conversed", "response-28")
			return err
		}},
		{"ActiveLeaf", 0, 1, func() error { _, err := s.store.Responses.ActiveLeaf(s.ctx, "app", "conversed"); return err }},
		{"MoveActivePath", 0, 2, func() error {
			return s.store.Responses.MoveActivePath(s.ctx, "app", "conversed", retry.ID, at.Add(2*time.Hour))
		}},
		{"MoveActivePathUntil", 0, 2, func() error {
			return s.store.Responses.MoveActivePathUntil(s.ctx, "app", "conversed", "response-29",
				at.Add(3*time.Hour), at.Add(4*time.Hour))
		}},
		{"Rewind", 1, 1, func() error {
			return s.store.Responses.Rewind(s.ctx, "app", "conversed", "response-20", at.Add(5*time.Hour))
		}},
	})
}

func (s *StoreSuite) TestAnInvalidResponseIsNotWritten() {
	s.opened("one", "app", time.Now(), nil)
	err := s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "invalid", SessionID: "one", CustomerID: "app", Status: "lost"})
	s.refusedUnwritten(err, "agent_responses", "id", "invalid")
}

func (s *StoreSuite) TestAnotherCustomersWriteToAResponseIdChangesNothing() {
	writes := []struct {
		name  string
		write func() error
		want  error
	}{
		{"Insert", func() error {
			return s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "one", SessionID: "theirs", CustomerID: "somebody-else", Said: "not app's"})
		}, ErrIDTaken},
		{"Finish", func() error {
			return s.store.Responses.Finish(s.ctx, "somebody-else", "one", ResponseFailed, "not app's", s.base.Add(time.Hour))
		}, nil},
	}
	for _, c := range writes {
		s.Run(c.name, func() {
			s.SetupTest()
			s.opened("one", "app", s.base, nil)
			s.opened("theirs", "somebody-else", s.base, nil)
			s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "one", SessionID: "one", CustomerID: "app", Said: "app's", CreatedAt: s.base}))
			before, err := s.store.Responses.Get(s.ctx, "app", "one", "one")
			s.Require().NoError(err)

			err = c.write()
			if c.want == nil {
				s.Require().NoError(err)
			} else {
				s.ErrorIs(err, c.want, "the writer is told the id is not its own")
			}

			after, err := s.store.Responses.Get(s.ctx, "app", "one", "one")
			s.Require().NoError(err)
			s.Equal(before, after, "app's response is as it was")
		})
	}
}

func (s *StoreSuite) TestAnotherCustomersFinishedTurnDoesNotMoveASessionsClock() {
	s.opened("one", "app", s.base, nil)
	before, err := s.store.Sessions.Get(s.ctx, "app", "one")
	s.Require().NoError(err)
	// The foreign key is the session id alone, so another customer's turn can name it.
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "theirs", SessionID: "one", CustomerID: "somebody-else", CreatedAt: s.base}))

	s.Require().NoError(s.store.Responses.Finish(s.ctx, "somebody-else", "theirs", ResponseCompleted, "", s.base.Add(time.Hour)))

	after, err := s.store.Sessions.Get(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal(before.LastResponseAt, after.LastResponseAt, "app's session was not answered")
	s.Equal(before.UpdatedAt, after.UpdatedAt)
}

func (s *StoreSuite) TestAnotherCustomersFinishOfAnAppsResponseDoesNotMoveItsSessionsClock() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "mine", SessionID: "one", CustomerID: "app", CreatedAt: s.base}))
	before, err := s.store.Sessions.Get(s.ctx, "app", "one")
	s.Require().NoError(err)

	s.Require().NoError(s.store.Responses.Finish(s.ctx, "somebody-else", "mine", ResponseCompleted, "", s.base.Add(time.Hour)))

	after, err := s.store.Sessions.Get(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal(before.LastResponseAt, after.LastResponseAt, "app's session was not answered")
	s.Equal(before.UpdatedAt, after.UpdatedAt)
}

func (s *StoreSuite) TestTheSameCustomerWritingARowAgainUnderItsIdIsNotAnError() {
	s.opened("one", "app", s.base, nil)
	writes := map[string]func() error{
		"Responses.Insert": func() error {
			return s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "first", SessionID: "one", CustomerID: "app", CreatedAt: s.base})
		},
		"BranchSwitches.Insert": func() error {
			return s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{
				ID: "switch", SessionID: "one", CustomerID: "app", Kind: SwitchEdit, TargetResponseID: "first",
				ForkMessageID: "m-one", HiddenFrom: s.base, CreatedAt: s.base,
			})
		},
		"ConversationDocuments.Insert": func() error {
			return s.store.ConversationDocuments.Insert(s.ctx, &ConversationDocument{
				ID: "document", CustomerID: "app", OwnerUserID: "jlahey", SessionID: "one",
				Name: "document.pdf", SHA256: "digest", Markdown: "text", CreatedAt: s.base,
			})
		},
	}
	for _, name := range []string{"Responses.Insert", "BranchSwitches.Insert", "ConversationDocuments.Insert"} {
		s.Run(name, func() {
			s.Require().NoError(writes[name]())
			s.NoError(writes[name](), "a retry of the customer's own row")
		})
	}
}

func (s *StoreSuite) TestAnOwnerScopedWriteWithoutACustomerIsRefused() {
	at := s.base.Add(time.Hour)
	writes := map[string]func() error{
		"Sessions.Close":          func() error { return s.store.Sessions.Close(s.ctx, "", "one", s.base, at, "hangup") },
		"Sessions.SetToolTimeout": func() error { return s.store.Sessions.SetToolTimeout(s.ctx, "", "one", "user", 5_000) },
		"Sessions.SawVideo":       func() error { return s.store.Sessions.SawVideo(s.ctx, "", "one") },
		"Sessions.Voiced":         func() error { return s.store.Sessions.Voiced(s.ctx, "", "one") },
		"Responses.Finish": func() error {
			return s.store.Responses.Finish(s.ctx, "", "one", ResponseCompleted, "", at)
		},
		"Calls.Finish":       func() error { return s.store.Calls.Finish(s.ctx, "", "one", at) },
		"Calls.ChangeModels": func() error { return s.store.Calls.ChangeModels(s.ctx, &Call{ID: "one", LLM: "openai/gpt-5"}) },
		"Recordings.Finish": func() error {
			return s.store.Recordings.Finish(s.ctx, "", "one", "deepgram", "nova-3", nil, "")
		},
		"ScheduledTaskRuns.FinishManual": func() error {
			return s.store.ScheduledTaskRuns.FinishManual(s.ctx, "", "one", "delivered", "", "", "")
		},
		"ConnectorConnections.PutClient": func() error {
			return s.store.ConnectorConnections.PutClient(s.ctx, "", "one", ConnectorConnectionClient{ClientID: "client"})
		},
		"ConnectorConnections.PutValidation": func() error {
			return s.store.ConnectorConnections.PutValidation(s.ctx, "", "one",
				ConnectorConnectionValidation{Status: "ok", CheckedAt: at})
		},
		"SimulationRuns.Finish": func() error { return s.store.SimulationRuns.Finish(s.ctx, SimulationRun{ID: "one"}) },
		"CampaignAttempts.End": func() error {
			return s.store.CampaignAttempts.End(s.ctx, "", "one", "answered", "", "", "", at, nil)
		},
	}
	for name, write := range writes {
		s.Run(name, func() {
			s.Error(write(), "a write naming no customer would match only rows with none")
		})
	}
}
