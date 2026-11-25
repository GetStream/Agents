//go:build integration

package store

import "time"

// knowledgeDocument records text posted to a customer's knowledge base.
func (s *StoreSuite) knowledgeDocument(customerID, namespace, source string) KnowledgeDocument {
	document := KnowledgeDocument{CustomerID: customerID, Namespace: namespace, Source: source, Text: "the text of " + source,
		Passages: 3, State: KnowledgeDocumentIndexed}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &document))
	return document
}

func (s *StoreSuite) TestAKnowledgeDocumentIsOnlyItsOwnCustomers() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.knowledgeDocument("other-app", "docs", "faq.md")

	_, err := s.store.KnowledgeDocuments.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoKnowledgeDocument)
	listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)
	_, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoKnowledgeDocument)
	key, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	interrupted, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "other-app", mine.ID, mine.Namespace, mine.Source, 9)
	s.Require().NoError(err)
	s.True(interrupted, "it has a document of the source too")
	untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal(3, untouched.Passages)
	s.Equal(key, untouched.ObjectKey)
	deleted, err := s.store.KnowledgeDocuments.Delete(s.ctx, "other-app", mine.ID, key)
	s.Require().NoError(err)
	s.False(deleted)
	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &KnowledgeDocument{ID: mine.ID, CustomerID: "other-app"}, "")
	s.Require().NoError(err)
	s.False(finished)
}

func (s *StoreSuite) TestAnInvalidKnowledgeDocumentIsNotWritten() {
	var invalid *ValidationError
	document := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", State: "lost"}
	s.Require().ErrorAs(s.store.KnowledgeDocuments.Upsert(s.ctx, &document), &invalid)
	s.Equal("state", invalid.Field)
	file := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.pdf"}
	s.Require().ErrorAs(s.store.KnowledgeDocuments.Pend(s.ctx, &file), &invalid)
	s.Equal("object_key", invalid.Field)

	listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{})
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestPostingASourceAgainKeepsItsID() {
	first := s.knowledgeDocument("acme-app", "docs", "faq.md")
	again := s.knowledgeDocument("acme-app", "docs", "faq.md")

	s.Equal(first.ID, again.ID)
	listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{WithText: true})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal("the text of faq.md", listed[0].Text)
}

func (s *StoreSuite) TestAListingLeavesTheTextOutUnlessAskedFor() {
	s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.knowledgeDocument("acme-app", "other-docs", "terms.md")

	listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{Namespace: "docs"})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Empty(listed[0].Text)
	withText, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{Namespace: "docs", WithText: true})
	s.Require().NoError(err)
	s.Require().Len(withText, 1)
	s.Equal("the text of faq.md", withText[0].Text)
}

// A read of a file writes the row only while the row still waits for that file: an upload of
// the same source since then is the newer one.
func (s *StoreSuite) TestAFileReadOvertakenByANewerUploadLeavesTheRowAlone() {
	first := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.pdf", ObjectKey: "one"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &first))
	second := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.pdf", ObjectKey: "two"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &second))
	s.Equal(first.ID, second.ID)

	covered, err := s.store.KnowledgeDocuments.Cover(s.ctx, "acme-app", first.ID, "one", 9)
	s.Require().NoError(err)
	s.False(covered)
	failed, err := s.store.KnowledgeDocuments.Fail(s.ctx, "acme-app", first.ID, "one", "unreadable", true)
	s.Require().NoError(err)
	s.False(failed)
	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &KnowledgeDocument{ID: first.ID, CustomerID: "acme-app", Passages: 4}, "two")
	s.Require().NoError(err)
	s.True(finished)

	read, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", first.ID)
	s.Require().NoError(err)
	s.Equal(KnowledgeDocumentIndexed, read.State)
	s.Equal(4, read.Passages)
	s.Empty(read.ObjectKey)
}

func (s *StoreSuite) TestDeletingAKnowledgeBaseForgetsItsDocumentsAndPages() {
	s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.knowledgeDocument("acme-app", "docs", "terms.md")
	s.knowledgeDocument("acme-app", "other-docs", "kept.md")
	s.knowledgeURL("acme-app", "https://acme.example/faq", 0)

	var documents, pages int64
	var left bool
	s.Equal(10, s.countQueries(func() {
		keys, err := s.store.KnowledgeDocuments.TakeBase(s.ctx, "acme-app", "docs")
		s.Require().NoError(err)
		pages, err = s.store.KnowledgeURLs.DeleteBase(s.ctx, "acme-app", "docs")
		s.Require().NoError(err)
		documents, left, err = s.store.KnowledgeDocuments.DeleteBase(s.ctx, "acme-app", "docs", keys)
		s.Require().NoError(err)
	}), "begin, lock, take and commit, the pages, begin, lock, delete, look and commit")
	s.False(left)
	s.Equal(int64(2), documents)
	s.Equal(int64(1), pages)
	listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal("kept.md", listed[0].Source)
	urls, err := s.store.KnowledgeURLs.Select(s.ctx, "acme-app", KnowledgeURLFilter{})
	s.Require().NoError(err)
	s.Empty(urls)
}

// A document written since, by a post, an upload or a read that told the removal, is left to be
// taken again, and one another removal holds goes with the base.
func (s *StoreSuite) TestDeletingAKnowledgeBaseLeavesTheDocumentsItNoLongerHolds() {
	for _, source := range []string{"gone.md", "posted.md", "uploaded.md", "read.md", "removed.md"} {
		s.knowledgeDocument("acme-app", "docs", source)
	}
	waiting := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "waiting.md", ObjectKey: "acme-app/one/waiting.md"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &waiting))
	other, elsewhere := s.knowledgeDocument("other-app", "docs", "gone.md"), s.knowledgeDocument("acme-app", "other-docs", "gone.md")
	removed := s.knowledgeDocument("acme-app", "docs", "removed.md")
	removal, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", removed.ID)
	s.Require().NoError(err)

	keys, err := s.store.KnowledgeDocuments.TakeBase(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.Len(keys, 3)
	s.Contains(keys, removal)
	s.Contains(keys, "acme-app/one/waiting.md")
	taken, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{Namespace: "docs"})
	s.Require().NoError(err)
	listed := map[string]string{}
	for _, document := range taken {
		listed[document.Source] = document.State + " " + document.Error
	}
	s.Equal(map[string]string{"gone.md": "indexed ", "posted.md": "indexed ", "uploaded.md": "indexed ", "read.md": "indexed ",
		"waiting.md": "pending ", "removed.md": "failed Its removal did not finish. Delete it again."}, listed)
	for _, document := range []KnowledgeDocument{other, elsewhere} {
		untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, document.CustomerID, document.ID)
		s.Require().NoError(err)
		s.Equal(document.ObjectKey, untouched.ObjectKey)
		s.Equal("indexed", untouched.State)
	}

	s.knowledgeDocument("acme-app", "docs", "posted.md")
	upload := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "uploaded.md", ObjectKey: "acme-app/one/uploaded.md"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &upload))
	_, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", newID(), "docs", "read.md", 3)
	s.Require().NoError(err)

	later := s.knowledgeDocument("acme-app", "docs", "later.md")
	held, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", later.ID)
	s.Require().NoError(err)
	posting := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "posting.md", Passages: 2, State: KnowledgeDocumentPending}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &posting))

	failing := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "failing.md", State: KnowledgeDocumentFailed, Error: "why"}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &failing))

	documents, left, err := s.store.KnowledgeDocuments.DeleteBase(s.ctx, "acme-app", "docs", keys)
	s.Require().NoError(err)
	s.Equal(int64(2), documents)
	s.True(left)
	kept, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{Namespace: "docs"})
	s.Require().NoError(err)
	states, errs := map[string]string{}, map[string]string{}
	for _, document := range kept {
		states[document.Source], errs[document.Source] = document.State, document.Error
	}
	s.Equal(map[string]string{"posted.md": "failed", "uploaded.md": "pending", "read.md": "failed", "posting.md": "pending",
		"later.md": "failed", "failing.md": "failed", "waiting.md": "failed"}, states)
	let, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", waiting.ID)
	s.Require().NoError(err)
	s.True(let.BeingRemoved(), "its file's read may have written since the base was dropped")
	later, err = s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", later.ID)
	s.Require().NoError(err)
	s.Equal(held, later.ObjectKey, "the removal that took it still holds it")
	s.Equal(map[string]string{
		"posted.md": "Its removal did not finish. Delete it again.", "read.md": "Its removal did not finish. Delete it again.",
		"later.md": "Its removal did not finish. Delete it again.", "failing.md": "Its removal did not finish. Delete it again.",
		"waiting.md":  "Its removal did not finish. Delete it again.",
		"uploaded.md": "The file could not be read yet, and is tried again.", "posting.md": "",
	}, errs)
	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &posting, posting.ObjectKey)
	s.Require().NoError(err)
	s.False(finished, "its passages may have gone, so it is written again")
	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &upload, "acme-app/one/uploaded.md")
	s.Require().NoError(err)
	s.False(finished)
	for _, document := range []KnowledgeDocument{other, elsewhere} {
		untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, document.CustomerID, document.ID)
		s.Require().NoError(err)
		s.Equal(document.ObjectKey, untouched.ObjectKey)
		s.Equal("indexed", untouched.State)
	}
}

// What is left of a base while it is written is not for its delete to take again.
func (s *StoreSuite) TestAKnowledgeBaseIsDeletedOnceOnlyUnfinishedDocumentsAreLeft() {
	post := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", Passages: 2, State: KnowledgeDocumentPending}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &post))
	upload := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.pdf", ObjectKey: "acme-app/one/faq.pdf"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &upload))

	documents, left, err := s.store.KnowledgeDocuments.DeleteBase(s.ctx, "acme-app", "docs", nil)
	s.Require().NoError(err)
	s.Zero(documents)
	s.False(left)
}

// Each delete of a base changes its rows in an order of its own, so they take turns.
func (s *StoreSuite) TestTwoDeletesOfAKnowledgeBaseAtOnceBothFinish() {
	a, b := s.knowledgeDocument("acme-app", "docs", "a.md"), s.knowledgeDocument("acme-app", "docs", "b.md")
	keys := []string{"removing:one", "removing:two"}
	for i, document := range []KnowledgeDocument{a, b} {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE knowledge_documents SET object_key = ? WHERE id = ?", keys[i], document.ID)
		s.Require().NoError(err)
	}
	// The delete of one key keeps its row until the other's is deleted too, which then each
	// want the row the other holds.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, `CREATE FUNCTION meet() RETURNS trigger AS $$ BEGIN PERFORM pg_sleep(1); RETURN OLD; END $$ LANGUAGE plpgsql`)
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "DROP FUNCTION meet() CASCADE")
		s.NoError(err)
	}()
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "CREATE TRIGGER meet AFTER DELETE ON knowledge_documents FOR EACH ROW EXECUTE FUNCTION meet()")
	s.Require().NoError(err)

	deleted := make(chan error, 2)
	for _, key := range keys {
		go func() {
			_, _, err := s.store.KnowledgeDocuments.DeleteBase(s.ctx, "acme-app", "docs", []string{key})
			deleted <- err
		}()
	}

	s.NoError(<-deleted)
	s.NoError(<-deleted)
	left, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{})
	s.Require().NoError(err)
	s.Empty(left)
}

// A take that meets a delete holding a later row waits for it before it holds an earlier one.
func (s *StoreSuite) TestATakeOfAKnowledgeBaseWaitsForItsDelete() {
	s.knowledgeDocument("acme-app", "docs", "a.md")
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = tx.ExecContext(s.ctx, "SELECT pg_advisory_xact_lock(hashtextextended('acme-app/docs', 3771))")
	s.Require().NoError(err)
	taken := make(chan error, 1)
	go func() {
		_, err := s.store.KnowledgeDocuments.TakeBase(s.ctx, "acme-app", "docs")
		taken <- err
	}()
	s.Eventually(func() bool {
		var waiting bool
		err := s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_database d ON d.oid = l.database "+
			"WHERE d.datname = current_database() AND l.locktype = 'advisory' AND NOT l.granted)").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 5*time.Millisecond)
	s.Require().NoError(tx.Rollback())
	s.NoError(<-taken)
}

func (s *StoreSuite) TestAKnowledgeBaseIsTakenAndDeletedByItsCustomerAndName() {
	for _, named := range [][2]string{{"", "docs"}, {"acme-app", ""}} {
		_, err := s.store.KnowledgeDocuments.TakeBase(s.ctx, named[0], named[1])
		s.Error(err)
		_, _, err = s.store.KnowledgeDocuments.DeleteBase(s.ctx, named[0], named[1], nil)
		s.Error(err)
		_, err = s.store.KnowledgeURLs.DeleteBase(s.ctx, named[0], named[1])
		s.Error(err)
	}
}

func (s *StoreSuite) TestOnlyTheListedPagesOfAKnowledgeBaseGoWithIt() {
	for _, page := range []KnowledgeURL{
		{CustomerID: "acme-app", Namespace: "docs", URL: "https://acme.example/faq"},
		{CustomerID: "acme-app", Namespace: "docs", URL: "https://acme.example/gone"},
		{CustomerID: "acme-app", Namespace: "other-docs", URL: "https://acme.example/faq"},
		{CustomerID: "other-app", Namespace: "docs", URL: "https://acme.example/faq"},
	} {
		s.Require().NoError(s.store.KnowledgeURLs.Insert(s.ctx, &page))
		if page.URL == "https://acme.example/gone" {
			s.Require().NoError(s.store.KnowledgeURLs.Delete(s.ctx, "acme-app", page.ID))
		}
	}

	marked, err := s.store.KnowledgeURLs.DeleteBase(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.Equal(int64(1), marked)
	for customer, kept := range map[string]int{"acme-app": 1, "other-app": 1} {
		pages, err := s.store.KnowledgeURLs.Select(s.ctx, customer, KnowledgeURLFilter{})
		s.Require().NoError(err)
		s.Len(pages, kept, customer)
	}
}

func (s *StoreSuite) TestAPostThatLeavesAnotherKeyIsNotFinishedOver() {
	first, second := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "faq.md")
	key := first.ObjectKey
	first.ObjectKey = NewPost()
	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &first, key)
	s.Require().NoError(err)
	s.True(finished)

	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &second, second.ObjectKey)
	s.Require().NoError(err)
	s.False(finished)
}

func (s *StoreSuite) TestOnlyADocumentMadeSinceAReadsWasRemovedIsFailedByTheRead() {
	mine, other := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "hours.md")
	_, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, "docs", "faq.md", 8)
	s.Require().NoError(err)
	its, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal("indexed", its.State)

	_, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", newID(), "docs", "faq.md", 8)
	s.Require().NoError(err)
	failed, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal("failed", failed.State)
	s.Equal("The passages may not all be its own: send the document again or delete it.", failed.Error)
	untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", other.ID)
	s.Require().NoError(err)
	s.Equal("indexed", untouched.State)
}

// A post that failed says why on the row another post of its source shares.
func (s *StoreSuite) TestAPostFinishesOverTheErrorOfAnother() {
	failing := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", State: KnowledgeDocumentFailed, Error: "why"}
	post := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &failing))

	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &post, post.ObjectKey)
	s.Require().NoError(err)
	s.True(finished)
}

// The passages an overtaken read wrote may be among a post's that has not finished.
func (s *StoreSuite) TestAPostAnOvertakenReadWroteOverDoesNotFinish() {
	post := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", Passages: 2, State: KnowledgeDocumentPending}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &post))
	_, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", newID(), "docs", "faq.md", 8)
	s.Require().NoError(err)

	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &post, post.ObjectKey)
	s.Require().NoError(err)
	s.False(finished)
	again := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", Passages: 2, State: KnowledgeDocumentPending}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &again))
	s.Equal(8, again.Covered, "the read's are the post's to remove")
	s.Equal(2, again.Passages)
	shared := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", Passages: 2, State: KnowledgeDocumentPending}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &shared))
	s.Equal(8, shared.Covered)
	s.Equal(2, shared.Passages)
	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &again, again.ObjectKey)
	s.Require().NoError(err)
	s.True(finished)
}

func (s *StoreSuite) TestAFileWhosePassagesWereMetDoesNotFinishUntilTheyAreWrittenAgain() {
	meet := map[string]func(KnowledgeDocument) error{
		"a removal that failed": func(document KnowledgeDocument) error {
			return s.store.KnowledgeDocuments.FailRemoval(s.ctx, "acme-app", document.ID)
		},
		"an older read": func(document KnowledgeDocument) error {
			_, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", newID(), "docs", document.Source, 2)
			return err
		},
	}
	for name, met := range meet {
		file := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: name, ObjectKey: "acme-app/one/" + name}
		s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &file))
		s.Require().NoError(met(file))
		waiting, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", file.ID)
		s.Require().NoError(err)
		s.Equal("pending", waiting.State, name)
		s.Equal("The file could not be read yet, and is tried again.", waiting.Error, name)
		finish := func() bool {
			finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &file, "acme-app/one/"+name)
			s.Require().NoError(err)
			return finished
		}

		s.False(finish(), name)
		covered, err := s.store.KnowledgeDocuments.Cover(s.ctx, "acme-app", file.ID, "acme-app/one/"+name, 2)
		s.Require().NoError(err)
		s.True(covered, name)
		s.True(finish(), name)
	}
}

func (s *StoreSuite) TestKnowledgeDocumentQueryBudgets() {
	for range 20 {
		s.knowledgeDocument("acme-app", "docs", newID()+".md")
	}
	file := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.pdf", ObjectKey: "one"}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &file)) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeDocuments.Cover(s.ctx, "acme-app", file.ID, "one", 5)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeDocuments.Fail(s.ctx, "acme-app", file.ID, "one", "slow", false)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeDocuments.Finish(s.ctx, &file, "one")
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.KnowledgeDocuments.Select(s.ctx, "acme-app", KnowledgeDocumentFilter{Namespace: "docs"})
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", file.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.knowledgeDocument("acme-app", "docs", "faq.pdf") }))
	var key string
	s.Equal(1, s.countQueries(func() {
		var err error
		key, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", file.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		deleted, err := s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", file.ID, key)
		s.Require().NoError(err)
		s.True(deleted)
	}))
}

func (s *StoreSuite) TestARemovalDeletesOnlyTheDocumentItStillHolds() {
	mine, other := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "hours.md")
	key, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)

	deleted, err := s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", other.ID, key)
	s.Require().NoError(err)
	s.False(deleted)
	untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", other.ID)
	s.Require().NoError(err)
	s.Equal(KnowledgeDocumentIndexed, untouched.State)

	interrupted, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, 3)
	s.Require().NoError(err)
	s.True(interrupted)
	deleted, err = s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", mine.ID, key)
	s.Require().NoError(err)
	s.False(deleted, "passages were written since")
	held, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.NotEqual(key, held.ObjectKey)
	s.Equal(KnowledgeDocumentFailed, held.State)

	key, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	deleted, err = s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", mine.ID, key)
	s.Require().NoError(err)
	s.True(deleted)
}

func (s *StoreSuite) TestARemovalTakenAfterAPostHoldsTheDocumentUnderAnotherKey() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	first, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.knowledgeDocument("acme-app", "docs", "faq.md")
	_, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)

	deleted, err := s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", mine.ID, first)
	s.Require().NoError(err)
	s.False(deleted)
}

func (s *StoreSuite) TestARemovalSaysWhenItTookTheDocument() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	before, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)

	_, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)

	taken, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.True(taken.UpdatedAt.After(before.UpdatedAt))
}

func (s *StoreSuite) TestOnlyTheDocumentWhoseRemovalFailedSaysSo() {
	mine, other := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "hours.md")
	before, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	waiting := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "manual.pdf", ObjectKey: "acme-app/one/manual.pdf"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &waiting))
	s.Require().NoError(s.store.KnowledgeDocuments.FailRemoval(s.ctx, "acme-app", waiting.ID))
	pending, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", waiting.ID)
	s.Require().NoError(err)
	s.Equal(KnowledgeDocumentPending, pending.State, "its file still waits to be read")

	s.Require().NoError(s.store.KnowledgeDocuments.FailRemoval(s.ctx, "other-app", mine.ID))
	untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal(KnowledgeDocumentIndexed, untouched.State)
	s.Require().NoError(s.store.KnowledgeDocuments.FailRemoval(s.ctx, "acme-app", mine.ID))

	failed, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Require().NoError(s.store.KnowledgeDocuments.FailRemoval(s.ctx, "acme-app", mine.ID))
	again, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal(failed.ObjectKey, again.ObjectKey, "another removal may hold it")
	s.Equal(KnowledgeDocumentFailed, failed.State)
	s.Equal("Its removal did not finish. Delete it again.", failed.Error)
	s.True(failed.UpdatedAt.After(before.UpdatedAt))
	untouched, err = s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", other.ID)
	s.Require().NoError(err)
	s.Equal(KnowledgeDocumentIndexed, untouched.State)
}

// Posts of one source share its row, so the last to finish is the one kept. A removal or an
// upload since a post began is not one of them.
func (s *StoreSuite) TestAPostFinishesOnlyWhileNoRemovalOrUploadTookItsDocument() {
	first, second := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "faq.md")
	for _, post := range []KnowledgeDocument{second, first, second} {
		post.Text, post.Bytes, post.ContentType = "the last text", 13, "text/plain"
		finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &post, post.ObjectKey)
		s.Require().NoError(err)
		s.True(finished)
	}
	kept, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", first.ID)
	s.Require().NoError(err)
	s.Equal("the last text", kept.Text)
	s.Equal(int64(13), kept.Bytes)
	s.Equal("text/plain", kept.ContentType)

	_, _, err = s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", first.ID)
	s.Require().NoError(err)
	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &first, first.ObjectKey)
	s.Require().NoError(err)
	s.False(finished)

	again := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.Equal(first.ID, again.ID)
	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &first, first.ObjectKey)
	s.Require().NoError(err)
	s.False(finished, "a removal took it since, though a later post has it now")
	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &again, again.ObjectKey)
	s.Require().NoError(err)
	s.True(finished)

	upload := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", ObjectKey: "acme-app/one/faq.md"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &upload))
	finished, err = s.store.KnowledgeDocuments.Finish(s.ctx, &again, again.ObjectKey)
	s.Require().NoError(err)
	s.False(finished)
}

func (s *StoreSuite) TestAPostOfFewerPassagesStillCountsTheOnesBefore() {
	document := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", Passages: 8, State: KnowledgeDocumentIndexed}
	s.Require().NoError(s.store.KnowledgeDocuments.Upsert(s.ctx, &document))
	s.knowledgeDocument("acme-app", "docs", "faq.md")

	_, passages, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", document.ID)
	s.Require().NoError(err)
	s.Equal(8, passages)
}

func (s *StoreSuite) TestAnInterruptedRemovalCountsThePassagesWritten() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	for _, written := range []int{8, 2} {
		_, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
		s.Require().NoError(err)
		interrupted, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, written)
		s.Require().NoError(err)
		s.True(interrupted)
	}

	_, passages, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal(8, passages)
}

// A read overtaken by a post or an upload has written passages too.
func (s *StoreSuite) TestAnOvertakenReadsPassagesAreCountedOnItsDocumentOnly() {
	elsewhere := s.knowledgeDocument("acme-app", "other-docs", "faq.md")
	mine, other := s.knowledgeDocument("acme-app", "docs", "faq.md"), s.knowledgeDocument("acme-app", "docs", "hours.md")
	kept, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, 8)
	s.Require().NoError(err)
	s.True(kept)

	counted, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.Equal(8, counted.Passages)
	s.Equal(mine.ObjectKey, counted.ObjectKey, "the post still holds it")
	s.Empty(counted.Error)
	for _, document := range []KnowledgeDocument{other, elsewhere} {
		untouched, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", document.ID)
		s.Require().NoError(err)
		s.Equal(3, untouched.Passages)
	}
	kept, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, "docs", "gone.md", 8)
	s.Require().NoError(err)
	s.False(kept, "it is gone")
}

func (s *StoreSuite) TestAnOvertakenReadIsCountedWhateverHoldsItsDocument() {
	for _, key := range []string{"", "acme-app/one/faq.md", "posting:x"} {
		mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE knowledge_documents SET object_key = ?, passages = 3 WHERE id = ?", key, mine.ID)
		s.Require().NoError(err)

		kept, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, 8)
		s.Require().NoError(err)
		s.True(kept, key)
		counted, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
		s.Require().NoError(err)
		s.Equal(8, counted.Passages, key)
		s.Equal(key, counted.ObjectKey)
	}
}

func (s *StoreSuite) TestEachInterruptHoldsTheDocumentUnderAnotherKey() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	_, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	_, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, 3)
	s.Require().NoError(err)
	key, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	_, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID, mine.Namespace, mine.Source, 3)
	s.Require().NoError(err)

	deleted, err := s.store.KnowledgeDocuments.Delete(s.ctx, "acme-app", mine.ID, key)
	s.Require().NoError(err)
	s.False(deleted)
}

func (s *StoreSuite) TestOnlyAnUploadedDocumentWaitsForAFile() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.False(mine.WaitsForFile())
	s.False((&KnowledgeDocument{}).WaitsForFile())
	_, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	taken, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.False(taken.WaitsForFile())

	upload := KnowledgeDocument{CustomerID: "acme-app", Namespace: "docs", Source: "faq.md", ObjectKey: "acme-app/one/faq.md"}
	s.Require().NoError(s.store.KnowledgeDocuments.Pend(s.ctx, &upload))
	s.True(upload.WaitsForFile())
}

func (s *StoreSuite) TestAPostDoesNotFinishOnceTheRemovalItOvertookFailed() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	_, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	post := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.Require().NoError(s.store.KnowledgeDocuments.FailRemoval(s.ctx, "acme-app", mine.ID))

	finished, err := s.store.KnowledgeDocuments.Finish(s.ctx, &post, post.ObjectKey)
	s.Require().NoError(err)
	s.False(finished)
	failed, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.True(failed.BeingRemoved())
}

func (s *StoreSuite) TestADocumentIsBeingRemovedOnlyOnceARemovalTookIt() {
	mine := s.knowledgeDocument("acme-app", "docs", "faq.md")
	s.False(mine.BeingRemoved())
	_, _, err := s.store.KnowledgeDocuments.Take(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)

	taken, err := s.store.KnowledgeDocuments.Get(s.ctx, "acme-app", mine.ID)
	s.Require().NoError(err)
	s.True(taken.BeingRemoved())
}
