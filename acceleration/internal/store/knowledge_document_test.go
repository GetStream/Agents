//go:build integration

package store

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
	interrupted, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "other-app", mine.ID)
	s.Require().NoError(err)
	s.False(interrupted)
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
	s.Equal(4, s.countQueries(func() {
		var err error
		documents, pages, err = s.store.KnowledgeDocuments.DeleteBase(s.ctx, "acme-app", "docs")
		s.Require().NoError(err)
	}), "begin, both writes and commit")
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
	interrupted, err := s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", other.ID)
	s.Require().NoError(err)
	s.False(interrupted, "nothing removes it")

	interrupted, err = s.store.KnowledgeDocuments.Interrupt(s.ctx, "acme-app", mine.ID)
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
