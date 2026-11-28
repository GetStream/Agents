//go:build integration

package store

import (
	"time"
)

// knowledgeURL subscribes a customer's knowledge base to a page.
func (s *StoreSuite) knowledgeURL(customerID, url string, refreshHours int) KnowledgeURL {
	page := KnowledgeURL{CustomerID: customerID, Namespace: "docs", URL: url, RefreshHours: refreshHours}
	s.Require().NoError(s.store.KnowledgeURLs.Insert(s.ctx, &page))
	return page
}

func (s *StoreSuite) TestAKnowledgeURLIsOnlyItsOwnCustomers() {
	mine := s.knowledgeURL("acme-app", "https://acme.example/faq", 0)
	s.knowledgeURL("other-app", "https://acme.example/faq", 0)

	_, err := s.store.KnowledgeURLs.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoKnowledgeURL)
	listed, err := s.store.KnowledgeURLs.Select(s.ctx, "acme-app", KnowledgeURLFilter{Namespace: "docs"})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)
	stolen := mine
	stolen.CustomerID = "other-app"
	s.ErrorIs(s.store.KnowledgeURLs.Update(s.ctx, &stolen), ErrNoKnowledgeURL)
	s.ErrorIs(s.store.KnowledgeURLs.Delete(s.ctx, "other-app", mine.ID), ErrNoKnowledgeURL)
}

func (s *StoreSuite) TestAnInvalidKnowledgeURLIsNotWritten() {
	var invalid *ValidationError
	page := KnowledgeURL{CustomerID: "acme-app", Namespace: "docs", URL: "https://acme.example", RefreshHours: -1}
	s.Require().ErrorAs(s.store.KnowledgeURLs.Insert(s.ctx, &page), &invalid)
	s.Equal("refresh_hours", invalid.Field)

	listed, err := s.store.KnowledgeURLs.Select(s.ctx, "acme-app", KnowledgeURLFilter{})
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestASubscribedPageIsFoundByItsURLUntilItIsDeleted() {
	page := s.knowledgeURL("acme-app", "https://acme.example/faq", 0)

	found, err := s.store.KnowledgeURLs.GetByURL(s.ctx, "acme-app", "docs", "https://acme.example/faq")
	s.Require().NoError(err)
	s.Equal(page.ID, found.ID)
	_, err = s.store.KnowledgeURLs.GetByURL(s.ctx, "acme-app", "other-docs", "https://acme.example/faq")
	s.ErrorIs(err, ErrNoKnowledgeURL)

	s.Require().NoError(s.store.KnowledgeURLs.Delete(s.ctx, "acme-app", page.ID))
	_, err = s.store.KnowledgeURLs.GetByURL(s.ctx, "acme-app", "docs", "https://acme.example/faq")
	s.ErrorIs(err, ErrNoKnowledgeURL)
}

// A page is due once its interval has passed since it was last read. One never read, one
// never refreshed and one deleted are not.
func (s *StoreSuite) TestOnlyAPageWhoseIntervalHasPassedIsDue() {
	read := func(url string, refreshHours int, ago time.Duration) KnowledgeURL {
		page := s.knowledgeURL("acme-app", url, refreshHours)
		page.State = KnowledgeURLIndexed
		s.Require().NoError(s.store.KnowledgeURLs.Update(s.ctx, &page))
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE knowledge_urls SET updated_at = ? WHERE id = ?", time.Now().Add(-ago), page.ID)
		s.Require().NoError(err)
		return page
	}
	due := read("https://acme.example/due", 1, 2*time.Hour)
	read("https://acme.example/fresh", 24, 2*time.Hour)
	read("https://acme.example/never", 0, 48*time.Hour)
	deleted := read("https://acme.example/deleted", 1, 2*time.Hour)
	s.Require().NoError(s.store.KnowledgeURLs.Delete(s.ctx, "acme-app", deleted.ID))
	s.knowledgeURL("acme-app", "https://acme.example/pending", 1)

	s.Equal(1, s.countQueries(func() {
		pages, err := s.store.KnowledgeURLs.SelectDue(s.ctx, time.Now(), 10)
		s.Require().NoError(err)
		s.Require().Len(pages, 1)
		s.Equal(due.ID, pages[0].ID)
	}))
}

func (s *StoreSuite) TestKnowledgeURLQueryBudgets() {
	for range 20 {
		s.knowledgeURL("acme-app", "https://acme.example/"+newID(), 0)
	}
	page := KnowledgeURL{CustomerID: "acme-app", Namespace: "docs", URL: "https://acme.example/faq"}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.KnowledgeURLs.Insert(s.ctx, &page)) }))
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.KnowledgeURLs.Select(s.ctx, "acme-app", KnowledgeURLFilter{Namespace: "docs"})
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeURLs.Get(s.ctx, "acme-app", page.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeURLs.GetByURL(s.ctx, "acme-app", "docs", page.URL)
		s.Require().NoError(err)
	}))
	page.State = KnowledgeURLIndexed
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.KnowledgeURLs.Update(s.ctx, &page)) }))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.KnowledgeURLs.Delete(s.ctx, "acme-app", page.ID)) }))
}

// Any is what tells a session its agent reads web pages, so only a page that is still in that
// customer's knowledge base counts.
func (s *StoreSuite) TestAKnowledgeBaseIsFilledFromPagesWhileItHasOne() {
	page := s.knowledgeURL("acme-app", "https://acme.example/faq", 0)
	s.knowledgeURL("other-app", "https://other.example/faq", 0)

	for _, c := range []struct {
		customer, namespace string
		any                 bool
	}{
		{"acme-app", "docs", true},
		{"acme-app", "other-docs", false},
		{"third-app", "docs", false},
	} {
		found, err := s.store.KnowledgeURLs.Any(s.ctx, c.customer, c.namespace)
		s.Require().NoError(err)
		s.Equal(c.any, found, c.customer+"/"+c.namespace)
	}
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeURLs.Any(s.ctx, "acme-app", "docs")
		s.Require().NoError(err)
	}))

	s.Require().NoError(s.store.KnowledgeURLs.Delete(s.ctx, "acme-app", page.ID))
	found, err := s.store.KnowledgeURLs.Any(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.False(found, "a deleted page still counted")
	_, err = s.store.KnowledgeURLs.Any(s.ctx, "", "docs")
	s.ErrorIs(err, errCustomerRequired)
}
