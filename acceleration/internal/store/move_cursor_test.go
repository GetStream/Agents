//go:build integration

package store

import "time"

func (s *StoreSuite) TestAMoveCursorOnlyMovesForward() {
	none, err := s.store.MoveCursors.Get(s.ctx, "acme", "eu")
	s.Require().NoError(err)
	s.Nil(none)

	five, three := int64(5), int64(3)
	s.Require().NoError(s.store.MoveCursors.Upsert(s.ctx, &MoveCursor{CustomerID: "acme", Source: "eu", Cursor: &five, ReadAt: s.base}))
	s.Require().NoError(s.store.MoveCursors.Upsert(s.ctx, &MoveCursor{CustomerID: "acme", Source: "eu", Cursor: &three, ReadAt: s.base.Add(-time.Hour)}))

	saved, err := s.store.MoveCursors.Get(s.ctx, "acme", "eu")
	s.Require().NoError(err)
	s.Require().NotNil(saved.Cursor)
	s.Equal(int64(5), *saved.Cursor)
	other, err := s.store.MoveCursors.Get(s.ctx, "globex", "eu")
	s.Require().NoError(err)
	s.Nil(other, "a cursor is only its own customer's")
}

func (s *StoreSuite) TestAnInvalidMoveCursorIsNotWritten() {
	err := s.store.MoveCursors.Upsert(s.ctx, &MoveCursor{CustomerID: "acme", ReadAt: s.base})
	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("source", invalid.Field)
}

func (s *StoreSuite) TestMoveCursorQueryBudgets() {
	five := int64(5)
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.MoveCursors.Upsert(s.ctx, &MoveCursor{CustomerID: "acme", Source: "eu", Cursor: &five, ReadAt: s.base}))
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.MoveCursors.Get(s.ctx, "acme", "eu")
		s.Require().NoError(err)
	}))
}
