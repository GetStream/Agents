//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) TestStreamFallbackUsesAddUpPerCustomerAndKeepTheLatest() {
	s.Require().NoError(s.store.StreamFallbackUses.Add(s.ctx, &StreamFallbackUse{CustomerID: "app-a", Uses: 2, LastAt: s.base.Add(time.Hour)}))
	s.Require().NoError(s.store.StreamFallbackUses.Add(s.ctx, &StreamFallbackUse{CustomerID: "app-a", Uses: 3, LastAt: s.base}))
	s.Require().NoError(s.store.StreamFallbackUses.Add(s.ctx, &StreamFallbackUse{CustomerID: "app-b", Uses: 1, LastAt: s.base.Add(2 * time.Hour)}))
	s.Require().NoError(s.store.StreamFallbackUses.Add(s.ctx, &StreamFallbackUse{CustomerID: "app-c", Uses: 1, LastAt: s.base.Add(-time.Hour)}))

	uses, err := s.store.StreamFallbackUses.Select(s.ctx, s.base)
	s.Require().NoError(err)

	s.Require().Len(uses, 2, "app-c last used it before")
	s.Equal("app-b", uses[0].CustomerID, "the most recent first")
	s.Equal("app-a", uses[1].CustomerID)
	s.Equal(int64(5), uses[1].Uses)
	s.True(s.base.Add(time.Hour).Equal(uses[1].LastAt), "a late write of an earlier use does not move it back")
	s.True(s.base.Add(time.Hour).Equal(uses[1].FirstAt))
}

func (s *StoreSuite) TestStreamFallbackUseQueryBudgets() {
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.StreamFallbackUses.Add(s.ctx, &StreamFallbackUse{CustomerID: "app-a", Uses: 1, LastAt: s.base}))
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.StreamFallbackUses.Select(s.ctx, s.base)
		s.Require().NoError(err)
	}))
}
