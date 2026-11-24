//go:build integration

package store

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

func (s *StoreSuite) routerConfig(customerID, name string) RouterConfig {
	config := RouterConfig{CustomerID: customerID, Name: name}
	s.Require().NoError(s.store.RouterConfigs.Insert(s.ctx, &config))
	return config
}

func (s *StoreSuite) TestANameNoRouterConfigHoldsIsCachedForAMinuteOnly() {
	s.routerConfig("acme-app", "fast")
	_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.Require().NoError(err)
	_, err = s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "nobody")
	s.Require().ErrorIs(err, ErrNoRouterConfig)

	s.Positive(s.expiryOf(routerConfigNameKey("acme-app", "nobody")), "the miss is cached")
	s.LessOrEqual(s.expiryOf(routerConfigNameKey("acme-app", "nobody")), time.Minute)
	s.Greater(s.expiryOf(routerConfigNameKey("acme-app", "fast")), 59*time.Minute, "a config found is still cached for the hour")
}

func (s *StoreSuite) TestARouterConfigIsOnlyItsOwnCustomers() {
	mine := s.routerConfig("acme-app", "fast")
	s.routerConfig("other-app", "fast")

	_, err := s.store.RouterConfigs.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoRouterConfig)
	listed, err := s.store.RouterConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)
	stolen := mine
	stolen.CustomerID = "other-app"
	s.ErrorIs(s.store.RouterConfigs.Update(s.ctx, &stolen), ErrNoRouterConfig)
	s.ErrorIs(s.store.RouterConfigs.Delete(s.ctx, "other-app", mine.ID), ErrNoRouterConfig)
}

func (s *StoreSuite) TestAnInvalidRouterConfigIsNotWritten() {
	err := s.store.RouterConfigs.Insert(s.ctx, &RouterConfig{CustomerID: "acme-app"})
	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("name", invalid.Field)

	listed, err := s.store.RouterConfigs.Select(s.ctx, "acme-app")
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestARenamedRouterConfigIsNoLongerFoundUnderItsOldName() {
	config := s.routerConfig("acme-app", "fast")
	_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.Require().NoError(err)
	_, err = s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "cheap")
	s.ErrorIs(err, ErrNoRouterConfig)

	config.Name = "cheap"
	s.Require().NoError(s.store.RouterConfigs.Update(s.ctx, &config))

	_, err = s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.ErrorIs(err, ErrNoRouterConfig)
	found, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "cheap")
	s.Require().NoError(err)
	s.Equal(config.ID, found.ID)

	s.Require().NoError(s.store.RouterConfigs.Delete(s.ctx, "acme-app", config.ID))
	_, err = s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "cheap")
	s.ErrorIs(err, ErrNoRouterConfig)
	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.ErrorIs(err, ErrNoRouterConfig)
}

func (s *StoreSuite) TestARenameARequestGaveUpOnIsNotCachedAgainByAReadBeforeItCommits() {
	config := s.routerConfig("acme-app", "fast")
	_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.Require().NoError(err)

	renamed := config
	renamed.Name = "cheap"
	err = s.readWhileWriting(s.lockingRow("router_configs", config.ID), func(ctx context.Context) error {
		return s.store.RouterConfigs.Update(ctx, &renamed)
	}, func() {
		_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
		s.Require().NoError(err, "the config is not renamed yet")
		_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
		s.Require().NoError(err)
	}, bun.Tx.Commit)

	_, getErr := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.ErrorIs(getErr, ErrNoRouterConfig)
	found, getErr := s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(getErr)
	s.Equal("cheap", found.Name)
	s.NoError(err, "the rename outlives the request that asked for it")
}

func (s *StoreSuite) TestAnInsertARequestGaveUpOnIsNotCachedAsAbsentByAReadBeforeItCommits() {
	_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.Require().ErrorIs(err, ErrNoRouterConfig)
	other := RouterConfig{ID: newID(), CustomerID: "acme-app", Name: "fast"}

	config := RouterConfig{CustomerID: "acme-app", Name: "fast"}
	err = s.readWhileWriting(func(holder bun.Tx) {
		_, err := holder.NewInsert().Model(&other).Exec(s.ctx)
		s.Require().NoError(err)
	}, func(ctx context.Context) error {
		return s.store.RouterConfigs.Insert(ctx, &config)
	}, func() {
		_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
		s.Require().ErrorIs(err, ErrNoRouterConfig, "the name is not taken yet")
	}, bun.Tx.Rollback)

	found, getErr := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
	s.Require().NoError(getErr, "the insert landed once the other let the name go")
	s.Equal(config.ID, found.ID)
	s.NoError(err, "the insert outlives the request that asked for it")
}

func (s *StoreSuite) TestRouterConfigQueryBudgets() {
	for range 20 {
		s.routerConfig("acme-app", "r"+newID())
	}
	config := RouterConfig{CustomerID: "acme-app", Name: "fast"}

	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.RouterConfigs.Insert(s.ctx, &config)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.RouterConfigs.Select(s.ctx, "acme-app")
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.RouterConfigs.GetByName(s.ctx, "acme-app", "fast")
		s.Require().NoError(err)
	}))
	config.Name = "cheap"
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.RouterConfigs.Update(s.ctx, &config)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	if s.store.cache != nil {
		for range 2 {
			_, err := s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
			s.Require().NoError(err)
		}
		s.Equal(0, s.countQueries(func() {
			s.Equal(0, s.countRedis(func() {
				_, err := s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
				s.Require().NoError(err)
			}))
		}))
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.RouterConfigs.Update(s.ctx, &config)) }),
			"forgetting the id and both names is one round trip")
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.RouterConfigs.Delete(s.ctx, "acme-app", config.ID)) }))
		return
	}
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.RouterConfigs.Delete(s.ctx, "acme-app", config.ID)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
}
