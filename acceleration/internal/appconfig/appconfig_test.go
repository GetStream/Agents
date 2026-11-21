//go:build integration

package appconfig

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// settleFor is how long an invalidation has to reach the other node. Redis pushes it as
// soon as the key is deleted, so this is generous rather than expected.
const settleFor = 2 * time.Second

// AppConfigSuite runs two stores over one Postgres and one Redis, which is what two
// replicas of the router are. A change made through one has to be what the other reads,
// because the alternative is an agent that follows old instructions somewhere for an hour.
type AppConfigSuite struct {
	suite.Suite
	ctx   context.Context
	db    *store.Store
	first *Store
	other *Store
}

func TestAppConfigSuite(t *testing.T) {
	suite.Run(t, new(AppConfigSuite))
}

func (s *AppConfigSuite) SetupSuite() {
	dsn, redisAddr := os.Getenv("ROUTER_POSTGRES_DSN"), os.Getenv("ROUTER_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN and ROUTER_REDIS_ADDR must be set")
	}
	s.ctx = context.Background()

	db, err := store.Open(dsn)
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.db = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })

	logger := slog.New(slog.DiscardHandler)
	s.first = s.node(redisAddr, logger)
	s.other = s.node(redisAddr, logger)
}

func (s *AppConfigSuite) node(redisAddr string, logger *slog.Logger) *Store {
	node, err := New(Options{Store: s.db, Address: redisAddr, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(node.Close)
	return node
}

// app writes an organization and an app, and returns the app id.
func (s *AppConfigSuite) app() string {
	stamp := time.Now().UnixNano()
	organization := store.Organization{Name: fmt.Sprintf("org-%d", stamp)}
	s.Require().NoError(s.db.Organizations.Insert(s.ctx, &organization))

	app := store.App{OrganizationID: organization.ID, Name: fmt.Sprintf("app-%d", stamp)}
	s.Require().NoError(s.db.Apps.Insert(s.ctx, &app))
	return app.ID
}

func (s *AppConfigSuite) TestCache() {
	s.Run("a save whose request is cancelled right after the commit still drops the old value", func() {
		name := key("policy", "app", fmt.Sprintf("app-%d", time.Now().UnixNano()))
		for _, node := range []*Store{s.first, s.other} {
			_, err := read(s.ctx, node, name, func(context.Context) (string, error) {
				return "before the save", nil
			})
			s.Require().NoError(err)
		}

		cancelled, cancel := context.WithCancel(s.ctx)
		cancel()
		s.first.forget(cancelled, name)

		for _, node := range []*Store{s.first, s.other} {
			s.Eventually(func() bool {
				now, err := read(s.ctx, node, name, func(context.Context) (string, error) {
					return "after the save", nil
				})
				return err == nil && now == "after the save"
			}, settleFor, 10*time.Millisecond, "a node still reads the value from before the save")
		}
	})

	s.Run("a read that loaded before a save never puts what it loaded back after it", func() {
		// One node is reading a value from Postgres while a save deletes it, and a second
		// read of the same value on that node is waiting. Neither may leave the old value in
		// Redis, where every node would read it until it expires.
		name := key("policy", "app", fmt.Sprintf("app-%d", time.Now().UnixNano()))
		loading, release, older := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(older)
			_, _ = read(context.Background(), s.first, name, func(context.Context) (string, error) {
				close(loading)
				<-release
				return "before the save", nil
			})
		}()
		<-loading
		s.first.forget(s.ctx, name)

		newer, second := make(chan struct{}), make(chan string, 1)
		go func() {
			got, _ := read(context.Background(), s.first, name, func(context.Context) (string, error) {
				close(newer)
				<-older
				return "after the save", nil
			})
			second <- got
		}()
		// The second read may wait for the first rather than load at all, so this waits on
		// it only for as long as it takes to start loading when it does.
		select {
		case <-newer:
		case <-time.After(200 * time.Millisecond):
		}
		close(release)
		s.Equal("after the save", <-second)

		for _, node := range []*Store{s.first, s.other} {
			now, err := read(s.ctx, node, name, func(context.Context) (string, error) {
				return "after the save", nil
			})
			s.Require().NoError(err)
			s.Equal("after the save", now)
		}
	})
}

func (s *AppConfigSuite) TestAgentConfig() {
	s.Run("a renamed config is no longer found under the name it had", func() {
		appID := s.app()

		config := store.AgentConfig{CustomerID: appID, Name: "support", Mode: "text"}
		s.Require().NoError(s.first.CreateAgentConfig(s.ctx, &config))

		found, exists, err := s.other.AgentConfigByName(s.ctx, appID, "support")
		s.Require().NoError(err)
		s.Require().True(exists)
		s.Equal(config.ID, found.ID)

		config.Name = "sales"
		s.Require().NoError(s.first.UpdateAgentConfig(s.ctx, &config))

		s.Eventually(func() bool {
			_, exists, err := s.other.AgentConfigByName(s.ctx, appID, "support")
			return err == nil && !exists
		}, settleFor, 10*time.Millisecond, "the other node still answers to the old name")

		renamed, exists, err := s.other.AgentConfigByName(s.ctx, appID, "sales")
		s.Require().NoError(err)
		s.Require().True(exists)
		s.Equal(config.ID, renamed.ID)
	})

	s.Run("an edited skill is read with its new instructions", func() {
		appID := s.app()

		config := store.AgentConfig{CustomerID: appID, Name: "refunds", Mode: "text"}
		s.Require().NoError(s.first.CreateAgentConfig(s.ctx, &config))

		skill := store.Skill{
			CustomerID: appID, ConfigID: config.ID,
			Name: "refund", Description: "issue a refund", Instructions: "ask first",
		}
		s.Require().NoError(s.first.CreateSkill(s.ctx, &skill))

		named, err := s.other.SkillsNamed(s.ctx, appID, config.ID, []string{"refund"})
		s.Require().NoError(err)
		s.Require().Len(named, 1)
		s.Equal("ask first", named[0].Instructions)

		skill.Instructions = "refund without asking"
		s.Require().NoError(s.first.UpdateSkill(s.ctx, &skill))

		s.Eventually(func() bool {
			named, err := s.other.SkillsNamed(s.ctx, appID, config.ID, []string{"refund"})
			return err == nil && len(named) == 1 && named[0].Instructions == "refund without asking"
		}, settleFor, 10*time.Millisecond, "the other node is still following the old instructions")
	})
}

func (s *AppConfigSuite) TestWithoutRedis() {
	s.Run("a deployment with no redis reads postgres and still sees its own writes", func() {
		plain, err := New(Options{Store: s.db, Logger: slog.New(slog.DiscardHandler)})
		s.Require().NoError(err)
		defer plain.Close()

		appID := s.app()
		config := store.AgentConfig{CustomerID: appID, Name: "support", Mode: "text"}
		s.Require().NoError(plain.CreateAgentConfig(s.ctx, &config))
		found, exists, err := plain.AgentConfigByName(s.ctx, appID, "support")
		s.Require().NoError(err)
		s.Require().True(exists)
		s.Equal(config.ID, found.ID)

		s.Require().NoError(plain.DeleteAgentConfig(s.ctx, appID, config.ID))
		_, exists, err = plain.AgentConfigByName(s.ctx, appID, "support")
		s.Require().NoError(err)
		s.False(exists)
	})
}
