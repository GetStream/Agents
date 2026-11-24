//go:build integration

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// OptOutsCLISuite runs router opt-outs against Postgres.
type OptOutsCLISuite struct {
	suite.Suite
	ctx      context.Context
	settings config.Config
	store    *store.Store
}

func TestOptOutsCLISuite(t *testing.T) {
	suite.Run(t, new(OptOutsCLISuite))
}

func (s *OptOutsCLISuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	s.settings = config.Defaults()
	s.settings.Postgres.DSN = testenv.Database(dsn, "router_cli")
	opened, err := openStore(s.ctx, s.settings)
	s.Require().NoError(err)
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
}

// Catches an opt-out stored as 12025550123, before E.164 was enforced, still letting a dial
// to +12025550123 through after `router opt-outs normalize`, an email handle being rewritten
// or logged, and a number with no one reading going unlogged or logged with the number itself.
func (s *OptOutsCLISuite) TestNormalizeMakesAnOptOutStoredBeforeE164RefuseTheNumber() {
	customer, number, handle, unread := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, `INSERT INTO opt_outs (id, customer_id, recipient, channel, source, created_at)
		VALUES (?, ?, '12025550123', 'voice', 'api', now()), (?, ?, 'jane@icloud.com', 'imessage', 'keyword', now()),
		(?, ?, '(202) 555-0140', 'sms', 'api', now())`,
		number, customer, handle, customer, unread, customer)
	s.Require().NoError(err)
	s.Require().False(s.optedOut(customer, "+12025550123", "voice"))

	var logs bytes.Buffer
	s.Require().NoError(dispatchCommand("opt-outs", []string{"normalize"}, s.settings, slog.New(slog.NewTextHandler(&logs, nil))))

	s.True(s.optedOut(customer, "+12025550123", "voice"), "a dial to the number in E.164 is refused")
	s.True(s.optedOut(customer, "jane@icloud.com", "imessage"), "the iMessage handle is kept as it is")
	s.True(s.optedOut(customer, "(202) 555-0140", "sms"), "the national number is kept as written")
	s.Contains(logs.String(), "opt_out="+unread)
	s.NotContains(logs.String(), "555-0140", "logged by id, never the number")
	s.NotContains(logs.String(), "opt_out="+handle, "a handle is counted, not logged")
	s.NotContains(logs.String(), "opt_out="+number)
}

func (s *OptOutsCLISuite) optedOut(customer, recipient, channel string) bool {
	blocked, err := s.store.OptOuts.OptedOut(s.ctx, customer, recipient, channel)
	s.Require().NoError(err)
	return blocked
}
