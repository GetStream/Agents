//go:build integration

package channels

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// RecordConversationSuite is a person's first message on a router that lost the race to record
// its conversation: the message goes on in the conversation recorded first.
type RecordConversationSuite struct {
	suite.Suite
	ctx     context.Context
	store   *store.Store
	manager *session.Manager
	service *Service
}

func TestRecordConversationSuite(t *testing.T) { suite.Run(t, new(RecordConversationSuite)) }

func (s *RecordConversationSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	db, err := store.Open(testenv.Database(dsn, "channels_record"))
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
	s.store = db

	logger := slog.New(slog.DiscardHandler)
	config := routing.ModalityConfig{
		Providers: []routing.ProviderConfig{{Provider: "stub", Model: "stub-model", Languages: []string{"en"}, Realtime: true}},
		Aliases: map[string]routing.Alias{
			"en-low-latency": {Languages: []string{"en"}, RequireRealtime: true},
			"llm-flow":       {Languages: []string{"en"}, RequireRealtime: true},
		},
	}
	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return &llmtest.Thinker{Answer: "Sure."}, nil })
	reasoner, err := llmrouter.New(llmrouter.Options{Config: config, Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)
	chat := chattest.NewServer(s.T())
	apps := streamapp.NewClients(streamapp.NewDeployment(streamapp.DeploymentOptions{
		APIKey: "deploy-key", Secret: "deploy-secret", BaseURL: chat.URL, App: 4,
	}), streamapp.ClientsOptions{})
	manager, err := session.NewManager(session.ManagerOptions{
		LLM: reasoner, Stream: apps, Conversations: persistent.NewForChats(persistent.StreamApps(apps)), Logger: logger,
		Edge: func(context.Context, session.Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			return nil, nil
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = manager.Shutdown() })
	s.manager = manager
	s.service = &Service{store: db, sessions: manager, logger: logger}
}

// spec is a first message's session, kept in Chat for a person writing to customerID's agent.
func spec(customerID string) session.Spec {
	return session.Spec{CustomerID: customerID, Text: true, PersistConversation: true, LLMTarget: "en-low-latency",
		Caller: routing.Caller{UserID: "phone:+15550100", Verified: true}}
}

func (s *RecordConversationSuite) TestAFirstMessageGoesOnInTheConversationRecordedFirst() {
	customerID, configID := "record-"+uuid.NewString(), uuid.NewString()
	first, err := s.manager.Create(s.ctx, spec(customerID))
	s.Require().NoError(err)
	kept := first.Spec().ConversationID
	_, err = s.manager.Close(first.ID(), session.OwnerOf(first.Spec()))
	s.Require().NoError(err)
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &store.ContactMapEntry{
		CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550100", UserID: "phone:+15550100", ConversationID: kept,
	}))

	orphan, err := s.manager.Create(s.ctx, spec(customerID))
	s.Require().NoError(err)
	s.Require().NotEqual(kept, orphan.Spec().ConversationID)
	identity := store.ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550100", UserID: "phone:+15550100"}

	answering, err := s.service.recordConversation(s.ctx, orphan, spec(customerID), &identity, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _, _ = s.manager.Close(answering.ID(), session.OwnerOf(answering.Spec())) })
	s.Equal(kept, answering.Spec().ConversationID, "the message goes on in the conversation recorded first")
	s.Equal(kept, identity.ConversationID)
	s.False(s.manager.Running(customerID, orphan.ID()), "the session the lost race opened is closed")
}

// Opening the kept conversation can fail; the message is then answered where it was opened
// rather than dropped.
func (s *RecordConversationSuite) TestAFirstMessageIsAnsweredWhereItOpenedWhenTheKeptConversationCannotOpen() {
	customerID, configID := "record-"+uuid.NewString(), uuid.NewString()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &store.ContactMapEntry{
		CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550100", UserID: "phone:+15550100",
		ConversationID: "agent:never-made-" + uuid.NewString(),
	}))
	orphan, err := s.manager.Create(s.ctx, spec(customerID))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _, _ = s.manager.Close(orphan.ID(), session.OwnerOf(orphan.Spec())) })
	identity := store.ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550100", UserID: "phone:+15550100"}

	answering, err := s.service.recordConversation(s.ctx, orphan, spec(customerID), &identity, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	s.Equal(orphan.ID(), answering.ID())
	s.True(s.manager.Running(customerID, orphan.ID()))
}
