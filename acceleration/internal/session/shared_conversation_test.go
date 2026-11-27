//go:build integration

package session

import (
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	getstream "github.com/GetStream/getstream-go/v5"
)

const anyasInbox = "PRIVATE-INBOX-CONTENT-OF-ANYA"

// membersConversation is a conversation of anya's open to its members, anya and ben, in which
// each of them asked the agent once, and each reply ran a tool in that member's own session:
// anya's read her inbox. It returns the conversation's id.
func (s *EpisodeReadingSuite) membersConversation() string {
	id := "support-" + uuid.NewString()
	creator := s.configID
	_, err := s.chat.Client.Chat().GetOrCreateChannel(s.ctx, "agent", id, &getstream.GetOrCreateChannelRequest{
		Data: &getstream.ChannelInput{CreatedByID: &creator,
			Members: []getstream.ChannelMemberRequest{{UserID: s.configID}, {UserID: "anya"}, {UserID: "ben"}},
			Custom: map[string]any{
				persistent.CustomerField: s.customerID, "support_agent_id": s.configID,
				"support_owner_id": "anya", "support_access": "members",
				persistent.TriggerField: persistent.SessionCommandTrigger,
			}},
	})
	s.Require().NoError(err)
	cid := "agent:" + id
	s.asked(cid, "anya", "turn-anya", "check my inbox", anyasInbox, -600)
	s.asked(cid, "ben", "turn-ben", "check the weather", "sunny in Lisbon", -300)
	return cid
}

// asked records one exchange of user's in cid, seconds after the test's start: their message
// and the agent's reply in Chat, and the reply's tool result in a session of user's own.
func (s *EpisodeReadingSuite) asked(cid, user, turn, question, result string, seconds int) {
	at := s.at(seconds)
	stamp := at.Format(time.RFC3339Nano)
	wire := func(role string) map[string]any {
		return map[string]any{
			"support_runtime": map[string]any{"role": role, "state": "completed", "answer_start": 0, "turn_id": turn,
				"response_started_at": stamp, "state_started_at": stamp, "duration_ms": 0},
			"support_message": map[string]any{"schema_version": 1, "sequence": 0, "state": "completed"},
		}
	}
	s.write(cid, user, question, wire("user"))
	s.write(cid, s.configID, "Done.", wire("assistant"))
	sessionID, responseID := uuid.NewString(), uuid.NewString()
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &store.AgentSession{
		ID: sessionID, CustomerID: s.customerID, AgentName: "docs", ConfigID: s.configID, UserID: user,
		CallerKind: string(auth.KindAuthenticated), State: store.SessionClosed, ConversationID: cid, CreatedAt: at,
	}))
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &store.AgentResponse{
		ID: responseID, SessionID: sessionID, CustomerID: s.customerID, Said: question, CreatedAt: at,
	}))
	items := []store.AgentResponseItem{
		{Kind: store.ItemToolResult, ToolName: "inbox", Text: result, Payload: map[string]any{"call_id": "call-" + turn, "arguments": `{}`}},
		{Kind: store.ItemAnswer, Text: "Done.", Payload: map[string]any{"turn_id": turn}},
	}
	for i := range items {
		items[i].ResponseID, items[i].SessionID, items[i].Ordinal, items[i].At = responseID, sessionID, i, at.Add(time.Duration(i)*time.Second)
	}
	s.Require().NoError(s.store.ResponseItems.InsertMany(s.ctx, items))
	s.Require().NoError(s.store.Responses.Finish(s.ctx, s.customerID, responseID, store.ResponseCompleted, "", at.Add(time.Minute)))
}

// resumedBy opens a session of user's, signed in, on cid with tool calls replayed, and returns
// the tool results the model was handed with what user asked next.
func (s *EpisodeReadingSuite) resumedBy(cid, user string) []string {
	s.at(0)
	include := true
	created, err := s.manager.Create(s.ctx, Spec{
		CustomerID: s.customerID, AgentID: s.configID, AgentName: "Athena", Text: true, PersistConversation: true,
		ConversationID: cid, Caller: routing.Caller{UserID: user}, CallerKind: auth.KindAuthenticated,
		ResumeHistory: store.ResumeHistory{IncludeTools: &include}, LLMTarget: "en-low-latency",
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _, _ = s.manager.Close(created.ID(), OwnerOf(created.Spec())) })
	next := "anything new? " + uuid.NewString()
	_, _, err = created.RespondCommand(s.ctx, uuid.NewString(), next, "", nil, options.LLM{})
	s.Require().NoError(err)
	var results []string
	for _, message := range s.replyTo(next).Input {
		if message.Role == llm.ToolResult {
			results = append(results, message.Content)
		}
	}
	return results
}

// Defect: a member resuming a conversation open to its members is handed another member's raw
// tool results, which Chat never showed them. Each member is still handed their own.
func (s *EpisodeReadingSuite) TestAMemberIsHandedOnlyTheirOwnToolResultsInASharedConversation() {
	bens := s.resumedBy(s.membersConversation(), "ben")
	anyas := s.resumedBy(s.membersConversation(), "anya")

	s.False(slices.ContainsFunc(bens, func(r string) bool { return strings.Contains(r, anyasInbox) }), "ben was handed %q", bens)
	s.Contains(bens, "sunny in Lisbon")
	s.Contains(anyas, anyasInbox)
	s.NotContains(anyas, "sunny in Lisbon")
}

// Defect: a member's session in a conversation open to its members opens the member's own
// connection, whose results the other members then read: the multi-person rule held in a thread
// channel only. A session of a conversation of its own still tries the connection it chose, which
// fails here only because the suite's manager has connectors off.
func (s *EpisodeReadingSuite) TestAPerUserConnectorIsRefusedInASharedConversation() {
	manifest, err := core.ParseManifest([]byte(`
id: custom_inbox
revision: 1
name: Inbox
endpoints:
  mcp: https://inbox.invalid/mcp
schemes: [none]
sources:
  - kind: mcp
    endpoint: mcp
`))
	s.Require().NoError(err)
	_, err = s.store.ConnectorDefinitions.Revise(s.ctx, s.customerID, manifest)
	s.Require().NoError(err)
	config := store.AgentConfig{CustomerID: s.customerID, Name: "agent-" + uuid.NewString(), Connectors: []store.ConnectorBinding{{
		Name: "inbox", ConnectorID: "custom_inbox", Connection: store.ConnectionBinding{Type: selectionSession},
		Tools: []store.ToolGrant{{Name: "read"}},
	}}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &config))
	opened := func(cid string, call bool) []ConnectorUnavailable {
		spec := Spec{
			CustomerID: s.customerID, AgentID: s.configID, ConfigID: config.ID, ConnectorBindings: config.Connectors,
			ConnectorSelections: []ConnectorSelection{{Name: "inbox", ConnectionID: "bens-inbox"}},
			Text:                true, PersistConversation: true, ConversationID: cid, LLMTarget: "en-low-latency",
			Caller: routing.Caller{UserID: "ben"}, CallerKind: auth.KindAuthenticated,
		}
		// A call on a conversation reads it without writing to it.
		if call {
			spec.Text, spec.PersistConversation, spec.CallID = false, false, "call-"+uuid.NewString()
			spec.STTTarget, spec.TTSTarget = "en-low-latency", "en-low-latency"
			s.chat.PutCall("agent", spec.CallID, "ben")
		}
		created, err := s.manager.Create(s.ctx, spec)
		s.Require().NoError(err)
		s.T().Cleanup(func() { _, _ = s.manager.Close(created.ID(), OwnerOf(created.Spec())) })
		return created.connectorsUnavailable
	}

	shared, called, own := opened(s.membersConversation(), false), opened(s.membersConversation(), true), opened("", false)

	s.Equal([]ConnectorUnavailable{{Name: "inbox", ConnectorID: "custom_inbox", Reason: unavailableShared}}, shared)
	s.Equal([]ConnectorUnavailable{{Name: "inbox", ConnectorID: "custom_inbox", Reason: unavailableShared}}, called, "a call on the conversation")
	s.Equal([]ConnectorUnavailable{{Name: "inbox", ConnectorID: "custom_inbox", Reason: unavailableOpenFailed}}, own)
}
