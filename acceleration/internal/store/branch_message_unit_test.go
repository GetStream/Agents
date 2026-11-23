package store

import "testing"

func TestBranchMessageValidate(t *testing.T) {
	refuses(t, func() AgentBranchMessage {
		return AgentBranchMessage{SessionID: "s", OriginalMessageID: "m", CustomerID: "app"}
	}, (*AgentBranchMessage).Validate, map[string]func(*AgentBranchMessage){
		"session_id":          func(m *AgentBranchMessage) { m.SessionID = "" },
		"original_message_id": func(m *AgentBranchMessage) { m.OriginalMessageID = "" },
		"customer_id":         func(m *AgentBranchMessage) { m.CustomerID = "" },
	})
}
