package store

import "testing"

func TestResponseItemValidate(t *testing.T) {
	refuses(t, func() AgentResponseItem {
		return AgentResponseItem{ResponseID: "r", SessionID: "s", Kind: "message"}
	}, (*AgentResponseItem).Validate, map[string]func(*AgentResponseItem){
		"response_id": func(i *AgentResponseItem) { i.ResponseID = "" },
		"session_id":  func(i *AgentResponseItem) { i.SessionID = "" },
		"kind":        func(i *AgentResponseItem) { i.Kind = "" },
		"ordinal":     func(i *AgentResponseItem) { i.Ordinal = -1 },
	})
}
