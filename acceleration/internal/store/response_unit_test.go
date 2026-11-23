package store

import "testing"

func TestResponseValidate(t *testing.T) {
	refuses(t, func() AgentResponse {
		return AgentResponse{ID: "r", SessionID: "s", CustomerID: "app", Status: ResponseCompleted}
	}, (*AgentResponse).Validate, map[string]func(*AgentResponse){
		"id":          func(r *AgentResponse) { r.ID = "" },
		"session_id":  func(r *AgentResponse) { r.SessionID = "" },
		"customer_id": func(r *AgentResponse) { r.CustomerID = "" },
		"status":      func(r *AgentResponse) { r.Status = "lost" },
	})
}
