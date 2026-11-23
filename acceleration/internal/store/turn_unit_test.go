package store

import "testing"

func TestTurnValidate(t *testing.T) {
	refuses(t, func() Turn { return Turn{CustomerID: "app", AgentID: "a", TurnID: "t"} },
		(*Turn).Validate, map[string]func(*Turn){
			"customer_id": func(r *Turn) { r.CustomerID = "" },
			"agent_id":    func(r *Turn) { r.AgentID = "" },
			"turn_id":     func(r *Turn) { r.TurnID = "" },
		})
}
