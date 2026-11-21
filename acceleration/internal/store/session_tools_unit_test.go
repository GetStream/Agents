package store

import "testing"

func TestSessionToolsValidate(t *testing.T) {
	refuses(t, func() agentSessionTools { return agentSessionTools{SessionID: "s"} },
		(*agentSessionTools).Validate, map[string]func(*agentSessionTools){
			"session_id": func(r *agentSessionTools) { r.SessionID = "" },
		})
}
