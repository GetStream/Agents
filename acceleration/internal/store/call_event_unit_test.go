package store

import "testing"

func TestCallEventValidate(t *testing.T) {
	refuses(t, func() CallEvent { return CallEvent{CustomerID: "app", CallID: "c"} },
		(*CallEvent).Validate, map[string]func(*CallEvent){
			"customer_id": func(e *CallEvent) { e.CustomerID = "" },
			"call_id":     func(e *CallEvent) { e.CallID = "" },
		})
}
