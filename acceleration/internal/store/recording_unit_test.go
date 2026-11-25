package store

import "testing"

func TestRecordingValidate(t *testing.T) {
	refuses(t, func() Recording { return Recording{CustomerID: "app", Modality: "stt"} },
		(*Recording).Validate, map[string]func(*Recording){
			"customer_id": func(r *Recording) { r.CustomerID = "" },
			"modality":    func(r *Recording) { r.Modality = "llm" },
		})
}
