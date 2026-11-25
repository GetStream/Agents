package routing

// LoggedCall is what every per-call timing line ("model call timing", "speech synthesis
// timing", "speech transcription timing", "speech-to-speech timing") says about whose call it was and what served it,
// under the same names on each so one query filters them all. The lines are logged with no
// request context, so the app is named here, under the field the request lines name it with
// (api.NewLogHandler), or it could not be filtered by app. served is what else a modality
// says about what served the call, placed right after the model.
func LoggedCall(callID string, owner Owner, config ProviderConfig, served ...any) []any {
	fields := []any{"call", callID}
	if owner.CustomerID != "" {
		fields = append(fields, "app_id", owner.CustomerID)
	}
	if owner.AgentID != "" {
		fields = append(fields, "agent_id", owner.AgentID)
	}
	fields = append(fields, "provider", config.Provider, "model", config.Model)
	fields = append(fields, served...)
	if config.Region != "" {
		fields = append(fields, "region", config.Region)
	}
	if config.BYOK {
		fields = append(fields, "byok", true)
	}
	return fields
}
