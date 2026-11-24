package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A device opens a session on the models, limits, greeting, cost labels and phone line its
// app's backend chose; only the backend may name them.
func TestADeviceMayNotChooseASessionsPhoneModelsOrLabels(t *testing.T) {
	models := "llm, stt, tts, sts, search, voice, max_tokens, max_tool_rounds and model_overwrites are sent server-side"
	fields := map[string]struct {
		sent    CreateSessionRequest
		refusal string
	}{
		"phone":            {CreateSessionRequest{Phone: &SessionPhone{Number: "+15550100"}}, "phone is sent server-side"},
		"vendor call":      {CreateSessionRequest{Phone: &SessionPhone{VendorCallId: new("v3:call")}}, "phone is sent server-side"},
		"llm":              {CreateSessionRequest{Llm: new("openai/gpt-5")}, models},
		"stt":              {CreateSessionRequest{Stt: new("deepgram/nova-3")}, models},
		"tts":              {CreateSessionRequest{Tts: new("cartesia/sonic-3")}, models},
		"sts":              {CreateSessionRequest{Sts: new("openai/gpt-realtime")}, models},
		"search":           {CreateSessionRequest{Search: new("exa/search")}, models},
		"voice":            {CreateSessionRequest{Voice: new("marin")}, models},
		"max_tokens":       {CreateSessionRequest{MaxTokens: new(64000)}, models},
		"max_tool_rounds":  {CreateSessionRequest{MaxToolRounds: new(-1)}, models},
		"model_overwrites": {CreateSessionRequest{ModelOverwrites: &ModelOverwrites{Thinking: new(ModelOverwritesThinking("high"))}}, models},
		"greeting":         {CreateSessionRequest{Greeting: &Greeting{Text: "Your refund is approved."}}, "greeting is sent server-side"},
		"tags":             {CreateSessionRequest{Tags: &map[string]string{"project": "another"}}, "tags are sent server-side"},
		"empty llm":        {CreateSessionRequest{Llm: new("")}, models},
		"empty greeting":   {CreateSessionRequest{Greeting: &Greeting{}}, "greeting is sent server-side"},
		"empty tags":       {CreateSessionRequest{Tags: &map[string]string{}}, "tags are sent server-side"},
	}
	backend := context.WithValue(context.Background(), serverSideContextKey{}, true)
	for field, refused := range fields {
		t.Run(field, func(t *testing.T) {
			failure, ok := refuseSessionRequest(context.Background(), refused.sent)
			require.True(t, ok, "a device is refused")
			require.Equal(t, ErrorTypePermission, failure.Type)
			require.Contains(t, failure.Message, refused.refusal)

			_, ok = refuseSessionRequest(backend, refused.sent)
			require.False(t, ok, "the backend names it")
		})
	}
}

// What a device may still say about its own session is let through.
func TestADeviceStillLabelsAndShapesItsOwnSession(t *testing.T) {
	sent := CreateSessionRequest{
		ConfigId: new("c"), Title: new("Trip"), Description: new("Lisbon"), ProjectId: new("p"),
		Custom: &map[string]any{"pinned": true}, Incognito: new(true), Ui: new(false),
		Timezone: new("Europe/Lisbon"), Languages: &[]string{"pt"}, Keyterms: &[]string{"Alfama"},
		Tools: &[]SessionTool{{Name: "get_location"}}, StartVoice: new(true),
	}

	_, refused := refuseSessionRequest(context.Background(), sent)

	require.False(t, refused)
}
