package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVoiceBindingValidate(t *testing.T) {
	valid := func() VoiceBinding {
		return VoiceBinding{VoiceID: "voice", Provider: "elevenlabs", State: VoiceReady}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*VoiceBinding){
		"voice_id": func(m *VoiceBinding) { m.VoiceID = "" },
		"provider": func(m *VoiceBinding) { m.Provider = "" },
		"state":    func(m *VoiceBinding) { m.State = "lost" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
