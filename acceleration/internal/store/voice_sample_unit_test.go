package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVoiceSampleValidate(t *testing.T) {
	valid := func() VoiceSample {
		return VoiceSample{VoiceID: "voice", ObjectKey: "one.wav"}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*VoiceSample){
		"voice_id":   func(m *VoiceSample) { m.VoiceID = "" },
		"object_key": func(m *VoiceSample) { m.ObjectKey = "" },
		"bytes":      func(m *VoiceSample) { m.Bytes = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
