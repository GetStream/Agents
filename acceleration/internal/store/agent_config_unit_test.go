package store

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeConfigVideoFrames(t *testing.T) {
	for _, requested := range []int{0, 1, 8} {
		config := AgentConfig{VideoMaxFrames: requested}
		normalizeConfig(&config)
		expected := requested
		if expected == 0 {
			expected = 1
		}
		require.Equal(t, expected, config.VideoMaxFrames)
	}
}

// TestUpdatingAConfigWritesEveryFieldOfIt holds the update's column list against the
// model, since a field the two disagree about is a setting that saves once and then
// silently stops saving - which is worse than one that never worked.
func TestUpdatingAConfigWritesEveryFieldOfIt(t *testing.T) {
	// What an update cannot change: who owns the row, which row it is, when it appeared,
	// and whether it is gone. Deleting is its own statement.
	immutable := []string{"id", "customer_id", "created_at", "deleted_at"}

	model := reflect.TypeFor[AgentConfig]()
	for index := range model.NumField() {
		tag := model.Field(index).Tag.Get("bun")
		column, _, _ := strings.Cut(tag, ",")
		if column == "" || strings.HasPrefix(tag, "table:") || slices.Contains(immutable, column) {
			continue
		}
		require.Containsf(t, configColumns, column,
			"AgentConfig.%s is stored on create and dropped on every update", model.Field(index).Name)
	}

	for _, column := range configColumns {
		require.NotContainsf(t, immutable, column, "%s is not an update's to change", column)
	}
}

func TestAgentConfigValidate(t *testing.T) {
	valid := func() AgentConfig {
		config := AgentConfig{CustomerID: "app", Name: "support"}
		normalizeConfig(&config)
		return config
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*AgentConfig){
		"customer_id":      func(c *AgentConfig) { c.CustomerID = "" },
		"name":             func(c *AgentConfig) { c.Name = "" },
		"mode":             func(c *AgentConfig) { c.Mode = "video" },
		"role":             func(c *AgentConfig) { c.Role = "admin" },
		"region":           func(c *AgentConfig) { c.Region = "EU" },
		"greeting_mode":    func(c *AgentConfig) { c.GreetingMode = "loud" },
		"video_max_frames": func(c *AgentConfig) { c.VideoMaxFrames = 0 },
	} {
		config := valid()
		broken(&config)
		var invalid *ValidationError
		require.True(t, errors.As(config.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
