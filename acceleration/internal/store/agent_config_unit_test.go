package store

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestNormalizeConfigVideoFrames(t *testing.T) {
	for _, requested := range []int{0, 1, 8} {
		config := AgentConfig{Video: AgentVideo{MaxFrames: requested}}
		normalizeConfig(&config)
		expected := requested
		if expected == 0 {
			expected = 1
		}
		require.Equal(t, expected, config.Video.MaxFrames)
	}
}

// TestUpdatingAConfigWritesEveryFieldOfIt holds the update's column list against the
// model, since a field the two disagree about is a setting that saves once and then
// silently stops saving - which is worse than one that never worked.
func TestUpdatingAConfigWritesEveryFieldOfIt(t *testing.T) {
	// What an update cannot change: who owns the row, which row it is, when it appeared,
	// and whether it is gone. Deleting is its own statement.
	immutable := []string{"id", "customer_id", "created_at", "deleted_at"}

	table := pgdialect.New().Tables().Get(reflect.TypeFor[AgentConfig]())
	for _, field := range table.Fields {
		if slices.Contains(immutable, field.Name) {
			continue
		}
		require.Containsf(t, configColumns, field.Name,
			"AgentConfig.%s is stored on create and dropped on every update", field.GoName)
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
	boxed := valid()
	boxed.Keyterms = make([]string, stt.MaxKeyterms)
	boxed.Sandbox = AgentSandbox{Provider: SandboxDaytona, Options: sandbox.Config{
		CPU: 16, Setup: []string{"pip install pandas"}, AllowedDomains: []string{"pypi.org"},
	}}
	require.NoError(t, boxed.Validate())
	unset := AgentConfig{CustomerID: "app", Name: "support"}
	require.NoError(t, unset.Validate(), "a default not yet filled in is the default's")

	for field, broken := range map[string]func(*AgentConfig){
		"customer_id":           func(c *AgentConfig) { c.CustomerID = "" },
		"name":                  func(c *AgentConfig) { c.Name = "" },
		"mode":                  func(c *AgentConfig) { c.Mode = "video" },
		"role":                  func(c *AgentConfig) { c.Role = "admin" },
		"region":                func(c *AgentConfig) { c.Region = "EU" },
		"routing_policy":        func(c *AgentConfig) { c.RoutingPolicy = "eu" },
		"greeting_mode":         func(c *AgentConfig) { c.GreetingMode = "loud" },
		"video_max_frames":      func(c *AgentConfig) { c.Video.MaxFrames = -1 },
		"keyterms":              func(c *AgentConfig) { c.Keyterms = make([]string, stt.MaxKeyterms+1) },
		"sandbox_provider":      func(c *AgentConfig) { c.Sandbox.Provider = "e2b" },
		"sandbox_options.cpu":   func(c *AgentConfig) { c.Sandbox.Options.CPU = 17 },
		"sandbox_options.setup": func(c *AgentConfig) { c.Sandbox.Options.Setup = []string{" "} },
		"sandbox_options.allowed_domains": func(c *AgentConfig) {
			c.Sandbox.Options.AllowedDomains = []string{"203.0.113.7:443"}
		},
		"sandbox_options.network_allow_list": func(c *AgentConfig) {
			c.Sandbox.Options.NetworkAllowList = []string{"10.0.0.0"}
		},
		"sandbox_options": func(c *AgentConfig) {
			c.Sandbox.Options.AllowedDomains = []string{"example.com"}
			c.Sandbox.Options.NetworkAllowList = []string{"10.0.0.0/8"}
		},
	} {
		config := valid()
		broken(&config)
		var invalid *ValidationError
		require.True(t, errors.As(config.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
