//go:build integration

package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/blob"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/voices"
)

// BuildVoicesSuite builds the voice service as the router's start does.
type BuildVoicesSuite struct {
	suite.Suite
}

func TestBuildVoicesSuite(t *testing.T) { suite.Run(t, new(BuildVoicesSuite)) }

// Without the gate nothing refuses the clone, and without the models the allowlist refuses it
// before money does.
func (s *BuildVoicesSuite) TestTheGateAndTheRoutedModelsDecideWhoMayHaveAVoice() {
	s.T().Setenv("ELEVENLABS_API_KEY", "unused")
	ctx, logger := s.T().Context(), slog.New(slog.DiscardHandler)
	money := policytest.Start(s.T(), "build_voices")
	bucket, err := blob.Open(ctx, "file://"+s.T().TempDir())
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(bucket.Close()) })
	service, err := buildVoices(money.Store, bucket, nil, money.Enforcer,
		[]routing.ProviderConfig{{Provider: "elevenlabs", Model: "eleven_v4"}}, logger)
	s.Require().NoError(err)
	owner := money.Owner(s.T(), 0)
	s.Require().NoError(money.Enforcer.Save(ctx, store.ScopeOrganization, owner.OrganizationID,
		store.PolicyDocument{AllowedModels: &[]string{"elevenlabs/eleven_v4"}}))
	voice, err := service.Create(ctx, owner.CustomerID, "founder", "")
	s.Require().NoError(err)
	s.Require().NoError(service.AddSample(ctx, owner.CustomerID, voice.ID,
		voices.Sample{Name: "a.wav", ContentType: "audio/wav", Content: []byte("speech")}))

	s.ErrorIs(service.Prepare(ctx, owner.CustomerID, voice.ID, []string{"elevenlabs"}), policy.ErrNoCredits)
}
