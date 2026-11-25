//go:build integration

package together

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmsuite"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openweights/hosttest"
)

type TogetherIntegrationSuite struct {
	llmsuite.Suite
}

func TestTogetherIntegrationSuite(t *testing.T) {
	suite.Run(t, &TogetherIntegrationSuite{hosttest.Live(t, Host, New, "zai-org/GLM-5.3-Flash")})
}

// TogetherNemotronIntegrationSuite runs Nemotron 3.5 Super VL, which also answers the
// image tests the text-only GLM above skips.
type TogetherNemotronIntegrationSuite struct {
	llmsuite.Suite
}

func TestTogetherNemotronIntegrationSuite(t *testing.T) {
	suite.Run(t, &TogetherNemotronIntegrationSuite{hosttest.Live(t, Host, New, "nvidia/NVIDIA-Nemotron-3.5-Super-VL")})
}

// TogetherKimiIntegrationSuite runs Kimi K3.1 Pro, whose open weights must take the Kimi
// template's thinking switch for it to answer a live turn without reasoning first.
type TogetherKimiIntegrationSuite struct {
	llmsuite.Suite
}

func TestTogetherKimiIntegrationSuite(t *testing.T) {
	suite.Run(t, &TogetherKimiIntegrationSuite{hosttest.Live(t, Host, New, "moonshotai/Kimi-K3.1-Pro")})
}
