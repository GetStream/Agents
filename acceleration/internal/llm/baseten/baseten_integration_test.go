//go:build integration

package baseten

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmsuite"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openweights/hosttest"
)

type BasetenIntegrationSuite struct {
	llmsuite.Suite
}

func TestBasetenIntegrationSuite(t *testing.T) {
	suite.Run(t, &BasetenIntegrationSuite{hosttest.Live(t, Host, New, "zai-org/GLM-5.3-Flash")})
}

// BasetenKimiIntegrationSuite runs Kimi K3.1 Pro, whose open weights must take the Kimi
// template's thinking switch for it to answer a live turn without reasoning first.
type BasetenKimiIntegrationSuite struct {
	llmsuite.Suite
}

func TestBasetenKimiIntegrationSuite(t *testing.T) {
	suite.Run(t, &BasetenKimiIntegrationSuite{hosttest.Live(t, Host, New, "moonshotai/Kimi-K3.1-Pro")})
}
