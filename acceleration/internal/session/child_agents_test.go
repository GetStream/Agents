package session

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ChildRunnerSuite is a child agent's tool as the parent's model calls it.
type ChildRunnerSuite struct {
	suite.Suite
}

func TestChildRunnerSuite(t *testing.T) {
	suite.Run(t, new(ChildRunnerSuite))
}

func (s *ChildRunnerSuite) TestACallWithNoInputIsTheModelsToCorrect() {
	runner := &childRunner{children: map[string]store.ChildAgent{"ask_a": {}}}

	_, err := runner.Run(s.T().Context(), llm.ToolCall{Name: "ask_a", Arguments: `{"input":"  "}`})

	s.EqualError(err, "ask_a was called with no input")
	s.ErrorIs(err, harness.ErrCorrectable)
}
