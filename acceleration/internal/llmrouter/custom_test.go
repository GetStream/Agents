package llmrouter

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type CustomModelsUnitSuite struct {
	suite.Suite
}

func TestCustomModelsUnitSuite(t *testing.T) {
	suite.Run(t, new(CustomModelsUnitSuite))
}

// A database that cannot be reached says nothing about whether the customer holds the model.
func (s *CustomModelsUnitSuite) TestADatabaseFailureIsNoRefusal() {
	down, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = down.Close() })

	_, _, err = NewCustomModels(down, nil, false).CustomModel(s.T().Context(), "acme", "gemma")

	s.Require().Error(err)
	s.NotErrorIs(err, routing.ErrRequest)
	s.NotErrorIs(err, store.ErrNoCustomModel)
}
