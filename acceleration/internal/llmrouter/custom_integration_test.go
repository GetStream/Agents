//go:build integration

package llmrouter

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// CustomModelsSuite resolves the models customers serve themselves from rows in Postgres.
type CustomModelsSuite struct {
	suite.Suite
	ctx        context.Context
	store      *store.Store
	sealer     *auth.Sealer
	customerID string
}

func TestCustomModelsSuite(t *testing.T) {
	suite.Run(t, new(CustomModelsSuite))
}

func (s *CustomModelsSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN must be set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(dsn)
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.sealer, err = auth.NewSealer("custom models suite")
	s.Require().NoError(err)
}

func (s *CustomModelsSuite) SetupTest() {
	s.customerID = "custom-" + uuid.NewString()
}

// keep stores a model whose key is sealed by sealer, as the API stores one.
func (s *CustomModelsSuite) keep(name string, sealer *auth.Sealer) {
	sealed, err := sealer.SealWithAAD("sk-endpoint", []byte(s.customerID))
	s.Require().NoError(err)
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &store.CustomModel{
		CustomerID: s.customerID, Name: name, BaseURL: "https://models.example/v1", Model: "gemma-4",
		APIKeySealed: sealed, KEKVersion: sealer.CurrentVersion(),
	}))
}

func (s *CustomModelsSuite) TestAModelTheCustomerDoesNotHoldIsTheirsToCorrect() {
	_, _, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, "missing")

	s.Require().ErrorIs(err, routing.ErrRequest)
	s.ErrorContains(err, `"custom/missing" is not one of this customer's models`)
}

// A deployment whose key encryption key changed cannot open what was sealed under the old one.
// The caller can do nothing about that, so it is no refusal of theirs.
func (s *CustomModelsSuite) TestAKeyTheDeploymentCannotOpenIsNoRefusal() {
	rotated, err := auth.NewSealer("another key encryption key")
	s.Require().NoError(err)
	s.keep("gemma", rotated)

	_, _, err = NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, "gemma")

	s.Require().Error(err)
	s.NotErrorIs(err, routing.ErrRequest)
}

func (s *CustomModelsSuite) TestAKeyTheDeploymentSealedOpens() {
	s.keep("gemma", s.sealer)

	_, endpoint, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, "gemma")

	s.Require().NoError(err)
	s.Equal("sk-endpoint", endpoint.APIKey)
}
