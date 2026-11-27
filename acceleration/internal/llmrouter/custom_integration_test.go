//go:build integration

package llmrouter

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
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

// CustomModelKeySuite is how a customer's model's sealed key is read back: bound to its
// customer, as keys are sealed, or bound to the model, as a later release seals them. It reads
// through the name cache a session reads through.
type CustomModelKeySuite struct {
	suite.Suite
	ctx        context.Context
	store      *store.Store
	sealer     *auth.Sealer
	customerID string
}

func TestCustomModelKeySuite(t *testing.T) {
	suite.Run(t, new(CustomModelKeySuite))
}

func (s *CustomModelKeySuite) SetupSuite() {
	dsn, address := os.Getenv("ROUTER_POSTGRES_DSN"), os.Getenv("ROUTER_REDIS_ADDR")
	if dsn == "" || address == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN and ROUTER_REDIS_ADDR must be set")
	}
	s.ctx = context.Background()
	opened, err := redisready.Retry(address, func() (*store.Store, error) {
		return store.OpenShards(store.Options{DSN: testenv.Database(dsn, "llmrouter_custom"), Redis: store.Redis{Address: address}})
	})
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.store = opened
	s.sealer, err = auth.NewSealer("custom-model-key-kek")
	s.Require().NoError(err)
}

func (s *CustomModelKeySuite) SetupTest() {
	s.customerID = "custom-key-" + uuid.NewString()
}

// model stores a model of the customer's whose key is sealedFor its id, under version.
func (s *CustomModelKeySuite) model(sealedFor func(id string) []byte, version int) store.CustomModel {
	model := store.CustomModel{CustomerID: s.customerID, Name: "tuned-" + uuid.NewString()[:8],
		BaseURL: "https://8.8.8.8/v1", Model: "acme/tuned"}
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &model))
	model.APIKeySealed, model.KEKVersion = sealedFor(model.ID), version
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model))
	return model
}

// sealed seals key with sealer under additional, whatever the model's id.
func (s *CustomModelKeySuite) sealed(sealer *auth.Sealer, key string, additional func(id string) []byte) func(string) []byte {
	return func(id string) []byte {
		sealed, err := sealer.SealWithAAD(key, additional(id))
		s.Require().NoError(err)
		return sealed
	}
}

// customerOnly is the AAD keys are sealed with: the customer's id, whatever the model.
func (s *CustomModelKeySuite) customerOnly(string) []byte { return []byte(s.customerID) }

// read is the model as a session reads it, through the name cache.
func (s *CustomModelKeySuite) read(model store.CustomModel) store.CustomModel {
	read, err := s.store.CustomModels.GetByName(s.ctx, s.customerID, model.Name)
	s.Require().NoError(err)
	return read
}

// legacyOpensOf reads router_sealed_legacy_opens_total for one kind of record.
func (s *CustomModelKeySuite) legacyOpensOf(record string) float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	s.Require().NoError(err)
	for _, family := range families {
		if family.GetName() != "router_sealed_legacy_opens_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "record" && pair.GetValue() == record {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// Catches the key's AAD naming only the customer, or a fallback that opens any of the
// customer's blobs: a key sealed for one model, put in another model's row, does not open.
func (s *CustomModelKeySuite) TestAKeyMovedFromAnotherModelDoesNotOpen() {
	other := store.NewID()
	model := s.model(s.sealed(s.sealer, "sk-other", func(string) []byte { return customModelKeyAAD(s.customerID, other) }),
		s.sealer.CurrentVersion())

	_, _, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.ErrorContains(err, "auth: open secret")
}

// Catches another customer's model of the same id opening here: the customer is in the AAD.
func (s *CustomModelKeySuite) TestAKeySealedForAnotherCustomersModelOfTheSameIdDoesNotOpen() {
	model := s.model(s.sealed(s.sealer, "sk-other", func(id string) []byte { return customModelKeyAAD("another-app", id) }),
		s.sealer.CurrentVersion())

	_, _, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.ErrorContains(err, "auth: open secret")
}

// Catches a key the next release seals bound to its model not opening here, which would make
// that release impossible to roll back.
func (s *CustomModelKeySuite) TestAKeySealedBoundToItsModelOpens() {
	model := s.model(s.sealed(s.sealer, "sk-own", func(id string) []byte { return customModelKeyAAD(s.customerID, id) }),
		s.sealer.CurrentVersion())

	_, endpoint, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
}

// Catches a key sealed bound only to its customer, as every key is, going dark, being written
// back on a read, or counted under another kind of record than a custom model: the count is what says when that form can
// stop being opened.
func (s *CustomModelKeySuite) TestAKeySealedBoundOnlyToItsCustomerOpensIsLeftAsItIsAndIsCounted() {
	model := s.model(s.sealed(s.sealer, "sk-own", s.customerOnly), s.sealer.CurrentVersion())
	before := s.legacyOpensOf("custom_model")

	_, endpoint, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
	s.Equal(model.APIKeySealed, s.read(model).APIKeySealed, "a read writes nothing")
	s.GreaterOrEqual(s.legacyOpensOf("custom_model"), before+1)
}

// Catches a key bound only to its customer being opened under the current key rather than the
// one it was sealed under.
func (s *CustomModelKeySuite) TestALegacyKeyUnderAnOlderKeyOpensWithItsStoredVersion() {
	before, err := auth.NewSealer("old-key")
	s.Require().NoError(err)
	model := s.model(s.sealed(before, "sk-own", s.customerOnly), 1)
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)

	_, endpoint, err := NewCustomModels(s.store, rotated, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
	s.Equal(1, s.read(model).KEKVersion)
}

// Catches the parts of a model key's AAD running together: a colon inside one part cannot make
// two models give the same bytes.
func (s *CustomModelKeySuite) TestTheKeysAADPartsDoNotRunTogether() {
	s.NotEqual(customModelKeyAAD("app:model", "id"), customModelKeyAAD("app", "model:id"))
}
