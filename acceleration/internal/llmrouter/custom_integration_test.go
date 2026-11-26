//go:build integration

package llmrouter

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// CustomModelKeySuite is how a customer's model's sealed key is read back: bound to the model,
// and still readable from a model stored before it was, which is then sealed again bound to it.
// It reads through the name cache a session reads through.
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

// model stores a model of the customer's under id, its key sealed as sealed under version.
func (s *CustomModelKeySuite) model(id string, sealed []byte, version int) store.CustomModel {
	model := store.CustomModel{ID: id, CustomerID: s.customerID, Name: "tuned-" + uuid.NewString()[:8],
		BaseURL: "https://8.8.8.8/v1", Model: "acme/tuned", APIKeySealed: sealed, KEKVersion: version}
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &model))
	return model
}

// read is the model as a session reads it, through the name cache.
func (s *CustomModelKeySuite) read(model store.CustomModel) store.CustomModel {
	read, err := s.store.CustomModels.GetByName(s.ctx, s.customerID, model.Name)
	s.Require().NoError(err)
	return read
}

// Catches the key's AAD naming only the customer, or a fallback that opens any of the
// customer's blobs: a key sealed for one model, put in another model's row, does not open.
func (s *CustomModelKeySuite) TestAKeyMovedFromAnotherModelDoesNotOpen() {
	sealed, err := s.sealer.SealWithAAD("sk-other", CustomModelKeyAAD(s.customerID, store.NewID()))
	s.Require().NoError(err)
	model := s.model(store.NewID(), sealed, s.sealer.CurrentVersion())

	_, _, err = NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.ErrorContains(err, "auth: open secret")
	s.Equal(sealed, s.read(model).APIKeySealed, "nothing was sealed again from a key that did not open")
}

// Catches another customer's model of the same id opening here: the customer is in the AAD.
func (s *CustomModelKeySuite) TestAKeySealedForAnotherCustomersModelOfTheSameIdDoesNotOpen() {
	id := store.NewID()
	sealed, err := s.sealer.SealWithAAD("sk-other", CustomModelKeyAAD("another-app", id))
	s.Require().NoError(err)
	model := s.model(id, sealed, s.sealer.CurrentVersion())

	_, _, err = NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.ErrorContains(err, "auth: open secret")
}

// Catches a key bound to its model not opening, or being sealed again on every session.
func (s *CustomModelKeySuite) TestAKeySealedBoundToItsModelOpensAndIsLeftAsItIs() {
	id := store.NewID()
	sealed, err := s.sealer.SealWithAAD("sk-own", CustomModelKeyAAD(s.customerID, id))
	s.Require().NoError(err)
	model := s.model(id, sealed, s.sealer.CurrentVersion())

	_, endpoint, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
	s.Equal(sealed, s.read(model).APIKeySealed)
}

// Catches a model stored before the binding going dark, or staying in the old form, in the
// database or in the name cache a session reads through: it opens, and the next read holds the
// key bound to the model and nothing else.
func (s *CustomModelKeySuite) TestAKeySealedBoundOnlyToItsCustomerOpensAndIsSealedAgainBoundToTheModel() {
	sealed, err := s.sealer.SealWithAAD("sk-own", []byte(s.customerID))
	s.Require().NoError(err)
	model := s.model(store.NewID(), sealed, s.sealer.CurrentVersion())

	_, endpoint, err := NewCustomModels(s.store, s.sealer, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
	read := s.read(model)
	_, err = s.sealer.OpenWithAADVersion(read.APIKeySealed, []byte(s.customerID), read.KEKVersion)
	s.Error(err, "the customer-only form is gone from what a session reads")
	key, err := s.sealer.OpenWithAADVersion(read.APIKeySealed, CustomModelKeyAAD(s.customerID, model.ID), read.KEKVersion)
	s.Require().NoError(err)
	s.Equal("sk-own", key)
}

// Catches the old form being opened under the current key rather than the stored one, or
// sealed again under the old one: a rotation and the binding land on the same row.
func (s *CustomModelKeySuite) TestALegacyKeyUnderAnOlderKeyOpensAndIsSealedAgainUnderTheCurrentOne() {
	before, err := auth.NewSealer("old-key")
	s.Require().NoError(err)
	sealed, err := before.SealWithAAD("sk-own", []byte(s.customerID))
	s.Require().NoError(err)
	model := s.model(store.NewID(), sealed, 1)
	rotated, err := auth.NewSealerWithKeyring(2, map[int]string{1: "old-key", 2: "new-key"})
	s.Require().NoError(err)

	_, endpoint, err := NewCustomModels(s.store, rotated, false).CustomModel(s.ctx, s.customerID, model.Name)

	s.Require().NoError(err)
	s.Equal("sk-own", endpoint.APIKey)
	read := s.read(model)
	s.Equal(2, read.KEKVersion)
	key, err := rotated.OpenWithAADVersion(read.APIKeySealed, CustomModelKeyAAD(s.customerID, model.ID), 2)
	s.Require().NoError(err)
	s.Equal("sk-own", key)
}
