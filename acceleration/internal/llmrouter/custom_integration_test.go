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
	id := store.NewID()
	sealed, err := sealer.SealWithAAD("sk-endpoint", CustomModelKeyAAD(s.customerID, id))
	s.Require().NoError(err)
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &store.CustomModel{
		ID: id, CustomerID: s.customerID, Name: name, BaseURL: "https://models.example/v1", Model: "gemma-4",
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

// Catches the parts of a model key's AAD running together: a colon inside one part cannot make
// two models give the same bytes.
func (s *CustomModelKeySuite) TestTheKeysAADPartsDoNotRunTogether() {
	s.NotEqual(CustomModelKeyAAD("app:model", "id"), CustomModelKeyAAD("app", "model:id"))
}
