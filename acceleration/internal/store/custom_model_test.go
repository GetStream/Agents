//go:build integration

package store

import (
	"errors"
	"time"
)

// customModel stores a model for the customer, defaulting the fields a test does not care about.
func (s *StoreSuite) customModel(customerID, name string, change ...func(*CustomModel)) CustomModel {
	model := CustomModel{CustomerID: customerID, Name: name, BaseURL: "https://models.example.com/v1", Model: "llama"}
	for _, apply := range change {
		apply(&model)
	}
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &model))
	return model
}

func (s *StoreSuite) TestACustomModelIsOnlyItsOwnCustomers() {
	mine := s.customModel("app", "tuned")
	s.customModel("other", "tuned")

	_, err := s.store.CustomModels.Get(s.ctx, "other", mine.ID)
	s.ErrorIs(err, ErrNoCustomModel)
	listed, err := s.store.CustomModels.Select(s.ctx, "app", CustomModelFilter{})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)
	s.ErrorIs(s.store.CustomModels.Delete(s.ctx, "other", mine.ID), ErrNoCustomModel)
}

func (s *StoreSuite) TestAnInvalidCustomModelIsNotWritten() {
	err := s.store.CustomModels.Insert(s.ctx, &CustomModel{CustomerID: "app", Name: "tuned", BaseURL: "ftp://models", Model: "llama"})
	var invalid *ValidationError
	s.Require().True(errors.As(err, &invalid))
	s.Equal("base_url", invalid.Field)

	listed, err := s.store.CustomModels.Select(s.ctx, "app", CustomModelFilter{})
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestRenamingACustomModelForgetsTheOldName() {
	model := s.customModel("app", "tuned")
	_, err := s.store.CustomModels.GetByName(s.ctx, "app", "tuned")
	s.Require().NoError(err)

	model.Name = "renamed"
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model))

	_, err = s.store.CustomModels.GetByName(s.ctx, "app", "tuned")
	s.ErrorIs(err, ErrNoCustomModel)
	found, err := s.store.CustomModels.GetByName(s.ctx, "app", "renamed")
	s.Require().NoError(err)
	s.Equal(model.ID, found.ID)
}

func (s *StoreSuite) TestDeletingACustomModelForgetsItsName() {
	model := s.customModel("app", "tuned")
	_, err := s.store.CustomModels.GetByName(s.ctx, "app", "tuned")
	s.Require().NoError(err)

	s.Require().NoError(s.store.CustomModels.Delete(s.ctx, "app", model.ID))

	_, err = s.store.CustomModels.GetByName(s.ctx, "app", "tuned")
	s.ErrorIs(err, ErrNoCustomModel)
}

func (s *StoreSuite) TestCustomModelQueryBudgets() {
	for range 30 {
		s.customModel("app", "m"+newID())
	}
	model := s.customModel("app", "tuned")

	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.CustomModels.Select(s.ctx, "app", CustomModelFilter{Limit: 10})
		s.Require().NoError(err)
		s.Len(listed, 11)
	}))
	model.Model = "llama-2"
	s.Equal(3+s.redisTrips(1), s.countQueries(func() { s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model)) }), "begin, the write, its transaction's id when there is a cache, and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(3+s.redisTrips(1), s.countQueries(func() { s.Require().NoError(s.store.CustomModels.Delete(s.ctx, "app", model.ID)) }), "begin, the write, its transaction's id when there is a cache, and commit: a write of a cached row runs detached in a transaction of its own")
	if s.store.cache == nil {
		return
	}
	other := s.customModel("app", "cached")
	// The read that fills Redis invalidates this client's own local copy, so the second read
	// is the one that is kept locally.
	for range 2 {
		_, err := s.store.CustomModels.GetByName(s.ctx, "app", other.Name)
		s.Require().NoError(err)
	}
	s.Equal(0, s.countQueries(func() {
		s.Equal(0, s.countRedis(func() {
			_, err := s.store.CustomModels.GetByName(s.ctx, "app", other.Name)
			s.Require().NoError(err)
		}), "the second read is answered from the local cache")
	}))
	s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.CustomModels.Delete(s.ctx, "app", other.ID)) }),
		"forgetting a row is one round trip")
}

func (s *StoreSuite) TestACustomModelKeptWithItsOwnIdIsStoredUnderIt() {
	id := newID()

	model := s.customModel("app", "tuned", func(model *CustomModel) { model.ID = id })

	s.Equal(id, model.ID)
	stored, err := s.store.CustomModels.Get(s.ctx, "app", id)
	s.Require().NoError(err, "a key sealed under the app's data key is bound to this id")
	s.Equal("tuned", stored.Name)
}

// The replica reads the model by its name before the reseal and after a rename that kept its key,
// so a reseal that dropped the name it read, or none, leaves the replica on the old blob.
func (s *StoreSuite) TestAResealedCustomModelKeyIsWhatEveryReplicaReadsNextUnderItsCurrentName() {
	replica := s.replica()
	model := s.customModel("app", "tuned", func(model *CustomModel) { model.APIKeySealed, model.KEKVersion = []byte("one"), 1 })
	opened, err := replica.CustomModels.GetByName(s.ctx, "app", "tuned")
	s.Require().NoError(err)
	model.Name = "renamed"
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model))
	_, err = replica.CustomModels.GetByName(s.ctx, "app", "renamed")
	s.Require().NoError(err)

	resealed, err := s.store.CustomModels.Reseal(s.ctx, opened, []byte("under the data key"), 3)

	s.Require().NoError(err)
	s.True(resealed)
	s.Eventually(func() bool {
		read, err := replica.CustomModels.GetByName(s.ctx, "app", "renamed")
		return err == nil && string(read.APIKeySealed) == "under the data key" &&
			read.DataKeyVersion != nil && *read.DataKeyVersion == 3 && read.KEKVersion == 0
	}, 2*time.Second, 10*time.Millisecond, "the other replica still reads the key under the router's key")
}

func (s *StoreSuite) TestAResealLosesToACustomModelKeyPutAfterTheOldOneWasRead() {
	model := s.customModel("app", "tuned", func(model *CustomModel) { model.APIKeySealed, model.KEKVersion = []byte("one"), 1 })
	opened, err := s.store.CustomModels.Get(s.ctx, "app", model.ID)
	s.Require().NoError(err)
	model.APIKeySealed = []byte("two")
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model))

	resealed, err := s.store.CustomModels.Reseal(s.ctx, opened, []byte("the old key resealed"), 3)

	s.Require().NoError(err)
	s.False(resealed)
	stored, err := s.store.CustomModels.Get(s.ctx, "app", model.ID)
	s.Require().NoError(err)
	s.Equal([]byte("two"), stored.APIKeySealed, "the key put last is kept")
	s.Nil(stored.DataKeyVersion)
	s.Equal(1, stored.KEKVersion)
}

func (s *StoreSuite) TestAResealOfACustomModelOfAnotherCustomerWritesNothing() {
	model := s.customModel("app", "tuned", func(model *CustomModel) { model.APIKeySealed, model.KEKVersion = []byte("one"), 1 })
	opened := model
	opened.CustomerID = "other"

	resealed, err := s.store.CustomModels.Reseal(s.ctx, opened, []byte("resealed"), 3)

	s.Require().NoError(err)
	s.False(resealed)
	stored, err := s.store.CustomModels.Get(s.ctx, "app", model.ID)
	s.Require().NoError(err)
	s.Equal([]byte("one"), stored.APIKeySealed)
}
