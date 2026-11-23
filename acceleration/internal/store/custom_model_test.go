//go:build integration

package store

import (
	"errors"
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
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.CustomModels.Delete(s.ctx, "app", model.ID)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
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
