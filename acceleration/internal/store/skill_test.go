//go:build integration

package store

import (
	"time"
)

// skill stores a skill of the config's, defaulting the fields a test does not care about.
func (s *StoreSuite) skill(config AgentConfig, name string) Skill {
	skill := Skill{CustomerID: config.CustomerID, ConfigID: config.ID, Name: name, Description: "does " + name, Instructions: "do " + name}
	s.Require().NoError(s.store.Skills.Insert(s.ctx, &skill))
	return skill
}

func (s *StoreSuite) TestASkillIsOnlyItsOwnCustomers() {
	config := s.boundConfig("acme-app")
	mine := s.skill(config, "refund")

	_, err := s.store.Skills.Get(s.ctx, "other-app", mine.ID)
	s.ErrorIs(err, ErrNoSkill)
	named, err := s.store.Skills.GetByNames(s.ctx, "other-app", config.ID, []string{"refund"})
	s.Require().NoError(err)
	s.Empty(named)
	listed, err := s.store.Skills.Select(s.ctx, "other-app", SkillFilter{})
	s.Require().NoError(err)
	s.Empty(listed)
	s.ErrorIs(s.store.Skills.Delete(s.ctx, "other-app", mine.ID), ErrNoSkill)
}

func (s *StoreSuite) TestAnInvalidSkillIsNotWritten() {
	config := s.boundConfig("acme-app")
	err := s.store.Skills.Insert(s.ctx, &Skill{CustomerID: "acme-app", ConfigID: config.ID, Name: "refund", DeadlineMs: -1})
	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("deadline_ms", invalid.Field)

	listed, err := s.store.Skills.Select(s.ctx, "acme-app", SkillFilter{ConfigID: config.ID})
	s.Require().NoError(err)
	s.Empty(listed)
}

func (s *StoreSuite) TestSkillsAreSelectedByConfig() {
	first, second := s.boundConfig("acme-app"), s.boundConfig("acme-app")
	refund := s.skill(first, "refund")
	s.skill(second, "refund")

	listed, err := s.store.Skills.Select(s.ctx, "acme-app", SkillFilter{ConfigID: first.ID})
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(refund.ID, listed[0].ID)
	every, err := s.store.Skills.Select(s.ctx, "acme-app", SkillFilter{})
	s.Require().NoError(err)
	s.Len(every, 2)
}

func (s *StoreSuite) TestAnEditedSkillIsReadWithItsNewInstructionsOnAnotherNode() {
	other := s.secondNode()
	config := s.boundConfig("acme-app")
	skill := s.skill(config, "refund")
	named, err := other.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund", "think"})
	s.Require().NoError(err)
	s.Require().Len(named, 1, "a name the config defines no skill for is left out")
	s.Equal("do refund", named[0].Instructions)

	skill.Instructions = "refund without asking"
	s.Require().NoError(s.store.Skills.Update(s.ctx, &skill))
	s.Eventually(func() bool {
		named, err := other.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
		return err == nil && len(named) == 1 && named[0].Instructions == "refund without asking"
	}, 5*time.Second, 10*time.Millisecond, "the other node is still following the old instructions")

	added := s.skill(config, "escalate")
	s.Eventually(func() bool {
		named, err := other.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"escalate"})
		return err == nil && len(named) == 1 && named[0].ID == added.ID
	}, 5*time.Second, 10*time.Millisecond, "the other node does not see the skill added")

	s.Require().NoError(s.store.Skills.Delete(s.ctx, "acme-app", added.ID))
	s.Eventually(func() bool {
		named, err := other.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"escalate"})
		return err == nil && len(named) == 0
	}, 5*time.Second, 10*time.Millisecond, "the other node still offers the deleted skill")
}

func (s *StoreSuite) TestSkillQueryBudgets() {
	config := s.boundConfig("acme-app")
	names := []string{}
	for i := range 20 {
		names = append(names, s.skill(config, "skill-"+string(rune('a'+i))).Name)
	}
	skill := Skill{CustomerID: "acme-app", ConfigID: config.ID, Name: "refund"}

	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Skills.Insert(s.ctx, &skill)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.Skills.Select(s.ctx, "acme-app", SkillFilter{ConfigID: config.ID})
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, names)
		s.Require().NoError(err)
		s.Len(named, 20)
	}), "every skill named is read at once")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Skills.Get(s.ctx, "acme-app", skill.ID)
		s.Require().NoError(err)
	}))
	skill.Instructions = "ask first"
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Skills.Update(s.ctx, &skill)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
	if s.store.cache != nil {
		for range 2 {
			_, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, names)
			s.Require().NoError(err)
		}
		s.Equal(0, s.countQueries(func() {
			s.Equal(0, s.countRedis(func() {
				_, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
				s.Require().NoError(err)
			}), "the config's skills are read from the local cache")
		}))
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.Skills.Update(s.ctx, &skill)) }))
		s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.Skills.Delete(s.ctx, "acme-app", skill.ID)) }))
		return
	}
	s.Equal(3, s.countQueries(func() { s.Require().NoError(s.store.Skills.Delete(s.ctx, "acme-app", skill.ID)) }), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
}
