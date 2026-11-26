//go:build integration

package main

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A page of changes holds an agent config's identity to what a config write holds it to: one
// naming a user id Stream keeps for itself is refused as a whole.
func (s *ReplicateRepeatSuite) TestAPageGivingAnAgentAReservedIdentityIsRefused() {
	parent, _ := s.copiedParentAndChild(nil)
	s.changed(parent, func(config *store.AgentConfig) { config.Identity.UserID = "!anon" })

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrInvalidImport)
	s.ErrorContains(err, `identity.user_id may not start with !: Stream keeps those user ids for itself`)
	s.Empty(s.copied(parent).Identity.UserID, "nothing of the page is written")
}
