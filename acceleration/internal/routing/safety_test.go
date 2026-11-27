package routing

import "github.com/GetStream/Vision-Agents/acceleration/internal/auth"

func (s *RoutingSuite) TestAnAppAndUserSplitAtAnotherPlaceHaveAnotherSafetyIdentifier() {
	key := []byte("deployment safety key")

	s.NotEqual(safetyIdentifier(key, "a", "", "bc"), safetyIdentifier(key, "ab", "", "c"),
		"app a's user bc and app ab's user c are two people")
	s.NotEqual(safetyIdentifier(key, "a", "", "bc"), safetyIdentifier(key, "a", "b", "c"),
		"app a's user bc is not app a's party b with user c")
}

func (s *RoutingSuite) TestTheSafetyKeyIsTheKeyringsOwnForSafetyIdentifiers() {
	node, err := auth.NewSealer("deployment kek")
	s.Require().NoError(err)
	otherNode, err := auth.NewSealer("deployment kek")
	s.Require().NoError(err)
	otherDeployment, err := auth.NewSealer("another kek")
	s.Require().NoError(err)
	nodeCredential, _ := node.Derive("node-forward-v1")

	key := SafetyKey(node)

	s.Len(key, 32)
	s.Equal(key, SafetyKey(otherNode), "every node with the keyring hashes a user to the same identifier")
	s.NotEqual(key, SafetyKey(otherDeployment), "another keyring is another key")
	s.NotEqual(nodeCredential, key, "the key is derived for safety identifiers alone")
	s.Nil(SafetyKey(nil), "with no keyring there is no key")
}
