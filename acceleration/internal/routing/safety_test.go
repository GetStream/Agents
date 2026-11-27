package routing

func (s *RoutingSuite) TestAnAppAndUserSplitAtAnotherPlaceHaveAnotherSafetyIdentifier() {
	key := []byte("deployment safety key")

	s.NotEqual(safetyIdentifier(key, "a", "bc"), safetyIdentifier(key, "ab", "c"),
		"app a's user bc and app ab's user c are two people")
}
