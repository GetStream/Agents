//go:build integration

package store

import "time"

func (s *StoreSuite) TestResponseItemQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.opened("itemised", "app", at, nil)
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "itemised-response",
		SessionID: "itemised", CustomerID: "app", CreatedAt: at}))
	items := make([]AgentResponseItem, 0, 30)
	for i := range 30 {
		items = append(items, AgentResponseItem{ResponseID: "itemised-response", SessionID: "itemised", Ordinal: i,
			Kind: "message", Text: "part", At: at.Add(time.Minute)})
	}
	s.withinQueryBudgets([]queryBudget{
		{"InsertMany", 1, 0, func() error { return s.store.ResponseItems.InsertMany(s.ctx, items) }},
		{"Select", 0, 1, func() error {
			listed, err := s.store.ResponseItems.Select(s.ctx, "app", "itemised", ItemFilter{Limit: 10})
			s.Len(listed, 11)
			return err
		}},
		{"Redact", 1, 0, func() error {
			_, err := s.store.ResponseItems.Redact(s.ctx, time.Now(), 10)
			return err
		}},
	})
}

func (s *StoreSuite) TestABatchWithAnInvalidItemIsNotWritten() {
	s.opened("one", "app", time.Now(), nil)
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{ID: "answered", SessionID: "one", CustomerID: "app"}))
	err := s.store.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: "answered", SessionID: "one", Kind: "message", Ordinal: 0},
		{ResponseID: "answered", SessionID: "one", Ordinal: 1},
	})
	s.refusedUnwritten(err, "agent_response_items", "response_id", "answered")
}
