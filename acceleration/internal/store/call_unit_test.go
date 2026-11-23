package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallValidate(t *testing.T) {
	low, high := 0, 6
	refuses(t, func() Call {
		return Call{ID: "c", CustomerID: "app", SessionID: "s", Direction: Outbound}
	}, (*Call).Validate, map[string]func(*Call){
		"id":           func(c *Call) { c.ID = "" },
		"customer_id":  func(c *Call) { c.CustomerID = "" },
		"direction":    func(c *Call) { c.Direction = "sideways" },
		"review_score": func(c *Call) { c.ReviewScore = &high },
	})
	require.Error(t, (&Call{ID: "c", CustomerID: "app", ReviewScore: &low}).Validate(), "a score below 1 is refused too")
}
