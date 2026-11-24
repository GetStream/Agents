package store

import "testing"

func TestConversationDocumentValidate(t *testing.T) {
	refuses(t, func() ConversationDocument {
		return ConversationDocument{ID: "d", CustomerID: "app", SessionID: "s", Pages: 3}
	}, (*ConversationDocument).Validate, map[string]func(*ConversationDocument){
		"id":          func(d *ConversationDocument) { d.ID = "" },
		"customer_id": func(d *ConversationDocument) { d.CustomerID = "" },
		"session_id":  func(d *ConversationDocument) { d.SessionID = "" },
		"pages":       func(d *ConversationDocument) { d.Pages = -1 },
	})
}
