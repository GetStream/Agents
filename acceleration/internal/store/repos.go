package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels          *CustomModels
	Sessions              *Sessions
	SessionTools          *SessionTools
	Responses             *Responses
	ResponseItems         *ResponseItems
	BranchMessages        *BranchMessages
	BranchSwitches        *BranchSwitches
	ConversationDocuments *ConversationDocuments
	Calls                 *Calls
	Recordings            *Recordings
	Sandboxes             *Sandboxes
	Turns                 *Turns
	CallEvents            *CallEvents
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:          &CustomModels{store: s},
		Sessions:              &Sessions{store: s},
		SessionTools:          &SessionTools{store: s},
		Responses:             &Responses{store: s},
		ResponseItems:         &ResponseItems{store: s},
		BranchMessages:        &BranchMessages{store: s},
		BranchSwitches:        &BranchSwitches{store: s},
		ConversationDocuments: &ConversationDocuments{store: s},
		Calls:                 &Calls{store: s},
		Recordings:            &Recordings{store: s},
		Sandboxes:             &Sandboxes{store: s},
		Turns:                 &Turns{store: s},
		CallEvents:            &CallEvents{store: s},
	}
}
