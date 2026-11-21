package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels *CustomModels

	ConnectorDefinitions           *ConnectorDefinitions
	ConnectorBrokenRevisions       *ConnectorBrokenRevisions
	ConnectorConnections           *ConnectorConnections
	ConnectorAuthorizationAttempts *ConnectorAuthorizationAttempts
	ConnectionEventSubscriptions   *ConnectionEventSubscriptions
	EventDestinations              *EventDestinations
	EventDeliveries                *EventDeliveries
	ConnectorOAuthClients          *ConnectorOAuthClients
	ConnectorConfigTokens          *ConnectorConfigTokens
	ConnectorAuditEvents           *ConnectorAuditEvents
	ConnectorInvocations           *ConnectorInvocations
	ConnectorToolPins              *ConnectorToolPins
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels: &CustomModels{store: s},

		ConnectorDefinitions:           &ConnectorDefinitions{store: s},
		ConnectorBrokenRevisions:       &ConnectorBrokenRevisions{store: s},
		ConnectorConnections:           &ConnectorConnections{store: s},
		ConnectorAuthorizationAttempts: &ConnectorAuthorizationAttempts{store: s},
		ConnectionEventSubscriptions:   &ConnectionEventSubscriptions{store: s},
		EventDestinations:              &EventDestinations{store: s},
		EventDeliveries:                &EventDeliveries{store: s},
		ConnectorOAuthClients:          &ConnectorOAuthClients{store: s},
		ConnectorConfigTokens:          &ConnectorConfigTokens{store: s},
		ConnectorAuditEvents:           &ConnectorAuditEvents{store: s},
		ConnectorInvocations:           &ConnectorInvocations{store: s},
		ConnectorToolPins:              &ConnectorToolPins{store: s},
	}
}
