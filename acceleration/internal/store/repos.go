package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels                   *CustomModels
	APIKeys                        *APIKeys
	Activities                     *Activities
	AppOrganizations               *AppOrganizations
	Apps                           *Apps
	BranchMessages                 *BranchMessages
	BranchSwitches                 *BranchSwitches
	BusinessProfiles               *BusinessProfiles
	CallBridges                    *CallBridges
	CallEvents                     *CallEvents
	CallLegs                       *CallLegs
	CallParticipants               *CallParticipants
	CallResources                  *CallResources
	Calls                          *Calls
	CampaignAttempts               *CampaignAttempts
	CampaignContacts               *CampaignContacts
	Campaigns                      *Campaigns
	ChannelAccounts                *ChannelAccounts
	ChannelDeliveries              *ChannelDeliveries
	ChannelInbounds                *ChannelInbounds
	ChannelLinks                   *ChannelLinks
	ChannelThreadMembers           *ChannelThreadMembers
	ChannelThreadMessages          *ChannelThreadMessages
	ChannelThreads                 *ChannelThreads
	ChannelTurns                   *ChannelTurns
	ChatInbounds                   *ChatInbounds
	ConnectionEventSubscriptions   *ConnectionEventSubscriptions
	ConnectorAuditEvents           *ConnectorAuditEvents
	ConnectorAuthorizationAttempts *ConnectorAuthorizationAttempts
	ConnectorBrokenRevisions       *ConnectorBrokenRevisions
	ConnectorConfigTokens          *ConnectorConfigTokens
	ConnectorConnections           *ConnectorConnections
	ConnectorDefinitions           *ConnectorDefinitions
	ConnectorInvocations           *ConnectorInvocations
	ConnectorOAuthClients          *ConnectorOAuthClients
	ConnectorToolPins              *ConnectorToolPins
	ContactMapEntries              *ContactMapEntries
	ConversationDocuments          *ConversationDocuments
	DeliveriesSeen                 *DeliveriesSeen
	Episodes                       *Episodes
	EventDeliveries                *EventDeliveries
	EventDestinations              *EventDestinations
	Guests                         *Guests
	ImageJobs                      *ImageJobs
	LimitGrantUses                 *LimitGrantUses
	LimitGrants                    *LimitGrants
	Limits                         *Limits
	Notices                        *Notices
	OptOuts                        *OptOuts
	OrganizationTiers              *OrganizationTiers
	Organizations                  *Organizations
	PhoneNumbers                   *PhoneNumbers
	Policies                       *Policies
	ProviderKeys                   *ProviderKeys
	Recordings                     *Recordings
	ResponseItems                  *ResponseItems
	Responses                      *Responses
	ReviewLogs                     *ReviewLogs
	SIPTrunks                      *SIPTrunks
	SandboxRecipients              *SandboxRecipients
	Sandboxes                      *Sandboxes
	SessionTools                   *SessionTools
	Sessions                       *Sessions
	Spend                          *Spend
	StreamAppTokens                *StreamAppTokens
	StreamFallbackUses             *StreamFallbackUses
	StreamTrunks                   *StreamTrunks
	TelephonySuspensions           *TelephonySuspensions
	Turns                          *Turns
	UsageResets                    *UsageResets
	UseCases                       *UseCases
	UserPhones                     *UserPhones
	WaitingMessages                *WaitingMessages
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:                   &CustomModels{store: s},
		APIKeys:                        &APIKeys{store: s},
		Activities:                     &Activities{store: s},
		AppOrganizations:               &AppOrganizations{store: s},
		Apps:                           &Apps{store: s},
		BranchMessages:                 &BranchMessages{store: s},
		BranchSwitches:                 &BranchSwitches{store: s},
		BusinessProfiles:               &BusinessProfiles{store: s},
		CallBridges:                    &CallBridges{store: s},
		CallEvents:                     &CallEvents{store: s},
		CallLegs:                       &CallLegs{store: s},
		CallParticipants:               &CallParticipants{store: s},
		CallResources:                  &CallResources{store: s},
		Calls:                          &Calls{store: s},
		CampaignAttempts:               &CampaignAttempts{store: s},
		CampaignContacts:               &CampaignContacts{store: s},
		Campaigns:                      &Campaigns{store: s},
		ChannelAccounts:                &ChannelAccounts{store: s},
		ChannelDeliveries:              &ChannelDeliveries{store: s},
		ChannelInbounds:                &ChannelInbounds{store: s},
		ChannelLinks:                   &ChannelLinks{store: s},
		ChannelThreadMembers:           &ChannelThreadMembers{store: s},
		ChannelThreadMessages:          &ChannelThreadMessages{store: s},
		ChannelThreads:                 &ChannelThreads{store: s},
		ChannelTurns:                   &ChannelTurns{store: s},
		ChatInbounds:                   &ChatInbounds{store: s},
		ConnectionEventSubscriptions:   &ConnectionEventSubscriptions{store: s},
		ConnectorAuditEvents:           &ConnectorAuditEvents{store: s},
		ConnectorAuthorizationAttempts: &ConnectorAuthorizationAttempts{store: s},
		ConnectorBrokenRevisions:       &ConnectorBrokenRevisions{store: s},
		ConnectorConfigTokens:          &ConnectorConfigTokens{store: s},
		ConnectorConnections:           &ConnectorConnections{store: s},
		ConnectorDefinitions:           &ConnectorDefinitions{store: s},
		ConnectorInvocations:           &ConnectorInvocations{store: s},
		ConnectorOAuthClients:          &ConnectorOAuthClients{store: s},
		ConnectorToolPins:              &ConnectorToolPins{store: s},
		ContactMapEntries:              &ContactMapEntries{store: s},
		ConversationDocuments:          &ConversationDocuments{store: s},
		DeliveriesSeen:                 &DeliveriesSeen{store: s},
		Episodes:                       &Episodes{store: s},
		EventDeliveries:                &EventDeliveries{store: s},
		EventDestinations:              &EventDestinations{store: s},
		Guests:                         &Guests{store: s},
		ImageJobs:                      &ImageJobs{store: s},
		LimitGrantUses:                 &LimitGrantUses{store: s},
		LimitGrants:                    &LimitGrants{store: s},
		Limits:                         &Limits{store: s},
		Notices:                        &Notices{store: s},
		OptOuts:                        &OptOuts{store: s},
		OrganizationTiers:              &OrganizationTiers{store: s},
		Organizations:                  &Organizations{store: s},
		PhoneNumbers:                   &PhoneNumbers{store: s},
		Policies:                       &Policies{store: s},
		ProviderKeys:                   &ProviderKeys{store: s},
		Recordings:                     &Recordings{store: s},
		ResponseItems:                  &ResponseItems{store: s},
		Responses:                      &Responses{store: s},
		ReviewLogs:                     &ReviewLogs{store: s},
		SIPTrunks:                      &SIPTrunks{store: s},
		SandboxRecipients:              &SandboxRecipients{store: s},
		Sandboxes:                      &Sandboxes{store: s},
		SessionTools:                   &SessionTools{store: s},
		Sessions:                       &Sessions{store: s},
		Spend:                          &Spend{store: s},
		StreamAppTokens:                &StreamAppTokens{store: s},
		StreamFallbackUses:             &StreamFallbackUses{store: s},
		StreamTrunks:                   &StreamTrunks{store: s},
		TelephonySuspensions:           &TelephonySuspensions{store: s},
		Turns:                          &Turns{store: s},
		UsageResets:                    &UsageResets{store: s},
		UseCases:                       &UseCases{store: s},
		UserPhones:                     &UserPhones{store: s},
		WaitingMessages:                &WaitingMessages{store: s},
	}
}
