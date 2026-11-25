package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels                   *CustomModels
	APIKeys                        *APIKeys
	Activities                     *Activities
	ActivityDigests                *ActivityDigests
	AgentConfigs                   *AgentConfigs
	AgentLogs                      *AgentLogs
	AppOrganizations               *AppOrganizations
	Apps                           *Apps
	AuditEntries                   *AuditEntries
	BranchMessages                 *BranchMessages
	BranchSwitches                 *BranchSwitches
	Buckets                        *Buckets
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
	ChannelLinkFailures            *ChannelLinkFailures
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
	ImageJobs                      *ImageJobs
	KnowledgeDocuments             *KnowledgeDocuments
	KnowledgeNamespaces            *KnowledgeNamespaces
	KnowledgeURLs                  *KnowledgeURLs
	LimitGrantUses                 *LimitGrantUses
	LimitGrants                    *LimitGrants
	Limits                         *Limits
	MoveCursors                    *MoveCursors
	Notices                        *Notices
	OptOuts                        *OptOuts
	OrganizationTiers              *OrganizationTiers
	Organizations                  *Organizations
	PhoneNumberOrders              *PhoneNumberOrders
	PhoneNumbers                   *PhoneNumbers
	Policies                       *Policies
	PriceChanges                   *PriceChanges
	PriceEntries                   *PriceEntries
	ProviderKeys                   *ProviderKeys
	Recordings                     *Recordings
	Requests                       *Requests
	ResponseItems                  *ResponseItems
	Responses                      *Responses
	ReviewLogs                     *ReviewLogs
	RouterConfigs                  *RouterConfigs
	SIPTrunks                      *SIPTrunks
	SandboxRecipients              *SandboxRecipients
	Sandboxes                      *Sandboxes
	ScheduledTaskRuns              *ScheduledTaskRuns
	ScheduledTasks                 *ScheduledTasks
	SessionTools                   *SessionTools
	Sessions                       *Sessions
	SimulationCases                *SimulationCases
	SimulationRuns                 *SimulationRuns
	Simulations                    *Simulations
	Skills                         *Skills
	Spend                          *Spend
	StreamAppTokens                *StreamAppTokens
	StreamFallbackUses             *StreamFallbackUses
	StreamTrunks                   *StreamTrunks
	TagBuckets                     *TagBuckets
	TelephonySuspensions           *TelephonySuspensions
	TurnBuckets                    *TurnBuckets
	Turns                          *Turns
	UsageResets                    *UsageResets
	UseCases                       *UseCases
	UserPhones                     *UserPhones
	Users                          *Users
	VoiceBindings                  *VoiceBindings
	VoiceSamples                   *VoiceSamples
	Voices                         *Voices
	WaitingMessages                *WaitingMessages
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:                   &CustomModels{store: s},
		APIKeys:                        &APIKeys{store: s},
		Activities:                     &Activities{store: s},
		ActivityDigests:                &ActivityDigests{store: s},
		AgentConfigs:                   &AgentConfigs{store: s},
		AgentLogs:                      &AgentLogs{store: s},
		AppOrganizations:               &AppOrganizations{store: s},
		Apps:                           &Apps{store: s},
		AuditEntries:                   &AuditEntries{store: s},
		BranchMessages:                 &BranchMessages{store: s},
		BranchSwitches:                 &BranchSwitches{store: s},
		Buckets:                        &Buckets{store: s},
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
		ChannelLinkFailures:            &ChannelLinkFailures{store: s},
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
		ImageJobs:                      &ImageJobs{store: s},
		KnowledgeDocuments:             &KnowledgeDocuments{store: s},
		KnowledgeNamespaces:            &KnowledgeNamespaces{store: s},
		KnowledgeURLs:                  &KnowledgeURLs{store: s},
		LimitGrantUses:                 &LimitGrantUses{store: s},
		LimitGrants:                    &LimitGrants{store: s},
		Limits:                         &Limits{store: s},
		MoveCursors:                    &MoveCursors{store: s},
		Notices:                        &Notices{store: s},
		OptOuts:                        &OptOuts{store: s},
		OrganizationTiers:              &OrganizationTiers{store: s},
		Organizations:                  &Organizations{store: s},
		PhoneNumberOrders:              &PhoneNumberOrders{store: s},
		PhoneNumbers:                   &PhoneNumbers{store: s},
		Policies:                       &Policies{store: s},
		PriceChanges:                   &PriceChanges{store: s},
		PriceEntries:                   &PriceEntries{store: s},
		ProviderKeys:                   &ProviderKeys{store: s},
		Recordings:                     &Recordings{store: s},
		Requests:                       &Requests{store: s},
		ResponseItems:                  &ResponseItems{store: s},
		Responses:                      &Responses{store: s},
		ReviewLogs:                     &ReviewLogs{store: s},
		RouterConfigs:                  &RouterConfigs{store: s},
		SIPTrunks:                      &SIPTrunks{store: s},
		SandboxRecipients:              &SandboxRecipients{store: s},
		Sandboxes:                      &Sandboxes{store: s},
		ScheduledTaskRuns:              &ScheduledTaskRuns{store: s},
		ScheduledTasks:                 &ScheduledTasks{store: s},
		SessionTools:                   &SessionTools{store: s},
		Sessions:                       &Sessions{store: s},
		SimulationCases:                &SimulationCases{store: s},
		SimulationRuns:                 &SimulationRuns{store: s},
		Simulations:                    &Simulations{store: s},
		Skills:                         &Skills{store: s},
		Spend:                          &Spend{store: s},
		StreamAppTokens:                &StreamAppTokens{store: s},
		StreamFallbackUses:             &StreamFallbackUses{store: s},
		StreamTrunks:                   &StreamTrunks{store: s},
		TagBuckets:                     &TagBuckets{store: s},
		TelephonySuspensions:           &TelephonySuspensions{store: s},
		TurnBuckets:                    &TurnBuckets{store: s},
		Turns:                          &Turns{store: s},
		UsageResets:                    &UsageResets{store: s},
		UseCases:                       &UseCases{store: s},
		UserPhones:                     &UserPhones{store: s},
		Users:                          newUsers(s),
		VoiceBindings:                  &VoiceBindings{store: s},
		VoiceSamples:                   &VoiceSamples{store: s},
		Voices:                         &Voices{store: s},
		WaitingMessages:                &WaitingMessages{store: s},
	}
}
