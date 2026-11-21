package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels      *CustomModels
	PhoneNumbers      *PhoneNumbers
	SIPTrunks         *SIPTrunks
	StreamTrunks      *StreamTrunks
	CallBridges       *CallBridges
	CallResources     *CallResources
	CallLegs          *CallLegs
	CallParticipants  *CallParticipants
	PhoneCalls        *PhoneCalls
	Campaigns         *Campaigns
	CampaignContacts  *CampaignContacts
	CampaignAttempts  *CampaignAttempts
	BusinessProfiles  *BusinessProfiles
	UseCases          *UseCases
	ReviewLogs        *ReviewLogs
	OptOuts           *OptOuts
	SandboxRecipients *SandboxRecipients
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:      &CustomModels{store: s},
		PhoneNumbers:      &PhoneNumbers{store: s},
		SIPTrunks:         &SIPTrunks{store: s},
		StreamTrunks:      &StreamTrunks{store: s},
		CallBridges:       &CallBridges{store: s},
		CallResources:     &CallResources{store: s},
		CallLegs:          &CallLegs{store: s},
		CallParticipants:  &CallParticipants{store: s},
		PhoneCalls:        &PhoneCalls{store: s},
		Campaigns:         &Campaigns{store: s},
		CampaignContacts:  &CampaignContacts{store: s},
		CampaignAttempts:  &CampaignAttempts{store: s},
		BusinessProfiles:  &BusinessProfiles{store: s},
		UseCases:          &UseCases{store: s},
		ReviewLogs:        &ReviewLogs{store: s},
		OptOuts:           &OptOuts{store: s},
		SandboxRecipients: &SandboxRecipients{store: s},
	}
}
