package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels *CustomModels

	Activities           *Activities
	APIKeys              *APIKeys
	AppOrganizations     *AppOrganizations
	Apps                 *Apps
	Guests               *Guests
	LimitGrants          *LimitGrants
	LimitGrantUses       *LimitGrantUses
	Limits               *Limits
	Notices              *Notices
	Organizations        *Organizations
	OrganizationTiers    *OrganizationTiers
	Policies             *Policies
	ProviderKeys         *ProviderKeys
	Spend                *Spend
	StreamAppTokens      *StreamAppTokens
	StreamFallbackUses   *StreamFallbackUses
	TelephonySuspensions *TelephonySuspensions
	UsageResets          *UsageResets
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels: &CustomModels{store: s},

		Activities:           &Activities{store: s},
		APIKeys:              &APIKeys{store: s},
		AppOrganizations:     &AppOrganizations{store: s},
		Apps:                 &Apps{store: s},
		Guests:               &Guests{store: s},
		LimitGrants:          &LimitGrants{store: s},
		LimitGrantUses:       &LimitGrantUses{store: s},
		Limits:               &Limits{store: s},
		Notices:              &Notices{store: s},
		Organizations:        &Organizations{store: s},
		OrganizationTiers:    &OrganizationTiers{store: s},
		Policies:             &Policies{store: s},
		ProviderKeys:         &ProviderKeys{store: s},
		Spend:                &Spend{store: s},
		StreamAppTokens:      &StreamAppTokens{store: s},
		StreamFallbackUses:   &StreamFallbackUses{store: s},
		TelephonySuspensions: &TelephonySuspensions{store: s},
		UsageResets:          &UsageResets{store: s},
	}
}
