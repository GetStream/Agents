package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels          *CustomModels
	ChannelAccounts       *ChannelAccounts
	ChannelDeliveries     *ChannelDeliveries
	ChannelInbounds       *ChannelInbounds
	ChannelLinks          *ChannelLinks
	ChannelThreadMembers  *ChannelThreadMembers
	ChannelThreadMessages *ChannelThreadMessages
	ChannelThreads        *ChannelThreads
	ChannelTurns          *ChannelTurns
	ChatInbounds          *ChatInbounds
	ContactMapEntries     *ContactMapEntries
	Episodes              *Episodes
	ImageJobs             *ImageJobs
	UserPhones            *UserPhones
	WaitingMessages       *WaitingMessages
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:          &CustomModels{store: s},
		ChannelAccounts:       &ChannelAccounts{store: s},
		ChannelDeliveries:     &ChannelDeliveries{store: s},
		ChannelInbounds:       &ChannelInbounds{store: s},
		ChannelLinks:          &ChannelLinks{store: s},
		ChannelThreadMembers:  &ChannelThreadMembers{store: s},
		ChannelThreadMessages: &ChannelThreadMessages{store: s},
		ChannelThreads:        &ChannelThreads{store: s},
		ChannelTurns:          &ChannelTurns{store: s},
		ChatInbounds:          &ChatInbounds{store: s},
		ContactMapEntries:     &ContactMapEntries{store: s},
		Episodes:              &Episodes{store: s},
		ImageJobs:             &ImageJobs{store: s},
		UserPhones:            &UserPhones{store: s},
		WaitingMessages:       &WaitingMessages{store: s},
	}
}
