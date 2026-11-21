package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels        *CustomModels
	AgentConfigs        *AgentConfigs
	Skills              *Skills
	RouterConfigs       *RouterConfigs
	Voices              *Voices
	VoiceSamples        *VoiceSamples
	VoiceBindings       *VoiceBindings
	KnowledgeURLs       *KnowledgeURLs
	KnowledgeDocuments  *KnowledgeDocuments
	KnowledgeNamespaces *KnowledgeNamespaces
	Simulations         *Simulations
	SimulationRuns      *SimulationRuns
	SimulationCases     *SimulationCases
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:        &CustomModels{store: s},
		AgentConfigs:        &AgentConfigs{store: s},
		Skills:              &Skills{store: s},
		RouterConfigs:       &RouterConfigs{store: s},
		Voices:              &Voices{store: s},
		VoiceSamples:        &VoiceSamples{store: s},
		VoiceBindings:       &VoiceBindings{store: s},
		KnowledgeURLs:       &KnowledgeURLs{store: s},
		KnowledgeDocuments:  &KnowledgeDocuments{store: s},
		KnowledgeNamespaces: &KnowledgeNamespaces{store: s},
		Simulations:         &Simulations{store: s},
		SimulationRuns:      &SimulationRuns{store: s},
		SimulationCases:     &SimulationCases{store: s},
	}
}
