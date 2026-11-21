package store

// repos are the store's repositories, one per model, reached as fields of Store:
// s.store.CustomModels.Get(ctx, customerID, id).
type repos struct {
	CustomModels      *CustomModels
	Requests          *Requests
	Buckets           *Buckets
	TagBuckets        *TagBuckets
	TurnBuckets       *TurnBuckets
	AgentLogs         *AgentLogs
	AuditEntries      *AuditEntries
	MoveCursors       *MoveCursors
	PriceChanges      *PriceChanges
	PriceEntries      *PriceEntries
	ScheduledTasks    *ScheduledTasks
	ScheduledTaskRuns *ScheduledTaskRuns
}

// wire gives every repository the store it reads and writes through.
func (s *Store) wire() {
	s.repos = repos{
		CustomModels:      &CustomModels{store: s},
		Requests:          &Requests{store: s},
		Buckets:           &Buckets{store: s},
		TagBuckets:        &TagBuckets{store: s},
		TurnBuckets:       &TurnBuckets{store: s},
		AgentLogs:         &AgentLogs{store: s},
		AuditEntries:      &AuditEntries{store: s},
		MoveCursors:       &MoveCursors{store: s},
		PriceChanges:      &PriceChanges{store: s},
		PriceEntries:      &PriceEntries{store: s},
		ScheduledTasks:    &ScheduledTasks{store: s},
		ScheduledTaskRuns: &ScheduledTaskRuns{store: s},
	}
}
