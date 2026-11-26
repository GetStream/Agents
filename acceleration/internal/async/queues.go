package async

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Canonical queue names. Keep in sync with config.AsyncQueueNames.
const (
	QueueEvents         Queue = "events"
	QueueWebhooks       Queue = "webhooks"
	QueueMedia          Queue = "media"
	QueueImagePost      Queue = "image-post"
	QueueImageDraw      Queue = "image-draw"
	QueueKnowledge      Queue = "knowledge"
	QueueKnowledgeFiles Queue = "knowledge-files"
	QueueSimulations    Queue = "simulations"
	QueueMaintenance    Queue = "maintenance"
)

// QueueSpec describes a canonical queue's scope and default priority weight.
type QueueSpec struct {
	Shard         bool
	Global        bool
	DefaultWeight int
}

// QueueCatalog defines every valid queue, its scope(s), and its default priority weight.
var QueueCatalog = map[Queue]QueueSpec{
	QueueEvents:         {Shard: true, DefaultWeight: 10},
	QueueWebhooks:       {Global: true, Shard: true, DefaultWeight: 6},
	QueueMedia:          {Shard: true, DefaultWeight: 4},
	QueueImagePost:      {Shard: true, DefaultWeight: 4},
	QueueImageDraw:      {Shard: true, DefaultWeight: 2},
	QueueKnowledge:      {Global: true, DefaultWeight: 2},
	QueueKnowledgeFiles: {Shard: true, DefaultWeight: 1},
	QueueSimulations:    {Shard: true, DefaultWeight: 1},
	QueueMaintenance:    {Shard: true, Global: true, DefaultWeight: 1},
}

// ValidQueue reports whether name is a registered queue name.
func ValidQueue(name string) bool {
	_, ok := QueueCatalog[Queue(name)]
	return ok
}

// QueueSupportsScope reports whether queue q can run in scope.
func QueueSupportsScope(q Queue, scope Scope) bool {
	spec, ok := QueueCatalog[q]
	if !ok {
		return false
	}
	if scope == ScopeShard {
		return spec.Shard
	}
	if scope == ScopeGlobal {
		return spec.Global
	}
	return false
}

// ParseQueues parses Celery-style queue specifications ("all", "events", "events:10")
// filtered to the requested scope. Returns an empty map if no selected queue applies to
// that scope.
func ParseQueues(entries []string, scope Scope) (map[Queue]int, error) {
	if len(entries) == 0 {
		entries = []string{"all"}
	}
	selected := map[Queue]int{}
	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if entry == "all" {
			for q, spec := range QueueCatalog {
				if QueueSupportsScope(q, scope) {
					if _, exists := selected[q]; !exists {
						selected[q] = spec.DefaultWeight
					}
				}
			}
			continue
		}
		name, weightRaw, hasWeight := strings.Cut(entry, ":")
		q := Queue(strings.TrimSpace(name))
		spec, ok := QueueCatalog[q]
		if !ok {
			return nil, fmt.Errorf("async: unknown queue %q", name)
		}
		weight := spec.DefaultWeight
		if hasWeight {
			parsed, err := strconv.Atoi(strings.TrimSpace(weightRaw))
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("async: invalid weight %q for queue %q", weightRaw, q)
			}
			weight = parsed
		}
		if QueueSupportsScope(q, scope) {
			selected[q] = weight
		}
	}
	return selected, nil
}

// QueueGroup is one worker pool's queue weights and concurrency cap. Capped queues
// (e.g. knowledge-files=2, image-draw=2) run in their own pool so heavy work cannot
// starve general queues.
type QueueGroup struct {
	Name        string
	Concurrency int
	Queues      map[Queue]int
}

// SplitQueueGroups partitions weights into one general pool plus one dedicated pool per
// queue that declares an explicit cap in caps.
func SplitQueueGroups(weights map[Queue]int, caps map[string]int, defaultConcurrency int) []QueueGroup {
	if len(weights) == 0 {
		return nil
	}
	if defaultConcurrency <= 0 {
		defaultConcurrency = 1
	}
	general := map[Queue]int{}
	var groups []QueueGroup
	for _, q := range slices.Sorted(maps.Keys(weights)) {
		if capN, capped := caps[string(q)]; capped && capN > 0 {
			groups = append(groups, QueueGroup{
				Name:        string(q),
				Concurrency: capN,
				Queues:      map[Queue]int{q: weights[q]},
			})
			continue
		}
		general[q] = weights[q]
	}
	if len(general) > 0 {
		groups = append([]QueueGroup{{
			Name:        "general",
			Concurrency: defaultConcurrency,
			Queues:      general,
		}}, groups...)
	}
	return groups
}
