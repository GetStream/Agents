package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// maxCallEvents is the most decisions one call keeps, the latest ones, and
// defaultCallEvents how many are read back when the caller names no limit.
const (
	maxCallEvents     = 10_000
	defaultCallEvents = 1000
)

// maxFloorChecks is the most chorus-asr floor checks one call keeps, the latest ones: a
// timeline shows forty a turn, and the model is asked every 300 ms while the caller talks
// over the agent.
const maxFloorChecks = 4000

// The kinds of FloorCheck: with the floor free, whether the caller has finished, or while the
// agent was talking, whether they stop or shorten it, at a pause or while they talked on.
const (
	FloorCheckPause     = "pause"
	FloorCheckOverAgent = "over_agent"
)

// FloorCheck is one check the chorus-asr model made on the floor: where it was asked, every
// answer's probability, and what the router made of it. A value the model did not give is
// absent.
type FloorCheck struct {
	At        time.Time     `json:"at"`
	Kind      string        `json:"kind"`
	PausedMs  *float64      `json:"paused_ms,omitempty"`
	Options   []FloorOption `json:"options,omitempty"`
	Ruling    FloorRuling   `json:"ruling"`
	Acted     bool          `json:"acted"`
	LatencyMs float64       `json:"latency_ms"`
	Point     string        `json:"point,omitempty"`
}

// FloorOption is one answer the model was offered, and its probability.
type FloorOption struct {
	Option      string  `json:"option"`
	Probability float64 `json:"probability"`
}

// FloorRuling is what the model ruled the agent do once the turn has ended: Tool is the tool
// the caller wants, and Cannot that none of the agent's can do it.
type FloorRuling struct {
	Disposition string `json:"disposition"`
	Floor       string `json:"floor"`
	Tool        string `json:"tool,omitempty"`
	Cannot      bool   `json:"cannot,omitempty"`
}

// CallEvent is one judgement the conversation made about how to handle a call: why the
// agent waited rather than answering, why it read something as not meant for it, why it
// stopped mid-sentence.
type CallEvent struct {
	CustomerID string `json:"customer_id"`
	// CallID is the session the judgement was made in.
	CallID  string    `json:"call_id"`
	AgentID string    `json:"agent_id"`
	At      time.Time `json:"at"`
	// Kind is what was decided.
	Kind string `json:"kind"`
	// Reason is why, in words.
	Reason string `json:"reason"`
	// TurnID is the exchange it was about, so a judgement lines up with the timings of
	// the turn it produced.
	TurnID      string `json:"turn_id,omitempty"`
	Participant string `json:"participant,omitempty"`
	// Said is what was heard, or what the agent decided to say.
	Said string `json:"said,omitempty"`
	// LatencyMs is what the flow controller took to rule, where anything was asked.
	LatencyMs *float64 `json:"latency_ms,omitempty"`
	// FloorCheck makes the event one of the chorus-asr model's checks on the floor rather
	// than a judgement of the conversation's, about TurnID. Checks are kept apart from the
	// judgements (CallEvents.SelectFloorChecks), the latest maxFloorChecks of a call by when
	// they were asked, so that a call's checks never crowd its judgements out.
	FloorCheck *FloorCheck `json:"floor_check,omitempty"`
}

// Validate refuses a judgement that names no customer or call.
func (e *CallEvent) Validate() error {
	return validate("call event").
		required("customer_id", e.CustomerID).
		required("call_id", e.CallID).
		done()
}

// CallEvents are the recent judgements and floor checks of every call, in the Redis every
// router shares, so a call's timeline reads the same from whichever router serves it.
// Without Redis none are kept.
type CallEvents struct {
	store *Store
}

// InsertMany keeps a batch of judgements for RecentFor after the last one, in one round trip
// however many calls the batch spans. A call keeps its latest maxCallEvents, and its
// maxFloorChecks floor checks asked last.
func (r *CallEvents) InsertMany(ctx context.Context, events []CallEvent) error {
	if len(events) == 0 {
		return nil
	}
	for i := range events {
		if err := events[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}
	client := r.store.sharedRedis()
	if client == nil {
		return nil
	}
	byCall := map[string][]string{}
	var order []string
	// A call's floor checks are ranked by when they were asked, as a check over the agent is
	// reported once its words are in, after checks asked since: its latest maxFloorChecks are
	// kept by that, not by when they came.
	checks := map[string][]rueidis.Completed{}
	var checkKeys []string
	for _, event := range events {
		if event.At.IsZero() {
			event.At = time.Now().UTC()
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: encode call event: %w", err))
		}
		if event.FloorCheck != nil {
			key := floorChecksKey(event.CustomerID, event.CallID)
			if _, seen := checks[key]; !seen {
				checkKeys = append(checkKeys, key)
			}
			checks[key] = append(checks[key],
				client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(event.At.UnixMilli()), string(encoded)).Build())
			continue
		}
		key := callEventsKey(event.CustomerID, event.CallID)
		if _, seen := byCall[key]; !seen {
			order = append(order, key)
		}
		byCall[key] = append(byCall[key], string(encoded))
	}

	commands := make(rueidis.Commands, 0, 3*(len(order)+len(checkKeys)))
	for _, key := range order {
		commands = append(commands,
			client.B().Rpush().Key(key).Element(byCall[key]...).Build(),
			client.B().Ltrim().Key(key).Start(-maxCallEvents).Stop(-1).Build(),
			client.B().Pexpire().Key(key).Milliseconds(RecentFor.Milliseconds()).Build(),
		)
	}
	for _, key := range checkKeys {
		commands = append(commands, checks[key]...)
		commands = append(commands,
			client.B().Zremrangebyrank().Key(key).Start(0).Stop(-maxFloorChecks-1).Build(),
			client.B().Pexpire().Key(key).Milliseconds(RecentFor.Milliseconds()).Build(),
		)
	}
	for _, response := range client.DoMulti(ctx, commands...) {
		if err := response.Error(); err != nil {
			return stack.Wrap(fmt.Errorf("store: record call events: %w", err))
		}
	}
	return nil
}

// Select returns up to limit of the judgements made on one call between from and to, oldest
// first. A nil to reads up to now. Judgements older than RecentFor are gone.
func (r *CallEvents) Select(ctx context.Context, customerID, callID string, from time.Time, to *time.Time, limit int) ([]CallEvent, error) {
	if customerID == "" || callID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a call id are required"))
	}
	client := r.store.sharedRedis()
	if client == nil {
		return nil, nil
	}
	members, err := client.Do(ctx, client.B().Lrange().Key(callEventsKey(customerID, callID)).
		Start(0).Stop(-1).Build()).AsStrSlice()
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call events: %w", err))
	}

	if limit <= 0 {
		limit = defaultCallEvents
	}
	if forgotten := time.Now().Add(-RecentFor); from.Before(forgotten) {
		from = forgotten
	}
	events := make([]CallEvent, 0, len(members))
	for _, member := range members {
		var event CallEvent
		if err := json.Unmarshal([]byte(member), &event); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: decode call event: %w", err))
		}
		if event.At.Before(from) || (to != nil && event.At.After(*to)) {
			continue
		}
		events = append(events, event)
	}
	slices.SortStableFunc(events, func(a, b CallEvent) int { return a.At.Compare(b.At) })
	return events[:min(len(events), limit)], nil
}

// SelectFloorChecks returns the chorus-asr floor checks kept for one call between from and
// to, oldest first, each with the turn it was about when that was known as it was asked. A
// nil to reads up to now. Checks older than RecentFor are left out, as their turns are gone.
func (r *CallEvents) SelectFloorChecks(ctx context.Context, customerID, callID string, from time.Time, to *time.Time) ([]CallEvent, error) {
	if customerID == "" || callID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a call id are required"))
	}
	client := r.store.sharedRedis()
	if client == nil {
		return nil, nil
	}
	now := time.Now()
	if forgotten := now.Add(-RecentFor); from.Before(forgotten) {
		from = forgotten
	}
	if to == nil {
		to = &now
	}
	until := strconv.FormatInt(to.UnixMilli(), 10)
	members, err := client.Do(ctx, client.B().Zrange().Key(floorChecksKey(customerID, callID)).
		Min(strconv.FormatInt(from.UnixMilli(), 10)).Max(until).Byscore().Build()).AsStrSlice()
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: floor checks: %w", err))
	}
	checks := make([]CallEvent, 0, len(members))
	for _, member := range members {
		var check CallEvent
		if err := json.Unmarshal([]byte(member), &check); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: decode floor check: %w", err))
		}
		if check.FloorCheck == nil || check.At.Before(from) || check.At.After(*to) {
			continue
		}
		checks = append(checks, check)
	}
	slices.SortStableFunc(checks, func(a, b CallEvent) int { return a.At.Compare(b.At) })
	return checks, nil
}

func callEventsKey(customerID, callID string) string {
	return "call_events:{" + customerID + "}:" + callID
}

func floorChecksKey(customerID, callID string) string {
	return "floor_checks:{" + customerID + "}:" + callID
}
