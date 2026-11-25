package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// RecentFor is how long a call's turns and decisions are kept. They explain a call somebody
// is looking at now; what lasts is in agent_logs and the turn stats.
const RecentFor = time.Hour

// Turn is one exchange in a conversation, measured the way the caller experienced it.
// The legs are pointers because a pipeline need not have all of them, and an interrupted
// turn stops partway through.
type Turn struct {
	CustomerID string `json:"customer_id"`
	AgentID    string `json:"agent_id"`
	CallID     string `json:"call_id,omitempty"`
	// TurnID is the agent's own identifier for the exchange, unique per agent.
	TurnID             string            `json:"turn_id"`
	Tags               map[string]string `json:"tags,omitempty"`
	StartedAt          time.Time         `json:"started_at"`
	CadenceMs          *float64          `json:"cadence_ms,omitempty"`
	DecisionMs         *float64          `json:"decision_ms,omitempty"`
	ModelToFirstTextMs *float64          `json:"model_to_first_text_ms,omitempty"`
	TextToTTSMs        *float64          `json:"text_to_tts_ms,omitempty"`
	TTSToAudioMs       *float64          `json:"tts_to_audio_ms,omitempty"`
	// STTLatencyMs is the provider's decode time for the transcript that settled the turn.
	STTLatencyMs *float64 `json:"stt_latency_ms,omitempty"`
	// LLMTTFTMs is the wait between asking the model and its first token.
	LLMTTFTMs *float64 `json:"llm_ttft_ms,omitempty"`
	// TTSTTFBMs is the wait between sending the first sentence and the first audio.
	TTSTTFBMs *float64 `json:"tts_ttfb_ms,omitempty"`
	// RoundtripMs is the whole delay: settled transcript to first audio published.
	RoundtripMs *float64 `json:"roundtrip_ms,omitempty"`
	// SpeechEndToAudioMs is voice in to voice out.
	SpeechEndToAudioMs *float64 `json:"speech_end_to_audio_ms,omitempty"`
	// FirstFrameQueuedMs is settled transcript to the edge queueing the first frame of the reply,
	// FirstAudibleFrameMs to the track taking the first frame that was not silence, and
	// SpeechEndToAudibleMs is SpeechEndToAudioMs measured to that moment. RoundtripMs stops at
	// publishing returning, which for a long first chunk is later than both.
	FirstFrameQueuedMs   *float64 `json:"first_frame_queued_ms,omitempty"`
	FirstAudibleFrameMs  *float64 `json:"first_audible_frame_ms,omitempty"`
	SpeechEndToAudibleMs *float64 `json:"speech_end_to_audible_ms,omitempty"`
	// AudioOutMs is how much speech the agent published for this turn.
	AudioOutMs *float64 `json:"audio_out_ms,omitempty"`
	// AudioDroppedMs is speech that was synthesised for this turn but never published.
	// Set on a turn that was not interrupted, it means the agent cut itself off.
	AudioDroppedMs *float64 `json:"audio_dropped_ms,omitempty"`
	Interrupted    bool     `json:"interrupted,omitempty"`
	// Guardrail is the guardrail's check of the turn. Nil for a turn nothing screened.
	Guardrail *GuardrailCheck `json:"guardrail,omitempty"`
}

// GuardrailCheck is one check a guardrail made of a turn, allowed or refused. A value the
// check did not produce is absent: a webhook answers yes or no, and a check that failed
// answered nothing.
type GuardrailCheck struct {
	// Kind is how the turn was checked: decision_model, llm or webhook.
	Kind string `json:"kind"`
	// Model is the "provider/model" that answered.
	Model       string   `json:"model,omitempty"`
	Probability *float64 `json:"probability,omitempty"`
	Threshold   *float64 `json:"threshold,omitempty"`
	Allowed     bool     `json:"allowed"`
	LatencyMs   float64  `json:"latency_ms"`
	// HeldMs is how long a reply the model had begun waited on the verdict.
	HeldMs float64 `json:"held_ms,omitempty"`
	// Mode is when the check ran: parallel or blocking.
	Mode string `json:"mode"`
	// Failed is a check that could not be made, whose turn was answered anyway.
	Failed bool `json:"failed,omitempty"`
}

// Validate refuses a turn that names no customer, agent or turn.
func (t *Turn) Validate() error {
	return validate("turn").
		required("customer_id", t.CustomerID).
		required("agent_id", t.AgentID).
		required("turn_id", t.TurnID).
		done()
}

// Turns are the recent turns of every agent, in the Redis every router shares: kept by agent
// and ordered by when they started, so a call is read back by the agent and the window it ran
// in, from whichever router serves the read. Without Redis none are kept.
type Turns struct {
	store *Store
}

// Insert keeps one turn for RecentFor, in one round trip.
func (r *Turns) Insert(ctx context.Context, turn Turn) error {
	if err := turn.Validate(); err != nil {
		return stack.Wrap(err)
	}
	client := r.store.sharedRedis()
	if client == nil {
		return nil
	}
	member, err := json.Marshal(turn)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: encode turn: %w", err))
	}

	key := turnsKey(turn.CustomerID, turn.AgentID)
	forgotten := time.Now().Add(-RecentFor).UnixMilli()
	for _, response := range client.DoMulti(ctx,
		client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(turn.StartedAt.UnixMilli()), string(member)).Build(),
		client.B().Zremrangebyscore().Key(key).Min("-inf").Max("("+strconv.FormatInt(forgotten, 10)).Build(),
		client.B().Pexpire().Key(key).Milliseconds(RecentFor.Milliseconds()).Build(),
	) {
		if err := response.Error(); err != nil {
			return stack.Wrap(fmt.Errorf("store: record turn: %w", err))
		}
	}
	return nil
}

// Select returns the turns one agent finished between from and to, oldest first. A nil to
// reads up to now. Turns older than RecentFor are gone.
func (r *Turns) Select(ctx context.Context, customerID, agentID string, from time.Time, to *time.Time) ([]Turn, error) {
	if customerID == "" || agentID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and an agent id are required"))
	}
	client := r.store.sharedRedis()
	if client == nil {
		return nil, nil
	}
	until := "+inf"
	if to != nil {
		until = strconv.FormatInt(to.UnixMilli(), 10)
	}
	members, err := client.Do(ctx, client.B().Zrange().Key(turnsKey(customerID, agentID)).
		Min(strconv.FormatInt(from.UnixMilli(), 10)).Max(until).Byscore().Build()).AsStrSlice()
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call turns: %w", err))
	}

	turns := make([]Turn, 0, len(members))
	for _, member := range members {
		var turn Turn
		if err := json.Unmarshal([]byte(member), &turn); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: decode turn: %w", err))
		}
		turns = append(turns, turn)
	}
	return turns, nil
}

func turnsKey(customerID, agentID string) string {
	return "turns:{" + customerID + "}:" + agentID
}
