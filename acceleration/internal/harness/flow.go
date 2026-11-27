package harness

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Disposition says what to do with a stable transcript revision.
type Disposition string

const (
	Wait    Disposition = "wait"
	Ignore  Disposition = "ignore"
	Respond Disposition = "respond"
	Clarify Disposition = "clarify"
)

// dispositions are every Disposition, which is both what Valid accepts and what the flow
// controller's schema allows.
var dispositions = []Disposition{Wait, Ignore, Respond, Clarify}

func (d Disposition) Valid() bool { return slices.Contains(dispositions, d) }

// Floor says how the agent should handle speech arriving while it is talking.
type Floor string

const (
	Stop     Floor = "stop"
	Shorten  Floor = "shorten"
	Continue Floor = "continue"
)

// floors are every Floor, which is both what Valid accepts and what the flow controller's
// schema allows.
var floors = []Floor{Stop, Shorten, Continue}

func (f Floor) Valid() bool { return slices.Contains(floors, f) }

// flowSchema holds the model to the values parseFlow accepts. With only a JSON object asked
// for, Gemma named "continue", a floor, as the disposition.
var flowSchema = &llm.JSONSchema{
	Name: "flow_ruling",
	Schema: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"disposition": map[string]any{"type": "string", "enum": dispositions},
			"floor":       map[string]any{"type": "string", "enum": floors},
		},
		"required":             []string{"disposition", "floor"},
		"additionalProperties": false,
	},
}

// FlowTurn is one transcript candidate for the flow controller.
type FlowTurn struct {
	ID           string
	Instructions string
	History      []llm.Message
	Participant  string
	Text         string
	Speaking     bool
	// Reply is what the agent is currently saying, so the controller can tell a cough
	// overlapping a read-back from a caller who is correcting it, and ReplyID the turn it is
	// saying it for, which a check over it is about.
	Reply   string
	ReplyID string
	// Heard is how much of Reply the caller has heard by now, as the agent's voice has played
	// it, when Measured says the agent could tell; otherwise Reply stands for it.
	Heard    string
	Measured bool
	// Unsaid says the agent is on a turn that has written and said nothing yet, such as the
	// answer after a tool's result before its first words: Reply is then the agent's line
	// before it, which was said whole, and the agent has so far said nothing of the turn.
	Unsaid bool
	// Unfinished says the words are what has been heard so far of an utterance still in
	// progress, so the only question is who keeps the floor rather than what to answer.
	Unfinished bool
	// AnotherVoice says the words came from someone other than the participant whose
	// microphone they arrived on, which the transcribers that tell voices apart can hear.
	// It is evidence rather than a verdict: somebody else in the room is usually talking
	// to the room, but they may equally have leaned in to answer for the caller.
	AnotherVoice bool
	// Waited says the same words were waited on before, so the speaker has since been quiet
	// a while longer.
	Waited bool
	// Ended says the transcriber marked the end of the turn, or is one that never does.
	Ended bool
	// InParts says the speaker went on talking before hearing any of the agent's reply to
	// what they had said, so they are giving one thought in parts and a part that sounds
	// complete may not be the end of it.
	InParts bool
	// Audio and Tools are for a Hearer: the speaker's last 16 s, and the tools the
	// customer declared.
	Audio Audio
	Tools []llm.Tool
	// Paused is how long the words had held still when they were put, and FirstPause says
	// no earlier pause in the speaker's turn was waited out: what a Hearer ends a turn on.
	Paused     time.Duration
	FirstPause bool
	// ParticipantID is the speaker's own, stable ID, which a Hearer counts their checks by:
	// Participant is how the prompt calls them, and two speakers may be called alike.
	ParticipantID string
}

// flowLifecycle is the coalescer state of a flow controller.
type flowLifecycle int

const (
	flowIdle flowLifecycle = iota
	flowRunning
	flowRunningWithQueued
	flowClosed
)

type flow struct {
	// call is the context of the call (Options.Call). The decisions start under it.
	call  context.Context
	model *llmrouter.Session
	// judge, when set, is asked in place of model.
	judge Judge
	// hearer, when set, is asked first, and judge or model only when it can't answer.
	hearer      Hearer
	emitter     *Emitter
	logger      *slog.Logger
	onModelCall func(llm.CallTiming)
	// deadline is FlowDeadline, shortened by tests that wait it out.
	deadline time.Duration

	mu sync.Mutex
	// state is the coalescer lifecycle:
	//   flowIdle:              active == nil, queued == nil
	//   flowRunning:           active != nil, queued == nil
	//   flowRunningWithQueued: active != nil, queued != nil
	//   flowClosed:            active == nil, queued == nil
	state flowLifecycle
	// active is the candidate currently running. Identifying it by pointer rather than
	// turn.ID keeps a later Decide with the same turn.ID from overwriting or forgetting it.
	active *candidate
	// queued is the newest candidate that has not started yet. A later Decide replaces it,
	// so the controller only ever sees the latest words plus at most one round trip in flight.
	queued *candidate
	// retired are controllers a swap replaced, closed with the flow so a decision they
	// were still making can settle.
	retired []*llmrouter.Session
	running sync.WaitGroup
}

// FlowDeadline bounds one floor decision. The conversation model may sit for minutes;
// the controller must not, or a 429 leaves the caller unanswered.
const FlowDeadline = 3 * time.Second

// Ongoing speech cannot wait for the conversation model to finish reasoning.
// Noise is filtered before an overlap reaches the controller; on timeout yield
// the floor, without submitting the caller's unfinished words as a new turn.
const overlapDeadline = 750 * time.Millisecond

// candidate is one transcript revision the controller is deciding about.
type candidate struct {
	turn    FlowTurn
	askedAt time.Time
	stream  *llm.Stream
	// done is set under f.mu once the candidate has either claimed its ruling or been
	// abandoned by Cancel or Close, so a late Cancel or completion cannot act twice.
	done bool
	// ctx and cancel abandon Create itself, not only a stream that has already started. A
	// revision that moved on while the HTTP request was still waiting for headers used to
	// leave that round trip occupying the model, which is how a barge-in sat for seconds
	// behind a decision nobody needed.
	ctx    context.Context
	cancel context.CancelFunc
}

type flowAnswer struct {
	Disposition Disposition `json:"disposition"`
	Floor       Floor       `json:"floor"`
}

// flowHistory is how much of the conversation the controller is shown. What it decides is
// whether the latest words belong to that conversation, which the last few turns settle.
const flowHistory = 6

const flowInstructions = `You control the floor of a live voice conversation. You never talk
to the caller and you never answer their question: another model does that. Your only output
is one JSON object and no other text:
{"disposition":"wait|ignore|respond|clarify","floor":"stop|shorten|continue"}

Choose wait when the words are probably incomplete, especially when they end on a PIN,
member ID, phone number, or clock time that may still be growing.
Once the caller has said they will give everything at once, choose wait at each pause until
they ask something or plainly finish; a detail that sounds complete is not the end of the list.
The same holds when you are told the caller is giving one thought in parts: choose wait while
the new words only add to it, such as one more item of a list, and
respond once they ask something, correct or change something, or plainly finish.
Choose ignore only when the words are clearly background speech or addressed to somebody else.
A caller asking the agent anything is talking to it, however far the question is from what the
agent is for: choose respond, and the other model says what it cannot help with. Being off-topic
is never a reason to ignore.
Words in a different voice come from somebody else at the caller's microphone, who is
usually talking to the room rather than to the agent, so lean towards ignore unless they
plainly address it.
Words that open by calling somebody else in the room by a kinship term or a nickname are
addressed to them, not the agent: choose ignore, and continue if the agent is speaking. But if
the caller turns back to the agent in the same words, the part for the agent counts: respond.
Choose clarify when the caller addressed the agent but their request is ambiguous.
Choose respond for a complete thought addressed to the agent.
A recorded menu reading out its options is one thought and not several, however long the
pauses between them: choose wait until it has asked for a choice, and never interrupt one,
because it is not listening and starts again from the top if it is talked over.
If the agent is speaking, stop for a correction or direct interruption, shorten for a related
addition that makes the current answer too long, and continue for a brief acknowledgement,
a cough or other non-speech noise, or clearly unrelated background speech. If the agent is
not speaking, choose continue.
Words that only repeat what the agent is saying are the caller's line echoing it back, so
choose continue.
When the caller has not finished and is talking over the agent, decide only the floor and
always choose wait for the disposition: stop as soon as what has been said so far is a
correction, a new request, a question, or a direct interruption such as "wait", "no", or
"hang on"; shorten for a related addition; and continue while it is only an acknowledgement,
a noise, an echo of the agent's own words, or too short to tell.`

func newFlow(model *llmrouter.Session, emitter *Emitter, logger *slog.Logger) *flow {
	return &flow{
		model:    model,
		emitter:  emitter,
		logger:   logger,
		deadline: FlowDeadline,
	}
}

func (f *flow) Decide(ctx context.Context, turn FlowTurn) error {
	if strings.TrimSpace(turn.ID) == "" {
		return stack.Wrap(errors.New("harness: a flow candidate id is required"))
	}
	if strings.TrimSpace(turn.Text) == "" {
		return stack.Wrap(errors.New("harness: flow candidate text is required"))
	}

	deadline := f.deadline
	if turn.Unfinished && turn.Speaking {
		deadline = overlapDeadline
	}

	f.mu.Lock()
	if f.state == flowClosed {
		// The conversation is over, and Close is waiting for the decisions that were
		// started before it.
		f.mu.Unlock()
		return nil
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadline)
	asked := &candidate{turn: turn, askedAt: time.Now(), ctx: cctx, cancel: cancel}

	var dropped *candidate
	switch f.state {
	case flowIdle:
		f.active = asked
		f.state = flowRunning
		f.running.Add(1)
		f.mu.Unlock()
		goroutine.Go(f.call, func() { f.run(asked) })
		return nil
	case flowRunning:
		f.queued = asked
		f.state = flowRunningWithQueued
	default:
		dropped = f.queued
		f.queued = asked
	}
	f.mu.Unlock()
	if dropped != nil {
		dropped.cancel()
	}
	return nil
}

// run asks the model and waits for the decision. It is a goroutine of its own because
// Create waits for response headers, and the transcript loop that called Decide cannot
// sit in that: a revision that arrives while it does is the one that should cancel it.
func (f *flow) run(asked *candidate) {
	turn := asked.turn
	defer f.running.Done()
	defer f.advance(asked)
	defer asked.cancel()

	if f.hearer != nil && f.runHearer(asked) {
		return
	}
	if f.judge != nil {
		f.runJudge(asked)
		return
	}

	f.mu.Lock()
	if f.active != asked || asked.done || f.model == nil {
		f.mu.Unlock()
		return
	}
	model := f.model
	f.mu.Unlock()
	stream, err := model.Create(asked.ctx, llm.ResponseParams{
		ID:           turn.ID,
		Purpose:      "flow",
		TurnID:       turn.ID,
		OnTiming:     f.onModelCall,
		Instructions: flowInstructions + "\n\nThe agent has been told:\n" + turn.Instructions,
		Input:        []llm.Message{{Role: llm.User, Content: flowQuestion(turn)}},
		// A decision is one small JSON object. Gemini 3 still thinks first, and 32 tokens
		// was only enough for `{"dis` — which we treated as continue, so barge-in never
		// stopped the agent.
		MaxOutputTokens: 512,
		Text:            llm.TextParams{Format: llm.FormatJSONSchema, Schema: flowSchema},
	})
	if err != nil {
		if !f.claim(asked) {
			return
		}
		if f.yieldExpiredOverlap(asked) {
			return
		}
		f.logger.Error("flow controller failed", "provider", model.Provider(), "model", model.Model(), "error", err)
		f.emitter.Send(Decided{
			CandidateID: turn.ID,
			TookMs:      millis(time.Since(asked.askedAt)),
			Err:         fmt.Errorf("harness: decide flow: %w", err),
		})
		return
	}

	f.mu.Lock()
	asked.stream = stream
	abandoned := f.active != asked || asked.done
	f.mu.Unlock()
	if abandoned {
		stream.Close()
		return
	}

	stopExpiry := context.AfterFunc(asked.ctx, func() { _ = stream.Close() })
	defer stopExpiry()
	f.consume(asked, stream, model)
}

// advance starts the newest queued turn, or marks the controller idle.
//
// Cancel of the active turn may already have started the queued turn. That leftover Create
// must not start it again, or two decisions about the same words race.
func (f *flow) advance(asked *candidate) {
	f.mu.Lock()
	if f.active != asked {
		f.mu.Unlock()
		return
	}
	next := f.promoteQueuedLocked()
	f.mu.Unlock()
	if next != nil {
		goroutine.Go(f.call, func() { f.run(next) })
	}
}

// promoteQueuedLocked transitions out of the current active candidate, starting the queued
// candidate if one is waiting or returning to flowIdle. The caller holds f.mu.
func (f *flow) promoteQueuedLocked() *candidate {
	if f.state == flowRunningWithQueued {
		next := f.queued
		f.queued = nil
		f.active = next
		f.state = flowRunning
		f.running.Add(1)
		return next
	}
	f.active = nil
	f.state = flowIdle
	return nil
}

// flowQuestion is the whole of what the controller is asked.
//
// The conversation is quoted inside the question rather than replayed as turns, because a
// model handed a conversation to continue continues it: primed with a caller's question in
// the user role, it answers the question instead of classifying it, and the caller's turn
// is lost to a model they never hear.
func flowQuestion(turn FlowTurn) string {
	var asked strings.Builder
	asked.WriteString("Conversation so far:\n")
	// The harness's notes and the data ahead of them are not the conversation the controller
	// rules on, and quoted here they would read as the caller's.
	history := WithoutNotes(turn.History)
	if len(history) > flowHistory {
		history = history[len(history)-flowHistory:]
	}
	if len(history) == 0 {
		asked.WriteString("(nothing said yet)\n")
	}
	for _, message := range history {
		speaker := "Caller"
		if message.Role == llm.Assistant {
			speaker = "Agent"
		}
		fmt.Fprintf(&asked, "%s: %s\n", speaker, message.Content)
	}

	state := "is not speaking"
	if turn.Speaking {
		state = "is speaking right now"
		if reply := strings.TrimSpace(turn.Reply); reply != "" {
			state = fmt.Sprintf("is speaking right now and has so far said %q", reply)
		}
	}
	participant := strings.TrimSpace(turn.Participant)
	if participant == "" {
		participant = "An unknown speaker"
	}
	voice := ""
	if turn.AnotherVoice {
		voice = ", in a different voice from the one the agent has been talking to"
	}
	if turn.Unfinished {
		fmt.Fprintf(&asked, "\nThe agent %s.\n%s is still talking%s and has so far said: %q. "+
			"They have not finished.\n\nDecide only the floor. Return the JSON object.",
			state, participant, voice, turn.Text)
		return asked.String()
	}
	parts := ""
	if turn.InParts {
		parts = " They went on talking before hearing any of the agent's reply to what they " +
			"said before it, so they are giving one thought in parts."
	}
	fmt.Fprintf(&asked, "\nThe agent %s.\n%s has just said%s: %q%s\n\nReturn the JSON object.",
		state, participant, voice, turn.Text, parts)
	return asked.String()
}

func (f *flow) Cancel(candidateID string) error {
	f.mu.Lock()
	var dropped, cancelled, next *candidate
	var stream *llm.Stream

	if f.state == flowRunningWithQueued && f.queued.turn.ID == candidateID {
		dropped = f.queued
		f.queued = nil
		f.state = flowRunning
	}
	if (f.state == flowRunning || f.state == flowRunningWithQueued) && !f.active.done && f.active.turn.ID == candidateID {
		cancelled = f.active
		cancelled.done = true
		// run sets the stream under this lock. If it sets the stream after this, run closes
		// it once it sees the candidate is no longer active.
		stream = cancelled.stream
		// A cancelled Create can take a while to come back, and the turn waiting behind it
		// must not wait for that. Promote it now, and the leftover advance from run does
		// nothing because f.active has moved on.
		next = f.promoteQueuedLocked()
	}
	f.mu.Unlock()

	if dropped != nil {
		dropped.cancel()
	}
	if cancelled != nil {
		cancelled.cancel()
	}
	if next != nil {
		goroutine.Go(f.call, func() { f.run(next) })
	}
	if stream == nil {
		return nil
	}
	return stream.Close()
}

func (f *flow) Close() error {
	f.mu.Lock()
	alreadyClosed := f.state == flowClosed
	f.state = flowClosed
	queued := f.queued
	f.queued = nil
	active := f.active
	f.active = nil
	if active != nil {
		active.done = true
	}
	sessions := make([]*llmrouter.Session, 0, len(f.retired)+1)
	if f.model != nil {
		sessions = append(sessions, f.model)
		f.model = nil
	}
	sessions = append(sessions, f.retired...)
	f.retired = nil
	f.mu.Unlock()

	if queued != nil {
		queued.cancel()
	}
	if active != nil {
		active.cancel()
	}

	// Closing the session abandons whatever the controller is still deciding, which is
	// what lets every consumer reach the end of its stream.
	var failures []error
	for _, session := range sessions {
		if session == nil {
			continue
		}
		if err := session.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	f.running.Wait()
	if !alreadyClosed && f.judge != nil {
		if err := f.judge.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// setModel decides from the next candidate on another session.
func (f *flow) setModel(model *llmrouter.Session) {
	f.mu.Lock()
	if f.state == flowClosed {
		f.mu.Unlock()
		if model != nil {
			_ = model.Close()
		}
		return
	}
	if f.model != nil {
		f.retired = append(f.retired, f.model)
	}
	f.model = model
	f.mu.Unlock()
}

// consume waits for one decision and reports it.
func (f *flow) consume(asked *candidate, stream *llm.Stream, primary *llmrouter.Session) {
	candidateID := asked.turn.ID
	response, err := llm.Collect(stream)
	if !f.claim(asked) {
		// The transcript moved on, so nobody is waiting for this any more.
		return
	}
	took := millis(time.Since(asked.askedAt))
	if f.yieldExpiredOverlap(asked) {
		return
	}
	if err != nil {
		if response.Status == llm.StatusCancelled {
			if !errors.Is(asked.ctx.Err(), context.DeadlineExceeded) {
				return
			}
			// The deadline closed a stream that had started but sent no decision. Returning
			// here left the caller's finished question with no ruling, so it was never
			// answered: the ruling carries the timeout, as one for a Create that timed out does.
			err = fmt.Errorf("harness: decide flow: %w", asked.ctx.Err())
		}
		// The failing stream may come from a hedge or a failover, another provider behind the session.
		f.logger.Error("flow controller failed", "provider", cmp.Or(response.Provider, primary.Provider()),
			"model", cmp.Or(response.Model, primary.Model()), "error", err)
		f.emitter.Send(Decided{CandidateID: candidateID, TookMs: took, Err: err})
		return
	}

	answer, invalid, err := parseFlow(response.OutputText)
	var ruler Ruler
	if err != nil {
		invalidRulings.WithLabelValues(invalid).Inc()
		ruler = Unusable
		// Truncated JSON used to mean continue, which is how Gemini thinking ate the
		// token budget and the caller talking over the agent never took the floor.
		if asked.turn.Unfinished {
			f.logger.Warn("unusable flow decision while the caller is talking, stopping",
				"error", err, "answer", response.OutputText)
			answer = flowAnswer{Disposition: Wait, Floor: Stop}
		} else {
			f.logger.Warn("unusable flow decision, answering the caller anyway",
				"error", err, "answer", response.OutputText)
			answer = flowAnswer{Disposition: Respond, Floor: Continue}
		}
	} else {
		f.logger.Debug("flow controller answered",
			"candidate", candidateID, "answer", response.OutputText,
			"took_ms", response.TimeToFirstTokenMs)
		// A stream is stamped by the provider that wrote it, and a hedge or a failover is
		// another provider or model behind the same session.
		ruler = Primary
		if !primary.Wrote(response) {
			ruler = Fallback
		}
	}
	f.emitter.Send(Decided{
		CandidateID: candidateID,
		Disposition: answer.Disposition,
		Floor:       answer.Floor,
		TookMs:      took,
		Ruler:       ruler,
	})
}

// The caller has already claimed this candidate, so a superseded or closed
// request cannot produce a late interruption here.
func (f *flow) yieldExpiredOverlap(asked *candidate) bool {
	if !asked.turn.Unfinished || !asked.turn.Speaking || !errors.Is(asked.ctx.Err(), context.DeadlineExceeded) {
		return false
	}
	f.emitter.Send(Decided{
		CandidateID: asked.turn.ID, Disposition: Wait, Floor: Stop,
		TookMs: millis(time.Since(asked.askedAt)), Ruler: Deadline,
	})
	return true
}

// claim marks asked as settled and reports whether it was still the active candidate.
// False means the candidate was cancelled or the flow was closed, so nobody is waiting for its answer.
func (f *flow) claim(asked *candidate) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active != asked || asked.done {
		return false
	}
	asked.done = true
	return true
}

func millis(took time.Duration) float64 {
	return float64(took.Microseconds()) / 1000
}

// parseFlow reads the controller's answer. A refused one comes with what was wrong with it:
// decode, disposition or floor.
func parseFlow(text string) (flowAnswer, string, error) {
	var answer flowAnswer
	if err := json.Unmarshal([]byte(llm.Unfence(text)), &answer); err != nil {
		return flowAnswer{}, "decode", fmt.Errorf("harness: decode flow decision: %w", err)
	}
	if !answer.Disposition.Valid() {
		return flowAnswer{}, "disposition", fmt.Errorf("harness: invalid flow disposition %q", answer.Disposition)
	}
	if !answer.Floor.Valid() {
		return flowAnswer{}, "floor", fmt.Errorf("harness: invalid floor decision %q", answer.Floor)
	}
	return answer, "", nil
}
