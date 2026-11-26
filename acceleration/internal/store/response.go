package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What became of one response.
const (
	// ResponseRunning is a turn the agent is still taking.
	ResponseRunning = "running"
	// ResponseCompleted is one that finished and said something.
	ResponseCompleted = "completed"
	// ResponseFailed is one that could not be taken.
	ResponseFailed = "failed"
	// ResponseCancelled is one interrupted partway, which is a different thing from
	// failing: the caller stopped it, and what it had already said still counts.
	ResponseCancelled = "cancelled"
	// ResponseRefused is one the prompt injection screen refused. What it was asked is kept
	// on the row, but never handed to a model again.
	ResponseRefused = "refused"
)

// AgentResponse is one turn the agent took in a session.
type AgentResponse struct {
	bun.BaseModel `bun:"table:agent_responses,alias:ars"`

	ID         string `bun:"id,pk"`
	SessionID  string `bun:"session_id,notnull"`
	CustomerID string `bun:"customer_id,notnull"`
	// Said is what the person asked. It is on the row as well as being the first item, so
	// a list of turns reads without loading every turn's items.
	Said       string     `bun:"said,notnull"`
	Status     string     `bun:"status,notnull"`
	Error      string     `bun:"error,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	FinishedAt *time.Time `bun:"finished_at"`
	// RewoundAt is when a rewind went back past this turn, which takes it out of the
	// conversation without pretending it was never said.
	RewoundAt *time.Time `bun:"rewound_at"`
	// ParentResponseID is the response before this one on the path it was answered on, empty
	// for a session's first. Responses sharing a parent are alternatives to one another.
	ParentResponseID string `bun:"parent_response_id,nullzero"`
	// QuestionResponseID is the response whose question this one answers: its own id for a
	// new question or an edit, the replaced one's for a regenerate. Empty only on a row from
	// before branches were kept.
	QuestionResponseID string `bun:"question_response_id,nullzero"`
}

// Validate refuses a turn that names no session, customer or known status. An empty status
// is the running one Insert fills in.
func (r *AgentResponse) Validate() error {
	return validate("response").
		required("id", r.ID).
		required("session_id", r.SessionID).
		required("customer_id", r.CustomerID).
		oneOf("status", r.Status, "", ResponseRunning, ResponseCompleted, ResponseFailed, ResponseCancelled, ResponseRefused).
		done()
}

// ErrUnknownResponse is a response that is not part of the session's conversation: it does
// not exist, belongs to another session, or was rewound.
var ErrUnknownResponse = errors.New("store: that response is not part of this conversation")

// ResponsePosition is the last turn of a page.
type ResponsePosition struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

// ResponseFilter picks a page of a session's turns, oldest first.
type ResponseFilter struct {
	Limit int
	// After starts the page after this turn. Nil is the first page.
	After *ResponsePosition
}

// Exchange is one turn as a model is given it again: what was asked and what was answered.
type Exchange struct {
	ResponseID string
	Said       string
	Answer     string
	// Refused is a turn the prompt injection screen refused, whose Said is not to be replayed.
	Refused bool
	// At is when the response was started, set by Opening only.
	At time.Time
}

// Reply is what one reply a conversation's transcript wrote did: its answer and tool_result
// items, in the order they happened.
type Reply struct {
	Items []AgentResponseItem
	// Refused is a response the prompt injection screen refused, whose tool results since
	// its last answer are what the screen judged and are not to be replayed.
	Refused bool
}

// What a response's alternatives are counted among.
const (
	// SiblingsOfReply are the answers to one question: the response and its regenerations.
	SiblingsOfReply = "reply"
	// SiblingsOfQuestion are the questions asked at one point of the conversation: the
	// original and its edits, one per question however often each was answered.
	SiblingsOfQuestion = "question"
)

// Responses are the turns agents took in sessions, in the shard that ran them, and the tree
// of branches they make.
type Responses struct {
	store *Store
}

// ErrIDTaken is a row written under an id another customer's row already has.
var ErrIDTaken = errors.New("store: the id is another customer's")

// Insert records that the agent began a turn, with the response before it on its path and
// the question it answers. Writing the same turn again is not an error; writing one under
// another customer's response id is ErrIDTaken.
func (r *Responses) Insert(ctx context.Context, response *AgentResponse) error {
	if err := response.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if response.CreatedAt.IsZero() {
		response.CreatedAt = time.Now().UTC()
	}
	if response.Status == "" {
		response.Status = ResponseRunning
	}
	// A response that answers no earlier question asks its own, which is also what tells it
	// apart from a row written before branches were kept.
	if response.QuestionResponseID == "" {
		response.QuestionResponseID = response.ID
	}
	// What was asked is the conversation, read back to the model when it is carried on, so it
	// is kept whole, up to maxTurnText.
	response.Said = turnText(response.Said)

	// The update sets nothing new: it tells a retry, which matches its own row, from an id
	// another customer's row has, which matches none.
	result, err := r.store.shard.NewInsert().Model(response).
		On("CONFLICT (id) DO UPDATE").
		Set("id = EXCLUDED.id").
		Where("ars.customer_id = EXCLUDED.customer_id").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start response: %w", err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start response: %w", err))
	}
	if written == 0 {
		return stack.Wrap(fmt.Errorf("%w: response %s", ErrIDTaken, response.ID))
	}
	return nil
}

// Finish records how a turn ended and stamps the session's last activity, which is what a
// most-recently-used ordering of conversations reads.
func (r *Responses) Finish(ctx context.Context, customerID, id, status, failure string, at time.Time) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a response id are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if status == "" {
		status = ResponseCompleted
	}

	_, err := r.store.shard.NewUpdate().Model((*AgentResponse)(nil)).
		Set("status = ?", status).
		Set("error = ?", SafeLogText(failure)).
		Set("finished_at = ?", at).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("finished_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish response: %w", err))
	}

	// The session's clock moves with its turns rather than with its row being touched, so a
	// conversation somebody came back to sorts above one that was merely renamed.
	_, err = r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("last_response_at = ?", at).
		Set("updated_at = ?", at).
		TableExpr("agent_responses AS r").
		Where("r.id = ?", id).
		Where("r.customer_id = ?", customerID).
		Where("asn.id = r.session_id").
		Where("asn.customer_id = r.customer_id").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish response: stamp session: %w", err))
	}
	return nil
}

// Select returns a page of a session's turns, oldest first, which read in order are the
// conversation.
func (r *Responses) Select(ctx context.Context, customerID, sessionID string, filter ResponseFilter) ([]AgentResponse, error) {
	if customerID == "" || sessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	query := r.store.shard.NewSelect().Model((*AgentResponse)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("rewound_at IS NULL").
		Order("created_at ASC", "id ASC").
		Limit(SessionLimit(filter.Limit) + 1)
	if filter.After != nil {
		query = query.Where("(created_at, id) > (?, ?)", filter.After.CreatedAt, filter.After.ID)
	}
	var responses []AgentResponse
	if err := query.Scan(ctx, &responses); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session responses: %w", err))
	}
	return responses, nil
}

func oldestResponseFirst(a, b AgentResponse) int {
	return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), strings.Compare(a.ID, b.ID))
}

// Exchanges returns a session's conversation as a model would be given it again, oldest
// first: each turn's question and everything it answered. A non-empty upTo stops after that
// response, which is where a rewind or a fork carries on from.
//
// Only questions and answers are kept. A tool's call and its result were the agent working
// the answer out, and the answer is what the conversation carries forward, which is also all
// a transcript read out of Chat gives a model.
func (r *Responses) Exchanges(ctx context.Context, customerID, sessionID, upTo string) ([]Exchange, error) {
	if customerID == "" || sessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	var responses []AgentResponse
	err := r.store.shard.NewSelect().Model(&responses).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("rewound_at IS NULL").
		Order("created_at ASC", "id ASC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: exchanges: %w", err))
	}
	if upTo != "" {
		end := -1
		for i, response := range responses {
			if response.ID == upTo {
				end = i
				break
			}
		}
		if end < 0 {
			return nil, stack.Wrap(ErrUnknownResponse)
		}
		responses = responses[:end+1]
	}
	if len(responses) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(responses))
	for _, response := range responses {
		ids = append(ids, response.ID)
	}
	var answers []AgentResponseItem
	err = r.store.shard.NewSelect().Model(&answers).
		Where("response_id IN (?)", bun.In(ids)).
		Where("kind = ?", ItemAnswer).
		Order("response_id ASC", "ordinal ASC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: exchanges: answers: %w", err))
	}
	// An agent that speaks again once delegated work comes back answers one question twice,
	// and both halves are the answer.
	answered := map[string][]string{}
	for _, answer := range answers {
		answered[answer.ResponseID] = append(answered[answer.ResponseID], answer.Text)
	}

	exchanges := make([]Exchange, 0, len(responses))
	for _, response := range responses {
		exchanges = append(exchanges, Exchange{
			ResponseID: response.ID, Said: response.Said,
			Answer:  strings.Join(answered[response.ID], "\n\n"),
			Refused: response.Status == ResponseRefused,
		})
	}
	return exchanges, nil
}

// Replies are what the replies a conversation's transcript wrote under these turn ids did,
// by turn id: the answer and tool_result items of the response each one finished, in the
// order they happened, and whether the response was refused. Only the customer's own
// sessions on that conversation are read, so a turn id copied into another transcript finds
// nothing, and a rewound response is left out. A turn whose answer item names no turn id,
// as one recorded before they did, finds nothing either.
func (r *Responses) Replies(ctx context.Context, customerID, conversationID string, turnIDs []string) (map[string]Reply, error) {
	if customerID == "" || conversationID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a conversation id are required"))
	}
	if len(turnIDs) == 0 {
		return nil, nil
	}
	type answered struct {
		ResponseID string `bun:"response_id"`
		TurnID     string `bun:"turn_id"`
		Ordinal    int    `bun:"ordinal"`
		Status     string `bun:"status"`
	}
	var answers []answered
	err := r.store.shard.NewSelect().TableExpr("agent_response_items AS ari").
		ColumnExpr("ari.response_id, ari.payload->>'turn_id' AS turn_id, ari.ordinal, r.status").
		Join("JOIN agent_responses AS r ON r.id = ari.response_id").
		Join("JOIN agent_sessions AS s ON s.id = r.session_id").
		Where("s.customer_id = ?", customerID).
		Where("s.conversation_id = ?", conversationID).
		Where("r.customer_id = ?", customerID).
		Where("r.rewound_at IS NULL").
		Where("ari.kind = ?", ItemAnswer).
		Where("ari.payload->>'turn_id' IN (?)", bun.In(turnIDs)).
		Scan(ctx, &answers)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: reply items: %w", err))
	}
	if len(answers) == 0 {
		return map[string]Reply{}, nil
	}
	ids := make([]string, 0, len(answers))
	for _, a := range answers {
		ids = append(ids, a.ResponseID)
	}
	var items []AgentResponseItem
	err = r.store.shard.NewSelect().Model(&items).
		Where("response_id IN (?)", bun.In(ids)).
		Where("kind IN (?)", bun.In([]string{ItemAnswer, ItemToolResult})).
		Order("response_id ASC", "ordinal ASC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: reply items: %w", err))
	}
	// A response answered more than once, once its tools came back, is the reply its last
	// answer finished.
	last := map[string]answered{}
	for _, a := range answers {
		if held, ok := last[a.ResponseID]; !ok || a.Ordinal > held.Ordinal {
			last[a.ResponseID] = a
		}
	}
	byResponse := map[string][]AgentResponseItem{}
	for _, item := range items {
		byResponse[item.ResponseID] = append(byResponse[item.ResponseID], item)
	}
	replies := make(map[string]Reply, len(last))
	for responseID, a := range last {
		replies[a.TurnID] = Reply{Items: byResponse[responseID], Refused: a.Status == ResponseRefused}
	}
	return replies, nil
}

// Opening is the first thing said in a customer's conversation, across every session that
// held it, and when (At), or empty when none recorded anything said. Refused says the prompt
// injection screen refused it.
func (r *Responses) Opening(ctx context.Context, customerID, conversationID string) (Exchange, error) {
	if customerID == "" || conversationID == "" {
		return Exchange{}, stack.Wrap(errors.New("store: a customer and a conversation id are required"))
	}
	var first []AgentResponse
	err := r.store.shard.NewSelect().Model(&first).
		Column("ars.id", "ars.said", "ars.status", "ars.created_at").
		Join("JOIN agent_sessions AS s ON s.id = ars.session_id").
		Where("s.customer_id = ?", customerID).
		Where("s.conversation_id = ?", conversationID).
		Where("ars.customer_id = ?", customerID).
		Where("ars.rewound_at IS NULL").
		Where("ars.said <> ''").
		Order("ars.created_at ASC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return Exchange{}, stack.Wrap(fmt.Errorf("store: opening: %w", err))
	}
	if len(first) == 0 {
		return Exchange{}, nil
	}
	return Exchange{ResponseID: first[0].ID, Said: first[0].Said, Refused: first[0].Status == ResponseRefused, At: first[0].CreatedAt}, nil
}

// Rewind takes every response after kept out of the session's conversation.
func (r *Responses) Rewind(ctx context.Context, customerID, sessionID, kept string, at time.Time) error {
	if customerID == "" || sessionID == "" || kept == "" {
		return stack.Wrap(errors.New("store: a customer, a session and a response id are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	var keptAt AgentResponse
	err := r.store.shard.NewSelect().Model(&keptAt).Column("created_at", "id").Where("id = ?", kept).Where("customer_id = ?", customerID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: rewind responses: %w", err))
	}
	_, err = r.store.shard.NewUpdate().Model((*AgentResponse)(nil)).
		Set("rewound_at = ?", at).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("rewound_at IS NULL").
		Where("(created_at, id) > (?, ?)", keptAt.CreatedAt, keptAt.ID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: rewind responses: %w", err))
	}
	return nil
}

// Siblings returns the alternatives to one of a session's responses, oldest first, the
// response's own included. Of a reply, every response answering its question; of a question,
// the first response of each question asked after the same parent. A response from before
// branches were kept has none but itself.
func (r *Responses) Siblings(ctx context.Context, customerID, sessionID, responseID, of string) ([]AgentResponse, error) {
	if customerID == "" || sessionID == "" || responseID == "" {
		return nil, stack.Wrap(errors.New("store: a customer, a session and a response id are required"))
	}
	if of != SiblingsOfReply && of != SiblingsOfQuestion {
		return nil, stack.Wrap(fmt.Errorf("store: siblings of %q: want %s or %s", of, SiblingsOfReply, SiblingsOfQuestion))
	}

	response, err := r.Get(ctx, customerID, sessionID, responseID)
	if err != nil {
		return nil, err
	}
	if response.QuestionResponseID == "" {
		return []AgentResponse{response}, nil
	}
	query := r.store.shard.NewSelect().Model((*AgentResponse)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("question_response_id IS NOT NULL").
		Order("created_at ASC", "id ASC")
	if response.ParentResponseID == "" {
		query = query.Where("parent_response_id IS NULL")
	} else {
		query = query.Where("parent_response_id = ?", response.ParentResponseID)
	}
	if of == SiblingsOfReply {
		query = query.Where("question_response_id = ?", response.QuestionResponseID)
	}
	var siblings []AgentResponse
	if err := query.Scan(ctx, &siblings); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: siblings: %w", err))
	}
	if of == SiblingsOfQuestion {
		siblings = distinct(siblings, func(r AgentResponse) string { return r.QuestionResponseID })
	}
	return siblings, nil
}

// QuestionsAt counts the questions asked at one point of a session's conversation, after
// parentID or first for none, rewound or not: the questions a new one asked there stands
// beside. A row from no question (no question_response_id) is no question.
func (r *Responses) QuestionsAt(ctx context.Context, customerID, sessionID, parentID string) (int, error) {
	if customerID == "" || sessionID == "" {
		return 0, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	query := r.store.shard.NewSelect().Model((*AgentResponse)(nil)).
		ColumnExpr("count(DISTINCT question_response_id)").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("question_response_id IS NOT NULL")
	if parentID == "" {
		query = query.Where("parent_response_id IS NULL")
	} else {
		query = query.Where("parent_response_id = ?", parentID)
	}
	var questions int
	if err := query.Scan(ctx, &questions); err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: questions at: %w", err))
	}
	return questions, nil
}

// SiblingPosition is where response stands among siblings as Siblings returned them for of:
// its 1-based index and how many there are. A response not among them is 1 of 1.
func SiblingPosition(siblings []AgentResponse, response AgentResponse, of string) (index, count int) {
	at := slices.IndexFunc(siblings, func(sibling AgentResponse) bool {
		if of == SiblingsOfQuestion {
			return sibling.QuestionResponseID != "" && sibling.QuestionResponseID == response.QuestionResponseID
		}
		return sibling.ID == response.ID
	})
	if at < 0 {
		return 1, 1
	}
	return at + 1, len(siblings)
}

// BranchTail returns the path from one of a session's responses down to the newest response
// under it, that response first and the newest last. A response with nothing under it is a
// path of itself. The rows carry their place in the tree and their status, not what was
// said: Said, Error and FinishedAt are empty.
func (r *Responses) BranchTail(ctx context.Context, customerID, sessionID, fromResponseID string) ([]AgentResponse, error) {
	if customerID == "" || sessionID == "" || fromResponseID == "" {
		return nil, stack.Wrap(errors.New("store: a customer, a session and a response id are required"))
	}
	tree, err := r.tree(ctx, customerID, sessionID)
	if err != nil {
		return nil, err
	}
	from, ok := tree.byID[fromResponseID]
	if !ok {
		return nil, stack.Wrap(ErrUnknownResponse)
	}

	// The newest descendant, wherever it is under from, rather than the newest child's newest
	// child: an older branch can have been answered after a newer one was started.
	newest := from
	under := []string{from.ID}
	for len(under) > 0 {
		id := under[0]
		under = under[1:]
		for _, child := range tree.children[id] {
			if oldestResponseFirst(child, newest) > 0 {
				newest = child
			}
			under = append(under, child.ID)
		}
	}
	path := tree.pathTo(newest.ID, from.ID)
	slices.Reverse(path)
	return path, nil
}

// MoveActivePathUntil makes the path ending at leafResponseID the session's conversation. The
// responses after the point where it leaves the current conversation are rewound at at, and
// the ones on the new path after that point are brought back. An empty leafResponseID is a
// conversation with nothing in it yet, as the first question asked again leaves it: every
// response is rewound. Running it again changes nothing. It takes off the conversation only
// responses created at or before until, the zero time for every one: a switch finished after
// the person carried on leaves the turns asked since it began where they are.
func (r *Responses) MoveActivePathUntil(ctx context.Context, customerID, sessionID, leafResponseID string, at, until time.Time) error {
	if customerID == "" || sessionID == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	tree, err := r.tree(ctx, customerID, sessionID)
	if err != nil {
		return err
	}
	if _, ok := tree.byID[leafResponseID]; !ok && leafResponseID != "" {
		return stack.Wrap(ErrUnknownResponse)
	}

	// The fork is the newest response the new path shares with the conversation; everything
	// on the new path above it is to be brought back.
	var fork *AgentResponse
	var back []string
	for _, response := range tree.pathTo(leafResponseID, "") {
		if response.RewoundAt == nil {
			fork = &response
			break
		}
		back = append(back, response.ID)
	}

	// The old branch is rewound before the new one is brought back, so a router stopping in
	// between leaves the conversation cut short at the fork rather than two branches in it.
	query := r.store.shard.NewUpdate().Model((*AgentResponse)(nil)).
		Set("rewound_at = ?", at).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("rewound_at IS NULL")
	if fork != nil {
		query = query.Where("(created_at, id) > (?, ?)", fork.CreatedAt, fork.ID)
	}
	if !until.IsZero() {
		query = query.Where("created_at <= ?", until)
	}
	if _, err = query.Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: move active path: rewind: %w", err))
	}
	if len(back) == 0 {
		return nil
	}
	_, err = r.store.shard.NewUpdate().Model((*AgentResponse)(nil)).
		Set("rewound_at = NULL").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("id IN (?)", bun.In(back)).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: move active path: bring back: %w", err))
	}
	return nil
}

// Get is one of a session's responses, with its place in the tree and whether it is on the session's conversation (RewoundAt).
func (r *Responses) Get(ctx context.Context, customerID, sessionID, responseID string) (AgentResponse, error) {
	if customerID == "" || sessionID == "" || responseID == "" {
		return AgentResponse{}, stack.Wrap(errors.New("store: a customer, a session and a response id are required"))
	}
	var response AgentResponse
	err := r.store.shard.NewSelect().Model(&response).
		Where("id = ?", responseID).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentResponse{}, stack.Wrap(ErrUnknownResponse)
	}
	if err != nil {
		return AgentResponse{}, stack.Wrap(fmt.Errorf("store: session response: %w", err))
	}
	return response, nil
}

// ActiveChild is the response after parentID on a session's conversation: the one under it
// that is not rewound. ok is false when parentID is the newest response on the conversation.
// A response the session does not have is ErrUnknownResponse.
func (r *Responses) ActiveChild(ctx context.Context, customerID, sessionID, parentID string) (child AgentResponse, ok bool, err error) {
	if customerID == "" || sessionID == "" || parentID == "" {
		return AgentResponse{}, false, stack.Wrap(errors.New("store: a customer, a session and a response id are required"))
	}
	tree, err := r.tree(ctx, customerID, sessionID)
	if err != nil {
		return AgentResponse{}, false, err
	}
	if _, known := tree.byID[parentID]; !known {
		return AgentResponse{}, false, stack.Wrap(ErrUnknownResponse)
	}
	for _, candidate := range tree.children[parentID] {
		if candidate.RewoundAt == nil {
			return candidate, true, nil
		}
	}
	return AgentResponse{}, false, nil
}

// ActiveLeaf is the newest of a session's responses on its conversation, the one the next
// turn is answered after, or empty for a session with none.
func (r *Responses) ActiveLeaf(ctx context.Context, customerID, sessionID string) (string, error) {
	if customerID == "" || sessionID == "" {
		return "", stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	var newest []AgentResponse
	err := r.store.shard.NewSelect().Model((*AgentResponse)(nil)).
		Column("id", "created_at").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("rewound_at IS NULL").
		Order("created_at DESC", "id DESC").
		Limit(1).
		Scan(ctx, &newest)
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: active leaf: %w", err))
	}
	if len(newest) == 0 {
		return "", nil
	}
	return newest[0].ID, nil
}

// responseTree is a session's responses by id and by parent.
type responseTree struct {
	byID     map[string]AgentResponse
	children map[string][]AgentResponse
}

// tree reads every response of a session, rewound or not.
func (r *Responses) tree(ctx context.Context, customerID, sessionID string) (responseTree, error) {
	var responses []AgentResponse
	err := r.store.shard.NewSelect().Model((*AgentResponse)(nil)).
		Column("id", "session_id", "customer_id", "status", "created_at", "rewound_at", "parent_response_id", "question_response_id").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Order("created_at ASC", "id ASC").
		Scan(ctx, &responses)
	if err != nil {
		return responseTree{}, stack.Wrap(fmt.Errorf("store: response tree: %w", err))
	}
	tree := responseTree{byID: map[string]AgentResponse{}, children: map[string][]AgentResponse{}}
	for _, response := range responses {
		tree.byID[response.ID] = response
		if response.ParentResponseID != "" {
			tree.children[response.ParentResponseID] = append(tree.children[response.ParentResponseID], response)
		}
	}
	return tree, nil
}

// pathTo walks up from id through its parents and returns the responses it passed, id first,
// stopping after stop or at a response whose parent is not in the session. The walk is at
// most as long as the session, so parents that loop end it rather than hang it.
func (t responseTree) pathTo(id, stop string) []AgentResponse {
	var path []AgentResponse
	for range len(t.byID) {
		response, ok := t.byID[id]
		if !ok {
			break
		}
		path = append(path, response)
		if response.ID == stop || response.ParentResponseID == "" {
			break
		}
		id = response.ParentResponseID
	}
	return path
}
