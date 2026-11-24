package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What an episode came in on, the source field of its card (20261007042100_episodes.sql).
const (
	EpisodeCall     = "call"
	EpisodeSlack    = "slack"
	EpisodeIMessage = "imessage"
	EpisodeSMS      = "sms"
	EpisodeWhatsApp = "whatsapp"
	// EpisodeChat is a run of messages in one agent channel of the app chat
	// (20261212090000_chat_episodes.sql).
	EpisodeChat = "chat"
)

// episodeInProgress is an episode not closed yet, the status its card is written with.
const episodeInProgress = "in_progress"

// The statuses that close an episode (20261007042100_episodes.sql; T55, omnichannel.Closer):
// ended once it is closed, then summarized or summary_failed once its summary is written or
// could not be.
const (
	EpisodeEnded         = "ended"
	EpisodeSummaryFailed = "summary_failed"
)

// EpisodeSummarized is an episode whose card holds its summary (20261007042100_episodes.sql;
// T55 sets it). A card in any other status is read by the last lines of its thread channel.
const EpisodeSummarized = "summarized"

// Episode is one call, or one run of messages on one external thread, and its card in the
// person's omni-channel (20261007042100_episodes.sql).
type Episode struct {
	bun.BaseModel `bun:"table:episodes,alias:ep"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// ContactID is the contact map row of the person; the card is in its omni-channel.
	ContactID string `bun:"contact_id,notnull"`
	Source    string `bun:"source,notnull"`
	// ThreadChannel is the cid of the channel with the raw text: a thread channel, or a
	// call channel.
	ThreadChannel string `bun:"thread_channel,notnull"`
	// CallID and SessionID are a call's Stream call and router session; empty for a thread.
	CallID    string `bun:"call_id,nullzero"`
	SessionID string `bun:"session_id,nullzero"`
	// CardMessageID is the card's Stream Chat message id.
	CardMessageID string     `bun:"card_message_id,notnull"`
	Status        string     `bun:"status,notnull"`
	StartedAt     time.Time  `bun:"started_at,notnull"`
	EndedAt       *time.Time `bun:"ended_at"`
	// Began, when set, is when a call began by this process's monotonic clock. Open then
	// dates the episode by Postgres's clock, which dates a link (user_phones.updated_at)
	// and its boundary: that long before the statement that opens it, in place of StartedAt.
	// Only a call names it: a thread episode's touch needs its time before the insert.
	Began time.Time `bun:"-"`
	// StreamAppPK is the Stream app the omni-channel is in; zero, stored as NULL, is the
	// deployment's own.
	StreamAppPK int64     `bun:"stream_app_pk,nullzero"`
	CreatedAt   time.Time `bun:"created_at,notnull,default:current_timestamp"`
	// LastMessageAt is when a thread episode's last message was made or queued, which keeps it
	// from closing as idle (Episodes.CloseIdle); nil while it has only its first.
	LastMessageAt *time.Time `bun:"last_message_at"`
	// LeaseUntil and LeasedBy are until when, and by which router, an ended episode is held to
	// be summarized: the router that closed it, or took it again once the lease ran out. Both
	// are empty on an episode in progress or finished.
	LeaseUntil *time.Time `bun:"lease_until"`
	LeasedBy   string     `bun:"leased_by,nullzero"`
}

// Validate refuses an episode its card could not be written for.
func (e *Episode) Validate() error {
	return validate("episode").
		required("customer_id", e.CustomerID).
		required("contact_id", e.ContactID).
		oneOf("source", e.Source, EpisodeCall, EpisodeSlack, EpisodeIMessage, EpisodeSMS, EpisodeWhatsApp, EpisodeChat).
		required("thread_channel", e.ThreadChannel).
		check((e.Source == EpisodeCall) == (e.SessionID != ""), "session_id", "is named by a call episode, and only by one").
		required("card_message_id", e.CardMessageID).
		oneOf("status", e.Status, episodeInProgress, EpisodeEnded, EpisodeSummarized, EpisodeSummaryFailed).
		done()
}

// Episodes are the calls and runs of thread messages that get a card in a person's
// omni-channel, in the shared database.
type Episodes struct {
	store *Store
}

// Open opens the episode episode describes, unless its thread already has one open,
// or its call session already has one; episode is then that one. opened is whether this call
// opened it, and so whether its card is still to be written. Two first messages of one
// thread at once open one episode: the unique indexes decide which.
//
// Each message of a thread is the thread's last one for now, so a message that finds the
// episode open sets its last_message_at to StartedAt's time, which keeps the episode from
// closing as idle (CloseIdle); the
// first message is its started_at. The touch finds the episode open, or misses it, in one
// statement, and holds its row against a close until it commits: an episode the idle sweeper
// closed after the insert found it open is missed, and the message opens the next one.
func (r *Episodes) Open(ctx context.Context, episode *Episode) (opened bool, err error) {
	if episode.ID == "" {
		episode.ID = newID()
	}
	if episode.CardMessageID == "" {
		episode.CardMessageID = "episode-" + episode.ID
	}
	episode.Status = episodeInProgress
	if err := episode.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	if episode.StartedAt.IsZero() {
		episode.StartedAt = time.Now()
	}
	episode.StartedAt = episode.StartedAt.UTC().Truncate(time.Microsecond)
	at := episode.StartedAt
	// Twice at most: a second miss is the thread's episode closed and another opened between
	// the insert and the touch, twice over.
	for range 2 {
		insert := r.store.global.NewInsert().Model(episode).On("CONFLICT DO NOTHING")
		// bun returns a column a Value sets, so StartedAt is then the start Postgres holds.
		if !episode.Began.IsZero() {
			insert = insert.Value("started_at", "statement_timestamp() - make_interval(secs => ?)", time.Since(episode.Began).Seconds())
		}
		result, err := insert.Exec(ctx)
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open episode: %w", err))
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open episode: %w", err))
		}
		if inserted == 1 {
			return true, nil
		}
		if episode.SessionID != "" {
			err := r.store.global.NewSelect().Model(episode).Where("customer_id = ?", episode.CustomerID).
				Where("session_id = ?", episode.SessionID).Scan(ctx)
			if err != nil {
				return false, stack.Wrap(fmt.Errorf("store: open episode: %w", err))
			}
			return false, nil
		}
		err = r.store.global.NewRaw(touchThreadEpisodeQuery, at, episode.CustomerID, episode.ThreadChannel).Scan(ctx, episode)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open episode: %w", err))
		}
		return false, nil
	}
	return false, stack.Wrap(errors.New("store: open episode: the thread's episode closed twice while a message opened it"))
}

// touchThreadEpisodeQuery sets the last message of the thread's episode in progress, and
// reads the episode. The update locks the episode's row, so a sweep skips it meanwhile
// (idleEpisodesQuery), and a touch that waited for a close finds the episode no longer open. A
// message's time is when it was made or queued, so a message tried again after a later one
// keeps the later one's time: the episode stays open from its last message on.
const touchThreadEpisodeQuery = `
UPDATE episodes AS ep
SET last_message_at = GREATEST(ep.last_message_at, ?)
WHERE ep.customer_id = ? AND ep.thread_channel = ? AND ep.status = 'in_progress' AND ep.session_id IS NULL
RETURNING ep.*`

// ClosedEpisode is an episode a closer holds to summarize, with, from its contact map row, the
// omni-channel its card is in, the agent config whose LLM writes the summary and how the
// person is known, which a call's lines are checked against.
type ClosedEpisode struct {
	Episode        `bun:",extend"`
	ConversationID string `bun:"conversation_id,scanonly"`
	ConfigID       string `bun:"config_id,scanonly"`
	ContactKind    string `bun:"contact_kind,scanonly"`
	ContactAddress string `bun:"contact_address,scanonly"`
}

// withContact ends each statement that closes or takes episodes: the closed CTE's rows, read
// with their contact map row.
const withContact = `
SELECT closed.*, cm.conversation_id, cm.config_id, cm.kind AS contact_kind, ` + reachedAt + ` AS contact_address
FROM closed
JOIN contact_map AS cm ON cm.id = closed.contact_id`

// idleEpisodesQuery locks at most limit thread episodes whose last message, or whose start
// while they have none, is at or before the idle cutoff, and skips the ones another router,
// or a message (touchThreadEpisodeQuery), holds.
const idleEpisodesQuery = `
SELECT ep.id FROM episodes AS ep
WHERE ep.status = 'in_progress' AND ep.session_id IS NULL
  AND COALESCE(ep.last_message_at, ep.started_at) <= ?
ORDER BY ep.started_at
LIMIT ?
FOR UPDATE OF ep SKIP LOCKED`

// closeIdleEpisodesQuery closes the episodes idleEpisodesQuery locked, which no other router
// can close meanwhile, so each episode is closed once, by one router, and leases each to it.
// It is a statement of its own, so it reads the status and the last messages as they are once
// the rows are locked: it checks them again, so an episode already ended is not closed twice,
// and an episode a message touched after idleEpisodesQuery started is left alone.
const closeIdleEpisodesQuery = `
WITH closed AS (
    UPDATE episodes AS ep
    SET status = 'ended', ended_at = ?, lease_until = ?, leased_by = ?
    WHERE ep.id IN (?) AND ep.status = 'in_progress' AND ep.session_id IS NULL
      AND COALESCE(ep.last_message_at, ep.started_at) <= ?
    RETURNING ep.*
)` + withContact

// CloseIdle ends, at now, at most limit thread episodes of any customer with no message since
// idleSince, each leased to holder for its summary until leaseUntil. A call's episode is never
// idle: its call ends it (EndCall).
func (r *Episodes) CloseIdle(ctx context.Context, idleSince, now time.Time, limit int, leaseUntil time.Time, holder string) ([]ClosedEpisode, error) {
	closed := []ClosedEpisode{}
	if limit < 1 {
		return closed, nil
	}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var due []string
		if err := tx.NewRaw(idleEpisodesQuery, idleSince.UTC(), limit).Scan(ctx, &due); err != nil {
			return err
		}
		if len(due) == 0 {
			return nil
		}
		return tx.NewRaw(closeIdleEpisodesQuery, now.UTC(), leaseUntil.UTC(), holder, bun.In(due), idleSince.UTC()).Scan(ctx, &closed)
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: close idle episodes: %w", err))
	}
	return closed, nil
}

// endCallEpisodesQuery ends the call episodes still in progress of one call in one app, and
// leases each to the router that ended it. The
// status is checked on the row the update locks, so the event delivered twice, to two
// routers, ends each episode once. session_id IS NOT NULL is every call episode
// (20261007042100_episodes.sql), named so episodes_open_call finds them.
const endCallEpisodesQuery = `
WITH closed AS (
    UPDATE episodes
    SET status = 'ended', ended_at = ?, lease_until = ?, leased_by = ?
    WHERE source = 'call' AND session_id IS NOT NULL AND call_id = ? AND status = 'in_progress' AND (?)
    RETURNING *
)` + withContact

// EndCall ends, at now, the episodes in progress of the call callID in the app scope names,
// each leased to holder for its summary until leaseUntil. A call id is unique
// only within its app, so the scope is the app the call event came from, as
// ReleaseCallResourcesInApp takes it. None is no error: a call under an agent config without
// episode cards has none.
func (r *Episodes) EndCall(ctx context.Context, scope AppScope, callID string, now, leaseUntil time.Time, holder string) ([]ClosedEpisode, error) {
	closed := []ClosedEpisode{}
	if callID == "" {
		return closed, nil
	}
	err := r.store.global.NewRaw(endCallEpisodesQuery, now.UTC(), leaseUntil.UTC(), holder, callID, scope.clause()).Scan(ctx, &closed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: end call episodes: %w", err))
	}
	return closed, nil
}

// claimEpisodeSummariesQuery takes at most limit ended episodes whose summary lease ran out:
// a router closed them and stopped before it wrote their summary. Only an ended episode has a
// lease: closing sets it, and finishing drops it (FinishSummary). due locks the leases it takes
// and skips the ones another router holds; one that another router took or finished after the
// statement started is checked again as it locks it, and left alone.
const claimEpisodeSummariesQuery = `
WITH due AS (
    SELECT id FROM episodes
    WHERE lease_until <= ?
    ORDER BY lease_until
    LIMIT ?
    FOR UPDATE SKIP LOCKED
), closed AS (
    UPDATE episodes AS ep
    SET lease_until = ?, leased_by = ?
    FROM due
    WHERE ep.id = due.id
    RETURNING ep.*
)` + withContact

// ClaimSummaries takes at most limit ended episodes whose summary lease ran out at now, each
// leased to holder until leaseUntil.
func (r *Episodes) ClaimSummaries(ctx context.Context, now time.Time, limit int, leaseUntil time.Time, holder string) ([]ClosedEpisode, error) {
	claimed := []ClosedEpisode{}
	if limit < 1 {
		return claimed, nil
	}
	err := r.store.global.NewRaw(claimEpisodeSummariesQuery, now.UTC(), limit, leaseUntil.UTC(), holder).Scan(ctx, &claimed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: claim episode summaries: %w", err))
	}
	return claimed, nil
}

// FinishSummary sets an ended episode summarized or summary_failed and drops its lease. An
// episode no longer ended, finished already, is left as it is.
func (r *Episodes) FinishSummary(ctx context.Context, customerID, id, status string) error {
	if status != EpisodeSummarized && status != EpisodeSummaryFailed {
		return stack.Wrap(fmt.Errorf("store: %q is not a status a summary finishes with", status))
	}
	_, err := r.store.global.NewUpdate().Model((*Episode)(nil)).
		Set("status = ?", status).
		Set("lease_until = NULL").
		Set("leased_by = NULL").
		Where("ep.customer_id = ? AND ep.id = ? AND ep.status = ?", customerID, id, EpisodeEnded).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish episode summary: %w", err))
	}
	return nil
}

// anyEpisodeToSweepQuery is whether the idle sweeper has, or may get, an episode to close or
// summarize: an agent config that writes episode cards, on or record (AgentConfig.WritesCards),
// whose calls and app chats open episodes; a thread or chat episode still in progress
// (episodes_open_thread), whatever its config says now; or a summary lease still held
// (episodes_lease).
const anyEpisodeToSweepQuery = `
SELECT EXISTS (SELECT 1 FROM agent_configs WHERE (episode_cards_on OR episode_cards_record) AND deleted_at IS NULL)
    OR EXISTS (SELECT 1 FROM episodes WHERE status = 'in_progress' AND session_id IS NULL)
    OR EXISTS (SELECT 1 FROM episodes WHERE lease_until IS NOT NULL)`

// AnyToSweep reports whether an agent config of any customer has episode_cards on or
// record, so its calls and app chats may open episodes, or whether an episode opened before is still to
// close or to summarize: one that a config opened with the cards on and has turned them off
// since closes as any other (AI-2771).
func (r *Episodes) AnyToSweep(ctx context.Context) (bool, error) {
	var due bool
	if err := r.store.global.NewRaw(anyEpisodeToSweepQuery).Scan(ctx, &due); err != nil {
		return false, stack.Wrap(fmt.Errorf("store: any episode to sweep: %w", err))
	}
	return due, nil
}

// EpisodeCard is an episode as a session reads its card: the episode, and until when its
// thread channel's lines are its own.
type EpisodeCard struct {
	Episode `bun:",extend"`
	// Until is the earliest of when the episode ended, when its call's session closed and
	// when the next episode in its thread channel started. A call channel can hold several
	// callers' calls (episodes_call_session), so a call's lines end there. Nil while none
	// of them is known.
	Until *time.Time `bun:"until,scanonly"`
	// ContactKind and ContactAddress are how the episode's person is known, from its contact
	// map row: for a call, the caller's number, which is whose lines the card's are.
	ContactKind    string `bun:"contact_kind,scanonly"`
	ContactAddress string `bun:"contact_address,scanonly"`
	// ConfigID is the agent config of the episode's contact map row, which an app chat's
	// channel names.
	ConfigID string `bun:"config_id,scanonly"`
}

// CardsQuery is whose episode cards a session reads, and which of them it leaves out
// because it reads them word for word already.
type CardsQuery struct {
	CustomerID    string
	AgentConfigID string
	// ConversationID is the person's omni-channel: every contact map row of the customer
	// and agent that points at it is the person, whatever they came in on.
	ConversationID string
	// ExceptThread and ExceptSession leave out the session's own thread channel and its
	// own call's episode. Empty leaves out nothing.
	ExceptThread  string
	ExceptSession string
	Limit         int
}

// Cards are the newest episodes of one person, for one agent of a customer: those of
// the contact map rows of that customer and agent that point at the person's omni-channel,
// never another customer's or another agent's, and none a number linked to a user started
// before its link's boundary (historyBoundary). A boundary before the link's own time reads the
// rows the link set aside of its number too (setAsideRead).
func (r *Episodes) Cards(ctx context.Context, query CardsQuery) ([]EpisodeCard, error) {
	if query.CustomerID == "" || query.AgentConfigID == "" || query.ConversationID == "" || query.Limit <= 0 {
		return nil, stack.Wrap(errors.New("store: a customer, an agent config, an omni-channel and a limit are required"))
	}
	var cards []EpisodeCard
	err := r.store.global.NewSelect().Model(&cards).
		ColumnExpr("ep.*").
		ColumnExpr("cm.kind AS contact_kind, "+reachedAt+" AS contact_address, cm.config_id").
		// LEAST ignores NULLs, so Until is nil only when both are.
		ColumnExpr(`LEAST(ep.ended_at, (SELECT min(nx.started_at) FROM episodes AS nx
			WHERE nx.customer_id = ep.customer_id AND nx.thread_channel = ep.thread_channel
			AND nx.started_at > ep.started_at)) AS until`).
		Join(`JOIN (SELECT cm.* FROM contact_map AS cm WHERE cm.customer_id = ?0 AND cm.config_id = ?1
			AND cm.conversation_id = ?2
			UNION ALL `+setAsideRead+`) AS cm ON cm.id = ep.contact_id`,
			query.CustomerID, query.AgentConfigID, query.ConversationID, r.store.streamPins().deployment(), ContactStreamUser, ContactPhone).
		Where("ep.customer_id = ?", query.CustomerID).
		Where("cm.customer_id = ?", query.CustomerID).
		Where("cm.config_id = ?", query.AgentConfigID).
		Where("ep.thread_channel <> ?", query.ExceptThread).
		Where("ep.session_id IS DISTINCT FROM ?", query.ExceptSession).
		// What a linked number said before its link is not the user's, however the config came
		// to have episode_cards on: retireHistory sets it aside only under a config that wrote
		// cards, on or record, at the link. The link is found as linkOf finds a number's, a
		// set-aside row's by its number (reachedAt); no other kind of row has an E.164 address.
		Where(`NOT EXISTS (SELECT 1 FROM user_phones AS up `+historyJoin+` WHERE up.customer_id = cm.customer_id
			AND up.stream_app_pk = COALESCE(NULLIF(cm.stream_app_pk, ?), 0) AND up.phone = `+reachedAt+`
			AND ep.started_at < `+historyBoundary+`)`, r.store.streamPins().deployment()).
		OrderExpr("ep.started_at DESC").
		Limit(query.Limit).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: episode cards: %w", err))
	}
	if err := r.closeCardsWithSessions(ctx, query.CustomerID, cards); err != nil {
		return nil, err
	}
	return cards, nil
}

// closeCardsWithSessions ends each card no later than its call's session closed. Sessions are
// kept in the shards and episodes in the shared database, so they are read apart.
func (r *Episodes) closeCardsWithSessions(ctx context.Context, customerID string, cards []EpisodeCard) error {
	var ids []string
	for _, card := range cards {
		if card.SessionID != "" {
			ids = append(ids, card.SessionID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]AgentSession, error) {
		var closed []AgentSession
		err := db.NewSelect().Model(&closed).Column("id", "closed_at").
			Where("customer_id = ?", customerID).
			Where("id IN (?)", bun.In(ids)).
			Where("closed_at IS NOT NULL").
			Scan(ctx)
		return closed, err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: episode cards: sessions: %w", err))
	}
	// A session carried on in another shard has a row in each, and the latest close is when
	// it ended.
	closedAt := map[string]time.Time{}
	for _, list := range lists {
		for _, session := range list {
			if at := *session.ClosedAt; at.After(closedAt[session.ID]) {
				closedAt[session.ID] = at
			}
		}
	}
	for i := range cards {
		at, ok := closedAt[cards[i].SessionID]
		if ok && (cards[i].Until == nil || at.Before(*cards[i].Until)) {
			cards[i].Until = &at
		}
	}
	return nil
}
