package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// States of a picture an agent was asked to draw (ImageJob.State).
const (
	ImageJobQueued = "queued"
	ImageJobDone   = "done"
	ImageJobFailed = "failed"
)

// imageJobDoneKeep is how long a posted picture's row is kept, and imageJobFailedKeep a failed
// one's, for someone to find why. They hold the prompt, so not longer: the chat_inbound keeps.
const (
	imageJobDoneKeep   = chatInboundDoneKeep
	imageJobFailedKeep = chatInboundFailedKeep
)

// ImageJob is one picture an agent was asked to draw: what to draw, who pays, where it is
// posted and how far it got.
type ImageJob struct {
	bun.BaseModel `bun:"table:image_jobs,alias:ij"`

	// ID is the job, and names the message it posts.
	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK, CID and AgentUser are the conversation: its app, channel and the agent's
	// Stream user, who posts.
	StreamAppPK int64  `bun:"stream_app_pk,notnull"`
	CID         string `bun:"cid,notnull"`
	AgentUser   string `bun:"agent_user,notnull"`
	// SessionID, TurnID and ToolCallID are the call that asked, and RequestKey the request:
	// one job per key.
	SessionID  string `bun:"session_id,notnull"`
	TurnID     string `bun:"turn_id,notnull"`
	ToolCallID string `bun:"tool_call_id,notnull"`
	RequestKey string `bun:"request_key,notnull"`
	// AgentID, CallID, Region, Target, Tags and Attribution are what the draw is routed and
	// billed under. Attribution is a routing.Attribution, kept as JSON.
	AgentID     string            `bun:"agent_id,notnull"`
	CallID      string            `bun:"call_id,notnull"`
	Region      string            `bun:"region,notnull"`
	Target      string            `bun:"target,notnull"`
	Tags        map[string]string `bun:"tags,type:jsonb,nullzero"`
	Attribution json.RawMessage   `bun:"attribution,type:jsonb,notnull"`
	Prompt      string            `bun:"prompt,notnull"`
	AspectRatio string            `bun:"aspect_ratio,notnull"`
	State       string            `bun:"state,notnull"`
	// Attempts is how many times the job was claimed, the current claim included: the claim a
	// write holds the job by.
	Attempts int `bun:"attempts,notnull"`
	// Draws is how many draws were started that a provider took; Posts how many posts failed.
	// Released is how many claims were given back uncounted, so Attempts - Released are the
	// claims that ended unfinished or failed.
	Draws    int `bun:"draws,notnull"`
	Released int `bun:"released,notnull"`
	Posts    int `bun:"posts,notnull"`
	// NextAttemptAt is when the job is due; on a claimed row, the end of the claim's lease.
	NextAttemptAt time.Time `bun:"next_attempt_at,notnull"`
	// Drawn and DrawnType are the picture as drawn, until it is uploaded; Picture is it uploaded,
	// a sandbox.Attachment as JSON. Both nil until it is drawn.
	Drawn     []byte          `bun:"drawn,nullzero"`
	DrawnType string          `bun:"drawn_type,notnull"`
	Picture   json.RawMessage `bun:"picture,type:jsonb,nullzero"`
	// Message is what the job posts, once decided; Failed is a message saying why there is no
	// picture.
	Message    string     `bun:"message,notnull"`
	Failed     bool       `bun:"failed,notnull"`
	Outcome    string     `bun:"outcome,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	FinishedAt *time.Time `bun:"finished_at"`
}

// Validate refuses a job that could not be drawn or posted.
func (j *ImageJob) Validate() error {
	return validate("image job").
		required("id", j.ID).
		required("customer_id", j.CustomerID).
		required("cid", j.CID).
		required("agent_user", j.AgentUser).
		required("request_key", j.RequestKey).
		check(len(j.Attribution) > 0, "attribution", "is required").
		oneOf("state", j.State, ImageJobQueued, ImageJobDone, ImageJobFailed).
		done()
}

// ImageJobs are the pictures agents were asked to draw, a queue of them, in the shared
// database.
type ImageJobs struct {
	store *Store
}

// Queue writes a job to be drawn now, and reports whether it was new: a request
// already queued under its key queues nothing.
func (r *ImageJobs) Queue(ctx context.Context, job *ImageJob) (bool, error) {
	job.State = ImageJobQueued
	if err := job.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	if job.NextAttemptAt.IsZero() {
		job.NextAttemptAt = job.CreatedAt
	}
	result, err := r.store.global.NewInsert().Model(job).On("CONFLICT (request_key) DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: queue image job: %w", err))
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: queue image job: %w", err))
	}
	return inserted == 1, nil
}

// imageJobClaimed is every column of a claimed job but its drawn picture, up to 10 MiB each,
// which only an upload reads (Drawn).
const imageJobClaimed = `ij.id, ij.customer_id, ij.stream_app_pk, ij.cid, ij.agent_user, ij.session_id, ij.turn_id,
ij.tool_call_id, ij.request_key, ij.agent_id, ij.call_id, ij.region, ij.target, ij.tags, ij.attribution, ij.prompt,
ij.aspect_ratio, ij.state, ij.attempts, ij.draws, ij.released, ij.posts, ij.next_attempt_at, ij.drawn_type, ij.picture,
ij.message, ij.failed, ij.outcome, ij.created_at, ij.finished_at`

// Claim takes up to limit queued jobs due now, moves each one's next attempt to the end of
// lease and counts the claim. The drawn picture is left out of what it returns. decided takes the jobs whose message is decided, which only
// post; otherwise the ones still to be drawn. A job whose earlier claim's lease ran out, such as
// one a router stopped in the middle of, is due again, as claimChatInboundQuery's rows are.
func (r *ImageJobs) Claim(ctx context.Context, limit int, lease time.Duration, decided bool) ([]ImageJob, error) {
	return claim[ImageJob](ctx, r.store.global, imageJobs, lease, limit, queuedImageJobs(decided), imageJobClaimed)
}

// imageJobs is the queue of pictures to draw and post.
var imageJobs = queue{name: "image jobs", alias: "ij", key: "id"}

// queuedImageJobs narrows to the queued jobs of a kind: decided, with the message that posts
// them, or still to be drawn.
func queuedImageJobs(decided bool) func(*bun.SelectQuery) *bun.SelectQuery {
	return func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.Where("ij.state = ?", ImageJobQueued).Where("(ij.message <> '') = ?", decided)
	}
}

// Drawn is the picture the job keeps as drawn, nil when it keeps none.
func (r *ImageJobs) Drawn(ctx context.Context, id string) ([]byte, error) {
	var drawn []byte
	err := r.store.global.NewSelect().Model((*ImageJob)(nil)).Column("drawn").Where("id = ?", id).Scan(ctx, &drawn)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: image job drawn: %w", err))
	}
	return drawn, nil
}

// NextDue is when the next queued job of a kind is due, decided or still to be drawn as
// Claim takes them, and false when none is queued.
func (r *ImageJobs) NextDue(ctx context.Context, decided bool) (time.Time, bool, error) {
	return nextDue[ImageJob](ctx, r.store.global, imageJobs, queuedImageJobs(decided))
}

// StartDraw counts a draw the claim is about to start, before it starts: a router that
// stops in the middle of it leaves it counted. It reports false when the claim no longer holds
// the job.
func (r *ImageJobs) StartDraw(ctx context.Context, job ImageJob) (bool, error) {
	return r.update(ctx, job, "start image job draw", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("draws = draws + 1")
	})
}

// Release gives the job back for another claim at next, a claim that does not count
// against the job, without the draw the claim started when undraw: a draw no provider took, or
// one this router cut short as it stopped.
func (r *ImageJobs) Release(ctx context.Context, job ImageJob, undraw bool, next time.Time) (bool, error) {
	return r.update(ctx, job, "release image job", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		if undraw {
			update = update.Set("draws = greatest(draws - 1, 0)")
		}
		return update.Set("released = released + 1").Set("next_attempt_at = ?", next.UTC())
	})
}

// KeepDrawing keeps the picture a claim drew, and the message that posts it, so the job
// is never drawn again. It reports false when the claim no longer holds the job.
func (r *ImageJobs) KeepDrawing(ctx context.Context, job ImageJob, drawn []byte, mediaType, message string) (bool, error) {
	return r.update(ctx, job, "keep image job drawing", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("drawn = ?", drawn).Set("drawn_type = ?", mediaType).Set("message = ?", message).Set("failed = false")
	})
}

// DecideFailure decides that the job posts message, why there is no picture, and lets
// go of any picture it held. It reports false when the claim no longer holds the job.
func (r *ImageJobs) DecideFailure(ctx context.Context, job ImageJob, message string) (bool, error) {
	return r.update(ctx, job, "decide image job failure", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("message = ?", message).Set("failed = true").Set("drawn = NULL").Set("picture = NULL")
	})
}

// SavePicture keeps the uploaded picture and lets go of the drawn one. It reports false
// when the claim no longer holds the job.
func (r *ImageJobs) SavePicture(ctx context.Context, job ImageJob, picture json.RawMessage) (bool, error) {
	return r.update(ctx, job, "save image job picture", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("picture = ?", picture).Set("drawn = NULL")
	})
}

// RetryPost counts a post that failed and makes the job due again at next. It reports
// false when the claim no longer holds the job.
func (r *ImageJobs) RetryPost(ctx context.Context, job ImageJob, next time.Time) (bool, error) {
	return r.update(ctx, job, "retry image job post", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("posts = posts + 1").Set("next_attempt_at = ?", next.UTC())
	})
}

// Finish records that a claim is done with the job: posted (done), or failed with
// outcome. It reports false when the claim no longer holds the job.
func (r *ImageJobs) Finish(ctx context.Context, job ImageJob, state, outcome string, at time.Time) (bool, error) {
	if state != ImageJobDone && state != ImageJobFailed {
		return false, stack.Wrap(fmt.Errorf("store: an image job finishes done or failed, not %q", state))
	}
	return r.update(ctx, job, "finish image job", func(update *bun.UpdateQuery) *bun.UpdateQuery {
		return update.Set("state = ?", state).Set("outcome = ?", outcome).Set("finished_at = ?", at.UTC()).Set("drawn = NULL")
	})
}

// update changes a queued job only while the claim job came from still holds it: a
// slow claim and the one that took the job after its lease ran out never both write it.
func (r *ImageJobs) update(ctx context.Context, job ImageJob, doing string,
	set func(*bun.UpdateQuery) *bun.UpdateQuery) (bool, error) {
	result, err := imageJobs.heldBy(set(r.store.global.NewUpdate().Model((*ImageJob)(nil))), job.ID, job.Attempts).
		Where("state = ?", ImageJobQueued).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: %s: %w", doing, err))
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: %s: %w", doing, err))
	}
	return changed == 1, nil
}

// Prune deletes the jobs past their keep as of now, and returns how many.
func (r *ImageJobs) Prune(ctx context.Context, now time.Time) (int64, error) {
	pruned, err := deleteInBatches(ctx, r.store.global, "image_jobs", "(state = ? AND finished_at < ?) OR (state = ? AND finished_at < ?)",
		ImageJobDone, now.Add(-imageJobDoneKeep).UTC(), ImageJobFailed, now.Add(-imageJobFailedKeep).UTC())
	if err != nil {
		return pruned, stack.Wrap(fmt.Errorf("store: prune image jobs: %w", err))
	}
	return pruned, nil
}
