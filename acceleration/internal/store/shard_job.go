package store

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Kinds of shard job (ShardJob.Kind).
const (
	// ShardJobForgetUser rewrites or deletes what a shard holds of an app's end user
	// (ForgetUserOnShards).
	ShardJobForgetUser = "forget_user"
	// ShardJobClaimGuest moves a claimed guest's sessions onto the account (Users.ClaimGuest).
	ShardJobClaimGuest = "claim_guest"
	// ShardJobLegacyPins counts what customers still have in the deployment's own Stream app
	// (LegacyStreamPins).
	ShardJobLegacyPins = "legacy_pins"
	// ShardJobDeleteBranches deletes the hidden branch messages of a user's sessions
	// (BranchMessages.DeleteUser).
	ShardJobDeleteBranches = "delete_branch_messages"
	// ShardJobDeleteBranchMessage deletes the copies of one message Stream Chat deleted
	// (BranchMessages.DeleteMessage).
	ShardJobDeleteBranchMessage = "delete_branch_message"
	// ShardJobDeleteInvocations deletes the invocations of deleted connections
	// (ConnectorConnections.DeleteUser).
	ShardJobDeleteInvocations = "delete_invocations"
)

// shardJobKinds are the kinds this router runs. A router claims no run of a kind it does not
// know, which a newer release queued.
var shardJobKinds = []string{ShardJobForgetUser, ShardJobClaimGuest, ShardJobLegacyPins, ShardJobDeleteBranches,
	ShardJobDeleteBranchMessage, ShardJobDeleteInvocations}

// States of a shard's run of a job (ShardJobRun.State).
const (
	shardJobQueued = "queued"
	shardJobDone   = "done"
)

const (
	// shardJobEvery is how often a worker looks for runs due.
	shardJobEvery = 5 * time.Second
	// shardJobBatch is how many runs a worker claims at once, and runs one after another.
	shardJobBatch = 10
	// shardJobTimeout bounds one run, and shardJobLease, longer, the claim on it.
	shardJobTimeout = 5 * time.Minute
	shardJobLease   = 10 * time.Minute
	// shardJobRetry is how long a run that failed waits for its next attempt.
	shardJobRetry = 30 * time.Second
	// shardJobKeep is how long a job every shard ran is kept, and how often workers prune.
	shardJobKeep = time.Hour
	// shardJobWaitEvery is how often a caller waiting on a job reads its runs.
	shardJobWaitEvery = 250 * time.Millisecond
)

// ShardJob is work every shard runs on its own rows: no router writes another shard's database.
type ShardJob struct {
	bun.BaseModel `bun:"table:shard_jobs,alias:sj"`

	ID   string `bun:"id,pk"`
	Kind string `bun:"kind,notnull"`
	// CustomerID is the customer the job is about, empty for a job about the deployment.
	CustomerID string          `bun:"customer_id,notnull"`
	Args       json.RawMessage `bun:"args,type:jsonb,notnull"`
	CreatedAt  time.Time       `bun:"created_at,notnull"`
}

// Validate refuses a job no router could run.
func (j *ShardJob) Validate() error {
	return validate("shard job").
		required("id", j.ID).
		oneOf("kind", j.Kind, shardJobKinds...).
		check(len(j.Args) > 0, "args", "is required").
		done()
}

// ShardJobRun is one shard's run of a job: queued until that shard's routers ran it, then done
// with what it counted.
type ShardJobRun struct {
	bun.BaseModel `bun:"table:shard_job_runs,alias:sjr"`

	ID     int64           `bun:"id,pk,autoincrement"`
	JobID  string          `bun:"job_id,notnull"`
	Shard  string          `bun:"shard,notnull"`
	State  string          `bun:"state,notnull"`
	Counts json.RawMessage `bun:"counts,type:jsonb,nullzero"`
	// Attempts is how many times the run was claimed, the current claim included: the claim a
	// write holds the run by.
	Attempts      int        `bun:"attempts,notnull"`
	NextAttemptAt time.Time  `bun:"next_attempt_at,notnull"`
	FinishedAt    *time.Time `bun:"finished_at"`
}

// ErrShardJobPending is a wait for a job that ended before every shard ran it. Its shards run
// it still, once they can.
var ErrShardJobPending = errors.New("store: not every shard has run the job")

// ShardJobs are the jobs every shard runs on its own rows, in the shared database. Each shard's
// routers claim that shard's runs as a queue.
type ShardJobs struct {
	store *Store
}

var shardJobRuns = queue{name: "shard job runs", alias: "sjr", key: "id"}

const shardJobRunClaimed = "sjr.id, sjr.job_id, sjr.shard, sjr.state, sjr.counts, sjr.attempts, sjr.next_attempt_at, sjr.finished_at"

// queue writes a job and a run of it for every shard of the deployment through db, so a caller
// queues it in the transaction of the write it follows, and answers its id.
func (r *ShardJobs) queue(ctx context.Context, db bun.IDB, kind, customerID string, args any) (string, error) {
	encoded, err := json.Marshal(args)
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: queue shard job: %w", err))
	}
	job := &ShardJob{ID: newID(), Kind: kind, CustomerID: customerID, Args: encoded, CreatedAt: time.Now().UTC()}
	if err := job.Validate(); err != nil {
		return "", stack.Wrap(err)
	}
	runs := make([]ShardJobRun, 0, len(r.store.shardNames))
	for _, shard := range r.store.shardNames {
		runs = append(runs, ShardJobRun{JobID: job.ID, Shard: shard})
	}
	_, err = db.NewInsert().With("job", db.NewInsert().Model(job)).Model(&runs).Column("job_id", "shard").Exec(ctx)
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: queue shard job: %w", err))
	}
	return job.ID, nil
}

// RunDue runs this shard's runs that are due, up to shardJobBatch, one after another, and
// answers how many it finished.
func (r *ShardJobs) RunDue(ctx context.Context) (int, error) {
	finished, err := r.run(ctx, nil)
	return len(finished), err
}

// run claims this shard's due runs that pick narrows, runs each and records what it counted.
// A run that fails is due again after shardJobRetry; one not started because ctx ended is due
// at once.
func (r *ShardJobs) run(ctx context.Context, pick func(*bun.SelectQuery) *bun.SelectQuery) ([]ShardJobRun, error) {
	runs, err := claim[ShardJobRun](ctx, r.store.global, shardJobRuns, shardJobLease, shardJobBatch, func(query *bun.SelectQuery) *bun.SelectQuery {
		query = query.Where("sjr.shard = ?", r.store.ShardName()).Where("sjr.state = ?", shardJobQueued).
			Where("EXISTS (SELECT 1 FROM shard_jobs AS sj WHERE sj.id = sjr.job_id AND sj.kind IN (?))", bun.In(shardJobKinds))
		if pick != nil {
			query = pick(query)
		}
		return query
	}, shardJobRunClaimed)
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.JobID)
	}
	var jobs []ShardJob
	if err := r.store.global.NewSelect().Model(&jobs).Where("id IN (?)", bun.In(ids)).Scan(ctx); err != nil {
		return nil, stack.Wrap(errors.Join(fmt.Errorf("store: read shard jobs: %w", err), r.release(ctx, runs, 0)))
	}
	byID := make(map[string]ShardJob, len(jobs))
	for _, job := range jobs {
		byID[job.ID] = job
	}
	var finished []ShardJobRun
	var failed []error
	for i, run := range runs {
		if ctx.Err() != nil {
			failed = append(failed, r.release(ctx, runs[i:], 0))
			break
		}
		job := byID[run.JobID]
		running, cancel := context.WithTimeout(ctx, shardJobTimeout)
		counts, err := r.store.runShardJob(running, job)
		cancel()
		if err != nil {
			failed = append(failed, fmt.Errorf("store: %s job %s: %w", job.Kind, job.ID, err), r.release(ctx, runs[i:i+1], shardJobRetry))
			continue
		}
		if run.Counts, err = json.Marshal(counts); err != nil {
			failed = append(failed, err, r.release(ctx, runs[i:i+1], shardJobRetry))
			continue
		}
		held, err := r.finish(ctx, run)
		if err != nil {
			failed = append(failed, err)
			continue
		}
		if held {
			finished = append(finished, run)
		}
	}
	return finished, stack.Wrap(errors.Join(failed...))
}

// finish records a run done with what it counted, while its claim still holds it. It runs on
// after ctx ends: the run's work is done, and recording it saves running it again.
func (r *ShardJobs) finish(ctx context.Context, run ShardJobRun) (bool, error) {
	writing, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimSettle)
	defer cancel()
	result, err := shardJobRuns.heldBy(r.store.global.NewUpdate().Model((*ShardJobRun)(nil)), run.ID, run.Attempts).
		Set("state = ?", shardJobDone).
		Set("counts = ?", run.Counts).
		Set("finished_at = now()").
		Where("state = ?", shardJobQueued).
		Exec(writing)
	if err != nil {
		return false, fmt.Errorf("store: finish shard job run: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: finish shard job run: %w", err)
	}
	return changed == 1, nil
}

// release makes runs a claim holds due again after wait. It runs on after ctx ends, so a router
// stopping gives back what it did not run rather than leaving it to its lease.
func (r *ShardJobs) release(ctx context.Context, runs []ShardJobRun, wait time.Duration) error {
	writing, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimSettle)
	defer cancel()
	var failed []error
	for _, run := range runs {
		_, err := shardJobRuns.heldBy(r.store.global.NewUpdate().Model((*ShardJobRun)(nil)), run.ID, run.Attempts).
			Set("next_attempt_at = now() + make_interval(secs => ?)", wait.Seconds()).
			Where("state = ?", shardJobQueued).
			Exec(writing)
		if err != nil {
			failed = append(failed, fmt.Errorf("store: release shard job run: %w", err))
		}
	}
	return errors.Join(failed...)
}

// everywhere runs this shard's run of a job, unless a worker of this shard has it, and waits
// until every shard ran its own or ctx ends. It answers what each shard counted, in shard order,
// and deletes the job.
func everywhere[T any](ctx context.Context, s *Store, jobID string) ([]T, error) {
	if _, err := s.ShardJobs.run(ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.Where("sjr.job_id = ?", jobID)
	}); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(shardJobWaitEvery)
	defer ticker.Stop()
	var waiting []string
	for {
		var runs []ShardJobRun
		err := s.global.NewSelect().Model(&runs).Where("job_id = ?", jobID).Order("shard").Scan(ctx)
		if err != nil && ctx.Err() == nil {
			return nil, stack.Wrap(fmt.Errorf("store: read shard job runs: %w", err))
		}
		if err == nil {
			waiting = waiting[:0]
			for _, run := range runs {
				if run.State != shardJobDone {
					waiting = append(waiting, run.Shard)
				}
			}
			if len(waiting) == 0 {
				return countsOf[T](ctx, s, jobID, runs)
			}
		}
		select {
		case <-ctx.Done():
			return nil, stack.Wrap(fmt.Errorf("%w: job %s waits on shards %s: %w", ErrShardJobPending, jobID, strings.Join(waiting, ", "), ctx.Err()))
		case <-ticker.C:
		}
	}
}

// countsOf reads what each run counted, and deletes the job every shard ran.
func countsOf[T any](ctx context.Context, s *Store, jobID string, runs []ShardJobRun) ([]T, error) {
	counts := make([]T, len(runs))
	for i, run := range runs {
		if err := json.Unmarshal(run.Counts, &counts[i]); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: read shard job counts: %w", err))
		}
	}
	if _, err := s.global.NewDelete().Model((*ShardJob)(nil)).Where("id = ?", jobID).Exec(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: delete shard job: %w", err))
	}
	return counts, nil
}

// here runs this shard's run of a job, unless a worker of this shard has it, and answers what it
// counted: zero when it ran elsewhere.
func here[T any](ctx context.Context, s *Store, jobID string) (T, error) {
	var counts T
	finished, err := s.ShardJobs.run(ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.Where("sjr.job_id = ?", jobID)
	})
	if err != nil || len(finished) == 0 {
		return counts, err
	}
	if err := json.Unmarshal(finished[0].Counts, &counts); err != nil {
		return counts, stack.Wrap(fmt.Errorf("store: read shard job counts: %w", err))
	}
	return counts, nil
}

// Prune deletes the jobs every shard ran that were queued over shardJobKeep before now.
func (r *ShardJobs) Prune(ctx context.Context, now time.Time) (int64, error) {
	pruned, err := deleteInBatches(ctx, r.store.global, "shard_jobs",
		"created_at < ? AND NOT EXISTS (SELECT 1 FROM shard_job_runs AS sjr WHERE sjr.job_id = shard_jobs.id AND sjr.state = ?)",
		now.Add(-shardJobKeep).UTC(), shardJobQueued)
	if err != nil {
		return pruned, stack.Wrap(fmt.Errorf("store: prune shard jobs: %w", err))
	}
	return pruned, nil
}

// runShardJob runs a job's work on this shard's rows and answers what it counted.
func (s *Store) runShardJob(ctx context.Context, job ShardJob) (any, error) {
	switch job.Kind {
	case ShardJobForgetUser:
		var args forgetUserArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		return s.forgetUserHere(ctx, job.CustomerID, args)
	case ShardJobClaimGuest:
		var args claimGuestArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		moved, err := moveGuestSessions(ctx, s.shard, job.CustomerID, args)
		return guestSessions{Sessions: moved}, err
	case ShardJobLegacyPins:
		var args legacyPinsArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		return s.legacyPinsHere(ctx, args.Deployment)
	case ShardJobDeleteBranches:
		var args userArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		deleted, err := s.BranchMessages.deleteUserHere(ctx, job.CustomerID, args.UserID)
		return deletedRows{Deleted: deleted}, err
	case ShardJobDeleteBranchMessage:
		var args messageArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		deleted, err := s.BranchMessages.deleteMessageHere(ctx, job.CustomerID, args)
		return deletedRows{Deleted: deleted}, err
	case ShardJobDeleteInvocations:
		var args connectionsArgs
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return nil, err
		}
		deleted, err := s.ConnectorConnections.deleteInvocationsHere(ctx, job.CustomerID, args.Connections)
		return deletedRows{Deleted: deleted}, err
	}
	return nil, fmt.Errorf("store: no shard job of kind %q", job.Kind)
}

// userArgs name the end user a job is about.
type userArgs struct {
	UserID string `json:"user_id"`
}

// deletedRows is what a job that deletes counts.
type deletedRows struct {
	Deleted int64 `json:"deleted"`
}

// ShardJobWorker runs this shard's runs of the shard jobs as they come due, and prunes the jobs
// every shard ran.
type ShardJobWorker struct {
	jobs   *ShardJobs
	logger *slog.Logger
	every  time.Duration
}

// ShardJobWorker returns a worker looking for runs due every interval, or every shardJobEvery for
// none; Run starts it.
func (s *Store) ShardJobWorker(logger *slog.Logger, every time.Duration) *ShardJobWorker {
	return &ShardJobWorker{jobs: s.ShardJobs, logger: logger, every: cmp.Or(every, shardJobEvery)}
}

// Run runs what is due at once and then every interval until ctx is done.
func (w *ShardJobWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.every)
	defer ticker.Stop()
	var pruned time.Time
	for {
		if _, err := w.jobs.RunDue(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("could not run this shard's jobs, trying again", "in", w.every, "error", err)
		}
		if now := time.Now(); now.Sub(pruned) >= shardJobKeep {
			if _, err := w.jobs.Prune(ctx, now); err != nil && ctx.Err() == nil {
				w.logger.Error("could not prune the shard jobs every shard ran", "error", err)
			} else {
				pruned = now
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
