package limitnotice

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// anomalyEvery is how often the check runs. AWS Cost Anomaly Detection, the reference for
	// this alert, runs about three times a day
	// (https://docs.aws.amazon.com/cost-management/latest/userguide/manage-ad.html); once an hour
	// tells the operator of a runaway agent loop sooner, and costs one read of the day's
	// requests on each shard.
	anomalyEvery = time.Hour
	// anomalyFactor is how many times its median day an organization must spend today to be
	// noticed. Steady growth stays under it: 20% a day for the four days between the median day
	// and today is about 2x. A bug, abuse or a loop that keeps calling a model is many times more.
	anomalyFactor = 5
	// anomalyDays is how many days before today make an organization's baseline: one of each
	// weekday, so a quiet weekend does not lower it. Days without spend count as nothing, so an
	// organization that spent on fewer than half of them has a median of nothing and is never
	// noticed: a week of history is the least the check judges.
	anomalyDays = 7
	// anomalyMinSpendMicros is the least an organization must spend today, in millionths of a
	// dollar, to be noticed, so a tiny organization going from cents to a few times that stays
	// quiet: a jump that small is somebody trying the API, and nothing for an operator to do.
	// No document gives the figure; it is set to 20 dollars, the least a day's jump is worth an
	// operator's look, and is the one number here to move if the channel is too noisy or quiet.
	anomalyMinSpendMicros = 20_000_000
	// anomalyTimeout bounds one check, so a holder whose database stopped answering lets go of
	// the lock rather than keep it while nobody checks.
	anomalyTimeout = 2 * time.Minute
)

// AnomalyOptions configure Anomalies. Store is required. Without a Sender the check posts and
// notes nothing and only deletes the activity past its keep.
type AnomalyOptions struct {
	Store *store.Store
	// Sender notes and posts each anomaly, and the activity digest with Digest; only its Links'
	// Staff applies, an anomaly and a digest having no app.
	Sender *Sender
	Logger *slog.Logger
	// Every is how often the check runs; defaults to an hour.
	Every time.Duration
	// Now is the time the check runs at; defaults to time.Now.
	Now func() time.Time
	// Digest has the check also post the activity digest of the day before, once per UTC day
	// (postDigest).
	Digest bool
}

// Anomalies tells the operator, in the money limit notices' channel, of each organization whose
// spend today is anomalyFactor times its median day of the anomalyDays before, once per
// organization and day. Every router runs one, and the one holding the deployment's cost anomaly
// lock does the check; the others try the lock again at the next tick. The holder also deletes
// the activity past its keep, once a day (prune).
type Anomalies struct {
	store  *store.Store
	lock   *store.ShipLock
	sender *Sender
	logger *slog.Logger
	every  time.Duration
	now    func() time.Time
	digest bool

	// baselines is each organization's median day before baselineDay, read once per day. Only
	// the checking goroutine uses them.
	baselines   map[string]int64
	baselineDay time.Time
	// prunedDay is the UTC day this router last deleted the activity past its keep. Only the
	// checking goroutine uses it.
	prunedDay time.Time

	stop context.CancelFunc
	done chan struct{}
}

// NewAnomalies returns a check of every organization's spend; Start starts it.
func NewAnomalies(options AnomalyOptions) *Anomalies {
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	every := options.Every
	if every <= 0 {
		every = anomalyEvery
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Anomalies{
		store: options.Store, lock: options.Store.CostAnomalyLock(), sender: options.Sender,
		logger: logger, every: every, now: now, digest: options.Digest,
	}
}

// Start checks at once and then every interval until Close.
func (a *Anomalies) Start() {
	ctx, stop := context.WithCancel(context.Background())
	a.stop, a.done = stop, make(chan struct{})
	go a.run(ctx)
}

// Close stops the check, waits for one under way to end, and gives the lock back. What it had
// noted and not posted is posted by a Sender's next sweep.
func (a *Anomalies) Close() {
	if a.stop == nil {
		return
	}
	a.stop()
	<-a.done
}

func (a *Anomalies) run(ctx context.Context) {
	defer close(a.done)
	defer a.lock.Release()
	ticker := time.NewTicker(a.every)
	defer ticker.Stop()
	for {
		a.check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// check notes and posts today's anomalies when this router holds the lock. A holder whose note
// of the spend fails lets the lock go, so that a router that can check takes it at its next
// tick. A check that Close ends is not logged as failed.
func (a *Anomalies) check(ctx context.Context) {
	now := a.now().UTC()
	checking, cancel := context.WithTimeout(ctx, anomalyTimeout)
	defer cancel()
	held, err := a.lock.Hold(checking)
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Error("could not take the cost anomaly lock, trying again", "in", a.every, "error", err)
		}
		return
	}
	if !held {
		return
	}
	if a.digest && a.sender != nil {
		a.postDigest(checking, now)
	}
	a.prune(checking, now)
	if a.sender == nil {
		// A router with no Slack gives the lock back after its delete, since a router with one
		// sharing the database would otherwise never get it and never post.
		a.lock.Release()
		return
	}
	if err := a.note(checking, now); err != nil {
		a.lock.Release()
		if ctx.Err() == nil {
			a.logger.Error("could not check the organizations' spend for anomalies, trying again", "in", a.every, "error", err)
		}
		return
	}
}

// prune deletes the activity and the activity digests past their keep (store.Activities.Prune,
// store.ActivityDigests.Prune) once per UTC day: the rows
// age out a day at a time, so one delete a day keeps up. One that failed is tried again at the
// next check. A router that takes the lock later the same day deletes again, and finds nothing.
func (a *Anomalies) prune(ctx context.Context, now time.Time) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if a.prunedDay.Equal(today) {
		return
	}
	pruned, err := a.store.Activities.Prune(ctx, now)
	if err == nil {
		var digests int
		digests, err = a.store.ActivityDigests.Prune(ctx, now)
		pruned += digests
	}
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Error("could not delete the activity past its keep, trying again", "in", a.every, "error", err)
		}
		return
	}
	a.prunedDay = today
	if pruned > 0 {
		a.logger.Info("deleted the activity past its keep", "rows", pruned)
	}
}

// note writes the anomaly of each organization that has one on now's UTC day, and posts those
// it wrote.
func (a *Anomalies) note(ctx context.Context, now time.Time) error {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if !a.baselineDay.Equal(today) {
		baselines, err := a.baselinesBefore(ctx, today)
		if err != nil {
			return err
		}
		a.baselines, a.baselineDay = baselines, today
	}
	spent, err := a.store.Spend.Daily(ctx, today, today.AddDate(0, 0, 1))
	if err != nil {
		return err
	}
	for _, spend := range spent {
		baseline := a.baselines[spend.OrganizationID]
		if spend.CostMicros < anomalyMinSpendMicros || baseline <= 0 || spend.CostMicros < anomalyFactor*baseline {
			continue
		}
		notice, err := store.CostAnomaly{
			OrganizationID: spend.OrganizationID, Day: today, SpentMicros: spend.CostMicros, BaselineMicros: baseline,
		}.Notice()
		if err != nil {
			return err
		}
		if _, err := a.sender.Note(ctx, notice); err != nil {
			return err
		}
	}
	return nil
}

// baselinesBefore is each organization's median daily spend over the anomalyDays before day,
// for the organizations that spent on any of them.
func (a *Anomalies) baselinesBefore(ctx context.Context, day time.Time) (map[string]int64, error) {
	spent, err := a.store.Spend.Daily(ctx, day.AddDate(0, 0, -anomalyDays), day)
	if err != nil {
		return nil, err
	}
	days := map[string][]int64{}
	for _, spend := range spent {
		days[spend.OrganizationID] = append(days[spend.OrganizationID], spend.CostMicros)
	}
	baselines := make(map[string]int64, len(days))
	for organization, costs := range days {
		baselines[organization] = median(costs, anomalyDays)
	}
	return baselines, nil
}

// median is the middle of costs, of days days, the days missing from costs having cost nothing.
// days is odd, so the middle is one of them.
func median(costs []int64, days int) int64 {
	all := make([]int64, days)
	copy(all, costs)
	slices.Sort(all)
	return all[days/2]
}

// AnomalyMessage is the anomaly as it is posted, in Slack's mrkdwn, tagged with its environment
// as Message is.
func AnomalyMessage(environment string, links Links, anomaly store.CostAnomaly) string {
	var b strings.Builder
	b.WriteString(EnvironmentTag(environment))
	fmt.Fprintf(&b, ":chart_with_upwards_trend: Organization `%s` spent %s on %s (UTC), %.1fx its median day of the %d before (%s).",
		anomaly.OrganizationID, dollars(anomaly.SpentMicros), anomaly.Day.UTC().Format(time.DateOnly),
		float64(anomaly.SpentMicros)/float64(anomaly.BaselineMicros), anomalyDays, dollars(anomaly.BaselineMicros))
	if staff := link(links.Staff, store.LimitNotice{OrganizationID: anomaly.OrganizationID}); staff != "" {
		b.WriteString("\n<" + staff + "|Staff page>")
	}
	return b.String()
}

// dollars is an amount in millionths of a dollar, to the cent.
func dollars(micros int64) string {
	return fmt.Sprintf("$%.2f", float64(micros)/1e6)
}
