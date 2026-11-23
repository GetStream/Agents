package phone

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// MaxCallLength is the longest a leg may stay open: a leg still open then is ended by the
// sweep, and given away rather than billed, since nothing said how long it really lasted.
const MaxCallLength = 4 * time.Hour

// LegSweepEvery is how often the legs left open past MaxCallLength are ended, and the legs
// that ended lately checked for their row.
const LegSweepEvery = 10 * time.Minute

// legSweepBatch bounds how many legs one sweep ends or checks, so a backlog is worked through
// over a few sweeps rather than in one long statement.
const legSweepBatch = 500

// The legs a sweep checks were billed: those that ended between rebillFrom and rebillTo ago,
// long enough for the writer to have written their row, and recent enough that a process that
// died between ending a leg and writing its row has its legs checked on the next start.
const (
	rebillFrom = 2 * time.Hour
	rebillTo   = 5 * time.Minute
)

// keepParticipantsFor is how long a phone's join or leave no leg was waiting for is kept for
// the leg that opens after it: far longer than any delivery is late.
const keepParticipantsFor = 24 * time.Hour

// openAttempts and openFirstPause bound how often OpenLeg writes a leg's row before leaving it
// to the leg sweep: the first write and two more, 100 ms and then 200 ms after the one before,
// for a dropped connection or a failover. The call is up by then, so a leg with no row is a
// call nobody is billed for. unopenedMost bounds the legs a process keeps for the sweep until
// it stops; one past it is logged and not billed.
const (
	openAttempts   = 3
	openFirstPause = 100 * time.Millisecond
	unopenedMost   = 1000
)

// AttributeEndedBy says what ended a leg billed by anything other than its phone leaving or
// the call ending: the sweep.
const AttributeEndedBy = "ended_by"

// OpenLeg records a leg of a call, billed to owner once its phone has joined: the request that
// placed it, or the app whose number a caller rang. An outbound leg rings until its phone joins
// (Connect); pass started for one already connected, as a call that arrived is, from the leg's
// DialedAt, when the event announcing it was made, or now. Its country is where it rang unless
// the leg names its own. An inbound leg its Stream session already has is not opened again. A
// leg whose row cannot be written is tried again, then kept for the leg sweep (SweepLegs), which
// writes it with the join and leave its phone had meanwhile, as a leg written late is.
func (s *Service) OpenLeg(ctx context.Context, owner routing.Owner, leg store.CallLeg, started bool) {
	if s.store == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if owner.OrganizationID == "" {
		// Known when the row is written, but the concurrent calls of an organization are counted
		// from its open legs.
		membership, err := s.store.AppOrganizations.Get(ctx, owner.CustomerID)
		if err != nil {
			s.logger.Warn("could not tell whose organization a call leg is", "customer", owner.CustomerID, "error", err)
		}
		owner.OrganizationID = membership.OrganizationID
	}
	if leg.Direction == store.Outbound {
		s.gate.Dialed(ctx, owner.CustomerID, owner.OrganizationID, leg.RemoteNumber)
	}
	leg.CustomerID = owner.CustomerID
	leg.OrganizationID, leg.OrganizationTier = owner.OrganizationID, owner.OrganizationTier
	leg.APIKey, leg.UserID, leg.UsageCategories = owner.APIKey, owner.UserID, owner.UsageCategories
	leg.AgentID, leg.Tags = owner.AgentID, owner.Tags
	if leg.Country == "" {
		leg.Country = Country(leg.RemoteNumber)
	}
	leg.DialedAt = s.eventAt(leg.DialedAt)
	if started {
		leg.StartedAt = &leg.DialedAt
	}
	opened, err := s.store.CallLegs.Open(ctx, &leg)
	pause := openFirstPause
	for attempt := 1; attempt < openAttempts && err != nil && !opened && openAgain(err); attempt++ {
		time.Sleep(pause)
		pause *= 2
		opened, err = s.store.CallLegs.Open(ctx, &leg)
	}
	if err != nil && !opened && openAgain(err) && s.unopened.add(leg) {
		s.logger.Warn("could not record a call leg; the leg sweep records it", "call", leg.CallType+":"+leg.CallID,
			"vendor", leg.Vendor, "direction", leg.Direction, "error", err)
		return
	}
	s.recorded(ctx, leg, opened, err)
}

// openAgain says whether writing a leg's row again may pass: not when the store or Postgres
// refused the row itself, nor for a placed leg that is gone or dialled already.
func openAgain(err error) bool {
	return !store.Unwritable(err) && !errors.Is(err, store.ErrNoPlacedCall)
}

// recorded follows a leg's write: a leg its phone joined, and maybe left, before it was written
// is billed or started, and one that could not be written is logged.
func (s *Service) recorded(ctx context.Context, leg store.CallLeg, opened bool, err error) {
	switch {
	case err != nil && !opened:
		s.logger.Error("could not record a call leg; its minutes are not billed", "call", leg.CallType+":"+leg.CallID,
			"vendor", leg.Vendor, "direction", leg.Direction, "error", err)
	case err != nil:
		s.logger.Error("could not give a call leg the phone that joined before it; it starts when the phone's join is delivered again",
			"call", leg.CallType+":"+leg.CallID, "vendor", leg.Vendor, "direction", leg.Direction, "error", err)
	case leg.EndedAt != nil:
		// Its phone joined and left before it was recorded.
		s.settleLegs(ctx, []store.CallLeg{leg})
	case leg.StartedAt != nil:
		s.told(ctx, leg)
		s.awaitAgent(leg)
	}
}

// Connect starts the leg of a phone that joined a call: the caller of a call that arrived, or
// the person a placed call or a transfer rang, who has answered, told apart by the phone's
// number, empty when unknown. Its minutes run from at, when the event saying so was made, or
// now when that is zero. A phone already given its leg, by an earlier delivery, starts nothing.
//
// Stream does not order its call events, so a join no leg is waiting for yet is kept for the
// leg that opens later, and a phone whose leave came first has its leg ended at that leave,
// rather than open, and billed, until the call ends.
func (s *Service) Connect(ctx context.Context, scope store.AppScope, callType, callID, sessionID, participant, number string,
	at time.Time) error {
	if s.store == nil || participant == "" {
		return nil
	}
	at = s.eventAt(at)
	left, err := s.store.CallParticipants.Left(ctx, scope, callType, callID, participant)
	if err != nil {
		return err
	}
	if s.connecting != nil {
		s.connecting()
	}
	leg, connection, err := s.store.CallLegs.Connect(ctx, scope, callType, callID, sessionID, participant, number, at, left)
	if err != nil || connection == store.LegKnown {
		return err
	}
	if connection == store.NothingWaiting {
		if err := s.store.CallParticipants.Record(ctx, scope, store.CallParticipant{
			CallType: callType, CallID: callID, ParticipantSession: participant, Number: number, JoinedAt: &at,
		}, s.now()); err != nil {
			return err
		}
		// A leg opened between the two looks found nothing kept: it is connected here.
		leg, connection, err = s.store.CallLegs.Connect(ctx, scope, callType, callID, sessionID, participant, number, at, left)
		switch {
		case err != nil || connection == store.NothingWaiting:
			return err
		case connection == store.LegKnown:
			return s.store.CallParticipants.Forget(ctx, scope, callType, callID, participant, nil)
		}
	}
	if leg.EndedAt != nil {
		s.settleLegs(ctx, []store.CallLeg{leg})
	} else {
		s.told(ctx, leg)
		s.awaitAgent(leg)
		if left, err = s.store.CallParticipants.Left(ctx, scope, callType, callID, participant); err != nil {
			return err
		} else if left != nil {
			// The phone's leave was kept between the first look and the connect, which did not
			// see it: the leg ends at it, before the leave is forgotten.
			ended, err := s.store.CallLegs.EndParticipant(ctx, scope, callType, callID, participant, *left)
			if err != nil {
				return err
			}
			s.settleLegs(ctx, ended)
		}
	}
	return s.store.CallParticipants.Forget(ctx, scope, callType, callID, participant, left)
}

// Left ends the leg of a phone that left a call at at (or now, when zero), and bills its
// minutes. A leave no leg has the phone of is kept, for the phone's join delivered after it
// and for the leg that opens later.
func (s *Service) Left(ctx context.Context, scope store.AppScope, callType, callID, participant string, at time.Time) error {
	if s.store == nil || participant == "" {
		return nil
	}
	at = s.eventAt(at)
	ended, err := s.store.CallLegs.EndParticipant(ctx, scope, callType, callID, participant, at)
	if err != nil {
		return err
	}
	if len(ended) > 0 {
		s.settleLegs(ctx, ended)
		return nil
	}
	known, err := s.store.CallLegs.HasParticipant(ctx, scope, callType, callID, participant)
	if err != nil || known {
		return err
	}
	if err := s.store.CallParticipants.Record(ctx, scope, store.CallParticipant{
		CallType: callType, CallID: callID, ParticipantSession: participant, LeftAt: &at,
	}, s.now()); err != nil {
		return err
	}
	// A leg given the phone between the two looks did not see the leave: it is ended here.
	if ended, err = s.store.CallLegs.EndParticipant(ctx, scope, callType, callID, participant, at); err != nil {
		return err
	}
	s.settleLegs(ctx, ended)
	return nil
}

// EndLegs ends the legs of a call that ended at at (or now, when zero), in the app the event
// about it came from, and bills each whose phone joined for its minutes. A leg already ended,
// by its phone leaving, an earlier delivery of the same event, or the sweep, is not billed again.
func (s *Service) EndLegs(ctx context.Context, scope store.AppScope, callType, callID string, at time.Time) error {
	if s.store == nil {
		return nil
	}
	ended, err := s.store.CallLegs.EndCall(ctx, scope, callType, callID, s.eventAt(at))
	if err != nil {
		return err
	}
	s.settleLegs(ctx, ended)
	s.checkHooked(ctx, scope, callType, callID, ended)
	return nil
}

// heardRecheck is how long a call found with no outbound leg ringing is not asked about again,
// so a video call with many participants costs a read a minute, and a placed call whose agent
// joined before its leg was written is still heard by the agent's later events.
const heardRecheck = time.Minute

// Heard records that the call hook delivered a participant event of a call, anybody's, which
// is what tells a hook subscribed to them from one that is not (checkHooked). Only a call with
// an outbound leg still ringing is recorded, which is the only one checked: a process records
// each such call once an hour, and asks after any other call at most once every heardRecheck.
func (s *Service) Heard(ctx context.Context, scope store.AppScope, callType, callID string) error {
	if s.store == nil {
		return nil
	}
	key := strconv.FormatInt(scope.App, 10) + ":" + strconv.FormatBool(scope.Unpinned) + ":" + callType + ":" + callID
	now := s.now()
	if !s.heard.first(key, now, heardRecheck) {
		return nil
	}
	ringing, err := s.store.CallLegs.Ringing(ctx, scope, callType, callID)
	if err != nil || !ringing {
		if err != nil {
			s.heard.forget(key)
		}
		return err
	}
	if err := s.store.CallParticipants.RecordHeard(ctx, scope, callType, callID, now); err != nil {
		s.heard.forget(key)
		return err
	}
	s.heard.remember(key, now.Add(time.Hour))
	return nil
}

// checkHooked logs at ERROR, once an hour per app, a call that ended with an outbound leg its
// phone never joined and no participant event of it ever delivered. A hook subscribed to them
// delivers the agent's own join on every placed call, so this is an app whose call hook was not
// pointed again (cmd/phone hooks) since the participant events were added to it: its placed
// calls and transfers never connect, and their minutes are not billed.
func (s *Service) checkHooked(ctx context.Context, scope store.AppScope, callType, callID string, ended []store.CallLeg) {
	index := slices.IndexFunc(ended, func(leg store.CallLeg) bool {
		return leg.Direction == store.Outbound && !leg.Connected()
	})
	if index < 0 {
		return
	}
	heard, err := s.store.CallParticipants.Heard(ctx, scope, callType, callID)
	if err != nil {
		s.logger.Warn("could not tell whether the call hook delivers participant events", "call", callType+":"+callID, "error", err)
		return
	}
	leg := ended[index]
	if heard || !s.unhooked.first(strconv.FormatInt(leg.StreamAppPK, 10), s.now(), time.Hour) {
		return
	}
	s.logger.Error("call hook does not deliver participant events; re-run cmd/phone hooks",
		"stream_app", leg.StreamAppPK, "customer", leg.CustomerID, "call", callType+":"+callID,
		"hint", "a placed call ended unanswered and no participant event of it, not even the agent's join, was delivered: "+
			"placed calls and transfers in this app never connect, and their minutes are not billed")
}

// recently remembers keys until a time each, and at most recentlyMost of them, so a burst of
// new ones forgets the oldest rather than grow without bound.
type recently struct {
	mu    sync.Mutex
	until map[string]time.Time
}

const recentlyMost = 10_000

// first reports whether key is not remembered at now, and remembers it for long if so.
func (r *recently) first(key string, now time.Time, long time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if until, ok := r.until[key]; ok && now.Before(until) {
		return false
	}
	r.rememberLocked(key, now, now.Add(long))
	return true
}

// remember remembers key until until.
func (r *recently) remember(key string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rememberLocked(key, time.Time{}, until)
}

func (r *recently) rememberLocked(key string, now, until time.Time) {
	if r.until == nil {
		r.until = map[string]time.Time{}
	}
	if _, ok := r.until[key]; !ok && len(r.until) >= recentlyMost {
		maps.DeleteFunc(r.until, func(_ string, until time.Time) bool { return !now.Before(until) })
		if len(r.until) >= recentlyMost {
			clear(r.until)
		}
	}
	r.until[key] = until
}

// forget drops key, so it is first again.
func (r *recently) forget(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.until, key)
}

// unopenedLegs are the legs whose row OpenLeg could not write, kept for the leg sweep, at most
// unopenedMost of them.
type unopenedLegs struct {
	mu   sync.Mutex
	legs []store.CallLeg
}

// add keeps leg, and reports false when unopenedMost are kept already.
func (u *unopenedLegs) add(leg store.CallLeg) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.legs) >= unopenedMost {
		return false
	}
	u.legs = append(u.legs, leg)
	return true
}

// take hands over every leg kept.
func (u *unopenedLegs) take() []store.CallLeg {
	u.mu.Lock()
	defer u.mu.Unlock()
	legs := u.legs
	u.legs = nil
	return legs
}

// openUnopened writes the row of each leg OpenLeg could not, and follows it as OpenLeg would. A
// write that fails in a way a retry may pass leaves it and those after it for the next sweep.
func (s *Service) openUnopened(ctx context.Context) {
	legs := s.unopened.take()
	for i := range legs {
		leg := legs[i]
		opened, err := s.store.CallLegs.Open(ctx, &leg)
		if err != nil && !opened && openAgain(err) {
			for _, left := range legs[i:] {
				if !s.unopened.add(left) {
					s.recorded(ctx, left, false, err)
				}
			}
			return
		}
		s.recorded(ctx, leg, opened, err)
	}
}

// loseUnopened logs each leg still kept for the sweep when the sweep stops: its minutes are not
// billed.
func (s *Service) loseUnopened() {
	for _, leg := range s.unopened.take() {
		s.logger.Error("could not record a call leg before the router stopped; its minutes are not billed",
			"call", leg.CallType+":"+leg.CallID, "vendor", leg.Vendor, "direction", leg.Direction)
	}
}

// settleLegs bills the legs that ended whose phone joined. One that never connected was not
// answered, which a vendor does not bill: it is logged, since a call hook that does not deliver
// call.session_participant_joined would leave every placed call looking so.
func (s *Service) settleLegs(ctx context.Context, ended []store.CallLeg) {
	for _, leg := range ended {
		if !leg.Connected() {
			s.logger.Warn("a call leg ended without its phone joining the call; it is not billed",
				"customer", leg.CustomerID, "call", leg.CallType+":"+leg.CallID, "vendor", leg.Vendor,
				"direction", leg.Direction, "hint", "unanswered, or the call hook does not deliver call.session_participant_joined")
			continue
		}
		s.settleLeg(ctx, leg, true)
	}
	s.told(ctx, ended...)
}

// outcomeTimeout bounds keeping one leg's join or end for its call's outcome, so a slow
// write does not hold the Stream hook request that reported it.
const outcomeTimeout = 5 * time.Second

// told tells the outcome keeper when each leg's phone joined and when the leg ended. A leg the
// keeper has no call for, a transfer's or an inbound one, changes nothing. A failure is logged:
// the leg is already billed, and its outcome is all that is lost.
func (s *Service) told(ctx context.Context, legs ...store.CallLeg) {
	for _, leg := range legs {
		s.tell(ctx, leg)
	}
}

func (s *Service) tell(ctx context.Context, leg store.CallLeg) {
	// The leg is already ended, so a redelivered event will not tell again: a hook request
	// canceled now would lose the outcome for good.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outcomeTimeout)
	defer cancel()
	key := outcome.Key{
		CustomerID: leg.CustomerID, StreamAppPK: leg.StreamAppPK, CallType: leg.CallType, CallID: leg.CallID,
		Local: leg.LocalNumber, Remote: leg.RemoteNumber,
	}
	if leg.StartedAt != nil {
		if err := s.outcomes.Answered(ctx, key, *leg.StartedAt); err != nil {
			s.logger.Error("could not keep that a call was answered", "customer", leg.CustomerID,
				"call", leg.CallType+":"+leg.CallID, "error", err)
		}
	}
	if leg.EndedAt != nil {
		inbound := leg.Direction == store.Inbound
		ended := outcome.Leg{Vendor: leg.Vendor, Inbound: inbound, AnsweredAt: leg.StartedAt, Refused: inbound && s.refused(ctx, leg)}
		if err := s.outcomes.Ended(ctx, key, *leg.EndedAt, ended); err != nil {
			s.logger.Error("could not keep that a call ended", "customer", leg.CustomerID,
				"call", leg.CallType+":"+leg.CallID, "error", err)
		}
	}
}

// refusedFor is how long an arriving call the router refused stays marked: its leg ends by
// MaxCallLength, when the leg sweep, run every LegSweepEvery, ends what nothing else did.
const refusedFor = MaxCallLength + LegSweepEvery

// Refused marks the Stream session session of the customer's arriving call in app as one the
// router refused: no agent answers it, and it is ended at Stream. Its leg is counted failed when
// it ends, on whichever node ends it (tell). A mark that cannot be kept is logged, and the call
// is counted completed.
func (s *Service) Refused(ctx context.Context, customer string, app int64, callType, callID, session string) {
	if s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outcomeTimeout)
	defer cancel()
	if err := s.store.CallLegs.MarkRefused(ctx, customer, app, callType, callID, session, refusedFor); err != nil {
		s.logger.Warn("could not mark a refused arriving call; it is counted completed",
			"customer", customer, "call", callType+":"+callID, "error", err)
	}
}

// refusedReadTimeout bounds reading a leg's refusal mark, so a Redis that answers nothing
// leaves the rest of outcomeTimeout to keeping the leg's end. One read of one key is answered
// in well under a millisecond by a Redis that is up; a second is long past that.
const refusedReadTimeout = time.Second

// refused reports whether the call of an inbound leg was marked Refused. One that cannot be
// read in refusedReadTimeout is logged, and taken as not refused.
func (s *Service) refused(ctx context.Context, leg store.CallLeg) bool {
	if s.store == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, refusedReadTimeout)
	defer cancel()
	found, err := s.store.CallLegs.Refused(ctx, leg)
	if err != nil {
		s.logger.Warn("could not tell whether the router refused an arriving call; it is counted completed",
			"customer", leg.CustomerID, "call", leg.CallType+":"+leg.CallID, "error", err)
		return false
	}
	return found
}

// SweepLegs writes the legs OpenLeg could not, ends the legs of this shard open longer than
// MaxCallLength, and checks the legs that ended lately have their row, writing again any a
// process lost between ending the leg and writing it. It reports how many legs it ended.
//
// A leg whose phone joined and that nothing ended is logged at ERROR and recorded priced and
// given away at MaxCallLength: a call that ran four hours is rare, a lost session_ended is not,
// and nobody can tell which it was.
func (s *Service) SweepLegs(ctx context.Context) (int, error) {
	if s.store == nil {
		return 0, nil
	}
	s.openUnopened(ctx)
	now := s.now()
	ended, err := s.store.CallLegs.Sweep(ctx, now, MaxCallLength, legSweepBatch)
	if err != nil {
		return 0, err
	}
	for _, leg := range ended {
		if !leg.Connected() {
			s.logger.Warn("ended a call leg that rang for the longest a call may last without its phone joining",
				"customer", leg.CustomerID, "call", leg.CallType+":"+leg.CallID, "vendor", leg.Vendor)
			continue
		}
		s.logger.Error("ended a call leg nothing said had ended; it is recorded and not billed",
			"customer", leg.CustomerID, "call", leg.CallType+":"+leg.CallID, "vendor", leg.Vendor,
			"direction", leg.Direction, "started_at", leg.StartedAt, "recorded", MaxCallLength)
		s.settleLeg(ctx, leg, true)
	}
	s.told(ctx, ended...)
	if err := s.rebill(ctx, now); err != nil {
		return len(ended), err
	}
	for {
		deleted, err := s.store.CallParticipants.DeleteBefore(ctx, now.Add(-keepParticipantsFor), legSweepBatch)
		if err != nil {
			return len(ended), err
		}
		if deleted < legSweepBatch {
			return len(ended), nil
		}
	}
}

// rebill writes the row of each leg that ended lately and has none, under the usage id it
// was first meant to have, so a row written meanwhile is not written twice.
func (s *Service) rebill(ctx context.Context, now time.Time) error {
	legs, err := s.store.CallLegs.SelectEnded(ctx, now.Add(-rebillFrom), now.Add(-rebillTo), legSweepBatch)
	if err != nil || len(legs) == 0 {
		return err
	}
	ids := make([]uuid.UUID, len(legs))
	for i, leg := range legs {
		ids[i] = legUsageID(leg)
	}
	recorded, err := s.store.RecordedUsage(ctx, ids)
	if err != nil {
		return err
	}
	for i, leg := range legs {
		if recorded[ids[i]] {
			continue
		}
		s.logger.Warn("a call leg that ended had no usage row; it is recorded again",
			"customer", leg.CustomerID, "call", leg.CallType+":"+leg.CallID, "ended_at", leg.EndedAt)
		s.settleLeg(ctx, leg, false)
	}
	return nil
}

// RunLegSweep sweeps every interval until ctx ends, and then logs the legs it never wrote.
func (s *Service) RunLegSweep(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.loseUnopened()
			return
		case <-ticker.C:
			if _, err := s.SweepLegs(ctx); err != nil {
				s.logger.Error("could not sweep the call legs", "error", err)
			}
		}
	}
}

// settleLeg bills an ended leg's minutes to whoever it was opened for, and the first time,
// counts them toward the 10DLC sandbox, which gives them away while it holds the app, and an
// outbound leg's toward the organization's voice minutes a day. A leg the sweep ended is given
// away too, and not counted: nothing says how long it really lasted, and four hours of it would
// use up the sandbox's minutes.
func (s *Service) settleLeg(ctx context.Context, leg store.CallLeg, first bool) {
	if first && leg.EndedAt != nil && leg.EndReason != store.LegSwept {
		s.gate.Talked(ctx, leg.CustomerID, leg.OrganizationID, leg.Direction == store.Outbound, leg.EndedAt.Sub(*leg.StartedAt))
	}
	meter := routing.Meter{
		Recorder: s.recorder,
		Config:   routing.ProviderConfig{Provider: leg.Vendor, Model: ModelCallMinutes},
		Owner:    legOwner(leg),
		UsageID:  legUsageID(leg),
		BilledAt: *leg.StartedAt,
		Attributes: map[string]string{
			AttributeCountry:            leg.Country,
			AttributeDirection:          leg.Direction,
			routing.AttributeMeasuredBy: routing.MeasuredByRouter,
		},
		Unmeasured: true,
	}
	if leg.EndReason == store.LegSwept {
		meter.Free = true
		meter.Attributes[AttributeEndedBy] = store.LegSwept
	}
	if s.sandboxed(ctx, leg.CustomerID) {
		meter.Free = true
		meter.Attributes[AttributeDLCSandbox] = "true"
	}
	if err := routing.Settle(meter, leg); err != nil {
		s.logger.Error("could not bill a call leg", "call", leg.CallType+":"+leg.CallID, "error", err)
	}
}

// legUsageID names a leg's row. A leg's id is only unique within its shard, so the call and
// when it was dialled are part of it.
func legUsageID(leg store.CallLeg) uuid.UUID {
	return routing.UsageID("call_leg", leg.CustomerID, leg.CallType, leg.CallID, strconv.FormatInt(leg.ID, 10),
		strconv.FormatInt(leg.DialedAt.UnixMicro(), 10))
}

// legOwner is who a leg was opened for, read back from what it kept.
func legOwner(leg store.CallLeg) routing.Owner {
	return routing.Owner{
		CustomerID: leg.CustomerID,
		Attribution: routing.Attribution{
			OrganizationID: leg.OrganizationID, OrganizationTier: leg.OrganizationTier,
			APIKey: leg.APIKey, UserID: leg.UserID, UsageCategories: leg.UsageCategories,
		},
		AgentID: leg.AgentID,
		CallID:  leg.CallID,
		Tags:    leg.Tags,
	}
}

// eventLateness is the most an event's own time is trusted to be before it was delivered:
// Stream redelivers within minutes, and a time further back is a misread or a bogus one, which
// would start a leg hours early and have the sweep bill it for four.
const eventLateness = time.Hour

// eventAt is when a call event says it happened, which its legs are timed by, since Stream may
// deliver one late or before one made earlier. An event that says nothing, names a time still to
// come on this clock, or one more than eventLateness before now, happened now.
func (s *Service) eventAt(at time.Time) time.Time {
	now := s.now()
	if at.IsZero() || at.After(now) || now.Sub(at) > eventLateness {
		return now
	}
	return at
}

// now is the service's clock, which legs are timed by when an event says nothing.
func (s *Service) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}
