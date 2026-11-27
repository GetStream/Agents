package api

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// TypeLogCallSites is the async task type that reads Stream SFU stats for an ended phone call.
const TypeLogCallSites = "api:log_call_sites"

// LogCallSitesTask reads where Stream ran an ended phone call and records a call_sites log row.
type LogCallSitesTask struct {
	CustomerID    string `json:"customer_id"`
	AppID         int64  `json:"app_id"`
	CallType      string `json:"call_type"`
	CallID        string `json:"call_id"`
	StreamSession string `json:"stream_session"`
	SipHost       string `json:"sip_host,omitempty"`
}

func (LogCallSitesTask) TaskType() string   { return TypeLogCallSites }
func (LogCallSitesTask) Queue() async.Queue { return async.QueueMaintenance }
func (LogCallSitesTask) Scope() async.Scope { return async.ScopeShard }
func (LogCallSitesTask) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(2 * time.Minute),
		async.WithMaxRetry(1),
	}
}

// RegisterCallSiteTasks binds the call_sites lookup task to reg.
func (s *Server) RegisterCallSiteTasks(reg *async.Registry) {
	if s == nil {
		return
	}
	async.RegisterFunc(reg, func(ctx context.Context, task LogCallSitesTask) error {
		s.runLogCallSites(ctx, endedCall{
			customer:      task.CustomerID,
			app:           task.AppID,
			callType:      task.CallType,
			callID:        task.CallID,
			streamSession: task.StreamSession,
			sipHost:       task.SipHost,
		}, s.readCallStats, s.callStatsWaits)
		return nil
	})
}

// callSites is which SFU each side of a phone call was on, and how many SFUs Stream used.
type callSites struct {
	SFUsUsed int
	Agent    sideSite
	Caller   sideSite
}

// sideSite is the SFU one side of a call was on. Location is where Stream placed that side,
// and DistanceKm how far that is from the SFU; both are empty when Stream does not know.
type sideSite struct {
	SFU, Location, DistanceKm string
}

// sitesOf reads each side's SFU out of a session's participant stats. The phone caller is
// told apart by its participant id; anyone else is the agent. A side keeps the first SFU
// listed for it, as the tool keeps the agent's first joined row.
func sitesOf(stats *getstream.QueryCallSessionParticipantStatsResponse) callSites {
	sites := callSites{SFUsUsed: stats.Counts.SfusUsed}
	for _, participant := range stats.Participants {
		side := &sites.Agent
		if isCaller(participant) {
			side = &sites.Caller
		}
		for _, session := range participant.Sessions {
			if side.SFU == "" && hasSFU(session) {
				*side = siteOf(session)
			}
		}
	}
	return sites
}

func isCaller(participant getstream.CallStatsParticipant) bool {
	_, caller := phone.CallerNumber(participant.UserID)
	return caller
}

func hasSFU(session getstream.CallStatsParticipantSession) bool {
	return session.CurrentSfu != nil && *session.CurrentSfu != ""
}

func siteOf(session getstream.CallStatsParticipantSession) sideSite {
	site := sideSite{SFU: *session.CurrentSfu}
	if place := session.Location; place != nil {
		site.Location = joinPresent(textOf(place.City), textOf(place.Subdivision), textOf(place.CountryIsoCode))
	}
	if km := session.DistanceToSfuKilometers; km != nil {
		site.DistanceKm = strconv.FormatFloat(*km, 'f', 0, 64)
	}
	return site
}

// statsReady says the stats are ready: the agent and the caller are both listed, each with
// an SFU, and no session is still without one. Stream fills them in some time after the
// call ends, and may list one side before the other.
func statsReady(stats *getstream.QueryCallSessionParticipantStatsResponse) bool {
	sites := sitesOf(stats)
	if sites.Agent.SFU == "" || sites.Caller.SFU == "" {
		return false
	}
	for _, participant := range stats.Participants {
		for _, session := range participant.Sessions {
			if !hasSFU(session) {
				return false
			}
		}
	}
	return true
}

// details is the call_sites row's details, minus what is not about the SFUs.
func (sites callSites) details() map[string]any {
	details := map[string]any{"sfus_used": strconv.Itoa(sites.SFUsUsed)}
	sites.Agent.addTo(details, "agent")
	sites.Caller.addTo(details, "caller")
	return details
}

func (site sideSite) addTo(details map[string]any, side string) {
	details[side+"_current_sfu"] = site.SFU
	if site.Location != "" {
		details[side+"_location"] = site.Location
	}
	if site.DistanceKm != "" {
		details[side+"_distance_to_sfu_km"] = site.DistanceKm
	}
}

func textOf(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

func joinPresent(parts ...string) string {
	var present []string
	for _, part := range parts {
		if part != "" {
			present = append(present, part)
		}
	}
	return strings.Join(present, ", ")
}

// readCallStats asks Stream for one session's participant stats.
type readCallStats func(ctx context.Context, client *getstream.Stream, callType, callID, session string) (*getstream.QueryCallSessionParticipantStatsResponse, error)

func streamCallStats(ctx context.Context, client *getstream.Stream, callType, callID, session string) (*getstream.QueryCallSessionParticipantStatsResponse, error) {
	// One page is enough: a phone call has two participants, the agent and the caller.
	response, err := client.Video().QueryCallSessionParticipantStats(ctx, callType, callID, session, &getstream.QueryCallSessionParticipantStatsRequest{})
	if err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// defaultCallStatsWaits is how long to wait before each stats read. Stream fills a call's
// stats in some time after the call ends, so the first read waits and later ones try again.
var defaultCallStatsWaits = []time.Duration{5 * time.Second, 10 * time.Second, 10 * time.Second, 10 * time.Second}

// fetchCallSites reads a call's stats until they are ready (statsReady), waiting before
// each try. Stats that never fill give the last answer; no answer at all gives the
// last error.
func fetchCallSites(
	ctx context.Context,
	read func(context.Context) (*getstream.QueryCallSessionParticipantStatsResponse, error),
	waits []time.Duration,
) (callSites, error) {
	var last *getstream.QueryCallSessionParticipantStatsResponse
	var lastErr error
	for _, wait := range waits {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		stats, err := read(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		last = stats
		if statsReady(stats) {
			break
		}
	}
	if last == nil {
		return callSites{}, lastErr
	}
	return sitesOf(last), nil
}

// endedCall is a phone call that has just ended, and whose it was.
type endedCall struct {
	customer      string
	app           int64
	callType      string
	callID        string
	streamSession string
	sipHost       string
}

// endedPhoneCall says whether an ended call was a phone call of this hook's app. It has to
// be asked before the call's resources are released, since those are what mark a placed call.
func (s *Server) endedPhoneCall(r *http.Request, origin hookOrigin, event callEvent) (endedCall, bool) {
	if s.store == nil {
		return endedCall{}, false
	}
	callType, callID, split := strings.Cut(event.CallCid, ":")
	if !split {
		return endedCall{}, false
	}
	call := endedCall{callType: callType, callID: callID, streamSession: event.SessionID}

	resources, err := s.store.CallResources.Select(r.Context(), origin.scope(), callType, callID)
	if err != nil {
		s.logger.Warn("could not read an ended call's resources", "session_id", callID, "error", err)
	}
	if len(resources) > 0 {
		call.customer, call.app = resources[0].CustomerID, resources[0].StreamAppPK
	} else if rung, ok := customOf(event.Call.Custom)[phone.NumberKey]; ok {
		number, err := s.store.NumberRungInApp(r.Context(), origin.scope(), rung)
		if err != nil {
			// Not the error itself: it names the number.
			s.logger.Warn("could not tell whose number an ended call rang; its sites are not logged", "session_id", callID)
			return endedCall{}, false
		}
		call.customer, call.app = number.CustomerID, number.StreamAppPK
		call.sipHost = phone.BridgeHost(number.BridgeURI)
	} else {
		return endedCall{}, false
	}

	if !origin.deployment && call.customer != origin.customer {
		return endedCall{}, false
	}
	return call, true
}

// logCallSites reads where Stream ran an ended phone call and keeps it as a call_sites row.
// When s.async is configured, it enqueues a durable task onto QueueMaintenance so the HTTP
// pod does not hold a 2-minute sleeping goroutine across restarts.
func (s *Server) logCallSites(ctx context.Context, call endedCall) {
	if s.async != nil {
		var opts []async.Option
		if call.callID != "" && call.streamSession != "" {
			opts = append(opts, async.WithTaskID("call-sites:"+call.callID+":"+call.streamSession))
		}
		if _, err := async.Enqueue(ctx, s.async, LogCallSitesTask{
			CustomerID:    call.customer,
			AppID:         call.app,
			CallType:      call.callType,
			CallID:        call.callID,
			StreamSession: call.streamSession,
			SipHost:       call.sipHost,
		}, opts...); err != nil && !errors.Is(err, async.ErrTaskIDConflict) {
			s.logger.Warn("could not enqueue phone call sites lookup", "session_id", call.callID, "error", err)
		}
		return
	}
	read, waits := s.readCallStats, s.callStatsWaits
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		s.runLogCallSites(ctx, call, read, waits)
	}()
}

func (s *Server) runLogCallSites(ctx context.Context, call endedCall, read readCallStats, waits []time.Duration) {
	sites, err := s.fetchSites(ctx, call, read, waits)
	details := sites.details()
	details["stream_session_id"] = call.streamSession
	if call.sipHost != "" {
		details["stream_sip_host"] = call.sipHost
	}
	logged := append([]any{"session_id", call.callID}, attributesOf(details)...)
	if err != nil {
		// The row is the customer's to read; the error, naming Stream's hosts, is the log's.
		details["error_message"] = callSitesUnread
		s.logger.Warn("could not read a phone call's sites", append(logged, "error", err)...)
	} else {
		s.logger.Info("phone call sites", logged...)
	}
	s.recordCallSites(ctx, call, details)
}

// callSitesUnread is why a call_sites row names no SFUs.
const callSitesUnread = "the call's stats could not be read from Stream"

func (s *Server) fetchSites(ctx context.Context, call endedCall, read readCallStats, waits []time.Duration) (callSites, error) {
	bound, err := s.streamForApp(ctx, call.customer, call.app, true)
	if err != nil {
		return callSites{}, err
	}
	return fetchCallSites(ctx, func(ctx context.Context) (*getstream.QueryCallSessionParticipantStatsResponse, error) {
		return read(ctx, bound.Client, call.callType, call.callID, call.streamSession)
	}, waits)
}

// attributesOf is details as log attributes, in key order so the line reads the same each time.
func attributesOf(details map[string]any) []any {
	var attributes []any
	for _, key := range slices.Sorted(maps.Keys(details)) {
		attributes = append(attributes, key, details[key])
	}
	return attributes
}

const callSitesWriteTimeout = 10 * time.Second

// recordCallSites writes the call_sites row, under the session's config and agent when the
// router ran one for the call.
func (s *Server) recordCallSites(ctx context.Context, call endedCall, details map[string]any) {
	// The read's context may be spent by now; the row is the point of the read.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), callSitesWriteTimeout)
	defer cancel()
	entry := &store.AgentLog{
		CustomerID: call.customer,
		SessionID:  call.callID,
		Source:     "system",
		Severity:   "info",
		EventType:  "call_sites",
		Message:    "Call sites",
		Details:    details,
	}
	if session, err := s.store.Sessions.Get(ctx, call.customer, call.callID); err == nil {
		entry.ConfigID, entry.AgentID = session.ConfigID, session.AgentID
	}
	if err := s.store.AgentLogs.Insert(ctx, entry); err != nil {
		s.logger.Warn("could not record a phone call's sites", "session_id", call.callID, "error", err)
	}
}
