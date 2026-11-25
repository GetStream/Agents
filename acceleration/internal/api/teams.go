package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// errTeamsUnreadable refuses a config to an end user whose teams Stream did not tell, in an
// app that keeps each team's configs to its own users. Opening it anyway would let anybody in
// while Stream is down, and a 404 would say the config is gone when asking again may work.
var errTeamsUnreadable = unavailable("could not read the user's teams from Stream; try again shortly")

// errSharedStream is a customer acting in the deployment's shared Stream app, which is not its own.
var errSharedStream = errors.New("api: the customer has no Stream app of its own to read teams from")

// teamsGate is what keeps an app's configs of one team to its users: the teams of the end
// user asking, when the app has AgentTeams on and the caller is not its backend, which Chat
// does not limit by teams either. The zero gate refuses nothing.
type teamsGate struct {
	checked bool
	teams   []string
}

// teamsGateFor reads the settings and the caller's teams before any config is looked up, so
// that a failed read answers alike for a config that exists and one that does not.
func (s *Server) teamsGateFor(ctx context.Context, customerID string) (teamsGate, error) {
	if ServerSideFrom(ctx) {
		return teamsGate{}, nil
	}
	return s.userTeamsGate(ctx, customerID, CallerFrom(ctx))
}

// userTeamsGate is the gate of an end user, named outright where no request of theirs says
// who is asking.
func (s *Server) userTeamsGate(ctx context.Context, customerID string, user routing.Caller) (teamsGate, error) {
	settings, err := s.configs.Apps.GetSettings(ctx, customerID)
	if err != nil {
		return teamsGate{}, err
	}
	if !settings.AgentTeamsChecked() {
		return teamsGate{}, nil
	}
	teams, err := s.callerTeams(ctx, customerID, user)
	if err != nil {
		return teamsGate{}, err
	}
	return teamsGate{checked: true, teams: teams}, nil
}

// refuses reports whether config is of a team the end user asking is not in, which the caller
// is told as it is of a config that does not exist.
func (g teamsGate) refuses(config store.AgentConfig) bool {
	return g.checked && !hasCommonTeam(g.teams, strings.TrimSpace(config.Tags[session.TeamTag]))
}

// configOpenedWith is the config a session was opened with, as it is now, for a fork or a
// reopen that names none: nil when it was deleted since. To an end user the gate checks, a
// config of another team is unknown, as configFor answers, and so is a deleted one, which
// leaves no team to check.
func (s *Server) configOpenedWith(ctx context.Context, customerID, id string, teams teamsGate) (*store.AgentConfig, error) {
	read, err := s.store.AgentConfigs.Get(ctx, customerID, id)
	switch {
	case errors.Is(err, store.ErrNoAgentConfig) && !teams.checked:
		return nil, nil
	case errors.Is(err, store.ErrNoAgentConfig):
		return nil, errUnknownConfig
	case err != nil:
		return nil, err
	case teams.refuses(read):
		return nil, errUnknownConfig
	}
	return &read, nil
}

// refuseOutsideTeam refuses a session opened with a config to an end user the gate checks
// and who is not in that config's team now. The config is not read for a session opened with
// no config, which has no team to check, nor for a caller the gate does not check.
func (s *Server) refuseOutsideTeam(ctx context.Context, customerID, configID string) error {
	if configID == "" || s.store == nil {
		return nil
	}
	teams, err := s.teamsGateFor(ctx, customerID)
	if err != nil || !teams.checked {
		return err
	}
	_, err = s.configOpenedWith(ctx, customerID, configID, teams)
	return err
}

// refuseOwnerOutsideTeam is refuseOutsideTeam for owner, asked where no request of theirs is:
// a watcher relayed from another node, or an occurrence of their scheduled task. Their backend
// is not checked, as it is not on a request.
func (s *Server) refuseOwnerOutsideTeam(ctx context.Context, owner session.Owner, configID string) error {
	if configID == "" || s.store == nil || owner.Kind == auth.KindServer {
		return nil
	}
	teams, err := s.userTeamsGate(ctx, owner.CustomerID, routing.Caller{
		UserID: owner.UserID, Verified: owner.UserID != "" && owner.Kind.Verified(),
	})
	if err != nil || !teams.checked {
		return err
	}
	_, err = s.configOpenedWith(ctx, owner.CustomerID, configID, teams)
	return err
}

// callerTeams is the teams of the end user caller, as their user in the customer's own
// Stream app holds them: an app the customer holds a token for, or the deployment's own app
// when the customer is that app's. A customer that only borrows the deployment's app reads
// none: user ids there are not the customer's and whoever holds that app writes their teams.
// A caller with no token is in none: the user id it goes by is a claim nobody checked,
// and reading that user's teams would let anybody borrow them. What Stream tells is held for
// teamsFor (heldTeams).
func (s *Server) callerTeams(ctx context.Context, customerID string, caller routing.Caller) ([]string, error) {
	if !caller.Verified {
		return nil, nil
	}
	bound, ok, err := s.streamFor(ctx, customerID)
	switch {
	case err == nil && !ok:
		err = errNoStream
	case err == nil && !bound.Identity.Registered && !s.ownApp(customerID, bound.Identity.StreamApp):
		err = errSharedStream
	}
	if err == nil {
		key := heldTeamsKey(customerID, bound.Client, caller.UserID)
		if teams, held := s.teams.of(key); held {
			return teams, nil
		}
		var teams []string
		if teams, err = conversation.UserTeams(ctx, bound.Client, caller.UserID); err == nil {
			s.teams.keep(key, teams)
			return teams, nil
		}
	}
	s.logger.Error("could not read a user's teams from Stream", "user", caller.UserID, "error", err)
	return nil, errTeamsUnreadable
}

// teamsFor is how long a user's teams read from Stream are taken on trust. Every message and
// socket of a running chat in a teams app asks, and every occurrence of a task, and Stream rate
// limits the query per app. A user taken out of a team reaches its agents for up to this long
// after.
const teamsFor = 30 * time.Second

// maxHeldTeams bounds the users whose teams one router holds, across every app it serves.
const maxHeldTeams = 100_000

// heldTeams are end users' teams as Stream last told them, each for teamsFor, by customer, Stream
// app and user (heldTeamsKey). A read that failed is not held: while Stream fails, a user it told
// nothing in the last teamsFor is refused as before.
type heldTeams struct {
	mu sync.Mutex
	// now is the clock a read's age is told by, read under mu.
	now  func() time.Time
	read map[string]teamsRead
}

// teamsRead is one user's teams and when Stream told them.
type teamsRead struct {
	teams []string
	at    time.Time
}

func newHeldTeams() *heldTeams {
	return &heldTeams{now: time.Now, read: map[string]teamsRead{}}
}

// heldTeamsKey names a user of customer in the Stream app client acts in: a user id is the app's,
// and the same id under two customers or two apps is held twice.
func heldTeamsKey(customerID string, client *getstream.Stream, userID string) string {
	return customerID + "\x00" + client.BaseUrl() + "\x00" + client.ApiKey() + "\x00" + userID
}

// of is the teams held under key, when Stream told them less than teamsFor ago.
func (h *heldTeams) of(key string) ([]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	held, ok := h.read[key]
	if !ok || h.now().Sub(held.at) >= teamsFor {
		return nil, false
	}
	return held.teams, true
}

// keep holds teams under key from now. A full cache is emptied rather than walked, as
// conversation's known users are.
func (h *heldTeams) keep(key string, teams []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.read) >= maxHeldTeams {
		clear(h.read)
	}
	h.read[key] = teamsRead{teams: teams, at: h.now()}
}

// hasCommonTeam is Chat's shared-team rule for a config, which is in one team or none: a
// config of no team is open to a user in no team, and one of a team to the users in it.
func hasCommonTeam(teams []string, team string) bool {
	if team == "" {
		return len(teams) == 0
	}
	return slices.Contains(teams, team)
}
