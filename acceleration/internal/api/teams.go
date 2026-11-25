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
	// again asks Stream for the teams once more when they are the ones held rather than read for
	// this request, so that what they refuse is asked again before it stands (heldTeams.again).
	// nil when they were read for it.
	again func(context.Context) ([]string, bool)
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
	return s.callerTeams(ctx, customerID, user)
}

// refuses reports whether config is of a team the end user asking is not in, which the caller
// is told as it is of a config that does not exist. Held teams that refuse are asked of Stream
// again first, so a user added to the team since is let in.
func (g teamsGate) refuses(ctx context.Context, config store.AgentConfig) bool {
	team := strings.TrimSpace(config.Tags[session.TeamTag])
	if !g.checked || hasCommonTeam(g.teams, team) {
		return false
	}
	if g.again == nil {
		return true
	}
	teams, held := g.again(ctx)
	return !held || !hasCommonTeam(teams, team)
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
	case teams.refuses(ctx, read):
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

// callerTeams is the gate of the end user caller, by their teams as their user in the
// customer's own Stream app holds them: an app the customer holds a token for, or the
// deployment's own app when the customer is that app's. A customer that only borrows the
// deployment's app reads none: user ids there are not the customer's and whoever holds that
// app writes their teams. A caller with no token is in none: the user id it goes by is a claim
// nobody checked, and reading that user's teams would let anybody borrow them. What Stream
// tells is held for teamsFor (heldTeams), and held teams that refuse ask it again.
func (s *Server) callerTeams(ctx context.Context, customerID string, caller routing.Caller) (teamsGate, error) {
	if !caller.Verified {
		return teamsGate{checked: true}, nil
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
		read := func(ctx context.Context) ([]string, error) {
			return conversation.UserTeams(ctx, bound.Client, caller.UserID)
		}
		if teams, held := s.teams.of(key); held {
			again := func(ctx context.Context) ([]string, bool) {
				teams, held, err := s.teams.again(ctx, key, read)
				if err != nil {
					s.logger.Warn("could not read a user's teams from Stream again, the held ones stand",
						"user", caller.UserID, "error", err)
				}
				return teams, held
			}
			return teamsGate{checked: true, teams: teams, again: again}, nil
		}
		var teams []string
		if teams, err = read(ctx); err == nil {
			s.teams.keep(key, teams)
			return teamsGate{checked: true, teams: teams}, nil
		}
	}
	s.logger.Error("could not read a user's teams from Stream", "user", caller.UserID, "error", err)
	return teamsGate{}, errTeamsUnreadable
}

// teamsFor is how long a user's teams read from Stream are taken on trust. Every message and
// socket of a running chat in a teams app asks, and every occurrence of a task, and Stream rate
// limits the query per app. A user taken out of a team reaches its agents for up to this long
// after. A user added to one is not kept out as long: held teams that refuse ask again
// (teamsRecheck).
const teamsFor = 30 * time.Second

// teamsRecheck is how often held teams that refuse a user are asked of Stream again. A user
// refused over and over costs Stream one query this often, and one added to a team since the
// last time Stream was asked is let in once this long has passed at most.
const teamsRecheck = time.Second

// maxHeldTeams bounds the users whose teams one router holds, across every app it serves.
const maxHeldTeams = 100_000

// heldTeams are end users' teams as Stream last told them, each for teamsFor, by customer, Stream
// app and user (heldTeamsKey). A read that failed is not held: while Stream fails, a user it told
// nothing in the last teamsFor is refused as before, and held teams that refuse stand (again).
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
	// asked is when Stream was last asked again for a refusal of these teams, answered or not.
	// asking is closed once that read ends, and is nil when none is running.
	asked  time.Time
	asking chan struct{}
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

// again is the teams held under key once Stream is asked for them through read again, for a
// refusal they make: a user added to a team since is let in now, not once teamsFor has passed.
// Stream is asked for a key at most once per teamsRecheck, answered or not, and a refusal while
// it is being asked waits for that answer, so a user refused over and over does not ask each
// time. held is false when nothing is held under key any more. err is read's, for the caller
// that ran it; the teams held before stand.
func (h *heldTeams) again(ctx context.Context, key string, read func(context.Context) ([]string, error)) (teams []string, held bool, err error) {
	h.mu.Lock()
	entry, ok := h.read[key]
	asking := entry.asking
	due := ok && asking == nil && h.now().Sub(entry.asked) >= teamsRecheck
	if due {
		asking = make(chan struct{})
		entry.asked, entry.asking = h.now(), asking
		h.read[key] = entry
	}
	h.mu.Unlock()
	if !due {
		if asking != nil {
			select {
			case <-asking:
			case <-ctx.Done():
			}
		}
		teams, held = h.of(key)
		return teams, held, nil
	}

	fresh, err := read(ctx)
	h.mu.Lock()
	// The entry may have been replaced or dropped while Stream was asked: only the one asking
	// takes the answer.
	if entry, ok := h.read[key]; ok && entry.asking == asking {
		entry.asking = nil
		if err == nil {
			entry.teams, entry.at = fresh, h.now()
		}
		h.read[key] = entry
	}
	h.mu.Unlock()
	close(asking)
	if err != nil {
		teams, held = h.of(key)
		return teams, held, err
	}
	return fresh, true, nil
}

// hasCommonTeam is Chat's shared-team rule for a config, which is in one team or none: a
// config of no team is open to a user in no team, and one of a team to the users in it.
func hasCommonTeam(teams []string, team string) bool {
	if team == "" {
		return len(teams) == 0
	}
	return slices.Contains(teams, team)
}
