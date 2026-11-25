// Package transitional holds security changes that would alter what a legitimate caller
// sees, shipped switched off. Each keeps the old behaviour until its environment variable
// says otherwise, and counts every request the new behaviour would have changed, so its
// effect on real traffic is known before it is switched on.
package transitional

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcomes of a request a Switch's new behaviour applies to.
const (
	// OutcomeEnforced: the switch is on, and the new behaviour was applied.
	OutcomeEnforced = "enforced"
	// OutcomeWouldChange: the switch is off, so the old behaviour was kept.
	OutcomeWouldChange = "would_change"
)

var wouldChange = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "security_would_change_total",
	Help: "Requests a transitional security change applies to, by switch, app and outcome: enforced when the " +
		"switch is on, would_change when it is off and the old behaviour was kept (transitional.Switch)",
}, []string{"switch", "app_id", "outcome"})

// Switch is one transitional security change.
type Switch struct {
	// Name names it in the metric and the log, such as security.cha7277.reserved_user_ids.
	Name string
	// Env is the environment variable that turns it on: true for every app, or a comma-separated
	// list of app ids to turn it on for those alone. Unset, or anything else, keeps the old
	// behaviour.
	Env string
}

// forced holds the apps a test turned a switch on for (EnableForApp), by name and app.
var forced sync.Map

// On reports whether the new behaviour is switched on for app.
func (s Switch) On(app string) bool {
	if _, ok := forced.Load(s.Name + "\x00" + app); ok {
		return true
	}
	value := strings.TrimSpace(os.Getenv(s.Env))
	if on, err := strconv.ParseBool(value); err == nil {
		return on
	}
	if app == "" {
		return false
	}
	for _, listed := range strings.Split(value, ",") {
		if strings.TrimSpace(listed) == app {
			return true
		}
	}
	return false
}

// EnableForApp turns the switch on for app in this process, whatever the environment says,
// until the returned func is called. It is for tests, which run in parallel and so cannot set
// the environment one test at a time.
func (s Switch) EnableForApp(app string) (undo func()) {
	key := s.Name + "\x00" + app
	forced.Store(key, struct{}{})
	return func() { forced.Delete(key) }
}

// Applies records that the new behaviour applies to a request of app, and reports whether
// it is switched on. The caller applies the new behaviour only when it answers true.
func (s Switch) Applies(app, endpoint string) bool {
	on := s.On(app)
	outcome := OutcomeWouldChange
	if on {
		outcome = OutcomeEnforced
	}
	wouldChange.WithLabelValues(s.Name, app, outcome).Inc()
	slog.Default().Info("a transitional security change applies to a request",
		"switch", s.Name, "app_id", app, "endpoint", endpoint, "outcome", outcome)
	return on
}
