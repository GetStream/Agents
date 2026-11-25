package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

// AppScope is the Stream app a hook came from, as the rows it may act on are found by.
type AppScope struct {
	// App is the app's id. Zero matches only unpinned rows.
	App int64
	// Unpinned also matches rows with no pin, which were written into the deployment's
	// own app. Only a hook from the deployment's own app sets it.
	Unpinned bool
}

// AppOf is the scope of the Stream app a pin names, deployment being the deployment's own app's
// id: the rows of that app include those written there with no pin.
func AppOf(pin, deployment int64) AppScope {
	if pin == 0 || pin == deployment {
		return AppScope{App: deployment, Unpinned: true}
	}
	return AppScope{App: pin}
}

// where narrows a query, of any kind, to the rows in the scope's app.
func (a AppScope) where(query bun.QueryBuilder) bun.QueryBuilder {
	return query.WhereGroup(" AND ", func(q bun.QueryBuilder) bun.QueryBuilder {
		if a.App != 0 {
			q = q.WhereOr("stream_app_pk = ?", a.App)
		}
		if a.Unpinned {
			q = q.WhereOr("stream_app_pk IS NULL")
		}
		if a.App == 0 && !a.Unpinned {
			q = q.Where("false")
		}
		return q
	})
}

// clause is the scope's condition as where writes it, for a raw statement.
func (a AppScope) clause() bun.Safe {
	switch {
	case a.App != 0 && a.Unpinned:
		return bun.Safe(fmt.Sprintf("stream_app_pk = %d OR stream_app_pk IS NULL", a.App))
	case a.App != 0:
		return bun.Safe(fmt.Sprintf("stream_app_pk = %d", a.App))
	case a.Unpinned:
		return bun.Safe("stream_app_pk IS NULL")
	}
	return bun.Safe("false")
}

// ErrAmbiguousHook is a hook that matches rows of more than one customer in its app, which
// is acted on for nobody rather than for whichever came first.
var ErrAmbiguousHook = errors.New("store: more than one customer holds what that hook names")

// trunkURI reads a number's PhoneNumber.BridgeURI from its trunk.
const trunkURI = "(SELECT st.bridge_uri FROM stream_trunks AS st WHERE st.id = pn.stream_trunk_id " +
	"AND st.customer_id = pn.customer_id) AS bridge_uri"

// NumberByCallInApp finds the number whose lines reach a call, among the numbers attached
// in one app. A routing rule names the call its callers are put in, which is what an
// arriving call is recognised by; a number attached before its call was stored is found by
// the name the call is given after it. A call's id is only unique within its app, so each
// lookup is made there. A call that no number reaches and that is not named for a number,
// most often a video call, matches ErrNoNumber.
func (s *Store) NumberByCallInApp(ctx context.Context, scope AppScope, callType, callID string) (PhoneNumber, error) {
	if callID == "" {
		return PhoneNumber{}, errors.New("store: a call id is required")
	}
	if callType == "" {
		callType = "agent"
	}
	var held []PhoneNumber
	if err := s.global.NewSelect().Model(&held).ColumnExpr("pn.*").ColumnExpr(trunkURI).
		Where("stream_call_id = ?", callID).Where("stream_call_type = ?", callType).
		Where("released_at IS NULL").ApplyQueryBuilder(scope.where).Limit(20).Scan(ctx); err != nil {
		return PhoneNumber{}, fmt.Errorf("store: number by call: %w", err)
	}
	if len(held) == 0 {
		e164, named := strings.CutPrefix(callID, "phone-")
		if !named {
			return PhoneNumber{}, numberNotHeld{fmt.Sprintf("store: no number reaches call %s:%s", callType, callID)}
		}
		// A number on a customer's own trunk is lined for its calls out alone, which are known by
		// their resources before this is asked, and never answers a call.
		if err := s.global.NewSelect().Model(&held).ColumnExpr("pn.*").ColumnExpr(trunkURI).
			Where("e164 = ?", e164).Where("stream_trunk_id IS NOT NULL").Where("vendor <> ?", sipTrunkVendor).
			Where("released_at IS NULL").ApplyQueryBuilder(scope.where).Limit(20).Scan(ctx); err != nil {
			return PhoneNumber{}, fmt.Errorf("store: number by call: %w", err)
		}
	}
	switch customers(held, func(n PhoneNumber) string { return n.CustomerID }) {
	case 0:
		return PhoneNumber{}, fmt.Errorf("store: no number reaches call %s:%s", callType, callID)
	case 1:
		return held[0], nil
	}
	return PhoneNumber{}, ErrAmbiguousHook
}

// NumberRungInApp is the attached number an inbound call rang, among the numbers attached
// in one app: the call is named for the session answering it, so it carries the number.
func (s *Store) NumberRungInApp(ctx context.Context, scope AppScope, e164 string) (PhoneNumber, error) {
	var held []PhoneNumber
	if err := s.global.NewSelect().Model(&held).ColumnExpr("pn.*").ColumnExpr(trunkURI).
		Where("e164 = ?", e164).Where("attached_at IS NOT NULL").
		Where("released_at IS NULL").ApplyQueryBuilder(scope.where).Limit(20).Scan(ctx); err != nil {
		return PhoneNumber{}, fmt.Errorf("store: number rung: %w", err)
	}
	switch customers(held, func(n PhoneNumber) string { return n.CustomerID }) {
	case 0:
		return PhoneNumber{}, fmt.Errorf("store: no number %s is attached", e164)
	case 1:
		return held[0], nil
	}
	return PhoneNumber{}, ErrAmbiguousHook
}

// HeldCall is a call and the agent config its session ran under, empty for a call whose
// session is gone.
type HeldCall struct {
	Call     `bun:",extend"`
	ConfigID string `bun:"config_id,scanonly"`
}

// CallByAgentInApp is the newest call an agent ran in this shard, among the calls made in one
// app.
func (s *Store) CallByAgentInApp(ctx context.Context, scope AppScope, agentID string) (HeldCall, error) {
	if agentID == "" {
		return HeldCall{}, errors.New("store: an agent id is required")
	}
	var calls []HeldCall
	err := s.shard.NewSelect().Model(&calls).
		ColumnExpr("c.*").
		ColumnExpr("COALESCE((SELECT s.config_id FROM agent_sessions AS s WHERE s.customer_id = c.customer_id AND s.id = c.session_id), '') AS config_id").
		Where("c.agent_id = ?", agentID).
		ApplyQueryBuilder(scope.where).Order("c.started_at DESC").Limit(20).Scan(ctx)
	if err != nil {
		return HeldCall{}, fmt.Errorf("store: call by agent: %w", err)
	}
	switch customers(calls, func(c HeldCall) string { return c.CustomerID }) {
	case 0:
		return HeldCall{}, unknownCall(agentID)
	case 1:
		return calls[0], nil
	}
	return HeldCall{}, ErrAmbiguousHook
}

func customers[T any](rows []T, of func(T) string) int {
	seen := map[string]bool{}
	for _, row := range rows {
		seen[of(row)] = true
	}
	return len(seen)
}
