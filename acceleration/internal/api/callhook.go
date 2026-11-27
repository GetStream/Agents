package api

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// callPlans are the answers a vendor's call webhook gives, each in that vendor's dialect.
type callPlans struct {
	// vendor names the vendor in the store and in every line logged about its calls.
	vendor       string
	renderDial   func(phone.Bridge) (phone.Plan, error)
	renderReject func() phone.Plan
	renderHangup func() phone.Plan
}

// answerCall answers a vendor's signed call to the number to: it runs the checks a call to the
// number passes before it is answered (phone.Service.InboundBridge), then dials the number's
// trunk, or rejects or hangs up the call when one of them refuses it.
func (s *Server) answerCall(w http.ResponseWriter, r *http.Request, logger *slog.Logger, plans callPlans, to string) {
	if s.phone == nil {
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call: telephony is not configured", "to", to)
		return
	}

	bridge, err := s.phone.InboundBridge(r.Context(), plans.vendor, to)
	// InboundBridge notes the number's app once the number resolves, before it refuses one.
	logger = logger.With("app_id", activity.NotedCustomer(r.Context()))
	if errors.Is(err, phone.ErrNoSealer) {
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call: this router has no key to open trunk passwords: "+
			"set ROUTER_DEV_KEK (local), or ROUTER_AUTH_KEK_V1_WRAPPED with ROUTER_AUTH_KMS_KEY (hosted), then attach the number again", "to", to)
		return
	}
	if errors.Is(err, store.ErrNoNumber) {
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call to a number that is unknown, released or not attached", "to", to)
		return
	}
	// A refusal past InboundBridge, which noted the number's app, is answered 200 as the vendor
	// wants it; its code is noted for the activity record, which would count it answered.
	if errors.Is(err, phone.ErrSuspended) {
		noteRefusal(w, codeTelephonySuspended)
		plans.refuse(w, logger, "rejected a "+plans.vendor+" call to a number of an app whose telephony is suspended", "to", to)
		return
	}
	if answer, refused := moneyError(err); refused {
		noteRefusal(w, answer.Code)
		plans.refuse(w, logger, "rejected a "+plans.vendor+" call to a number of an organization that may not pay for it",
			"to", to, "code", answer.Code)
		return
	}
	if errors.Is(err, store.ErrNoStoredBridge) {
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call to a number attached before its trunk was kept: "+
			"attach the number again", "to", to)
		return
	}
	// The router's own failure, which an API route answers 500: refused, once noted.
	if err != nil {
		noteRefusal(w, internalError().Code)
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call: could not find its trunk", "to", to, "error", err)
		return
	}
	plan, err := plans.renderDial(bridge)
	if err != nil {
		noteRefusal(w, internalError().Code)
		plans.hangUp(w, logger, "hung up a "+plans.vendor+" call: could not render its trunk", "to", to, "error", err)
		return
	}
	writePlan(w, logger, plan)
}

func (p callPlans) hangUp(w http.ResponseWriter, logger *slog.Logger, why string, attributes ...any) {
	logger.Warn(why, attributes...)
	writePlan(w, logger, p.renderHangup())
}

// refuse rejects a call without answering it, for a refusal the customer is billed nothing for.
// It is a warning all the same: the person calling the customer's number reached nobody.
func (p callPlans) refuse(w http.ResponseWriter, logger *slog.Logger, why string, attributes ...any) {
	logger.Warn(why, attributes...)
	writePlan(w, logger, p.renderReject())
}

func writePlan(w http.ResponseWriter, logger *slog.Logger, plan phone.Plan) {
	w.Header().Set("Content-Type", plan.ContentType)
	if _, err := w.Write(plan.Body); err != nil {
		logger.Error("could not serve a call plan", "error", err)
	}
}
