package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
)

// phoneSweepEvery is a variable so a test of the router's start can sweep sooner.
var phoneSweepEvery = 10 * time.Minute

// A call longer than this is not a campaign call, and the session's own limits end it far
// sooner, so its trunk is one whose end hook never came.
const phoneCallMaxAge = 6 * time.Hour

// sweepPhoneOnce runs one pass of the phone maintenance sweep: deleting per-call trunks of
// calls that never reported their end, sweeping unanswered bridges, settling unrecorded
// number purchase/release orders, and retiring expired status URL tokens.
func sweepPhoneOnce(ctx context.Context, service *phone.Service, logger *slog.Logger) error {
	swept, err := service.SweepCallResources(ctx, time.Now().Add(-phoneCallMaxAge))
	if err != nil {
		logger.Error("could not sweep the trunks of calls that never ended", "error", err)
	} else if swept > 0 {
		logger.Info("swept the trunks of calls that never ended", "calls", swept)
	}
	bridges, err := service.SweepBridges(ctx)
	if err != nil {
		logger.Error("could not sweep the bridges of calls nobody answered", "error", err)
	} else if bridges > 0 {
		logger.Info("swept the bridges of calls nobody answered", "bridges", bridges)
	}
	orders, err := service.SweepOrders(ctx)
	if err != nil {
		logger.Error("could not settle the purchases and releases of numbers left unrecorded", "error", err)
	} else if orders > 0 {
		logger.Info("settled the purchases and releases of numbers left unrecorded", "orders", orders)
	}
	retired, err := service.SweepStatusTokens(ctx)
	if err != nil {
		logger.Error("could not retire the status urls of old placed calls", "error", err)
	} else if retired > 0 {
		logger.Info("retired the status urls of old placed calls", "calls", retired)
	}
	return nil
}
