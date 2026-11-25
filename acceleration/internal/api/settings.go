package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// AppSettings is what the router does for the calling app, as far as the app can see it.
type AppSettings struct {
	Stream        StreamSettings `json:"stream"`
	RoutingPolicy RoutingPolicy  `json:"routing_policy" doc:"Which regions the app's work may be placed in, unless its agent config sets its own routing_policy. closest when the app never set one."`
}

// UpdateAppSettings is what the calling app changes of its settings. A field left out stays.
type UpdateAppSettings struct {
	RoutingPolicy *RoutingPolicy `json:"routing_policy,omitempty" doc:"Which regions the app's work may be placed in, unless its agent config sets its own. Empty is closest."`
}

// RoutingPolicy is which regions an app's or an agent's work may be placed in (shardmap.Policy).
type RoutingPolicy string

func (RoutingPolicy) Schema(registry huma.Registry) *huma.Schema {
	ref := namedEnum(registry, "RoutingPolicy", "Which regions work may be placed in. closest is the region "+
		"nearest the person: where a request landed, the called number's country, or the region that last "+
		"served the user. us_east is the region holding the configs, for everything. us_only, eu_only and "+
		"canada_only keep all of the work and its data in that geography, and fail it (503 shard_unavailable) "+
		"rather than place it anywhere else when none of its regions is up. Empty is the app's own on an agent "+
		"config, and closest on the app.", "", string(shardmap.PolicyClosest), string(shardmap.PolicyUSEast),
		string(shardmap.PolicyUSOnly), string(shardmap.PolicyEUOnly), string(shardmap.PolicyCanadaOnly))
	// Named, so the Go SDK's constants are not a bare Empty and Closest.
	registry.Map()["RoutingPolicy"].Extensions = map[string]any{"x-enum-varnames": []string{"RoutingPolicyUnset",
		"RoutingPolicyClosest", "RoutingPolicyUSEast", "RoutingPolicyUSOnly", "RoutingPolicyEUOnly", "RoutingPolicyCanadaOnly"}}
	return ref
}

func (*AppSettings) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Description = "What the router does for the calling app. It never carries a secret."
	return schema
}

// StreamSettings is which Stream app the router writes the app's conversations and calls
// into, and whether that app holds what they need.
type StreamSettings struct {
	Tenancy     StreamTenancy    `json:"tenancy"`
	WritesInto  StreamWritesInto `json:"writes_into"`
	ChannelType StreamTypeState  `json:"channel_type" doc:"Whether the Stream app holds the agent channel type conversations are kept in. unsafe is one whose grants let somebody other than the app's backend make, change or join a conversation's channel, or read one they are not in."`
	CallType    StreamTypeState  `json:"call_type" doc:"Whether the Stream app holds the agent call type calls are made with. Stream reports no grants for it here, so it is present or missing."`
	CheckedAt   *time.Time       `json:"checked_at,omitempty" doc:"When Stream was asked. Absent when it could not be, and the types are then unknown. Answers are reused for a minute."`

	StreamAppID *int64 `json:"stream_app_id,omitempty" doc:"The calling app's own Stream app id, in app mode, when its work is written there."`
}

func (*StreamSettings) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Description = "Which Stream app the router writes the calling app's conversations, " +
		"transcripts, calls and phone lines into, and whether that app holds the types they need."
	return schema
}

// StreamTenancy is whose Stream app the router acts in.
type StreamTenancy string

func (StreamTenancy) Schema(registry huma.Registry) *huma.Schema {
	return namedEnum(registry, "StreamTenancy", "Whose Stream app the router acts in. deployment "+
		"is one app, the router's own, for every app it serves; app is each app's own.",
		config.TenancyDeployment, config.TenancyApp)
}

// StreamWritesInto is which Stream app the calling app's work is written into.
type StreamWritesInto string

const (
	WritesIntoThisApp       StreamWritesInto = "this_app"
	WritesIntoDeploymentApp StreamWritesInto = "deployment_app"
	WritesIntoNowhere       StreamWritesInto = "nowhere"
)

func (StreamWritesInto) Schema(registry huma.Registry) *huma.Schema {
	return namedEnum(registry, "StreamWritesInto", "Which Stream app the calling app's work is "+
		"written into: this_app is its own, deployment_app is the router's own app, shared with "+
		"every app it serves that has none, and nowhere is no app at all, so conversations are "+
		"not kept and calls cannot be made.",
		string(WritesIntoThisApp), string(WritesIntoDeploymentApp), string(WritesIntoNowhere))
}

// StreamTypeState is whether a Stream app holds a type the router needs.
type StreamTypeState string

func (StreamTypeState) Schema(registry huma.Registry) *huma.Schema {
	return namedEnum(registry, "StreamTypeState", "Whether a Stream app holds a type the router "+
		"needs. unknown is a type Stream could not be asked about.",
		string(streamapp.TypePresent), string(streamapp.TypeMissing),
		string(streamapp.TypeUnsafe), string(streamapp.TypeUnknown))
}

type appSettingsResponse struct {
	Body AppSettings
}

type updateAppSettingsRequest struct {
	Body UpdateAppSettings
}

func (s *Server) registerSettings(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "getAppSettings",
		Method:      http.MethodGet,
		Path:        "/v1/settings/app",
		Summary:     "What the router does for the calling app",
		Description: "Which Stream app the router writes the calling app's conversations and " +
			"calls into, and whether that app holds the agent channel and call types. Stream is " +
			"asked at most once a minute. Also which regions the app's work may be placed in.\n\nServer-side only: it needs a server-side token, so " +
			"it cannot be reached from an end user's device.",
		Responses: map[string]*huma.Response{"200": {Description: "The app's settings"}},
		Errors:    []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable},
	}, s.getAppSettings)
	huma.Register(api, huma.Operation{
		OperationID: "updateAppSettings",
		Method:      http.MethodPatch,
		Path:        "/v1/settings/app",
		Summary:     "Change the calling app's settings",
		Description: "Writes only the fields sent, and answers with the app's settings as they now are. " +
			"Work already placed stays where it is.\n\nServer-side only: it needs a server-side token, so " +
			"it cannot be reached from an end user's device.",
		Responses: map[string]*huma.Response{"200": {Description: "The app's settings"}},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusServiceUnavailable},
	}, s.updateAppSettings)
}

// updateAppSettings writes what the calling app changed of its settings.
func (s *Server) updateAppSettings(ctx context.Context, request *updateAppSettingsRequest) (*appSettingsResponse, error) {
	customerID, ok := CustomerFrom(ctx)
	if !ok {
		return nil, errMissingCustomer
	}
	if s.store == nil {
		return nil, errNoConfigs
	}
	if message, ok := s.residencyComplaint(request.Body.RoutingPolicy); !ok {
		return nil, invalidRequest(message)
	}
	if request.Body.RoutingPolicy != nil {
		err := s.store.Apps.SetRoutingPolicy(ctx, customerID, shardmap.Policy(*request.Body.RoutingPolicy))
		if errors.Is(err, store.ErrNoApp) {
			return nil, notFound("this app is not registered with the router, so it has no settings to change")
		}
		if err != nil {
			return nil, err
		}
	}
	return s.getAppSettings(ctx, nil)
}

// residencyComplaint refuses a residency policy whose geography this sharded deployment runs no
// shard in (shardmap.Map.Allowed): its work would be refused wherever it landed, and its
// reminders and campaign dials claimed by no shard. An unsharded deployment allows any.
func (s *Server) residencyComplaint(policy *RoutingPolicy) (string, bool) {
	if policy == nil {
		return "", true
	}
	if allowed := s.shards.Allowed(shardmap.Policy(*policy)); allowed == nil || len(allowed) > 0 {
		return "", true
	}
	geography := shardmap.Policy(*policy).Residency()
	return fmt.Sprintf("routing_policy %s keeps work in the %s geography, which this deployment runs no region in: "+
		"shard_routing.residency lists no shard for %s", *policy, geography, geography), false
}

// getAppSettings reports which Stream app the calling app's work goes into, and whether it
// is ready for it. A failure to reach Stream is reported, not answered with: the settings
// are worth reading most when Stream is the trouble.
func (s *Server) getAppSettings(ctx context.Context, _ *struct{}) (*appSettingsResponse, error) {
	customerID, ok := CustomerFrom(ctx)
	if !ok {
		return nil, errMissingCustomer
	}
	policy := shardmap.PolicyClosest
	if s.store != nil {
		stored, err := s.store.Apps.GetSettings(ctx, customerID)
		if err != nil {
			return nil, err
		}
		policy = shardmap.Effective(stored.RoutingPolicy, "")
	}
	settings := StreamSettings{
		Tenancy:     s.tenancy(),
		WritesInto:  WritesIntoNowhere,
		ChannelType: StreamTypeState(streamapp.TypeUnknown),
		CallType:    StreamTypeState(streamapp.TypeUnknown),
	}
	bound, found, err := s.streamFor(ctx, customerID)
	if errors.Is(err, streamapp.ErrStreamAppDisconnected) || (err == nil && !found) {
		return &appSettingsResponse{Body: AppSettings{Stream: settings, RoutingPolicy: RoutingPolicy(policy)}}, nil
	}
	if errors.Is(err, streamapp.ErrDeploymentAppUnknown) {
		return nil, streamWaiting()
	}
	if err != nil {
		return nil, unavailable("which Stream app this app acts in could not be read")
	}
	settings.WritesInto = s.writesInto(customerID, bound.Identity)
	if s.stream.PerApp() && settings.WritesInto == WritesIntoThisApp {
		settings.StreamAppID = &bound.Identity.StreamApp
	}
	if readiness, err := s.stream.Readiness(ctx, bound); err == nil {
		settings.ChannelType = StreamTypeState(readiness.ChannelType)
		settings.CallType = StreamTypeState(readiness.CallType)
		settings.CheckedAt = &readiness.CheckedAt
	} else {
		s.logger.Warn("stream: could not read what an app holds", "customer_id", customerID, "error", err)
	}
	return &appSettingsResponse{Body: AppSettings{Stream: settings, RoutingPolicy: RoutingPolicy(policy)}}, nil
}

// writesInto names the app an identity acts in, as the calling app should read it. The
// router's own app is the caller's own only when the caller is that app; to anybody else
// it is shared, and which app it is is not theirs to know.
func (s *Server) writesInto(customerID string, identity streamapp.Identity) StreamWritesInto {
	deployment := s.stream.DeploymentApp()
	if identity.StreamApp != 0 && identity.StreamApp != deployment {
		return WritesIntoThisApp
	}
	if deployment != 0 && customerID == streamapp.CustomerOf(deployment) {
		return WritesIntoThisApp
	}
	return WritesIntoDeploymentApp
}

// tenancy is whose Stream app the router acts in.
func (s *Server) tenancy() StreamTenancy {
	if s.stream != nil && s.stream.PerApp() {
		return config.TenancyApp
	}
	return config.TenancyDeployment
}
