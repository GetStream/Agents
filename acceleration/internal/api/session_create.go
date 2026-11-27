package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/danielgtaylor/huma/v2"
)

// createSession joins a call and returns the session running it.
func (s *Server) createSession(ctx context.Context, request *createSessionRequest) (*createSessionResponse, error) {
	customerID, ok := CustomerFrom(ctx)
	if !ok {
		return nil, errMissingCustomer
	}
	if s.sessions == nil {
		return nil, errNoSessions
	}
	if request.Body == nil {
		return nil, invalidRequest("a request body is required")
	}
	if failure, refused := refuseSessionRequest(ctx, *request.Body); refused {
		return nil, failure
	}
	if failure := s.refuseCall(ctx, customerID, *request.Body); failure != nil {
		return nil, failure
	}

	// A config is read before the session is created rather than inside it, so a caller
	// naming one that is not theirs is told so instead of getting a session that quietly
	// ignored it.
	config, failure := s.configFor(ctx, customerID, request.Body.ConfigId, request.Body.Agent)
	if failure != nil {
		return nil, failure
	}
	if failure, refused := s.sessionFloorRefusal(ctx, *request.Body); refused {
		return nil, failure
	}

	spec := specOf(*request.Body, customerID, config)
	keyDeviceAgent(ctx, &spec)
	ctx = s.threadConversation(ctx, customerID, &spec)
	// Who asked comes from the credential rather than from specOf, which merges the request
	// with the config and so only ever sees what the caller was willing to say about
	// themselves. Both halves are recorded, because the name is only worth what the kind
	// says it is: this pair is what the session is owned by and what every later request
	// for it is matched against.
	spec.Caller = CallerFrom(ctx)
	spec.CallerKind = KindFrom(ctx)
	spec.NameProject(request.Body.ProjectId, spec.CallerKind)
	created, err := s.sessions.Create(ctx, spec)
	if errors.Is(err, session.ErrSessionExists) {
		return nil, conflict(err.Error())
	}
	if errors.Is(err, session.ErrNoAgentCallType) {
		return nil, errNoAgentCallType
	}
	if unanswered, ok := errors.AsType[*session.CallUnansweredError](err); ok {
		return nil, callUnanswered(unanswered.Outcome)
	}
	if errors.Is(err, streamapp.ErrDeploymentAppUnknown) {
		return nil, err
	}
	if err != nil {
		s.logNotConfigured(ctx, customerID, err)
		// Everything that can go wrong here is the caller's spec, a provider that would not
		// start, or a refusal about money, and each is worth reading rather than a 500 with
		// the detail in a log the caller cannot see.
		return nil, refusedRequest(err)
	}
	s.rememberTimezone(ctx, customerID, spec)
	return &createSessionResponse{Body: sessionOf(ctx, created)}, nil
}

// logNotConfigured logs why a session was refused for a setting of this deployment, under the
// request id the caller is told to send: the session's error, which names the setting, and
// Stream's own answer for a Stream app that is missing or refuses its keys. The caller is
// answered in fixed words (refusedRequest).
func (s *Server) logNotConfigured(ctx context.Context, customerID string, err error) {
	if found, ok := errors.AsType[*session.UnkeptError](err); ok {
		s.logger.WarnContext(ctx, "a session that is not incognito has no Stream app to be kept in",
			"app_id", customerID, "request_id", RequestIDFrom(ctx), "error", err, "cause", found.Cause)
		return
	}
	if errors.Is(err, session.ErrNotConfigured) {
		s.logger.WarnContext(ctx, "a session lacks a setting of this deployment",
			"app_id", customerID, "request_id", RequestIDFrom(ctx), "error", err)
	}
}

// errUnkept is the answer for a session that is not incognito, on a deployment whose Stream app
// is missing or refuses its keys. The keys are the operator's, so the caller is not told which
// of the two it is: the log says (logNotConfigured).
var errUnkept = notConfigured("a session that is not incognito cannot be opened on this deployment: " +
	"open the session with incognito: true")

// errSessionNotConfigured is the answer for session.ErrNotConfigured, whose text names a
// setting of the deployment: the log has it (logNotConfigured).
var errSessionNotConfigured = notConfigured("this deployment is not set up for this session")

// unkeptAnswer is the answer for a session refused because the deployment's Stream app is
// missing or refuses its keys: a setting of the deployment rather than anything in the
// request, so not_configured, in words of our own. False for any other error.
func unkeptAnswer(err error) (APIError, bool) {
	for _, refusal := range []error{session.ErrNoStreamKeys, session.ErrStreamKeysRejected} {
		if errors.Is(err, refusal) {
			return errUnkept, true
		}
	}
	return APIError{}, false
}

// rememberTimezone stores the zone a session was opened in as its user's zone, so what a
// task scheduled later runs at is the hour this user means. The latest explicit zone wins,
// a session that names none says nothing, and an incognito session speaks for nobody. Nor
// does an anonymous caller: the user id it goes by is a claim nobody checked (recordUser).
func (s *Server) rememberTimezone(ctx context.Context, customerID string, spec session.Spec) {
	if s.store == nil || spec.Timezone == "" || spec.Incognito || KindFrom(ctx) == auth.KindAnonymous {
		return
	}
	caller := CallerFrom(ctx)
	if caller.UserID == "" {
		return
	}
	kind := store.UserKindAuthenticated
	if KindFrom(ctx) == auth.KindGuest {
		kind = store.UserKindGuest
	}
	if err := s.store.Users.SetTimezone(ctx, customerID, caller.UserID, kind, spec.Timezone); err != nil {
		s.logger.Error("could not store the user's timezone", "user", caller.UserID, "error", err)
	}
}

// callUnanswered refuses a phone session whose placed call already ended, nobody having
// answered; ended is how.
func callUnanswered(ended outcome.Outcome) APIError {
	return APIError{Type: ErrorTypeConflict, Code: codeCallUnanswered,
		Message: "the call ended without an answer: " + string(ended)}
}

// errDeviceHistory refuses a history sent by an end user's device.
var errDeviceHistory = forbidden("history is sent server-side: an assistant message in it is " +
	"what the agent said, which a device cannot vouch for")

// errDeviceInstructions refuses instructions sent by an end user's device.
var errDeviceInstructions = forbidden("instructions are sent server-side: what the agent is " +
	"told decides who it is, which a device may not")

var errDeviceFloorControl = forbidden("floor_control is sent server-side: whether a model rules when " +
	"the agent answers is the app's to decide, which a device may not")

// errDeviceTeam refuses a team named by an end user's device.
var errDeviceTeam = forbidden("team is sent server-side: a device's work is counted under the team its " +
	"token was signed for, and naming another would spend that team's budget")

// errDevicePhone refuses a phone line named by an end user's device.
var errDevicePhone = forbidden("phone is sent server-side: a phone session calls, transfers and " +
	"presses keys from the app's own numbers, which a device may not")

// errDeviceModels refuses models and limits chosen by an end user's device, as updateSession
// refuses a device changing them.
var errDeviceModels = forbidden("llm, stt, tts, sts, search, voice, max_tokens, max_tool_rounds and " +
	"model_overwrites are sent server-side: which models a session runs on, and how much they " +
	"may spend, is the app's to decide, which a device may not")

// errDeviceGreeting refuses a greeting sent by an end user's device.
var errDeviceGreeting = forbidden("greeting is sent server-side: what the agent opens with is " +
	"the agent speaking, which a device cannot vouch for")

// errDeviceTags refuses cost labels sent by an end user's device.
var errDeviceTags = forbidden("tags are sent server-side: they label whom the session's spend is " +
	"billed to, which a device may not decide")

var errDeviceResumeHistory = forbidden("resume_history is sent server-side: how much of a " +
	"conversation the model is sent is billed to the app, which a device may not decide")

// errDeviceMemoryUser refuses an end user's device naming whose memories the session reads.
var errDeviceMemoryUser = forbidden("memory.user_id is sent server-side unless it is the caller's own " +
	"user id: a session reads and writes the memories of the user it names")

// errAnonymousMemory refuses memory to a caller whose user id nobody checked.
var errAnonymousMemory = forbidden("memory needs a signed-in user: an anonymous caller's name is a claim " +
	"nobody checked, so the memories kept under it could be anyone's")

// keyDeviceAgent gives an end user's device's voice session an agent id nobody chose.
//
// Why: a voice session that names no agent id is keyed under its call id, which is its session
// id, and a device picks that. The agent id is the Chat user the conversation is created by
// and written as, which a device may not choose (errDeviceAgentID). A text session is given a
// fresh one already. One that carries on a conversation is left as it is: opening that
// conversation refuses an agent id other than the one it was kept under.
func keyDeviceAgent(ctx context.Context, spec *session.Spec) {
	if ServerSideFrom(ctx) || spec.Text || spec.AgentID != "" || spec.ConversationID != "" {
		return
	}
	spec.AgentID = session.NewAgentID()
}

// errDeviceAgentUser refuses an end user's device naming the user the agent joins and writes as.
var errDeviceAgentUser = forbidden("user_id is sent server-side unless it is the default " +
	session.DefaultUserID + " or the caller's own user id: the agent joins the call and writes " +
	"Chat messages as the user it names, which a device may not choose for somebody else")

// errDeviceAgentUserName refuses an end user's device naming the agent's display name.
var errDeviceAgentUserName = forbidden("user_name is sent server-side unless it is the default " +
	session.DefaultUserName + ": it is stored as the name of the Chat user the agent writes as, " +
	"which a device may not rename")

// errDeviceAgentID refuses an agent id named by an end user's device.
var errDeviceAgentID = forbidden("agent_id is sent server-side: the agent's Chat messages and the " +
	"channel it keeps are written as it, which a device may not choose")

// refuseDeviceAgentIdentity reports the failure a new session is refused with when an end
// user's device names who the agent is. The router's own credentials act as that identity,
// so a device may only ask for the default, or, for user_id, its own verified user id.
//
// Why: clients generated before the schema dropped its defaults (CHA-7262) fill them in for
// user_id and user_name, so a device sending exactly those is let through rather than refused.
func refuseDeviceAgentIdentity(ctx context.Context, request CreateSessionRequest) (APIError, bool) {
	if ServerSideFrom(ctx) {
		return APIError{}, false
	}
	if id := value(request.AgentId); id != "" {
		return errDeviceAgentID, true
	}
	if user := value(request.UserId); user != "" && user != session.DefaultUserID {
		caller := CallerFrom(ctx).UserID
		if !KindFrom(ctx).Verified() || caller == "" || user != caller {
			return errDeviceAgentUser, true
		}
	}
	if name := value(request.UserName); name != "" && name != session.DefaultUserName {
		return errDeviceAgentUserName, true
	}
	return APIError{}, false
}

// errAnonymousConversation refuses conversation_id to a caller whose user id nobody checked.
var errAnonymousConversation = forbidden("conversation_id needs a verified caller: an anonymous caller " +
	"carries its conversation on by its session id")

// refuseSessionRequest reports the failure a new session's request is refused with before
// anything is made for it. createSession and the session socket both call it, so a request one
// of them refuses the other refuses too, with the same error.
func refuseSessionRequest(ctx context.Context, request CreateSessionRequest) (APIError, bool) {
	if failure, refused := refuseReservedActingFor(ctx, "CreateSession"); refused {
		return failure, true
	}
	if failure, refused := refuseServerSideFields(ctx, request); refused {
		return failure, true
	}
	if failure, refused := refuseDeviceAgentIdentity(ctx, request); refused {
		return failure, true
	}
	// The same rules as createAuthorization's, since it is the same attempt that seals it.
	if returnURL := value(request.ReturnUrl); returnURL != "" {
		if err := checkReturnURL(returnURL); err != nil {
			return invalidRequest("return_url: " + err.Error()), true
		}
	}
	if plan := request.Phone; plan != nil && plan.Voicemail != nil &&
		plan.Voicemail.Action == VoicemailActionLeaveMessage && strings.TrimSpace(value(plan.Voicemail.Message)) == "" {
		return invalidRequest("phone.voicemail.message is required to leave a message"), true
	}
	if team := request.Team; team != nil {
		if err := usage.CheckTeam(*team); err != nil {
			return invalidRequest(fmt.Sprintf("team %q is not 1 to %d letters, digits, _ . and -", *team, usage.MaxTeamLength)), true
		}
		if signed := routing.AttributionFrom(ctx).Team(); signed != "" && signed != *team {
			return invalidRequest(fmt.Sprintf("team %q is not the team %q the token was signed for, which the session is counted under", *team, signed)), true
		}
	}
	for _, named := range [][2]string{
		{"user_id", value(request.UserId)}, {"agent_id", value(request.AgentId)}, {"memory.user_id", memoryUserID(request.Memory)},
	} {
		if failure, refused := refuseReservedUserID(named[0], named[1]); refused {
			return failure, true
		}
	}
	// A copy of a claimed guest's memory names its original under this label, so a filter on it
	// would read the copies as no other filter does.
	if request.Memory != nil && request.Memory.Filter != nil {
		if _, named := (*request.Memory.Filter)[memory.MovedFromLabel]; named {
			return invalidRequest("memory.filter may not name " + memory.MovedFromLabel + ", which the router writes"), true
		}
	}
	if failure, refused := refuseReservedNewly(ctx, "CreateSession", "call.id", callAgentID(request)); refused {
		return failure, true
	}
	// A zone nobody knows is refused where it is named, rather than at the first task
	// that would have run in it.
	if zone := value(request.Timezone); zone != "" {
		if _, err := scheduling.LoadZone(zone); err != nil {
			return invalidTimezone("timezone: " + err.Error()), true
		}
	}
	return APIError{}, false
}

// errDeviceCall refuses a call named by an end user's device.
var errDeviceCall = forbidden("call is sent server-side: which call the agent joins puts it " +
	"among the people on that call, which a device may not decide")

// errCallWithoutVoice refuses a call named for a session held in writing.
var errCallWithoutVoice = invalidRequest("call needs start_voice: a session held in writing is on no call")

// errCallWithPhone refuses a call named for a phone session, which is on its own call.
var errCallWithPhone = invalidRequest("call cannot be given with phone: a phone session is on its own call, agent:<session id>")

// errCallInAnotherApp refuses a call another Stream app holds than the one the session acts
// in: the agent would join a call of the same name in its own app, where nobody is.
var errCallInAnotherApp = invalidRequest("the call is in another Stream app than the one this session acts in")

// errCallInSharedApp refuses a call named by a session that acts in a Stream app shared with
// other customers: every customer's calls are in that app, so a named call could be anyone's.
var errCallInSharedApp = forbidden("call can be named only by a session that acts in the customer's own " +
	"Stream app: this one acts in an app shared with other customers, whose calls are not this customer's to join")

// refuseCall is the failure a new session naming a call is refused with, or nil. The session
// acts in the app the call is pinned to (callPinApp, as the manager resolves it), so the call
// is refused only when another session runs it in a different app.
func (s *Server) refuseCall(ctx context.Context, customerID string, request CreateSessionRequest) error {
	if request.Call == nil {
		return nil
	}
	if request.Phone != nil {
		return errCallWithPhone
	}
	if !value(request.StartVoice) {
		return errCallWithoutVoice
	}
	acting, err := s.callPinApp(ctx, customerID, request.Call.Type, request.Call.Id)
	if err != nil {
		return err
	}
	return s.refuseCallOutside(ctx, customerID, *request.Call, acting)
}

// refuseCallOutside refuses a call that callApp places in another Stream app than acting,
// or any call when acting is not the customer's own app.
func (s *Server) refuseCallOutside(ctx context.Context, customerID string, call SessionCall, acting int64) error {
	app, err := s.callApp(ctx, customerID, call.Type, call.Id)
	if err != nil {
		return err
	}
	if app != acting {
		return errCallInAnotherApp
	}
	if !s.ownApp(customerID, acting) {
		return errCallInSharedApp
	}
	return nil
}

// ownApp reports whether app is the customer's own Stream app (streamapp.Own).
func (s *Server) ownApp(customerID string, app int64) bool {
	var deployment int64
	if s.stream != nil {
		deployment = s.stream.DeploymentApp()
	}
	return streamapp.Own(customerID, app, deployment)
}

// refuseDeviceModels reports the failure a request is refused with when an end user's device
// named models or limits in it. A new session and a fork are both held to it.
func refuseDeviceModels(ctx context.Context, named bool) (APIError, bool) {
	if named && !ServerSideFrom(ctx) {
		return errDeviceModels, true
	}
	return APIError{}, false
}

// refuseServerSideFields reports the failure a new session is refused with when an end user's
// device sent a field only the backend may send.
//
// What the model is told is the backend's to decide: an assistant message in a device's
// history would be the agent having said it, and a device's instructions could make the
// agent another one.
func refuseServerSideFields(ctx context.Context, request CreateSessionRequest) (APIError, bool) {
	if request.History != nil && !ServerSideFrom(ctx) {
		return errDeviceHistory, true
	}
	if request.Instructions != nil && !ServerSideFrom(ctx) {
		return errDeviceInstructions, true
	}
	if request.Call != nil && !ServerSideFrom(ctx) {
		return errDeviceCall, true
	}
	if request.ResumeHistory != nil && !ServerSideFrom(ctx) {
		return errDeviceResumeHistory, true
	}
	if request.FloorControl != nil && !ServerSideFrom(ctx) {
		return errDeviceFloorControl, true
	}
	if request.Team != nil && !ServerSideFrom(ctx) {
		return errDeviceTeam, true
	}
	if request.Phone != nil && !ServerSideFrom(ctx) {
		return errDevicePhone, true
	}
	if failure, refused := refuseDeviceModels(ctx, request.Llm != nil || request.Stt != nil ||
		request.Tts != nil || request.Sts != nil || request.Search != nil || request.Voice != nil ||
		request.MaxTokens != nil || request.MaxToolRounds != nil || request.ModelOverwrites != nil); refused {
		return failure, true
	}
	if request.Greeting != nil && !ServerSideFrom(ctx) {
		return errDeviceGreeting, true
	}
	if request.Tags != nil && !ServerSideFrom(ctx) {
		return errDeviceTags, true
	}
	if value(request.LegacyConversationId) != "" && !KindFrom(ctx).Verified() {
		return errAnonymousConversation, true
	}
	if request.Memory != nil && value(request.Memory.UserId) != "" && !ServerSideFrom(ctx) {
		if !KindFrom(ctx).Verified() {
			return errAnonymousMemory, true
		}
		if *request.Memory.UserId != CallerFrom(ctx).UserID {
			return errDeviceMemoryUser, true
		}
	}
	return APIError{}, false
}

// callAgentID is the call id a voice session is keyed under as its agent id, when it names no
// agent id of its own (Spec.KeyedAgentID), and empty otherwise.
func callAgentID(request CreateSessionRequest) string {
	if request.Call == nil || value(request.AgentId) != "" {
		return ""
	}
	return request.Call.Id
}

// memoryUserID is the user id a session's memory names, empty when it names none.
func memoryUserID(memory *SessionMemory) string {
	if memory == nil {
		return ""
	}
	return value(memory.UserId)
}

// registerSessionCreate declares the operations served in session_create.go.
func (s *Server) registerSessionCreate(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "createSession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions",
		Summary:     "Join a call as a voice agent",
		Description: "The whole conversation runs here: the agent joins the call, transcribes what it hears, " +
			"answers it and speaks back, all through the routers. The caller keeps the session id " +
			"and watches the conversation over the events socket.\n" +
			"It returns once the agent is in the call, so a session that comes back is one that is " +
			"already listening. Tools declared here are the caller's own: the model asks for them " +
			"over the events socket and waits for the caller to answer.",
		Extensions:    map[string]any{clientAccessibleExtension: true},
		DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{
			"201": {Description: "The agent is in the call"},
			"409": errorResponse("A session with that id already exists, another session holds the conversation it names open, or the call placed for this phone session ended without an answer (call_unanswered)"),
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.createSession)
}

type createSessionRequest struct {
	Body *CreateSessionRequest `required:"true"`
}

type createSessionResponse struct {
	Body Session
}
