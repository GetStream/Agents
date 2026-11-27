package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/danielgtaylor/huma/v2"
)

// errNoSessions is what every session path says on a deployment that only inspects routing.
// It is a 404 rather than a 501 because the resource genuinely is not there: this router
// runs no conversations, so it holds no sessions to find.
var errNoSessions = notConfigured("this deployment does not run sessions")

// configFor resolves whichever way the caller addressed the agent.
//
// A name is the one a person actually knows the agent by, so it is worth supporting even
// though it costs a lookup. Both at once is refused rather than picking one: there is no
// sensible answer when they disagree, and quietly preferring the id would leave a caller
// wondering why the name they wrote had no effect.
func (s *Server) configFor(ctx context.Context, customerID string, configID, name *string) (*store.AgentConfig, error) {
	id, named := value(configID), value(name)
	switch {
	case id == "" && named == "":
		return nil, nil
	case id != "" && named != "":
		return nil, invalidRequest("name an agent by config_id or by agent, not both")
	case s.store == nil:
		return nil, errNoConfigs
	}

	teams, err := s.teamsGateFor(ctx, customerID)
	if err != nil {
		return nil, err
	}
	if id != "" {
		found, err := s.store.AgentConfigs.Get(ctx, customerID, id)
		if err != nil {
			return nil, unknownOr(err, store.ErrNoAgentConfig, errUnknownConfig)
		}
		if teams.refuses(ctx, found) {
			return nil, errUnknownConfig
		}
		return &found, nil
	}

	found, err := s.store.AgentConfigs.GetByName(ctx, customerID, named)
	if errors.Is(err, store.ErrNoAgentConfig) {
		// Refused rather than started unconfigured. A typo in a name would otherwise get a
		// working session with default instructions, which is far harder to notice than an
		// error: the agent answers, just not as the agent that was asked for.
		return nil, notFound("there is no agent called " + named)
	}
	if err != nil {
		return nil, err
	}
	if teams.refuses(ctx, found) {
		return nil, notFound("there is no agent called " + named)
	}
	return &found, nil
}

// resumeSession carries a conversation on with what the caller says about its tools.
func (s *Server) resumeSession(ctx context.Context, request *resumeSessionRequest) (*getSessionResponse, error) {
	if failure, refused := refuseReservedActingFor(ctx, "ResumeSession"); refused {
		return nil, failure
	}
	var over resumed
	if body := request.Body; body != nil {
		over.toolTimeoutMs = body.ToolTimeoutMs
		if body.Tools != nil {
			declared := declaredTools(*body.Tools)
			over.tools = &declared
		}
	}
	_, sent, failure := s.carryOn(ctx, request.Id, refusedRequest, over)
	switch {
	case failure == nil:
		// Held open for the watcher a resume is followed by, as for a message.
		sent()
	case !errors.Is(failure, errUnknownSession):
		return nil, failure
	}
	// Answered as a read is, so one that could not be carried on is still the caller's to
	// read, and one that is not theirs is not found either way.
	return s.getSession(ctx, &getSessionRequest{Id: request.Id})
}

// forkSession continues a conversation as a new one.
func (s *Server) forkSession(ctx context.Context, request *forkSessionRequest) (*forkSessionResponse, error) {
	if failure, refused := refuseReservedActingFor(ctx, "ForkSession"); refused {
		return nil, failure
	}
	customerID, ok := CustomerFrom(ctx)
	if !ok {
		return nil, errMissingCustomer
	}
	if s.sessions == nil {
		return nil, errNoSessions
	}

	parent, failure := s.storedOrLiveSession(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}

	body := ForkSessionRequest{}
	if request.Body != nil {
		body = *request.Body
	}
	if failure, refused := refuseDeviceModels(ctx, body.ModelOverwrites != nil); refused {
		return nil, failure
	}

	// A named agent on the fork replaces the parent's config wholesale, which is the point:
	// asking the same question of a different agent is the reason to fork.
	config, failure := s.configFor(ctx, customerID, body.ConfigId, body.Agent)
	if failure != nil {
		return nil, failure
	}

	thread, err := s.heldOnThread(ctx, parent)
	if err != nil {
		return nil, err
	}
	// A fork naming no config goes on with its parent's, as the config is now rather than as
	// the parent was opened with. A config deleted since leaves the fork what its parent had;
	// a config that cannot be read fails the fork rather than dropping what it decides. An end
	// user is checked against its team as if they had named it, and a deleted one is refused.
	var current *store.AgentConfig
	currentConfig := func() error {
		if config != nil || s.store == nil || parent.ConfigID() == "" {
			return nil
		}
		teams, err := s.teamsGateFor(ctx, customerID)
		if err != nil {
			return err
		}
		current, err = s.configOpenedWith(ctx, customerID, parent.ConfigID(), teams)
		return err
	}
	// An ended parent's fork is built from it, so it is read first. A live parent's fork, an
	// incognito one or one on a thread is refused by forkSpec before any config is needed.
	if parent.Live == nil && parent.Stored != nil && !thread {
		if err := currentConfig(); err != nil {
			return nil, err
		}
		// A backend naming the fork's project names its tag (Spec.NameProject), whatever the parent's.
		if body.ProjectId == nil || KindFrom(ctx) != auth.KindServer {
			if err := s.readProjectTag(ctx, parent.Stored); err != nil {
				return nil, err
			}
		}
	}
	spec, err := forkSpec(parent, body, config, current, thread)
	if err != nil {
		return nil, invalidRequest(err.Error())
	}
	// The connector bindings are the config's as it is now, even for a live parent. The
	// session re-resolves the parent's selections against them and against the caller asking
	// for the fork, and drops the ones the config no longer declares. A config deleted since
	// binds nothing, and its fork goes ahead as a fork did before bindings existed.
	if parent.Live != nil {
		if err := currentConfig(); err != nil {
			return nil, err
		}
	}
	if config == nil {
		spec.ConnectorBindings = nil
		if current != nil {
			spec.ConnectorBindings = current.Connectors
		}
	}
	recalled, err := s.recordedHistory(ctx, parent, body, spec.Recall)
	switch {
	case errors.Is(err, store.ErrUnknownResponse):
		return nil, notFound(err.Error())
	case errors.Is(err, errForkNeedsHistory):
		return nil, invalidRequest(err.Error())
	case err != nil:
		return nil, err
	}
	if recalled != nil {
		spec.Recall = &session.Recall{Messages: recalled}
	}
	if spec.ForkPoint, err = s.forkPoint(ctx, parent, body); err != nil {
		return nil, err
	}
	spec.CustomerID = customerID
	spec.NameProject(body.ProjectId, KindFrom(ctx))
	// A voice fork is keyed under the call it joins, which the request names.
	ctx = s.threadConversation(ctx, customerID, &spec)
	spec.Caller = CallerFrom(ctx)
	// The parent's tools go to the fork only for the kind of caller that declared them, as
	// they do for a conversation carried on (rowTools): a backend's tool on its end user's
	// fork would be the device's to answer (Session.seatRuns).
	if spec.CallerKind != KindFrom(ctx) {
		spec.Tools = nil
	}
	spec.CallerKind = KindFrom(ctx)
	if body.Messages == nil || *body.Messages {
		spec.Documents = s.forkedDocuments(ctx, parent, value(body.ResponseId), spec.Caller.UserID)
	}

	created, err := s.sessions.Create(ctx, spec)
	if errors.Is(err, session.ErrNoAgentCallType) {
		return nil, errNoAgentCallType
	}
	if errors.Is(err, streamapp.ErrDeploymentAppUnknown) {
		return nil, err
	}
	if err != nil {
		s.logNotConfigured(ctx, customerID, err)
		return nil, refusedRequest(err)
	}
	return &forkSessionResponse{Body: sessionOf(ctx, created)}, nil
}

// getSessionTokens says what the session's models read and wrote, and what their prompts
// were made of, while it runs and after.
func (s *Server) getSessionTokens(ctx context.Context, request *getSessionTokensRequest) (*getSessionTokensResponse, error) {
	customerID, ok := CustomerFrom(ctx)
	if !ok {
		return nil, errMissingCustomer
	}
	if s.store == nil {
		return nil, errNoCalls
	}
	row, err := s.store.Sessions.Get(ctx, customerID, request.Id)
	if err != nil {
		return nil, unknownOr(err, store.ErrNoSession, errUnknownSession)
	}
	spent, err := s.tokensOf(ctx, customerID, sessionSpan(row))
	if err != nil {
		return nil, err
	}
	return &getSessionTokensResponse{Body: *spent}, nil
}

// getSession returns one session, running or ended. A backend is also told what was made of
// it, from the review on its latest call, and what all of it spent.
func (s *Server) getSession(ctx context.Context, request *getSessionRequest) (*getSessionResponse, error) {
	found, failure := s.storedOrLiveSession(ctx, request.Id)
	if failure == nil && found.Live != nil && !canReadSession(ctx, found.Live.Spec()) {
		failure = errUnknownSession
	}
	if failure != nil {
		return nil, failure
	}

	var rendered Session
	if found.Live != nil {
		rendered = sessionOf(ctx, found.Live)
		rendered.PinnedAt = s.livePinnedAt(ctx, found.Live)
	} else {
		call, err := s.latestCall(ctx, *found.Stored)
		if err != nil {
			return nil, err
		}
		rendered = storedSessionOf(*found.Stored, call)
	}
	if KindFrom(ctx) != auth.KindServer || s.store == nil || value(rendered.Incognito) {
		return &getSessionResponse{Body: rendered}, nil
	}
	rendered.CallOutcome = s.callOutcome(ctx, request.Id)
	row := found.Stored
	if row == nil {
		written, err := s.store.Sessions.Get(ctx, OwnerFrom(ctx).CustomerID, request.Id)
		if err != nil {
			s.logger.Warn("could not read a session's row", "session", request.Id, "error", err)
			return &getSessionResponse{Body: rendered}, nil
		}
		row = &written
	}
	call, err := s.latestCall(ctx, *row)
	if err != nil {
		s.logger.Warn("could not read a session's latest call", "session", request.Id, "error", err)
		return &getSessionResponse{Body: rendered}, nil
	}
	if call.Summary != "" {
		rendered.Summary = &call.Summary
	}
	if call.ReviewNotes != "" {
		rendered.ReviewNotes = &call.ReviewNotes
	}
	rendered.ReviewScore = call.ReviewScore
	var record Call
	s.attachUsage(ctx, OwnerFrom(ctx).CustomerID, sessionSpan(*row), &record)
	rendered.Usage = record.Usage
	return &getSessionResponse{Body: rendered}, nil
}

// callOutcome is how the call placed for a session ended, nil when none was placed.
func (s *Server) callOutcome(ctx context.Context, sessionID string) *SessionCallOutcome {
	if s.phone == nil {
		return nil
	}
	call, err := s.phone.Outcomes().Read(ctx, OwnerFrom(ctx).CustomerID, sessionID)
	if errors.Is(err, outcome.ErrUnknownCall) {
		return nil
	}
	if err != nil {
		s.logger.Warn("could not read how a session's placed call ended", "session", sessionID, "error", err)
		return nil
	}
	shown := &SessionCallOutcome{AnsweredAt: call.AnsweredAt}
	if call.Outcome != "" {
		decided := CallOutcome(call.Outcome)
		shown.Outcome = &decided
	}
	if duration, ok := call.Duration(); ok {
		seconds := int(duration.Round(time.Second) / time.Second)
		shown.DurationSeconds = &seconds
	}
	return shown
}

// saySession speaks a piece of text without going through the model.
func (s *Server) saySession(ctx context.Context, request *saySessionRequest) (*struct{}, error) {
	found, failure := s.session(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}
	if request.Body == nil || request.Body.Text == "" {
		return nil, invalidRequest("there is nothing to say")
	}
	if found.Spec().Native() {
		return nil, errSayNative
	}

	// Anything else that stops it is the voice's, whose text names its vendor.
	if err := found.Say(ctx, request.Body.Text); err != nil {
		return nil, err
	}
	return nil, nil
}

// errSayNative refuses exact words to a speech-to-speech model, which says what it makes of a
// prompt rather than a script.
var errSayNative = invalidRequest("a speech-to-speech agent cannot say exact words: use respond")

// respondSession answers a piece of text through the model.
func (s *Server) respondSession(ctx context.Context, request *respondSessionRequest) (*respondSessionResponse, error) {
	// It answers once the turn starts, as createResponse does; a voice note's transcription is
	// the turn's cost.
	leaveUncosted(ctx)
	if request.Body != nil {
		if err := refuseOversizeWithoutAudio(request.Body.Audio, sizeOfRespond(request.Body)); err != nil {
			return nil, err
		}
	}
	found, sent, failure := s.sessionToAnswer(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}
	defer sent()
	if request.Body == nil || (request.Body.Text == "" && request.Body.Audio == nil && len(value(request.Body.Images)) == 0) {
		return nil, invalidRequest("there is nothing to answer")
	}
	wires := make([]wireImage, 0, len(value(request.Body.Images)))
	for _, sent := range value(request.Body.Images) {
		wires = append(wires, wireImage{URL: sent.Url, Detail: string(value(sent.Detail))})
	}
	images, err := imagesFromWire(wires)
	if err != nil {
		return nil, invalidContent(err)
	}

	id := value(request.Body.RequestId)
	text, notes, failure := s.withHeardNote(ctx, found, id, request.Body.Text, request.Body.Audio, len(images) > 0, options.LLM{})
	if failure != nil {
		return nil, failure
	}
	if id != "" && found.Durable() {
		receipt, _, err := found.RespondCommand(ctx, id, text, value(request.Body.ClientId), images, options.LLM{}, notes...)
		if err != nil {
			return nil, refusedResponse(err)
		}
		return &respondSessionResponse{Status: http.StatusOK, Body: &CommandReceipt{
			RequestId: receipt.CommandID, UserMessageId: receipt.UserMessageID,
			AssistantMessageId: receipt.AssistantMessageID, State: receipt.State, Duplicate: receipt.Duplicate,
		}}, nil
	}
	if _, err := found.Respond(ctx, text, images, options.LLM{}, images...); err != nil {
		return nil, refusedResponse(err)
	}
	return &respondSessionResponse{Status: http.StatusNoContent}, nil
}

// sizeOfRespond is what a respond body holds besides a voice note, counted as sizeOfResponse
// counts a createResponse body.
func sizeOfRespond(body *RespondRequest) int {
	size := len(body.Text)
	for _, image := range value(body.Images) {
		size += len(image.Url)
	}
	return size
}

// respondSessionResponse is a durable command's receipt, or nothing when the text named no
// command and the model is simply answering.
type respondSessionResponse struct {
	Status int
	Body   *CommandReceipt
}

// interruptSession abandons the reply being spoken.
func (s *Server) interruptSession(ctx context.Context, request *interruptSessionRequest) (*struct{}, error) {
	found, failure := s.session(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}

	found.Interrupt()
	return nil, nil
}

// rewindSession carries a conversation on from the end of one of its responses.
func (s *Server) rewindSession(ctx context.Context, request *rewindSessionRequest) (*struct{}, error) {
	if request.Body == nil || request.Body.ResponseId == "" {
		return nil, invalidRequest("name the response to carry on from")
	}
	if s.store == nil {
		return nil, errNoStore
	}
	// A conversation kept in Chat that ended is carried on, as a regenerate, an edit or an
	// activation carries it on: its rewind needs the conversation open.
	leaveUncosted(ctx)
	found, sent, failure := s.sessionToAnswer(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}
	defer sent()

	err := found.Rewind(ctx, session.RecordedIn(s.store), request.Body.ResponseId)
	// A conversation kept in Chat is rewound the way an edit is, and refused the same ways.
	if refused := branchRefused(err); refused != nil {
		return nil, refused
	}
	switch {
	case errors.Is(err, store.ErrUnknownResponse):
		return nil, notFound(err.Error())
	case errors.Is(err, session.ErrCannotRewind):
		return nil, invalidRequest(err.Error())
	case err != nil:
		return nil, err
	}
	return nil, nil
}

// getSessionCommand reports what one durable command ended as, without running anything.
func (s *Server) getSessionCommand(ctx context.Context, request *getSessionCommandRequest) (*getSessionCommandResponse, error) {
	found, failure := s.session(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}

	receipt, err := found.Command(request.RequestId)
	if err != nil {
		return nil, unknownOr(err, conversation.ErrCommandNotFound, errUnknownCommand)
	}
	return &getSessionCommandResponse{Body: receiptOf(receipt)}, nil
}

// interruptSessionCommand stops the named command and leaves every other one alone.
func (s *Server) interruptSessionCommand(ctx context.Context, request *interruptSessionCommandRequest) (*interruptSessionCommandResponse, error) {
	found, failure := s.session(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}

	receipt, err := found.InterruptCommand(request.RequestId)
	if errors.Is(err, conversation.ErrCommandNotFound) {
		return nil, errUnknownCommand
	}
	if err != nil {
		// The stop was taken but its durable outcome is not known, so the caller is told
		// to keep the intent and retry this command id rather than that it stopped.
		recordFailure(ctx, err, stack.Trace(err))
		return nil, unavailable("the command's outcome could not be read; retry with the same request id")
	}
	return &interruptSessionCommandResponse{Body: receiptOf(receipt)}, nil
}

// receiptOf renders a durable command receipt for the wire.
func receiptOf(receipt conversation.CommandReceipt) CommandReceipt {
	return CommandReceipt{
		RequestId:          receipt.CommandID,
		UserMessageId:      receipt.UserMessageID,
		AssistantMessageId: receipt.AssistantMessageID,
		State:              receipt.State,
		Duplicate:          receipt.Duplicate,
	}
}

// setSessionSettings moves one running session onto other models or another voice. The
// agent config it started from is untouched.
func (s *Server) setSessionSettings(ctx context.Context, request *setSessionSettingsRequest) (*setSessionSettingsResponse, error) {
	found, failure := s.session(ctx, request.Id)
	if failure != nil {
		return nil, failure
	}
	if request.Body == nil {
		return nil, invalidRequest("a request body is required")
	}

	body := request.Body
	settings := session.Settings{
		LLM: body.Llm, STT: body.Stt, TTS: body.Tts, STS: body.Sts,
		Voice: body.Voice, Temperature: body.Temperature, MaxOutputTokens: body.MaxOutputTokens,
	}
	if body.Thinking != nil {
		thinking := string(*body.Thinking)
		settings.Thinking = &thinking
	}
	if body.Verbosity != nil {
		verbosity := string(*body.Verbosity)
		settings.Verbosity = &verbosity
	}
	if err := found.SetSettings(ctx, settings); err != nil {
		return nil, settingsRefused(err)
	}
	return &setSessionSettingsResponse{Body: sessionOf(ctx, found)}, nil
}

// settingsRefused answers a move onto other models or another voice that failed with err.
// Opening a model reads a customer's own model from the database and asks the vendor for a
// word, and either failing is the router's; what is left is the move asked for.
func settingsRefused(err error) error {
	switch {
	case errors.Is(err, session.ErrCardedToNative):
		return errCardedSessionToNative
	case dependencyFailed(err), errors.Is(err, llmrouter.ErrCustomModelKey):
		return err
	default:
		return invalidRequest(err.Error())
	}
}

// errCardedSessionToNative is what setSessionSettings and updateSession answer a move onto a
// speech-to-speech model of a session that started with the person's episode cards
// (session.ErrCardedToNative).
var errCardedSessionToNative = APIError{Type: ErrorTypeInvalidRequest, Code: codeCardedSessionToNative,
	Message: "a session that started with the person's episode cards cannot move onto a speech-to-speech model; start a new session to use one"}

// errUnknownSession is what a caller is told about a session that is not theirs, which is the
// same thing they are told about one that never existed.
var errUnknownSession = APIError{Type: ErrorTypeNotFound, Code: codeSessionNotFound, Message: "no such session"}

// errUnknownCommand is what a caller is told about a command this conversation never
// accepted, which is the same thing they are told about one they may not touch.
var errUnknownCommand = APIError{Type: ErrorTypeNotFound, Code: codeCommandNotFound, Message: "no such command"}

// session finds a session belonging to the calling customer.
func (s *Server) session(ctx context.Context, id string) (*session.Session, error) {
	if _, ok := CustomerFrom(ctx); !ok {
		return nil, errMissingCustomer
	}
	if s.sessions == nil {
		return nil, errNoSessions
	}
	found, ok := s.sessions.Get(id, OwnerFrom(ctx))
	if !ok || !canReadSession(ctx, found.Spec()) {
		return nil, errUnknownSession
	}
	return found, nil
}

// storedOrLiveSession finds a session whether or not it is still running.
//
// Reading back a conversation that ended is the ordinary case for everything that takes a
// session id and does not talk to the agent -- its turns, its items, forking it -- so those
// go through here rather than through session, which only knows about live ones.
//
// A session that ended and one that belongs to somebody else are both reported as not
// found: a different answer for each would make this a way to discover whose an id is.
func (s *Server) storedOrLiveSession(ctx context.Context, id string) (session.Found, error) {
	if _, ok := CustomerFrom(ctx); !ok {
		return session.Found{}, errMissingCustomer
	}
	if s.sessions == nil {
		return session.Found{}, errNoSessions
	}

	owner := OwnerFrom(ctx)
	if live, ok := s.sessions.Get(id, owner); ok {
		return session.Found{Live: live}, nil
	}
	// The store refuses an empty id as an error of its own, which is not ErrNoSession.
	if s.store == nil || id == "" {
		return session.Found{}, errUnknownSession
	}
	row, err := s.storedSession(ctx, owner.CustomerID, id)
	if err != nil {
		return session.Found{}, unknownOr(err, store.ErrNoSession, errUnknownSession)
	}
	// The row carries who opened it, which is what the live path checks through the
	// manager. Skipping it here would let one of a customer's users read another's.
	if !owner.Reaches(session.Owner{
		CustomerID: row.CustomerID, UserID: row.UserID, Kind: auth.Kind(row.CallerKind),
	}) {
		return session.Found{}, errUnknownSession
	}
	return session.Found{Stored: &row}, nil
}

// storedSession reads a session's row. The row is written behind the conversation, and stopping
// takes a session out of the live set at once, so one stopped a moment ago may not have a row
// yet: on a miss it waits for the writer and reads again. A row already there is answered at
// once, so a stalled writer holds up only a read that missed.
func (s *Server) storedSession(ctx context.Context, customerID, id string) (store.AgentSession, error) {
	row, err := s.store.Sessions.Get(ctx, customerID, id)
	if err == nil || s.sessions == nil {
		return row, err
	}
	if flushErr := s.sessions.FlushRecords(ctx); flushErr != nil {
		return store.AgentSession{}, flushErr
	}
	return s.store.Sessions.Get(ctx, customerID, id)
}

// reopenSettleGap is how long a reopened conversation's agent stays quiet, with nothing left
// to do, before the message that reopened it is taken as answered.
const reopenSettleGap = 2 * time.Second

// reopenHoldMost bounds how long a reopened conversation is held open for its message.
const reopenHoldMost = 10 * time.Minute

// sessionToAnswer finds the session a message is put to, along with what to call once the
// message is in.
//
// A chat is never over for the person writing in it. Its session ends when the last watcher
// leaves, which says nothing about whether they are done, so one that ended is carried on
// under the same id, on the config it was opened with. It is held open until the agent has
// answered, so a message sent with no socket attached still gets its reply, and then ends
// the way any conversation does once nobody is watching.
func (s *Server) sessionToAnswer(ctx context.Context, id string) (*session.Session, func(), error) {
	return s.carryOn(ctx, id, refusedRequest, resumed{})
}

// resumed is what a resume says over what a conversation runs with. A nil field keeps it.
type resumed struct {
	toolTimeoutMs *int
	// tools replaces the caller's declared tools when not nil, an empty list with none.
	tools *[]harness.Tool
}

// over writes r onto the spec of a conversation being carried on.
func (r resumed) over(spec *session.Spec) {
	spec.ToolTimeoutMs = override(spec.ToolTimeoutMs, r.toolTimeoutMs)
	if r.tools != nil {
		spec.Tools = *r.tools
	}
}

// onto gives a conversation that is still running what of r it can take while it runs: the
// tool timeout. Its tools are the ones it was opened with until it ends and is carried on.
// Only the kind of caller that opened it changes it: a device does not set how long its
// backend's tools are waited for.
func (r resumed) onto(ctx context.Context, running *session.Session) {
	if r.toolTimeoutMs != nil && running.Spec().CallerKind == KindFrom(ctx) {
		running.SetToolTimeout(*r.toolTimeoutMs)
	}
}

// carryOn is sessionToAnswer, with refuse turning the error of a session that could not be carried
// on into the answer: refusedRequest for a request; a scheduled task's delivery keeps the refusal
// itself, to tell money that may not be paid from money that could not be checked (unpaid).
func (s *Server) carryOn(ctx context.Context, id string, refuse func(error) error, over resumed) (*session.Session, func(), error) {
	found, failure := s.session(ctx, id)
	// Only for an end user still in its config's team, running or ended here, as one carried on
	// from its row is.
	if failure == nil {
		if failure = s.refuseOutsideTeam(ctx, found.Spec().CustomerID, found.Spec().ConfigID); failure != nil {
			return nil, nil, failure
		}
	}
	var spec session.Spec
	switch {
	case failure == nil && (found.State() != session.Ended || !found.Spec().PersistConversation):
		if found.State() != session.Ended {
			over.onto(ctx, found)
		}
		return found, func() {}, nil
	case failure == nil:
		// Ended here, so the spec it ran on is still at hand, tools and instructions included.
		// It is carried on in writing, whether or not it ended on a call.
		spec = found.Spec().Written()
		spec.Phone, spec.Edge = nil, nil
		spec.Reopened = found.CreatedAt()
		// A fork cut at a response reads its parent's turns again as one carried on from its
		// row does, so a parent deleted since is not read out of what the fork was opened with.
		// One that cannot be read keeps what it was opened with, and so does one with no cut,
		// which was opened with a read of its parent's channel that nothing recorded can stand
		// in for.
		if spec.ForkPoint != nil && spec.ForkPoint.ResponseID != "" {
			recall, err := s.forkRecall(ctx, store.AgentSession{
				ID: id, CustomerID: spec.CustomerID, ForkedFrom: spec.ForkedFrom, ForkPoint: spec.ForkPoint,
			})
			if err != nil {
				s.unreadForkRecall(ctx, id, err)
			} else {
				spec.Recall = recall
			}
		}
		// Its spec has the tools and timeout of whoever carried it on last. Another kind of
		// caller takes the ones its row keeps for that kind.
		if spec.CallerKind != KindFrom(ctx) {
			if failure = s.toolsFromRow(ctx, &spec); failure != nil {
				return nil, nil, failure
			}
		}
	case errors.Is(failure, errUnknownSession):
		if failure = s.refuseHomedElsewhere(ctx, id); failure != nil {
			return nil, nil, failure
		}
		if spec, failure = s.reopenedFromRow(ctx, id); failure != nil {
			return nil, nil, failure
		}
	default:
		return nil, nil, failure
	}
	spec.Greeting = ""
	spec.Caller = CallerFrom(ctx)
	spec.CallerKind = KindFrom(ctx)
	over.over(&spec)

	reopened, err := s.sessions.Create(ctx, spec)
	if errors.Is(err, session.ErrSessionExists) || errors.Is(err, conversation.ErrAlreadyOpen) {
		// Another request carried it on first and met this one on the id or on the conversation,
		// which the winner opens before it is held under the id. The one running is the answer,
		// given a resume's timeout rather than losing it; its tools are its own.
		if running := s.carriedOnBy(ctx, id); running != nil {
			over.onto(ctx, running)
			return running, func() {}, nil
		}
	}
	if errors.Is(err, session.ErrSessionExists) {
		found, failure := s.session(ctx, id)
		return found, func() {}, failure
	}
	if errors.Is(err, streamapp.ErrDeploymentAppUnknown) {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, refuse(err)
	}
	events, detach := reopened.Watch()
	return reopened, func() { go holdUntilAnswered(reopened, events, detach) }, nil
}

// errSessionElsewhere is a session carried on outside the shard it lives in, which has its
// rows. onOwningNode sends a request about one there; what reaches here did not come that way.
var errSessionElsewhere = conflict("this session lives in another region and is carried on there")

// refuseHomedElsewhere refuses to carry on a session that lives in another shard.
func (s *Server) refuseHomedElsewhere(ctx context.Context, id string) error {
	if s.store == nil {
		return nil
	}
	customer, _ := CustomerFrom(ctx)
	home, err := s.store.SessionHomes.Elsewhere(ctx, customer, id)
	if err != nil {
		return err
	}
	if home != "" {
		return errSessionElsewhere
	}
	return nil
}

// toolsFromRow gives spec, ended here and last carried on by another kind of caller, the tools
// and timeout its row keeps. The row keeps them for the kind of caller that opened it
// (Sessions.Upsert), so a device carrying on its backend's conversation leaves the backend
// its tools. With no row it has no tools.
func (s *Server) toolsFromRow(ctx context.Context, spec *session.Spec) error {
	spec.Tools = nil
	if s.store == nil {
		return nil
	}
	// As for reopenedFromRow: a failed flush reads the row as it is.
	_ = s.sessions.FlushRecords(ctx)
	row, err := s.store.Sessions.Get(ctx, spec.CustomerID, spec.ID)
	if errors.Is(err, store.ErrNoSession) {
		return nil
	}
	if err != nil {
		return err
	}
	rowTools(ctx, spec, row)
	return nil
}

// errProjectTagUnread refuses to carry on or fork a session whose project tag a shard that did
// not answer may say.
var errProjectTagUnread = unavailable("what the session's usage is tagged with could not be read; try again shortly")

// readProjectTag gives row the project tag its usage has (store.Sessions.ReadProjectTag).
func (s *Server) readProjectTag(ctx context.Context, row *store.AgentSession) error {
	err := s.store.Sessions.ReadProjectTag(ctx, row)
	if errors.Is(err, store.ErrProjectTagUnread) {
		// A caller that hung up cuts the read off, which says nothing of a shard.
		if !errors.Is(ctx.Err(), context.Canceled) {
			s.logger.Error("could not read a session's project tag", "session", row.ID, "error", err)
		}
		return errProjectTagUnread
	}
	return err
}

// rowTools gives spec the caller's tools and how long they are waited for, as row keeps them.
// The tools go only to the kind of caller that declared them: a backend's tool carried on by
// its end user's device would be the device's to answer (Session.seatRuns).
func rowTools(ctx context.Context, spec *session.Spec, row store.AgentSession) {
	spec.ToolTimeoutMs = row.ToolTimeoutMs
	spec.Tools = nil
	if row.CallerKind == string(KindFrom(ctx)) {
		spec.Tools = session.DeclaredTools(row.CallerTools)
	}
}

// reopenRaceWait bounds how long a request that lost a reopen waits for the winner to be held.
// A conversation open under another id is never held under this one, and is refused after it.
const reopenRaceWait = 3 * time.Second

// carriedOnBy is the session another request is carrying id on in, once it is held and still
// running, or nil if none is within reopenRaceWait.
func (s *Server) carriedOnBy(ctx context.Context, id string) *session.Session {
	ctx, cancel := context.WithTimeout(ctx, reopenRaceWait)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if running, failure := s.session(ctx, id); failure == nil && running.State() != session.Ended {
			return running
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// reopenedFromRow is the spec a text session that ended in another process is carried on
// with: the config it was opened with, and what its row remembers about it.
func (s *Server) reopenedFromRow(ctx context.Context, id string) (session.Spec, error) {
	// A resume's timeout given to the session just before it ended may still be on its way to
	// the row. A failed flush reads the row as it is.
	_ = s.sessions.FlushRecords(ctx)
	stored, failure := s.storedOrLiveSession(ctx, id)
	if failure != nil {
		return session.Spec{}, failure
	}
	row := stored.Stored
	if row == nil || row.ConversationID == "" || row.UserID != CallerFrom(ctx).UserID {
		return session.Spec{}, errUnknownSession
	}
	var spec session.Spec
	if row.ConfigID != "" {
		config, failure := s.configFor(ctx, row.CustomerID, &row.ConfigID, nil)
		if failure != nil {
			return session.Spec{}, failure
		}
		spec = session.FromConfig(*config)
		actAs(&spec, config.Identity)
	}
	spec.ID = row.ID
	spec.Reopened = row.CreatedAt
	spec.CustomerID = row.CustomerID
	spec.Text = true
	spec.PersistConversation = true
	spec.ConversationID = row.ConversationID
	spec.Title, spec.Description = row.Title, row.Description
	if err := s.readProjectTag(ctx, row); err != nil {
		return session.Spec{}, err
	}
	spec.KeepProject(*row)
	spec.Custom = row.Custom
	spec.ModelOverwrites = row.ModelOverwrites
	if row.ResumeHistory != (store.ResumeHistory{}) {
		spec.ResumeHistory = row.ResumeHistory
	}
	if !row.FloorControl.IsZero() {
		spec.FloorControl = row.FloorControl
	}
	spec.ForkedFrom, spec.ForkPoint = row.ForkedFrom, row.ForkPoint
	// One whose parent cannot be read goes on with its own turns alone.
	recall, err := s.forkRecall(ctx, *row)
	if err != nil {
		s.unreadForkRecall(ctx, row.ID, err)
	}
	spec.Recall = recall
	// The caller's tools and how long they are waited for go on with the chat, as they do
	// for one carried on in the process that held it (Spec.Written).
	rowTools(ctx, &spec, *row)
	// The caller's connections go on with the chat; the session checks each against the
	// config as it is now and the caller asking, as it did when the chat opened.
	spec.ConnectorSelections = selectionsOf(*row)
	// And so do their consents, back to the page the chat was opened with.
	spec.ReturnURL = row.ReturnURL
	spec.NoUI = row.NoUI
	// Its usage stays under the key and categories it was opened with. The user is the one
	// asking, who is the row's own; the organization and its tier are as they are now.
	if row.APIKey != "" {
		spec.Attribution = routing.AttributionFrom(ctx)
		spec.Attribution.APIKey, spec.Attribution.UsageCategories = row.APIKey, row.UsageCategories
	}
	// Its team too: the one it was opened under is in those categories, and a team the config
	// was tagged with since does not move a conversation already under way.
	spec.Team = ""
	// Voice goes back to the Stream call it was last on once started.
	call, err := s.store.Calls.LastVoiced(ctx, row.CustomerID, row.ID)
	if err != nil && !errors.Is(err, store.ErrNoCall) {
		return session.Spec{}, err
	}
	return spec.Holding(session.Call{Type: call.CallType, ID: call.CallID}), nil
}

// forkRecall is the history a fork carried on reads out of its parent again: the parent's
// recorded turns on the path to the response the fork was cut at, as they stood then, whatever
// the parent said, rewound or regenerated since. A whole fork was cut at its parent's last
// response when it was made (forkSession), so the parent's later turns are not its. A fork
// that carried none, one of a parent that had recorded nothing, and one made before its fork
// point was kept have none.
//
// A parent deleted since, or its response at the cut, leaves the fork without its parent's
// turns: what was deleted is not handed to a model again. A read that fails is the error,
// which the caller copes with rather than refuse the fork its own turns.
func (s *Server) forkRecall(ctx context.Context, fork store.AgentSession) (*session.Recall, error) {
	point := fork.ForkPoint
	if fork.ForkedFrom == "" || point == nil || !point.Messages || point.ResponseID == "" || s.store == nil {
		return nil, nil
	}
	parent, err := s.store.Sessions.Get(ctx, fork.CustomerID, fork.ForkedFrom)
	if errors.Is(err, store.ErrNoSession) {
		if s.forkRecallLogs.Due("deleted:"+fork.CustomerID, time.Minute) {
			s.logger.InfoContext(ctx, "a reopened fork's parent was deleted; it goes on without its parent's turns",
				"session", fork.ID, "parent", fork.ForkedFrom)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	exchanges, err := s.store.Responses.ExchangesAt(ctx, fork.CustomerID, parent.ID, point.ResponseID)
	if errors.Is(err, store.ErrUnknownResponse) {
		if s.forkRecallLogs.Due("deleted:"+fork.CustomerID, time.Minute) {
			s.logger.InfoContext(ctx, "a reopened fork's fork point was deleted; it goes on without its parent's turns",
				"session", fork.ID, "parent", parent.ID, "response", point.ResponseID)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(exchanges) == 0 {
		return nil, nil
	}
	return &session.Recall{Messages: session.HistoryOf(exchanges)}, nil
}

// unreadForkRecall logs a reopened fork whose parent's turns could not be read, at most once a
// minute per app: every reopen of every fork of that app would say so while the store is down.
func (s *Server) unreadForkRecall(ctx context.Context, fork string, err error) {
	if customer, _ := CustomerFrom(ctx); s.forkRecallLogs.Due(customer, time.Minute) {
		s.logger.WarnContext(ctx, "could not read a reopened fork's parent turns", "session", fork, "error", err)
	}
}

// selectionsOf are the connections a stored session's caller chose for its session bindings.
func selectionsOf(row store.AgentSession) []session.ConnectorSelection {
	var chosen []session.ConnectorSelection
	for _, selection := range row.ConnectorSelections {
		chosen = append(chosen, session.ConnectorSelection{Name: selection.Name, ConnectionID: selection.ConnectionID})
	}
	return chosen
}

// holdUntilAnswered keeps a watcher on a reopened conversation until its agent settles.
func holdUntilAnswered(reopened *session.Session, events <-chan session.Event, detach func()) {
	defer detach()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	most := time.After(reopenHoldMost)
	last := time.Now()
	for {
		select {
		case _, open := <-events:
			if !open {
				return
			}
			last = time.Now()
		case <-most:
			return
		case <-ticker.C:
			if time.Since(last) >= reopenSettleGap && !reopened.Busy() {
				return
			}
		}
	}
}

// latestCalls is the latest call of each session found only as a row, by session id.
func (s *Server) latestCalls(ctx context.Context, found []session.Found) (map[string]store.Call, error) {
	var ids []string
	for _, one := range found {
		if one.Live == nil && one.Stored != nil {
			ids = append(ids, one.Stored.ID)
		}
	}
	if len(ids) == 0 || s.store == nil {
		return nil, nil
	}
	return s.store.Calls.Latest(ctx, OwnerFrom(ctx).CustomerID, ids)
}

// latestCall is the latest call of one stored session, zero when it has none.
func (s *Server) latestCall(ctx context.Context, row store.AgentSession) (store.Call, error) {
	calls, err := s.latestCalls(ctx, []session.Found{{Stored: &row}})
	return calls[row.ID], err
}

// sessionsOf renders a query's results, taking the live half where there is one: a session
// in flight knows what routing resolved its models to, which the row does not carry. calls
// is the latest call of each session found only as a row.
func sessionsOf(ctx context.Context, found []session.Found, calls map[string]store.Call) []Session {
	listed := make([]Session, 0, len(found))
	for _, one := range found {
		switch {
		case one.Live != nil:
			rendered := sessionOf(ctx, one.Live)
			// A live session that also has a row takes the row's afterthoughts: a title
			// somebody set from another tab is on the row and not on the running spec.
			if one.Stored != nil {
				mergeStored(&rendered, one.Stored)
			}
			listed = append(listed, rendered)
		case one.Stored != nil:
			listed = append(listed, storedSessionOf(*one.Stored, calls[one.Stored.ID]))
		}
	}
	return listed
}

// identifyAs gives a new session the identity its config sets wherever the request named none.
//
// Why: a device may not name who the agent is (refuseDeviceAgentIdentity), so the app's backend
// names it once on the config. A user_id or user_name at its default counts as naming none:
// clients generated before the schema dropped its defaults (CHA-7262) still send them. A session carrying on a conversation is given no agent id: opening the conversation
// refuses any but the one it was kept under, which the conversation supplies. Nor is an
// incognito session, which keeps no conversation: a message hook hands a message in
// agent:<agent id> to the newest session under that id that keeps none (routeArrivingMessage),
// so one shared agent id would hand any Chat user's message to the newest visitor's session.
// The agent id's Chat user, which a text session writes as, is made with the identity's
// user_name when a new conversation makes it.
func identifyAs(spec *session.Spec, identity store.AgentIdentity) {
	actAs(spec, identity)
	if identity.AgentID != "" && spec.AgentID == "" && spec.ConversationID == "" && !spec.Incognito {
		spec.AgentID, spec.AgentIDName = identity.AgentID, identity.UserName
	}
}

// actAs gives a session the user its config's identity sets wherever it named none
// (identifyAs). A session carried on from its row or forked is rebuilt from the config as it is
// now, and its row keeps no agent user, so it takes the config's too.
func actAs(spec *session.Spec, identity store.AgentIdentity) {
	if identity.UserID != "" && (spec.UserID == "" || spec.UserID == session.DefaultUserID) {
		spec.UserID = identity.UserID
	}
	if identity.UserName != "" && (spec.UserName == "" || spec.UserName == session.DefaultUserName) {
		spec.UserName = identity.UserName
	}
}

// specOf turns a request into what the session package needs, over whatever config it
// named. The customer comes from the trusted header rather than the body: a caller naming
// its own would be billing somebody else.
//
// The request wins wherever it says anything, so a caller can reuse a configuration and
// still change one thing about this call. A field the request omits is one the config
// decides, and a field neither mentions falls to the session defaults.
func specOf(request CreateSessionRequest, customerID string, config *store.AgentConfig) session.Spec {
	var spec session.Spec
	if config != nil {
		spec = session.FromConfig(*config)
	}
	spec.ID = value(request.Id)
	spec.CustomerID = customerID
	// A phone call is a call from the start. Anything else is held in writing until voice
	// is started, which start_voice does as the session opens.
	spec.Text = !value(request.StartVoice) && request.Phone == nil
	if request.Call != nil {
		spec.CallType, spec.CallID = request.Call.Type, request.Call.Id
	}
	// Over the config's team tag: the backend opening it knows which team the work is for.
	if request.Team != nil {
		spec.Team = *request.Team
	}
	// The conversation is kept in Stream Chat unless it is incognito, which Normalize turns
	// off, so any Chat client can read it back, typed and spoken.
	spec.PersistConversation = true
	spec.ConversationID = value(request.LegacyConversationId)
	for _, said := range value(request.History) {
		spec.History = append(spec.History, conversation.HistoryLine{
			Role: string(said.Role), Text: said.Text, Name: value(said.Name), At: value(said.CreatedAt),
		})
	}

	spec.Incognito = value(request.Incognito)
	if value(request.Instructions) != "" {
		spec.Instructions = *request.Instructions
	}
	spec.NoUI = request.Ui != nil && !*request.Ui
	spec.Title = override(spec.Title, request.Title)
	spec.Description = override(spec.Description, request.Description)
	if request.Custom != nil {
		spec.Custom = *request.Custom
	}
	spec.ResumeHistory = resumeHistoryOr(spec.ResumeHistory, request.ResumeHistory)
	spec.FloorControl = floorControlOr(spec.FloorControl, request.FloorControl)
	if request.ModelOverwrites != nil {
		spec.ModelOverwrites = modelOverwritesOf(*request.ModelOverwrites)
	}

	spec.Timezone = strings.TrimSpace(value(request.Timezone))
	spec.UserID = override(spec.UserID, request.UserId)
	spec.UserName = override(spec.UserName, request.UserName)
	spec.AgentID = override(spec.AgentID, request.AgentId)
	if config != nil {
		identifyAs(&spec, config.Identity)
	}
	if request.Greeting != nil {
		var mode string
		spec.Greeting, mode = greetingOf(request.Greeting)
		spec.VaryGreeting = mode == store.GreetingVariation
	}
	spec.Navigating = override(spec.Navigating, request.Navigating)
	spec.LLMTarget = override(spec.LLMTarget, request.Llm)
	spec.STTTarget = override(spec.STTTarget, request.Stt)
	spec.TTSTarget = override(spec.TTSTarget, request.Tts)
	spec.STSTarget = override(spec.STSTarget, request.Sts)
	spec.SearchTarget = override(spec.SearchTarget, request.Search)
	spec.Voice = override(spec.Voice, request.Voice)
	spec.MaxTokens = override(spec.MaxTokens, request.MaxTokens)
	spec.ToolTimeoutMs = override(spec.ToolTimeoutMs, request.ToolTimeoutMs)
	spec.MaxToolRounds = override(spec.MaxToolRounds, request.MaxToolRounds)
	spec.Backchannel = override(spec.Backchannel, request.Backchannel)
	spec.MinConfidence = override(spec.MinConfidence, request.MinConfidence)

	if request.Languages != nil {
		spec.LanguageHints = *request.Languages
	}
	if request.Keyterms != nil {
		spec.Keyterms = *request.Keyterms
	}
	for _, chosen := range value(request.ConnectorBindings) {
		spec.ConnectorSelections = append(spec.ConnectorSelections,
			session.ConnectorSelection{Name: chosen.Name, ConnectionID: chosen.ConnectionId})
	}
	spec.ReturnURL = value(request.ReturnUrl)
	// Cost labels are merged rather than replaced: a config labels which agent the spend
	// belongs to and a call labels which conversation, and both are worth billing on.
	if request.Tags != nil {
		if spec.Tags == nil {
			spec.Tags = routing.Tags{}
		}
		for key, tag := range *request.Tags {
			spec.Tags[key] = tag
		}
	}
	if request.Memory != nil {
		spec.Memory = session.MemorySpec{
			UserID: value(request.Memory.UserId),
			AppID:  value(request.Memory.AppId),
		}
		if request.Memory.Filter != nil {
			spec.Memory.Filter = *request.Memory.Filter
		}
	}
	if request.Phone != nil {
		spec.Phone = &session.PhoneSpec{
			Number:       request.Phone.Number,
			Vendor:       value(request.Phone.Vendor),
			VendorCallID: value(request.Phone.VendorCallId),
		}
		if plan := request.Phone.Voicemail; plan != nil {
			spec.Voicemail = &session.VoicemailPlan{}
			if plan.Action == VoicemailActionLeaveMessage {
				spec.Voicemail.Message = value(plan.Message)
			}
		}
	}
	if request.Video != nil {
		spec.VideoSource = override(spec.VideoSource, request.Video.Source)
		spec.VideoMaxFrames = override(spec.VideoMaxFrames, request.Video.MaxFrames)
	}
	if request.Tools != nil {
		for _, tool := range *request.Tools {
			spec.Tools = append(spec.Tools, toolOf(tool))
		}
	}
	return spec
}

// declaredTools are the caller's own tools, as a session's model is offered them.
func declaredTools(tools []SessionTool) []harness.Tool {
	declared := make([]harness.Tool, 0, len(tools))
	for _, tool := range tools {
		declared = append(declared, toolOf(tool))
	}
	return declared
}

// toolOf is one of the caller's tools as the session offers it.
func toolOf(tool SessionTool) harness.Tool {
	declared := harness.Tool{
		Name: tool.Name, Description: tool.Description, DisplayTitle: value(tool.DisplayTitle),
		Interruptible: value(tool.Interruptible), Timeout: time.Duration(value(tool.TimeoutMs)) * time.Millisecond,
	}
	if tool.Parameters != nil {
		declared.Parameters = *tool.Parameters
	}
	if tool.Executor != nil && *tool.Executor == SessionToolExecutorClient {
		declared.Client = true
	}
	if approval := tool.Approval; approval != nil && approval.Title != "" {
		declared.Approval = &harness.ToolApproval{
			Title: approval.Title, Message: value(approval.Message), ReasonArgument: value(approval.ReasonArgument),
			AllowTitle: value(approval.AllowTitle), DeclineTitle: value(approval.DeclineTitle),
		}
	}
	return declared
}

// sessionStateOf maps internal session lifecycle states onto the two-value API enum.
func sessionStateOf(state session.State) SessionState {
	if state == session.Ended {
		return Ended
	}
	return Live
}

// sessionOf renders a session for the wire. Its instructions go only to a backend: they are
// the backend's to set, and a brief the person using the agent is not meant to see would be
// read straight off their own session.
func sessionOf(ctx context.Context, found *session.Session) Session {
	spec := found.Spec()
	stt, model, voice, think := found.Resolved()
	instructions := spec.Instructions

	rendered := Session{
		ConversationId: &spec.ConversationID, ContextTruncated: &spec.ContextTruncated,
		Id:        found.ID(),
		CallId:    spec.CallID,
		CallType:  spec.CallType,
		UserId:    spec.UserID,
		AgentId:   spec.AgentID,
		State:     sessionStateOf(found.State()),
		Modality:  SessionModality(found.Modality()),
		CreatedAt: found.CreatedAt(),
	}
	if found.CapturesVideo() {
		rendered.Video = &SessionVideo{Source: &spec.VideoSource, MaxFrames: &spec.VideoMaxFrames}
	}
	if spec.Text {
		rendered.Text = &spec.Text
	}
	describe(&rendered, spec)
	if stt != "" {
		rendered.Stt = &stt
	}
	if model != "" {
		rendered.Llm = &model
	}
	if voice != "" {
		rendered.Tts = &voice
	}
	if think != "" {
		rendered.Subagent = &think
	}
	if speech := found.Speech(); speech != "" {
		rendered.Sts = &speech
	}
	_, speaking := found.Voice()
	rendered.Voice = optional(speaking)
	mode := SessionMode(found.Mode())
	rendered.Mode = &mode
	if instructions != "" && ServerSideFrom(ctx) {
		rendered.Instructions = &instructions
	}
	return rendered
}

// describe puts the caller's own labels on a rendered session. They are the same fields on
// either side of the wire, so a caller reads back what they asked for rather than having to
// remember it.
func describe(rendered *Session, spec session.Spec) {
	if spec.AgentName != "" {
		rendered.Agent = &spec.AgentName
	}
	if spec.ConfigID != "" {
		rendered.ConfigId = &spec.ConfigID
	}
	if spec.Incognito {
		rendered.Incognito = &spec.Incognito
	}
	if spec.Title != "" {
		rendered.Title = &spec.Title
	}
	if spec.Description != "" {
		rendered.Description = &spec.Description
	}
	if spec.Project != "" {
		rendered.ProjectId = &spec.Project
	}
	if team := usage.Team(spec.Attribution.UsageCategories); team != "" {
		rendered.Team = &team
	}
	if len(spec.Custom) > 0 {
		rendered.Custom = &spec.Custom
	}
	if spec.ForkedFrom != "" {
		rendered.ForkedFrom = &spec.ForkedFrom
	}
	if !spec.ModelOverwrites.Empty() {
		rendered.ModelOverwrites = modelOverwritesFor(spec.ModelOverwrites)
	}
}

// storedSessionOf renders a session that ended, from the row that outlived it and its latest
// call.
//
// The resolved models are absent rather than guessed. The row carries what the session was
// asked to use, not what routing picked on each turn, and reporting the request as though it
// were the answer would have a caller reading a failover that happened as a model that ran.
func storedSessionOf(row store.AgentSession, call store.Call) Session {
	rendered := Session{
		Id:        row.ID,
		CallId:    call.CallID,
		CallType:  call.CallType,
		UserId:    row.UserID,
		AgentId:   row.AgentID,
		State:     Live,
		CreatedAt: row.CreatedAt,
	}
	if row.State == store.SessionClosed {
		rendered.State = Ended
	}
	rendered.Region = optional(row.Region)
	// A session whose latest call had no Stream call was held in writing.
	if call.CallID == "" {
		text := true
		rendered.Text = &text
	}
	if row.ConversationID != "" {
		rendered.ConversationId = &row.ConversationID
	}
	mergeStored(&rendered, &row)
	return rendered
}

// mergeStored writes what only the row knows onto a rendered session: the labels, and when
// and why it ended.
func mergeStored(rendered *Session, row *store.AgentSession) {
	// A live session knows its modality before the row that records it.
	if rendered.Modality == "" {
		rendered.Modality = SessionModality(row.Modality)
	}
	if row.AgentName != "" {
		rendered.Agent = &row.AgentName
	}
	if row.ConfigID != "" {
		rendered.ConfigId = &row.ConfigID
	}
	if row.Title != "" {
		rendered.Title = &row.Title
	}
	if row.Description != "" {
		rendered.Description = &row.Description
	}
	if row.Project != "" {
		rendered.ProjectId = &row.Project
	}
	if row.Team != "" {
		rendered.Team = &row.Team
	}
	if len(row.Custom) > 0 {
		custom := row.Custom
		rendered.Custom = &custom
	}
	if row.ForkedFrom != "" {
		rendered.ForkedFrom = &row.ForkedFrom
	}
	if !row.ModelOverwrites.Empty() {
		rendered.ModelOverwrites = modelOverwritesFor(row.ModelOverwrites)
	}
	rendered.ClosedAt = row.ClosedAt
	rendered.EndReason = endReasonOf(row.EndReason)
	if info, known := session.EndReason(row.EndReason).Info(); known {
		fault, stage := SessionEndFault(info.Fault), SessionEndStage(info.Stage)
		rendered.EndFault, rendered.EndStage = &fault, &stage
	}
	rendered.LastResponseAt = row.LastResponseAt
	rendered.PinnedAt = row.PinnedAt
}

// SessionEndReason is why a session ended: a session.EndReasons value a row can keep.
type SessionEndReason string

func (SessionEndReason) Schema(registry huma.Registry) *huma.Schema {
	var values, meanings []string
	for _, info := range session.EndReasons {
		// A session that never opened has no row to keep its reason on.
		if info.Stage == session.StageStarting {
			continue
		}
		values = append(values, string(info.Reason))
		meanings = append(meanings, string(info.Reason)+", "+info.Meaning+
			" (fault "+string(info.Fault)+", stage "+string(info.Stage)+")")
	}
	return namedEnum(registry, "SessionEndReason", "Why the session ended, with whose fault it was and the stage it came at: "+
		strings.Join(meanings, "; ")+".", values...)
}

// SessionEndFault is whose doing a session's end was.
type SessionEndFault string

func (SessionEndFault) Schema(registry huma.Registry) *huma.Schema {
	return namedEnum(registry, "SessionEndFault", "Whose doing a session's end was: none, the agent or the app ended it; caller, the person on the call; router, the router itself; vendor, a model vendor; carrier, the phone carrier; customer_config, the app's configuration or its own key; customer_credit, the organization's credit or spend limits.",
		string(session.FaultNone), string(session.FaultCaller), string(session.FaultRouter), string(session.FaultVendor),
		string(session.FaultCarrier), string(session.FaultCustomerConfig), string(session.FaultCustomerCredit))
}

// SessionEndStage is how far a session had got when it ended.
type SessionEndStage string

func (SessionEndStage) Schema(registry huma.Registry) *huma.Schema {
	return namedEnum(registry, "SessionEndStage", "How far a session had got when it ended: ringing, before a person was on the line of a phone call; conversation, once the session was open.",
		string(session.StageRinging), string(session.StageConversation))
}

// modelOverwritesOf reads what the caller asked to change about the models. The name is
// longer than it wants to be because overwritesOf already means the per-provider option
// blocks a router config carries, which are a different thing entirely.
func modelOverwritesOf(sent ModelOverwrites) store.ModelOverwrites {
	return store.ModelOverwrites{
		LLM: value(sent.Llm), STT: value(sent.Stt), TTS: value(sent.Tts),
		STS: value(sent.Sts), Search: value(sent.Search),
		Thinking:        string(value(sent.Thinking)),
		Temperature:     sent.Temperature,
		MaxOutputTokens: sent.MaxOutputTokens,
		Verbosity:       string(value(sent.Verbosity)),
	}
}

// modelOverwritesFor renders them back, so a caller reads what they asked for.
func modelOverwritesFor(held store.ModelOverwrites) *ModelOverwrites {
	rendered := &ModelOverwrites{
		Temperature: held.Temperature, MaxOutputTokens: held.MaxOutputTokens,
	}
	if held.LLM != "" {
		rendered.Llm = &held.LLM
	}
	if held.STT != "" {
		rendered.Stt = &held.STT
	}
	if held.TTS != "" {
		rendered.Tts = &held.TTS
	}
	if held.STS != "" {
		rendered.Sts = &held.STS
	}
	if held.Search != "" {
		rendered.Search = &held.Search
	}
	if held.Thinking != "" {
		thinking := ModelOverwritesThinking(held.Thinking)
		rendered.Thinking = &thinking
	}
	if held.Verbosity != "" {
		verbosity := ModelOverwritesVerbosity(held.Verbosity)
		rendered.Verbosity = &verbosity
	}
	return rendered
}

// forkSpec is the parent's spec with the fork's request written over it.
//
// The parent is read from whichever half of Found has it. A live parent is preferable -- it
// carries the whole spec, tools and skills included -- but a conversation worth continuing
// has usually ended, so the row has to be enough on its own. thread is whether the parent's
// conversation is held on a thread channel (heldOnThread). current is the parent's own config
// as it is now, nil when the parent had none or it was deleted since.
func forkSpec(parent session.Found, request ForkSessionRequest, config, current *store.AgentConfig, thread bool) (session.Spec, error) {
	var spec session.Spec
	var parentID string
	// What the fork reads its history out of, which is the parent's channel and the agent
	// id that channel belongs to rather than whichever agent the fork runs as.
	var recall *session.Recall

	switch {
	case parent.Live != nil:
		// A fork is held in writing whatever the parent is doing: one person talks to one
		// agent, and voice is started on the fork when it is wanted there.
		// The fork's own call is agent:<fork id>, never the call the parent was on.
		spec = parent.Live.Spec().Written().Holding(session.Call{})
		spec.Phone, spec.Edge = nil, nil
		// Nor what the parent was as one conversation: the placed call it was, the hand-out it
		// answered, and when it first opened if it was carried on.
		spec.Voicemail, spec.Navigating = nil, false
		spec.CampaignID, spec.ContactID = "", ""
		spec.Dispatched, spec.HandedOutBy = false, ""
		spec.Reopened = time.Time{}
		parentID = parent.Live.ID()
		if spec.Incognito {
			return session.Spec{}, stack.Wrap(errors.New(
				"an incognito session is not recorded, so there is nothing to fork from"))
		}
		if spec.ConversationID != "" {
			recall = &session.Recall{AgentID: spec.AgentID, ConversationID: spec.ConversationID}
		}
	case parent.Stored != nil:
		// The row keeps none of what the config decides but the models the parent was given
		// in place of the config's, so the fork runs on the config as it is now, the way a
		// reopened conversation does (reopenedFromRow).
		row := parent.Stored
		spec = session.Spec{AgentName: row.AgentName, ConfigID: row.ConfigID}
		if current != nil {
			spec = session.FromConfig(*current)
			actAs(&spec, current.Identity)
		}
		spec.Text = true
		// The conversation goes on after its history, which a greeting would interrupt:
		// a reopen says none either (sessionToAnswer).
		spec.Greeting = ""
		spec.Title, spec.Description = row.Title, row.Description
		spec.KeepProject(*row)
		spec.Custom, spec.ModelOverwrites = row.Custom, row.ModelOverwrites
		spec.ConnectorSelections = selectionsOf(*row)
		spec.ReturnURL = row.ReturnURL
		spec.NoUI = row.NoUI
		parentID = row.ID
		if row.ConversationID != "" {
			recall = &session.Recall{AgentID: row.AgentID, ConversationID: row.ConversationID}
		}
	default:
		return session.Spec{}, stack.Wrap(errors.New("there is nothing to fork"))
	}
	// A thread channel's conversation is everyone's in the external thread (Spec.Shared). A
	// fork would carry their words into a conversation one caller owns, outside the thread
	// and the shared session's connector rule, so none is made, whoever asks: the backend
	// carries on in the thread by opening it again by its agent id (threadConversation).
	if thread {
		return session.Spec{}, stack.Wrap(errors.New("a thread channel's conversation is not forked"))
	}

	// A config named on the fork replaces the parent's models wholesale rather than merging
	// with them, because asking the same question of a different agent is the reason to
	// fork and a half-replaced agent would be neither one.
	if config != nil {
		fresh := session.FromConfig(*config)
		actAs(&fresh, config.Identity)
		fresh.Text = true
		fresh.Title, fresh.Description = spec.Title, spec.Description
		fresh.Project, fresh.ProjectTag, fresh.Custom = spec.Project, spec.ProjectTag, spec.Custom
		fresh.ConnectorSelections = spec.ConnectorSelections
		fresh.ReturnURL = spec.ReturnURL
		fresh.NoUI = spec.NoUI
		fresh.ToolTimeoutMs = spec.ToolTimeoutMs
		spec = fresh
	}

	spec.ForkedFrom = parentID
	spec.Title = override(spec.Title, request.Title)
	spec.Description = override(spec.Description, request.Description)
	spec.ToolTimeoutMs = override(spec.ToolTimeoutMs, request.ToolTimeoutMs)
	spec.Incognito = value(request.Incognito)
	if request.Custom != nil {
		spec.Custom = *request.Custom
	}
	if request.ModelOverwrites != nil {
		spec.ModelOverwrites = modelOverwritesOf(*request.ModelOverwrites)
	}

	// The fork is its own conversation, so it gets its own channel and its own agent id:
	// sharing the parent's would have two sessions writing into one transcript.
	spec.ID = ""
	spec.ConversationID = ""
	spec.AgentID, spec.AgentIDName = "", ""

	// History comes across by default: the usual reason to fork is to carry on from what was
	// already said. It needs a channel at both ends -- the parent's to read and the fork's to
	// write -- and a parent that kept none has none to hand over. Recall is cleared rather than
	// inherited, so a fork of a fork reads its own parent and not its grandparent. The parent's
	// channel holds only the parent's own turns, not the ones it was forked with, so the
	// grandparent's turns do not come across.
	spec.PersistConversation = true
	spec.Recall = nil
	if recall != nil && (request.Messages == nil || *request.Messages) {
		spec.PersistConversation = true
		spec.Recall = recall
	}
	// Nothing said before the parent opened comes across either.
	if request.Messages != nil && !*request.Messages {
		spec.History = nil
	}
	return spec, nil
}

// forkPoint is where a fork's history out of its parent stops, kept on its row so the fork reads
// the same history when it is carried on (forkRecall): the response the request named, or for
// a fork of all of it the parent's last response now, so the parent's later turns are never
// the fork's. A parent that has recorded no response yet has no cut.
func (s *Server) forkPoint(ctx context.Context, parent session.Found, request ForkSessionRequest) (*store.ForkPoint, error) {
	point := &store.ForkPoint{Messages: request.Messages == nil || *request.Messages, ResponseID: value(request.ResponseId)}
	if !point.Messages || point.ResponseID != "" || s.store == nil {
		return point, nil
	}
	// A running parent may have answered something the writer has not caught up with yet.
	if parent.Live != nil {
		if err := parent.Live.FlushRecords(ctx); err != nil {
			return nil, err
		}
	}
	leaf, err := s.store.Responses.ActiveLeaf(ctx, OwnerFrom(ctx).CustomerID, parent.ID())
	if err != nil {
		return nil, err
	}
	point.ResponseID = leaf
	return point, nil
}

var errForkNeedsHistory = errors.New(
	"response_id says where the carried history stops, so it cannot be combined with messages false")

// recordedHistory is the history a fork reads out of what its parent recorded rather than
// out of a Chat channel: up to the named response, or all of it for a parent that kept no
// channel to read. Nil leaves the fork carrying whatever forkSpec decided.
func (s *Server) recordedHistory(ctx context.Context, parent session.Found, request ForkSessionRequest, recall *session.Recall) ([]llm.Message, error) {
	carry := request.Messages == nil || *request.Messages
	responseID := value(request.ResponseId)
	switch {
	case responseID != "" && !carry:
		return nil, stack.Wrap(errForkNeedsHistory)
	case responseID == "" && (!carry || recall != nil):
		return nil, nil
	case s.store == nil && responseID != "":
		return nil, errNoStore
	case s.store == nil:
		return nil, nil
	}

	// A running parent may have said something the writer has not caught up with yet.
	if parent.Live != nil {
		if err := parent.Live.FlushRecords(ctx); err != nil {
			return nil, err
		}
	}
	exchanges, err := s.store.Responses.Exchanges(ctx, OwnerFrom(ctx).CustomerID, parent.ID(), responseID)
	if err != nil {
		return nil, err
	}
	if len(exchanges) == 0 && responseID == "" {
		return nil, nil
	}
	return session.HistoryOf(exchanges), nil
}

// forkedDocuments are the files a fork carries from its parent's conversation: those sent up to
// the turn it stops at, and only when the person forking it is the one who sent them. Anybody
// else's fork, a server's included, carries the words and not the files.
//
// Files that cannot be read leave the fork without them rather than unopened, as a session's
// other records do, and say so in the log.
func (s *Server) forkedDocuments(ctx context.Context, parent session.Found, until, owner string) []store.ConversationDocument {
	if s.store == nil {
		return nil
	}
	scope := store.DocumentScope{SessionID: parent.ID(), Owner: owner, UntilResponse: until}
	switch {
	case parent.Live != nil:
		scope.ConversationID = parent.Live.Spec().ConversationID
	case parent.Stored != nil:
		scope.ConversationID = parent.Stored.ConversationID
	}
	documents, err := s.store.ConversationDocuments.Select(ctx, OwnerFrom(ctx).CustomerID, scope)
	if err != nil {
		s.logger.Error("could not read the files a fork carries", "session", parent.ID(), "error", err)
		return nil
	}
	return documents
}

// value reads an optional field, which the generated types carry as pointers.
func value[T any](pointer *T) T {
	if pointer == nil {
		var zero T
		return zero
	}
	return *pointer
}

// override prefers what the request said over what the config did. An omitted field is
// the config's to decide, which is the whole point of naming one.
func override[T any](base T, requested *T) T {
	if requested == nil {
		return base
	}
	return *requested
}

// Persistent personal sessions use the same caller binding as their Chat channel.
func canReadSession(ctx context.Context, spec session.Spec) bool {
	return !spec.PersistConversation || spec.Caller.UserID == CallerFrom(ctx).UserID
}

// registerSessions declares the operations served in sessions.go.
func (s *Server) registerSessions(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "getSession",
		Method:      http.MethodGet,
		Path:        "/v1/agents/sessions/{id}",
		Summary:     "One session",
		Description: "Reading a session is open to the device holding it, for the same reason listing and " +
			"stopping are: it is the conversation the caller is having. A session belonging to " +
			"somebody else is reported as not found rather than refused, so this is not a way to " +
			"find out whose an id is.\n" +
			"A session that ended is read too. A backend is also given its summary, review and usage, " +
			"which a device asks the backend for, and the instructions of a running session, which a " +
			"device is never given.",
		Extensions: map[string]any{clientAccessibleExtension: true},
		Responses: map[string]*huma.Response{
			"200": {Description: "The session"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.getSession)
	huma.Register(api, huma.Operation{
		OperationID: "getSessionTokens",
		Method:      http.MethodGet,
		Path:        "/v1/agents/sessions/{id}/tokens",
		Summary:     "What a session's models read and wrote, and what their prompts were made of",
		Description: "Read while the session is running as well as after it, unlike the session's usage, " +
			"which is counted once it is over, and for a session held in writing as for one on a call. " +
			"A prompt's parts are estimated: no provider says how much of a prompt was instructions, " +
			"tools or images, so the router estimates it from each request and scales it to what the " +
			"provider counted. The parts sum to the input tokens.",
		Responses: map[string]*huma.Response{
			"200": {Description: "The session's tokens"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.getSessionTokens)
	huma.Register(api, huma.Operation{
		OperationID: "forkSession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/fork",
		Summary:     "Continue a conversation as a new one",
		Description: "Opens a session from another's spec, carrying its history across by default, and " +
			"records where it came from. The usual reason is to ask the same question of a different " +
			"model without losing the original answer, which is why anything in the request is " +
			"written over what the parent was opened with. Models are the backend's to change: a device " +
			"sending `model_overwrites` is refused with a 403, and its fork keeps the parent's overwrites, or runs on the config it names.\n" +
			"The parent is untouched and keeps running if it was running. Forking an incognito " +
			"session is refused rather than answered with an empty conversation: there is nothing " +
			"recorded to fork from, and pretending otherwise would hand back a session that quietly " +
			"lost everything the caller thought they were continuing.",
		Extensions:    map[string]any{clientAccessibleExtension: true},
		DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{
			"201": {Description: "The fork is running"},
			"403": errorResponse("`model_overwrites` sent by an end user's device, or an agent config the caller may not use"),
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.forkSession)
	huma.Register(api, huma.Operation{
		OperationID: "resumeSession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/resume",
		Summary:     "Carry a conversation on, with the caller's tool settings",
		Description: "Carries on a conversation held in writing under its own id, as a message to it does, " +
			"and answers it as getSession does. A conversation that ended is carried on with the tools " +
			"its caller declared and the tool_timeout_ms it ran with, wherever it ended; anything " +
			"named here is written over those. A conversation still running takes tool_timeout_ms " +
			"for the tool calls that start after it, and keeps its tools until it ends and is " +
			"carried on. One that cannot be carried on, such as a call that kept no conversation, " +
			"is answered as it is.",
		Extensions: map[string]any{clientAccessibleExtension: true},
		Errors:     []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.resumeSession)
	huma.Register(api, huma.Operation{
		OperationID: "rewindSession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/rewind",
		Summary:     "Go back to a response and carry on from there",
		Description: "The conversation continues as though nothing after the named response had been said: " +
			"the reply being spoken is abandoned, the agent's history is cut back to the end of that " +
			"response, and every later response is marked rewound, so neither the responses nor " +
			"their items list them again. The named response itself is kept.\n" +
			"The history is rebuilt from what the session recorded, the question and the answer of " +
			"each turn, so a session that recorded nothing cannot be rewound: an incognito one, one " +
			"on a deployment with no store, and a native speech-to-speech one, whose model keeps its " +
			"own context.\n" +
			"A conversation kept in Stream Chat is rewound there too, the way an edit of the turn " +
			"after the named response is: every later message is hidden from every read of the " +
			"channel, so reopening the conversation does not bring the rewound turns back, and they " +
			"are kept as a branch that listResponseSiblings, getResponseBranch and " +
			"activateResponse read and bring back. It counts toward the conversation's 100 edit " +
			"and regenerate points (409 branch_limit_reached), and is refused as an edit is: in a " +
			"thread, dispatched or shared conversation, on a call, for a response whose messages carry no " +
			"branch mark, such as a turn spoken on a call, and while an earlier switch is unfinished " +
			"(409 switch_unfinished). Rewinding to the newest response changes nothing.",
		Extensions:    map[string]any{clientAccessibleExtension: true},
		DefaultStatus: http.StatusNoContent,
		Responses: map[string]*huma.Response{
			"204": {Description: "The conversation carries on from the end of that response"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict},
	}, s.rewindSession)
	huma.Register(api, huma.Operation{
		OperationID: "saySession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/say",
		Summary:     "Speak a piece of text without going through the model",
		Description: "For when the caller already knows what should be said, such as a greeting. A model " +
			"would only add latency and cost to words that were never in question.",
		DefaultStatus: http.StatusNoContent,
		Responses: map[string]*huma.Response{
			"204": {Description: "The text is being spoken"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.saySession)
	huma.Register(api, huma.Operation{
		OperationID:     "respondSession",
		Method:          http.MethodPost,
		Path:            "/v1/agents/sessions/{id}/respond",
		MaxBodyBytes:    maxInlineAudioBody,
		BodyReadTimeout: uploadReadTimeout,
		Summary:         "Answer a piece of text through the model, as though it had been said",
		Description: "A text session that ended is reopened under the same id, on the config it was " +
			"opened with: a chat is never over for the person writing in it. A call that ended is " +
			"not found.\n" +
			"A voice note in audio is transcribed before the turn starts, and what was said is the " +
			"question, after any text. A voice note that cannot be heard is refused and no turn starts.",
		Responses: map[string]*huma.Response{
			"200": {Description: "Durable command accepted or replayed; only a new command starts inference"},
			"409": errorResponse("The request id was already accepted with different content"),
			"204": {Description: "The model is answering"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.respondSession)
	huma.Register(api, huma.Operation{
		OperationID: "interruptSession",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/interrupt",
		Summary:     "Abandon the reply being spoken",
		Description: "What a caller outside the call has instead of a voice. A murmur is not interrupted, " +
			"because it was meant to overlap with whoever is talking.",
		DefaultStatus: http.StatusNoContent,
		Responses: map[string]*huma.Response{
			"204": {Description: "The reply was abandoned, if there was one"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.interruptSession)
	huma.Register(api, huma.Operation{
		OperationID: "getSessionCommand",
		Method:      http.MethodGet,
		Path:        "/v1/agents/sessions/{id}/commands/{request_id}",
		Summary:     "What is known about one durable command",
		Description: "Reads a command's receipt without accepting, running or stopping anything. It is how a " +
			"client whose stop or submission had an unknown outcome reconciles the same request id " +
			"rather than inventing another one.",
		Responses: map[string]*huma.Response{
			"200": {Description: "The command's current receipt"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound},
	}, s.getSessionCommand)
	huma.Register(api, huma.Operation{
		OperationID: "interruptSessionCommand",
		Method:      http.MethodPost,
		Path:        "/v1/agents/sessions/{id}/commands/{request_id}/interrupt",
		Summary:     "Stop one named command, and nothing else",
		Description: "Abandons the reply that command is generating. Unlike interrupting the session, a stop " +
			"that arrives after its command finished replays that command's terminal receipt and " +
			"leaves the command running now alone, so a delayed stop for one question can never take " +
			"the answer to the next one.\n" +
			"A command accepted but not yet generating is prevented from starting. A command already " +
			"completed, failed, cancelled or interrupted returns what it ended as. An unknown " +
			"command is a 404, the same answer as a conversation the caller does not own.\n" +
			"Interrupting model work claims nothing about a tool whose external side effect already " +
			"happened.",
		Responses: map[string]*huma.Response{
			"200": {Description: "The command's terminal receipt"},
			"503": errorResponse("The stop was accepted but its durable outcome is unknown. The command is not reported stopped; retry the same request id."),
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound},
	}, s.interruptSessionCommand)
	huma.Register(api, huma.Operation{
		OperationID: "setSessionSettings",
		Method:      http.MethodPatch,
		Path:        "/v1/agents/sessions/{id}/settings",
		Summary:     "Change the models and voice of one running session",
		Description: "Deprecated: use updateSession. Swaps what the agent runs on without leaving the call, " +
			"for this session only: the agent config it started from is untouched. The new models " +
			"are opened before anything changes, so a target that does not route is refused and the " +
			"agent carries on as it was. They take over from the next turn; a reply being spoken " +
			"finishes on the models it started with.\n" +
			"Naming sts makes the session native, and an empty sts makes it a cascade again, on " +
			"whatever llm, stt and tts it names or had before. The conversation carries across: a " +
			"conversation model is handed the history on every turn, and a speech-to-speech model is " +
			"opened with the recent transcript in its instructions. A session that started with the " +
			"person's episode cards cannot be moved onto a speech-to-speech model: 400, " +
			"carded_session_to_native.",
		Responses: map[string]*huma.Response{
			"200": {Description: "The session, on its new models"},
		},
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, s.setSessionSettings)
}

type getSessionRequest struct {
	Id string `path:"id" doc:"The session, as returned when it was created."`
}

type getSessionResponse struct {
	Body Session
}

type getSessionTokensRequest struct {
	Id string `path:"id" doc:"The session, as returned when it was created."`
}

type getSessionTokensResponse struct {
	Body CallTokens
}

type forkSessionRequest struct {
	Id   string `path:"id" doc:"The session, as returned when it was created."`
	Body *ForkSessionRequest
}

type forkSessionResponse struct {
	Body Session
}

type resumeSessionRequest struct {
	Id   string `path:"id" doc:"The session, as returned when it was created."`
	Body *ResumeSessionRequest
}

type rewindSessionRequest struct {
	Id   string                `path:"id" doc:"The session, as returned when it was created."`
	Body *RewindSessionRequest `required:"true"`
}

type saySessionRequest struct {
	Id   string      `path:"id" doc:"The session, as returned when it was created."`
	Body *SayRequest `required:"true"`
}

type respondSessionRequest struct {
	Id   string          `path:"id" doc:"The session, as returned when it was created."`
	Body *RespondRequest `required:"true"`
}

type interruptSessionRequest struct {
	Id string `path:"id" doc:"The session, as returned when it was created."`
}

type getSessionCommandRequest struct {
	Id        string `path:"id" doc:"The session, as returned when it was created."`
	RequestId string `path:"request_id" doc:"The request id the question was sent with."`
}

type getSessionCommandResponse struct {
	Body CommandReceipt
}

type interruptSessionCommandRequest struct {
	Id        string `path:"id" doc:"The session, as returned when it was created."`
	RequestId string `path:"request_id" doc:"The request id the question was sent with."`
}

type interruptSessionCommandResponse struct {
	Body CommandReceipt
}

type setSessionSettingsRequest struct {
	Id   string                  `path:"id" doc:"The session, as returned when it was created."`
	Body *SessionSettingsRequest `required:"true"`
}

type setSessionSettingsResponse struct {
	Body Session
}

// ForkSessionRequest Continue a conversation as a new one. Everything the parent was opened with is inherited; anything named here is written over it, which is what makes a fork useful rather than a copy -- the usual reason to fork is to ask the same question of a different model.
type ForkSessionRequest struct {
	Agent           *string                 `json:"agent,omitempty"`
	ConfigId        *string                 `json:"config_id,omitempty"`
	Custom          *map[string]interface{} `json:"custom,omitempty"`
	Description     *string                 `json:"description,omitempty"`
	Incognito       *bool                   `json:"incognito,omitempty" doc:"Hold the fork off the record. The parent still exists; this conversation onwards is simply not kept."`
	ToolTimeoutMs   *int                    `json:"tool_timeout_ms,omitempty" doc:"How long the fork's model waits for the result of a tool the caller or a worker runs, in milliseconds, as on createSession. Omitted keeps a running parent's; a fork of a conversation that ended has 30 seconds. Zero is 30 seconds, as on createSession."`
	Messages        *bool                   `json:"messages,omitempty" doc:"Carry the parent's history into the fork, so the new conversation continues from what was already said. False starts the same configuration over from nothing, which is what comparing two answers to the same opening question wants." default:"true"`
	ModelOverwrites *ModelOverwrites        `json:"model_overwrites,omitempty" doc:"Server-side only: a device sending it is refused with a 403."`
	ProjectId       *string                 `json:"project_id,omitempty" doc:"What the fork belongs to. Omitted, the parent's is kept. Sent server-side it is also the \"project\" cost tag, as on createSession; a device's groups the fork, whose cost tag stays the project a backend named for its parent. A tag spelled out in a running parent's tags, or the agent config's, wins."`
	ResponseId      *string                 `json:"response_id,omitempty" doc:"Carry the parent's history only up to the end of this response, so the fork continues from that point rather than from where the parent is now. The history is read from what the parent recorded, which also lets a parent that kept no Chat transcript be forked with its history. Cannot be combined with messages false."`
	Title           *string                 `json:"title,omitempty"`
}

func (*ForkSessionRequest) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Properties["tool_timeout_ms"].Format = ""
	schema.Description = "Continue a conversation as a new one. Everything the parent was opened with is inherited; anything named here is written over it, which is what makes a fork useful rather than a copy -- the usual reason to fork is to ask the same question of a different model."
	return schema
}

// ResumeSessionRequest What a caller carrying a conversation on says about its tools, over what it ran with.
type ResumeSessionRequest struct {
	ToolTimeoutMs *int           `json:"tool_timeout_ms,omitempty" minimum:"0" doc:"How long the model waits for the result of a tool the caller or a worker runs, in milliseconds, as on createSession, in place of what the conversation ran with. Omitted keeps that, which a conversation that ended keeps too. Zero is 30 seconds."`
	Tools         *[]SessionTool `json:"tools,omitempty" doc:"The caller's own tools, in place of the ones the conversation was opened with, which it keeps when this is omitted. An empty list offers none. A conversation still running keeps its tools until it ends and is carried on."`
}

func (*ResumeSessionRequest) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Properties["tool_timeout_ms"].Format = ""
	schema.Description = "What a caller carrying a conversation on says about its tools, over what it ran with."
	return schema
}

// ModelOverwrites What to change about the models for one session, over whatever its agent config decided.
// It is one object rather than a dozen fields at the top level because it is one idea: everything here overrides the config, and a caller reading a session back wants to see what they changed in one place rather than diffed against a config they would have to fetch. Only the safe knobs are here. Instructions and tools are not, because a caller able to rewrite those could make a session impersonate a different agent. A backend sets one session's instructions with the instructions of CreateSessionRequest instead, which a device may not send.
type ModelOverwrites struct {
	Llm             *string                   `json:"llm,omitempty" doc:"A provider/model or a capability shortcut, in place of the config's."`
	MaxOutputTokens *int                      `json:"max_output_tokens,omitempty" doc:"Caps the reply, reasoning included. Omitted leaves the provider's default."`
	Search          *string                   `json:"search,omitempty"`
	Sts             *string                   `json:"sts,omitempty" doc:"A speech-to-speech target. Naming one here makes the session native even if the config did not, which means no transcriber, model or voice is opened."`
	Stt             *string                   `json:"stt,omitempty"`
	Temperature     *float64                  `json:"temperature,omitempty" doc:"How random the answer is. Omitted leaves the provider's own default, which is not the same as zero: zero is a real request for a deterministic model."`
	Thinking        *ModelOverwritesThinking  `json:"thinking,omitempty" doc:"How hard to reason before answering. It becomes the reasoning effort on the request, which is the vocabulary the providers that support one already speak, and means nothing to a model that does not reason." enum:"none,minimal,low,medium,high"`
	Tts             *string                   `json:"tts,omitempty"`
	Verbosity       *ModelOverwritesVerbosity `json:"verbosity,omitempty" doc:"How much detail to give. Dropped for models that do not take it." enum:"low,medium,high"`
}

func (*ModelOverwrites) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Properties["max_output_tokens"].Format = ""
	schema.Description = "What to change about the models for one session, over whatever its agent config decided.\nIt is one object rather than a dozen fields at the top level because it is one idea: everything here overrides the config, and a caller reading a session back wants to see what they changed in one place rather than diffed against a config they would have to fetch. Only the safe knobs are here. Instructions and tools are not, because a caller able to rewrite those could make a session impersonate a different agent. A backend sets one session's instructions with the instructions of CreateSessionRequest instead, which a device may not send."
	return schema
}

// ModelOverwritesThinking is the ModelOverwritesThinking schema.
type ModelOverwritesThinking string

// Defines values for ModelOverwritesThinking.
const (
	ModelOverwritesThinkingHigh    ModelOverwritesThinking = "high"
	ModelOverwritesThinkingLow     ModelOverwritesThinking = "low"
	ModelOverwritesThinkingMedium  ModelOverwritesThinking = "medium"
	ModelOverwritesThinkingMinimal ModelOverwritesThinking = "minimal"
	ModelOverwritesThinkingNone    ModelOverwritesThinking = "none"
)

// Valid indicates whether the value is a known member of the ModelOverwritesThinking enum.
func (e ModelOverwritesThinking) Valid() bool {
	switch e {
	case ModelOverwritesThinkingHigh:
		return true
	case ModelOverwritesThinkingLow:
		return true
	case ModelOverwritesThinkingMedium:
		return true
	case ModelOverwritesThinkingMinimal:
		return true
	case ModelOverwritesThinkingNone:
		return true
	default:
		return false
	}
}

// ModelOverwritesVerbosity is the ModelOverwritesVerbosity schema.
type ModelOverwritesVerbosity string

// Defines values for ModelOverwritesVerbosity.
const (
	ModelOverwritesVerbosityHigh   ModelOverwritesVerbosity = "high"
	ModelOverwritesVerbosityLow    ModelOverwritesVerbosity = "low"
	ModelOverwritesVerbosityMedium ModelOverwritesVerbosity = "medium"
)

// Valid indicates whether the value is a known member of the ModelOverwritesVerbosity enum.
func (e ModelOverwritesVerbosity) Valid() bool {
	switch e {
	case ModelOverwritesVerbosityHigh:
		return true
	case ModelOverwritesVerbosityLow:
		return true
	case ModelOverwritesVerbosityMedium:
		return true
	default:
		return false
	}
}

// RespondRequest is the RespondRequest schema.
type RespondRequest struct {
	ClientId  *string        `json:"client_id,omitempty" doc:"The install the command came from. It is written on the person's message as client_id, and a client tool called while answering is addressed to it." pattern:"^[A-Za-z0-9_.:-]{1,128}$"`
	RequestId *string        `json:"request_id,omitempty" doc:"Generated and sent by the SDKs, one per question, so a retry is answered once. Required for personal persistent text conversations, and ignored by a session not kept in Stream Chat. With one, photos and a voice note are uploaded and shown on the person's message. A retry with the same id, text, photos and voice note does not restart inference." pattern:"^[A-Za-z0-9_-]{1,128}$"`
	Audio     *AudioSource   `json:"audio,omitempty" doc:"A voice note to answer, as on createResponse. The router transcribes it and answers what was said, after text when both are sent; text may then be empty. Refused before any turn starts when it is not audio, has no words in it, or the deployment transcribes nothing in the agent's region."`
	Images    *[]ImageSource `json:"images,omitempty" doc:"Photos to answer about, as on createResponse. With a request_id, or with no text in a session kept in Stream Chat, they are data URIs, at most 8, uploaded to the conversation's channel and shown on the person's message. The model sees them with this question alone. Refused before any turn starts when the agent has neither a vision skill nor a conversation model that accepts images."`
	Text      string         `json:"text" doc:"What to answer, as though it had been said. May be empty when photos or audio carry the question."`
}

// RewindSessionRequest is the RewindSessionRequest schema.
type RewindSessionRequest struct {
	ResponseId string `json:"response_id" doc:"The response to carry on from. It is kept; everything after it is not."`
}

// SayRequest is the SayRequest schema.
type SayRequest struct {
	Text string `json:"text"`
}

// SessionSettingsRequest What to change about one running session's models. A field left out is left as it is. The same safe knobs as ModelOverwrites, plus the voice.
type SessionSettingsRequest struct {
	Llm             *string                          `json:"llm,omitempty" doc:"The conversation model, a provider/model or a capability shortcut."`
	MaxOutputTokens *int                             `json:"max_output_tokens,omitempty"`
	Sts             *string                          `json:"sts,omitempty" doc:"A speech-to-speech target, which makes the session native. Empty makes it a cascade again."`
	Stt             *string                          `json:"stt,omitempty"`
	Temperature     *float64                         `json:"temperature,omitempty"`
	Thinking        *SessionSettingsRequestThinking  `json:"thinking,omitempty" enum:"none,minimal,low,medium,high"`
	Tts             *string                          `json:"tts,omitempty"`
	Verbosity       *SessionSettingsRequestVerbosity `json:"verbosity,omitempty" enum:"low,medium,high"`
	Voice           *string                          `json:"voice,omitempty" doc:"The voice to speak in: a provider's own voice id, or one of your voices by id or by the name you gave it. Prefix it with custom: to mean only the latter: without the prefix a name that is not one of yours is passed through to the provider's library, and with it a name that is not one of yours is refused. A native session (sts) takes the vendor's own voice name instead, such as marin at OpenAI. Empty returns to the provider's default."`
}

func (*SessionSettingsRequest) TransformSchema(_ huma.Registry, schema *huma.Schema) *huma.Schema {
	schema.Properties["max_output_tokens"].Format = ""
	schema.Description = "What to change about one running session's models. A field left out is left as it is. The same safe knobs as ModelOverwrites, plus the voice."
	return schema
}
