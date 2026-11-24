package session

import (
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// callRecorder writes a session's calls off the conversation's path, through the session
// writer's queue: a call names its session, so its row is written behind the session's.
type callRecorder struct {
	records *sessionRecorder
}

// Started queues the row for a call that has just joined.
func (r *callRecorder) Started(row store.Call) {
	r.records.queueWrite(recordWrite{callStarted: &row})
}

// Changed queues the models a running call moved onto.
func (r *callRecorder) Changed(row store.Call) {
	r.records.queueWrite(recordWrite{callChanged: &row})
}

// Ended queues the time a call left.
func (r *callRecorder) Ended(customerID, id string, at time.Time) {
	r.records.queueWrite(recordWrite{customerID: customerID, callEnded: id, callEndedAt: at})
}

// callRow is the id of the session's current call row and when it began.
func (s *Session) callRow() (string, time.Time) {
	s.labelMu.Lock()
	defer s.labelMu.Unlock()
	return s.call, s.callStarted
}

// nextCall makes a new call row the session's current one, from at, and returns the one it
// replaces.
func (s *Session) nextCall(at time.Time) string {
	s.labelMu.Lock()
	defer s.labelMu.Unlock()
	previous := s.call
	s.call, s.callStarted = store.NewID(), at.UTC()
	return previous
}

// row is the session's current call as somebody reading it back later sees it.
func row(created *Session) store.Call {
	spec := created.Spec()
	id, started := created.callRow()
	call := store.Call{
		ID:          id,
		SessionID:   created.id,
		CustomerID:  spec.CustomerID,
		StreamAppPK: spec.StreamApp,
		CallType:    spec.CallType,
		CallID:      spec.CallID,
		AgentID:     spec.AgentID,
		CampaignID:  spec.CampaignID,
		ContactID:   spec.ContactID,
		Direction:   store.Inbound,
		StartedAt:   started,
		Tags:        spec.Tags,
		// The spec here has already had a config folded into it, so these are what the
		// call actually ran with rather than what either side asked for on its own.
		STT:          spec.STTTarget,
		TTS:          spec.TTSTarget,
		LLM:          spec.LLMTarget,
		STS:          spec.STSTarget,
		Subagent:     spec.SubagentTarget,
		Voice:        spec.Voice,
		Instructions: spec.prompt(),
	}
	// The session's row can be deleted while the conversation stays, and this is what then
	// names the channel the call's transcript is in.
	call.ConversationID = spec.ConversationID
	// Who the agent writes the transcript as (chatlog.Options.Agent), which tells its lines
	// apart from a member's line carrying the same fields.
	call.AgentUserID = spec.UserID
	for _, skill := range created.skills.Skills {
		call.Skills = append(call.Skills, skill.Name)
	}
	if spec.Phone != nil {
		call.FromNumber = spec.Phone.Number
		call.ToNumber = spec.Phone.To
		// A vendor call id means this process placed the call rather than answered it.
		if spec.Phone.VendorCallID != "" {
			call.Direction = store.Outbound
		}
	}
	return call
}
