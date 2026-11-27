package api

import (
	"context"
	"encoding/json"
	"net/http"
)

// deletedMessage is the part of a message.deleted event the router acts on.
type deletedMessage struct {
	ChannelType string `json:"channel_type"`
	ChannelID   string `json:"channel_id"`
	// DeletedForMe is a message hidden from one user only, which deletes nothing.
	DeletedForMe bool `json:"deleted_for_me"`
	Message      struct {
		ID string `json:"id"`
		// ResentFrom is the message an activation sent this one again as a copy of. A hook
		// carries a message's custom fields at the top level of the message (messageEvent.Source),
		// and the v2 API under custom: either is read. Raw, so a value of another type, which
		// anyone writing in the app can set, costs only itself.
		ResentFrom json.RawMessage `json:"resent_from"`
		Custom     struct {
			ResentFrom json.RawMessage `json:"resent_from"`
		} `json:"custom"`
	} `json:"message"`
}

// resentFrom is the message the deleted one was a copy of, empty for none.
func (e deletedMessage) resentFrom() string {
	for _, raw := range []json.RawMessage{e.Message.ResentFrom, e.Message.Custom.ResentFrom} {
		var from string
		if json.Unmarshal(raw, &from) == nil && from != "" {
			return from
		}
	}
	return ""
}

// receiveDeletedMessage answers a message.deleted event, from Stream Chat's delivery or the
// message webhook: the router deletes the copies it keeps of the message, so a hidden branch
// deleted through Chat is neither shown nor sent again. Done on the request, not queued: a
// delete is one statement, and the 503 for a failed one has Stream deliver it again. A delivery
// is not checked for staleness either: Stream delivers one again for longer than a hook stays
// fresh, and deleting again deletes nothing.
func (s *Server) receiveDeletedMessage(ctx context.Context, w http.ResponseWriter, origin hookOrigin, body []byte) {
	var event deletedMessage
	if err := json.Unmarshal(body, &event); err != nil {
		writeError(w, invalidRequest("could not read that message.deleted event"))
		return
	}
	if event.DeletedForMe || event.Message.ID == "" || event.ChannelType == "" || event.ChannelID == "" || s.store == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	if origin.deployment && origin.app == 0 && s.stream != nil && s.stream.DeploymentAppKnowable() {
		// Scoped to no app, the delete would reach only unpinned sessions and leave the copies of
		// sessions pinned to the deployment's app: Stream retries, and by then the app is known.
		writeStreamWaiting(w)
		return
	}
	// Only the hook's customer's sessions, as for a message (hookOrigin.owns): the deployment's
	// own app is every customer's, and the app scope keeps it to the sessions written there.
	customer := ""
	if !origin.deployment {
		customer = origin.customer
	}
	conversation := event.ChannelType + ":" + event.ChannelID
	deleted, err := s.store.BranchMessages.DeleteMessage(ctx, origin.scope(), customer, conversation, event.Message.ID, event.resentFrom())
	if err != nil {
		s.logger.ErrorContext(ctx, "could not delete the branch copies of a deleted message", "channel", event.ChannelID,
			"message", event.Message.ID, "stream_app", origin.app, "error", err)
		writeError(w, unavailable("could not delete the copies of that message"))
		return
	}
	if deleted > 0 {
		s.logger.InfoContext(ctx, "deleted the branch copies of a deleted message", "channel", event.ChannelID,
			"message", event.Message.ID, "stream_app", origin.app, "copies", deleted)
	}
	w.WriteHeader(http.StatusOK)
}
