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
	} `json:"message"`
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
	// Only the hook's customer's sessions, as for a message (hookOrigin.owns): the deployment's
	// own app is every customer's, and the app scope keeps it to the sessions written there.
	customer := ""
	if !origin.deployment {
		customer = origin.customer
	}
	conversation := event.ChannelType + ":" + event.ChannelID
	deleted, err := s.store.BranchMessages.DeleteMessage(ctx, origin.scope(), customer, conversation, event.Message.ID)
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
