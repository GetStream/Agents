//go:build integration

package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// linqKey is what the suite's iMessage lines verify Linq's deliveries with.
var linqKey = []byte("suite-linq-signing-key")

// AI-3745: a message in a group chat is not answered in the sender's own conversation, which
// holds their history and reaches their connectors, and the group does not become where the
// sender's 1:1 messages, such as a scheduled task's, go. A delivery that does not say
// is_group false is a group's.
func (s *ChannelsSuite) TestAMessageInAGroupChatIsNotAnsweredAndKeepsThePersonsChat() {
	for i, isGroup := range []any{true, nil} {
		number, writer, oneToOne, group := s.number(), "+"+s.writer(), s.utils.uuid(), s.utils.uuid()
		line, config := s.iMessageLine(number)
		s.knownIn(config, writer, oneToOne)

		s.Require().Equal(http.StatusAccepted, s.deliverLinq(line, s.linqSays(group, writer, "what is on my calendar?", isGroup)),
			"is_group %v", isGroup)

		// Until the message is dropped, or answered, so a broken rule fails on what it did.
		s.Require().Eventually(func() bool {
			return strings.Count(s.logged.String(), `msg="not answering a message in a group chat" customer=`+s.customerID()) == i+1 ||
				len(s.meta.postedIn(group)) > 0
		}, settleFor, 50*time.Millisecond, "is_group %v", isGroup)
		held := s.contact(config, writer)
		s.Equal(oneToOne, held.Thread, "is_group %v: the 1:1 chat is still where their messages go", isGroup)
		s.Empty(held.ConversationID, "is_group %v: no conversation of theirs was opened", isGroup)
		s.Empty(s.meta.postedIn(group), "is_group %v: nothing was posted to the group", isGroup)
	}
}

// The control: a 1:1 chat with is_group false is answered, and becomes where the person's
// messages go, as on the base.
func (s *ChannelsSuite) TestAMessageInAOneToOneChatIsAnsweredAndMovesThePersonsChat() {
	number, writer, before, now := s.number(), "+"+s.writer(), s.utils.uuid(), s.utils.uuid()
	line, config := s.iMessageLine(number)
	s.knownIn(config, writer, before)

	s.Require().Equal(http.StatusAccepted, s.deliverLinq(line, s.linqSays(now, writer, "what is on my calendar?", false)))

	s.Require().Eventually(func() bool { return len(s.meta.postedIn(now)) > 0 }, settleFor, 50*time.Millisecond)
	s.Equal([]string{"Noted."}, s.meta.postedIn(now))
	held := s.contact(config, writer)
	s.Equal(now, held.Thread)
	s.NotEmpty(held.ConversationID)
}

// A STOP in a group is still recorded and confirmed there, the one message the sender is owed,
// as the channel bridge does (AI-2015).
func (s *ChannelsSuite) TestAStopInAGroupChatIsRecordedAndConfirmedThere() {
	number, writer, oneToOne, group := s.number(), "+"+s.writer(), s.utils.uuid(), s.utils.uuid()
	line, config := s.iMessageLine(number)
	s.knownIn(config, writer, oneToOne)

	s.Require().Equal(http.StatusAccepted, s.deliverLinq(line, s.linqSays(group, writer, "STOP", true)))

	s.Require().Eventually(func() bool { return len(s.meta.postedIn(group)) > 0 }, settleFor, 50*time.Millisecond)
	s.Contains(s.meta.postedIn(group)[0], "unsubscribed")
	blocked, err := s.store.OptOuts.OptedOut(context.Background(), s.customerID(), writer, string(channels.IMessage))
	s.Require().NoError(err)
	s.True(blocked)
	s.Equal(oneToOne, s.contact(config, writer).Thread)
}

// iMessageLine is a Linq line on number and a text agent answering on it, which knows a
// number only once its person linked it.
func (s *ChannelsSuite) iMessageLine(number string) (ChannelAccount, AgentConfig) {
	var line ChannelAccount
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels",
		ConnectChannelRequest{
			Kind: string(channels.IMessage), Number: number, Token: pointerTo("linq-key"),
			Signing: pointerTo("whsec_" + base64.StdEncoding.EncodeToString(linqKey)),
		}, &line))
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name: "channelled-" + s.utils.uuid(), Mode: pointerTo(AgentModeText), Llm: pointerTo("noted/noted-model"),
		Channels: &AgentChannels{Imessage: &ChannelLineRequest{Number: number},
			Identity: pointerTo(ChannelIdentity(store.ChannelIdentityLink))},
	}, &config))
	return line, config
}

// knownIn records writer as a user who linked their number and last wrote in chat.
func (s *ChannelsSuite) knownIn(config AgentConfig, writer, chat string) {
	s.Require().NoError(s.store.ContactMapEntries.Upsert(context.Background(), &store.ContactMapEntry{
		CustomerID: s.customerID(), ConfigID: config.Id, Kind: string(channels.IMessage), Address: writer,
		Thread: chat, UserID: s.data.createUser().userID,
	}))
}

// contact is what the contact map holds for writer.
func (s *ChannelsSuite) contact(config AgentConfig, writer string) store.ContactMapEntry {
	held, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id,
		string(channels.IMessage), writer)
	s.Require().NoError(err)
	return held
}

// linqSays is a message.received from writer in chat whose data.chat.is_group is isGroup, or
// which has none when isGroup is nil.
func (s *ChannelsSuite) linqSays(chat, writer, text string, isGroup any) []byte {
	about := map[string]any{"id": chat, "owner_handle": map[string]any{"handle": "+12025550100"}}
	if isGroup != nil {
		about["is_group"] = isGroup
	}
	raw, err := json.Marshal(map[string]any{"event_type": "message.received", "data": map[string]any{
		"id": "msg-" + s.utils.uuid(), "direction": "inbound", "chat": about,
		"sender_handle": map[string]any{"handle": writer},
		"parts":         []any{map[string]any{"type": "text", "value": text}},
	}})
	s.Require().NoError(err)
	return raw
}

// deliverLinq posts body to a line's webhook, signed as Linq signs it (Standard Webhooks).
func (s *ChannelsSuite) deliverLinq(line ChannelAccount, body []byte) int {
	id, stamp := "msg_"+s.utils.uuid(), strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, linqKey)
	mac.Write([]byte(id + "." + stamp + "."))
	mac.Write(body)
	header := http.Header{}
	header.Set("Webhook-Id", id)
	header.Set("Webhook-Timestamp", stamp)
	header.Set("Webhook-Signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	status, _ := s.post(line.WebhookUrl, header, body)
	return status
}
