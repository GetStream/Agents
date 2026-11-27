//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// appSecret is what the suite's WhatsApp lines are signed with.
const appSecret = "suite-app-secret"

// ChannelsSuite covers a channel end to end: the app connects a line, an agent names its
// number, and a message WhatsApp delivers is answered back over WhatsApp. Meta's Cloud API
// is stood in for by metaAPI, which keeps what it was sent.
type ChannelsSuite struct {
	RouterSuite
	meta *metaAPI
	// logged is what the router logged, at debug and up.
	logged *lockedLog
}

func TestChannelsSuite(t *testing.T) {
	runSuite(t, new(ChannelsSuite))
}

func (s *ChannelsSuite) SetupSuite() {
	s.meta = &metaAPI{}
	// The organization's money is read from a usage service of the suite's own, so the gate refuses
	// as it does an organization that may not pay.
	s.limited = true
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.daytona = daytonatest.Start(s.T())
	s.channelAPI = httptest.NewTLSServer(http.HandlerFunc(s.meta.serve))
	s.T().Cleanup(s.channelAPI.Close)
	s.RouterSuite.SetupSuite()
}

func (s *ChannelsSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.usage.Credit(s.T(), s.app.organization.ID, 10_000_000)
}

func (s *ChannelsSuite) TestConnectingALineAnswersWithWhereToDeliver() {
	line := s.connect(s.number())

	s.Equal(ChannelKind(channels.WhatsApp), line.Kind)
	s.Contains(line.WebhookUrl, channels.HookPath)
	s.False(line.Delivering, "Meta's webhook is set where the app lives, not from here")
}

// The credentials are sent once and never read back, the way a password is.
func (s *ChannelsSuite) TestTheCredentialsAreNeverShownAgain() {
	number := s.number()
	s.connect(number)

	var listed []ChannelAccount
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/channels", nil, &listed))

	shown, found := "", false
	for _, line := range listed {
		if line.Number == number {
			raw, err := json.Marshal(line)
			s.Require().NoError(err)
			shown, found = string(raw), true
		}
	}
	s.Require().True(found)
	s.NotContains(shown, "meta-token")
	s.NotContains(shown, appSecret)
}

func (s *ChannelsSuite) TestTheCredentialsAreSealedInTheDatabase() {
	number := s.number()
	s.connect(number)

	stored, err := s.store.ChannelAccounts.GetByNumber(context.Background(), s.customerID(), "whatsapp", number)
	s.Require().NoError(err)
	s.NotContains(string(stored.SecretsSealed), "meta-token")
	s.Positive(stored.SecretsKEKVersion)
}

// Catches connect sealing a line in a form the previous release cannot open, which breaks a
// rollback and the old pods of a rolling deploy: what it stored opens the way that release
// opens a line, bound to the customer only.
func (s *ChannelsSuite) TestTheCredentialsOpenAsThePreviousReleaseOpensThem() {
	number := s.number()
	s.connect(number)

	stored, err := s.store.ChannelAccounts.GetByNumber(context.Background(), s.customerID(), "whatsapp", number)
	s.Require().NoError(err)
	raw, err := s.sealer.OpenWithAADVersion(stored.SecretsSealed, []byte(s.customerID()), stored.SecretsKEKVersion)
	s.Require().NoError(err)
	var line channels.Secrets
	s.Require().NoError(json.Unmarshal([]byte(raw), &line))
	s.Equal("meta-token", line.Token)
}

// Rotating a token must not mean setting the webhook up with Meta all over again.
func (s *ChannelsSuite) TestReconnectingALineKeepsItsWebhookURL() {
	number := s.number()
	first := s.connect(number)

	again := s.connect(number)

	s.Equal(first.WebhookUrl, again.WebhookUrl)
	s.Equal(first.Id, again.Id)
}

func (s *ChannelsSuite) TestAnAgentNamingANumberNobodyConnectedIsRefused() {
	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name:     "channelled-" + s.utils.uuid(),
		Mode:     pointerTo(AgentModeText),
		Llm:      pointerTo("llm-flow"),
		Channels: &AgentChannels{Whatsapp: &ChannelLineRequest{Number: s.number()}},
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(body), "has not connected")
}

// A line that could not be looked up is the router's failure, not a number for the caller to fix.
func (s *ChannelsSuite) TestALineThatCannotBeLookedUpIsAnInternalError() {
	number := s.number()
	s.connect(number)
	var plain AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name: "channelled-" + s.utils.uuid(), Mode: pointerTo(AgentModeText), Llm: pointerTo("llm-flow"),
	}, &plain))
	s.failQueries(number)

	line := map[string]any{"whatsapp": map[string]any{"number": number}}
	for name, call := range map[string]func() (int, []byte){
		"create": func() (int, []byte) {
			return s.serverClient.call(http.MethodPost, "/v1/agents/configs", map[string]any{
				"name": "channelled-" + s.utils.uuid(), "mode": "text", "llm": "llm-flow", "channels": line})
		},
		"update": func() (int, []byte) {
			return s.serverClient.call(http.MethodPut, "/v1/agents/configs/"+plain.Id, map[string]any{
				"name": plain.Name, "mode": "text", "llm": "llm-flow", "channels": line})
		},
		"patch": func() (int, []byte) {
			return s.serverClient.call(http.MethodPatch, "/v1/agents/configs/"+plain.Id, map[string]any{"channels": line})
		},
		"sync": func() (int, []byte) {
			return s.serverClient.call(http.MethodPost, "/v1/agents/sync", map[string]any{
				"name": "channelled-" + s.utils.uuid(), "hash": "v1", "mode": "text", "channels": line})
		},
	} {
		status, body := call()

		s.Equal(http.StatusInternalServerError, status, name)
		s.Contains(string(body), `"code":"internal_error"`, name)
		s.NotContains(string(body), "store:", name)
		s.NotContains(string(body), "deadline", name)
	}
}

// A provider can take days to approve a number, and the directory naming it should still sync.
func (s *ChannelsSuite) TestASyncNamingANumberNobodyConnectedIsStoredWithAWarning() {
	number := s.number()

	var synced SyncAgentResult
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/sync", map[string]any{
		"name": "channelled-" + s.utils.uuid(), "hash": "v1", "mode": "text",
		"channels": map[string]any{"whatsapp": map[string]any{"number": number}},
	}, &synced))

	s.Require().Len(synced.Warnings, 1)
	s.Contains(synced.Warnings[0], "has not connected "+number)
	s.Require().NotNil(synced.Config.Channels)
	s.Equal(number, synced.Config.Channels.Whatsapp.Number)
}

// Two agents on one number would both answer every message to it.
func (s *ChannelsSuite) TestANumberAnotherAgentAlreadyAnswersOnIsRefused() {
	number := s.number()
	s.connect(number)
	first := s.agentOn(number, "")

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name:     "channelled-" + s.utils.uuid(),
		Mode:     pointerTo(AgentModeText),
		Llm:      pointerTo("llm-flow"),
		Channels: &AgentChannels{Whatsapp: &ChannelLineRequest{Number: number}},
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(body), first.Name)
}

func (s *ChannelsSuite) TestTheSameAgentMayBeSavedAgainOnItsOwnNumber() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, "")

	status := s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+config.Id,
		AgentConfigPatch{Greeting: &Greeting{Text: "Hello there"}}, nil)

	s.Equal(http.StatusOK, status)
}

// A line has to be one the router can bill and configure, which an SMS number only is when
// the app bought it here.
func (s *ChannelsSuite) TestATextLineOnANumberTheAppDoesNotHoldIsRefused() {
	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/channels", ConnectChannelRequest{
		Kind:    string(channels.SMS),
		Number:  s.number(),
		Token:   pointerTo("telnyx-key"),
		Signing: pointerTo("a-public-key"),
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(body), "holds no number")
}

func (s *ChannelsSuite) TestAChannelNobodyCarriesIsRefused() {
	status := s.serverClient.do(http.MethodPost, "/v1/agents/channels", ConnectChannelRequest{
		Kind: "telegram", Number: s.number(), Token: pointerTo("a-token"),
	}, nil)

	s.Equal(http.StatusBadRequest, status)
}

func (s *ChannelsSuite) TestOnlyTheAppsOwnBackendMayConnectALine() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/channels", ConnectChannelRequest{
			Kind: string(channels.WhatsApp), Number: s.number(),
			AccountId: pointerTo("1345390685325117"), Token: pointerTo("meta-token"),
			Signing: pointerTo(appSecret), Challenge: pointerTo("pick-anything"),
		}, nil)
	})
}

func (s *ChannelsSuite) TestALineOfAnotherAppIsNotFound() {
	line := s.connect(s.number())

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodDelete, "/v1/agents/channels/"+line.Id, nil, nil)
	})
}

// What Meta asks for before it will deliver: the verify token back, with its challenge.
func (s *ChannelsSuite) TestMetasWebhookCheckIsAnsweredWithItsChallenge() {
	line := s.connect(s.number())

	status, body := s.get(line.WebhookUrl + "?hub.mode=subscribe&hub.verify_token=" +
		url.QueryEscape(s.challenge()) + "&hub.challenge=1158201444")

	s.Equal(http.StatusOK, status)
	s.Equal("1158201444", string(body))
}

// Meta is the one provider that checks a URL with a GET, so a line of another kind agrees to
// none, whatever verify token it was connected with.
func (s *ChannelsSuite) TestAWebhookCheckOfALineThatIsNotWhatsAppsIsRefused() {
	for _, challenge := range []string{"", s.challenge()} {
		var line ChannelAccount
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels",
			ConnectChannelRequest{
				Kind: string(channels.IMessage), Number: s.number(), Token: pointerTo("linq-key"),
				Signing: pointerTo("whsec_bGlucQ=="), Challenge: pointerTo(challenge),
			}, &line))

		status, body := s.get(line.WebhookUrl + "?hub.mode=subscribe&hub.verify_token=" +
			url.QueryEscape(challenge) + "&hub.challenge=1158201444")

		s.Equal(http.StatusForbidden, status, "verify token %q", challenge)
		s.NotContains(string(body), "1158201444", "verify token %q", challenge)
	}
}

func (s *ChannelsSuite) TestAWebhookCheckWithTheWrongTokenIsRefused() {
	line := s.connect(s.number())

	status, _ := s.get(line.WebhookUrl + "?hub.mode=subscribe&hub.verify_token=guessed&hub.challenge=1158201444")

	s.Equal(http.StatusForbidden, status)
}

func (s *ChannelsSuite) TestAMessageIsAnsweredBackOnWhatsApp() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer := s.writer()

	s.Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))

	s.Equal("Noted.", s.answer(writer))
}

// A file the code the agent ran made is sent back with its words.
func (s *ChannelsSuite) TestAFileTheAgentsCodeMadeIsSentBack() {
	number := s.number()
	line := s.connect(number)
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name:     "channelled-" + s.utils.uuid(),
		Mode:     pointerTo(AgentModeText),
		Llm:      pointerTo("scheduling/scheduling-model"),
		Sandbox:  pointerTo(Daytona),
		Channels: &AgentChannels{Whatsapp: &ChannelLineRequest{Number: number}},
	}, nil))
	report := s.codeHandsBackAReport()
	writer := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "how did I walk?"))

	s.Require().Eventually(func() bool { return len(s.meta.documentsTo(writer)) > 0 },
		settleFor, 50*time.Millisecond, "no file was sent to %s", writer)
	s.Equal([]string{report}, s.meta.documentsTo(writer))
	s.Equal("Done.", s.answer(writer))
}

// A conversation of the writer's own is what makes the agent's memory of them theirs, and it
// is in Stream Chat like any other, so the dashboard shows it.
func (s *ChannelsSuite) TestTheConversationIsKeptForTheNumberThatWrote() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, "")
	writer := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))
	s.Require().NotEmpty(s.answer(writer))

	held, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+writer)
	s.Require().NoError(err)
	s.Equal("phone:+"+writer, held.UserID)
	s.NotEmpty(held.ConversationID, "the next message carries this one on")
}

func (s *ChannelsSuite) TestASecondMessageCarriesOnTheSameConversation() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, "")
	writer := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))
	s.Require().NotEmpty(s.answer(writer))
	first, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+writer)
	s.Require().NoError(err)

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "and now?"))
	s.Eventually(func() bool { return len(s.meta.to(writer)) == 2 }, settleFor, 50*time.Millisecond)

	second, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+writer)
	s.Require().NoError(err)
	s.Equal(first.ConversationID, second.ConversationID)
}

// A provider that did not hear back in time sends the same message again, and the agent
// answering it twice is the agent talking over itself.
func (s *ChannelsSuite) TestADeliveryRetriedIsAnsweredOnce() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "what is open?"))
	s.Require().NotEmpty(s.answer(writer))
	s.Equal(http.StatusAccepted, s.deliver(line, writer, id, "what is open?"))

	time.Sleep(time.Second)
	s.Len(s.meta.to(writer), 1)
}

// STOP is the carrier's word, not the agent's: it is obeyed before any agent reads it.
func (s *ChannelsSuite) TestSomebodyWhoTextsStopIsNotWrittenToAgain() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "stop"))
	s.Contains(s.answer(writer), "unsubscribed", "the confirmation is the one message they are still owed")
	optedOut, err := s.store.OptOuts.OptedOut(context.Background(), s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut)

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))
	time.Sleep(time.Second)
	s.Len(s.meta.to(writer), 1)
}

// A STOP the store could not record fails its delivery and is taken when the provider delivers
// it again; once taken, a further delivery of it is neither recorded nor answered again. A
// trigger refuses the app's opt-outs for the first delivery.
func (s *ChannelsSuite) TestAStopTheStoreRefusedIsObeyedWhenDeliveredAgain() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()
	ctx := context.Background()
	_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
		CREATE FUNCTION channels_suite_refuse_opt_out() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'refused by the test'; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER channels_suite_refuse_opt_out BEFORE INSERT ON opt_outs FOR EACH ROW
			WHEN (NEW.customer_id = '`+s.customerID()+`') EXECUTE FUNCTION channels_suite_refuse_opt_out();
		COMMIT`)
	s.Require().NoError(err)
	refusing := true
	allow := func() {
		if !refusing {
			return
		}
		refusing = false
		_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
			DROP TRIGGER channels_suite_refuse_opt_out ON opt_outs;
			DROP FUNCTION channels_suite_refuse_opt_out();
			COMMIT`)
		s.Require().NoError(err)
	}
	defer allow()

	body := s.delivery(writer, id, "Stop")
	status, answered := s.post(line.WebhookUrl, s.signed(body), body)
	s.Equal(http.StatusInternalServerError, status, "a STOP not recorded is the provider's to deliver again")
	s.Contains(string(answered), "something went wrong")
	s.NotContains(string(answered), "refused by the test", "the store's error stays in the log")
	optedOut, err := s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.False(optedOut)
	allow()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "Stop"))
	optedOut, err = s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut, "the STOP delivered again is recorded")
	s.Contains(s.answer(writer), "unsubscribed")

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "Stop"))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "help"))
	s.Equal("Reply STOP to unsubscribe.", s.reply(writer, 2))
	optedOut, err = s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut, "HELP lifts no opt-out")
	s.Len(s.meta.to(writer), 2, "the STOP is confirmed once, however often it is delivered")
}

// A provider that hangs up on a slow delivery and delivers it again still leaves the person
// opted out, confirmed once: the hang-up does not cut the write, so the delivery hung up on
// records the opt-out itself, and the second is answered as already taken. A trigger
// makes the app's opt-out slower than the provider waits.
func (s *ChannelsSuite) TestAStopWhoseDeliveryWasHungUpOnIsObeyed() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()
	ctx := context.Background()
	_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
		CREATE FUNCTION channels_suite_slow_opt_out() RETURNS trigger AS $$
			BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER channels_suite_slow_opt_out BEFORE INSERT ON opt_outs FOR EACH ROW
			WHEN (NEW.customer_id = '`+s.customerID()+`') EXECUTE FUNCTION channels_suite_slow_opt_out();
		COMMIT`)
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
			DROP TRIGGER channels_suite_slow_opt_out ON opt_outs;
			DROP FUNCTION channels_suite_slow_opt_out();
			COMMIT`)
		s.Require().NoError(err)
	}()

	body := s.delivery(writer, id, "Stop")
	request, err := http.NewRequest(http.MethodPost, s.server.URL+line.WebhookUrl, bytes.NewReader(body))
	s.Require().NoError(err)
	request.Header = s.signed(body)
	request.Header.Set("Content-Type", "application/json")
	impatient := &http.Client{Timeout: 200 * time.Millisecond}
	_, err = impatient.Do(request)
	s.Require().Error(err, "the provider hangs up before the opt-out is stored")
	s.Require().Eventually(func() bool {
		optedOut, err := s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
		s.Require().NoError(err)
		return optedOut
	}, settleFor, 50*time.Millisecond, "the STOP hung up on is recorded by its own delivery")
	s.Contains(s.answer(writer), "unsubscribed")

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "Stop"))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "help"))
	s.Equal("Reply STOP to unsubscribe.", s.reply(writer, 2))
	s.Len(s.meta.to(writer), 2, "the STOP is confirmed once")
}

// A START the store could not record fails its delivery, leaving the person opted out, and
// is taken when the provider delivers it again. A trigger refuses lifting the app's opt-outs
// for the first delivery.
func (s *ChannelsSuite) TestAStartTheStoreRefusedIsObeyedWhenDeliveredAgain() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()
	ctx := context.Background()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "STOP"))
	s.Require().Contains(s.answer(writer), "unsubscribed")
	_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
		CREATE FUNCTION channels_suite_refuse_opt_in() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'refused by the test'; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER channels_suite_refuse_opt_in BEFORE UPDATE ON opt_outs FOR EACH ROW
			WHEN (NEW.customer_id = '`+s.customerID()+`') EXECUTE FUNCTION channels_suite_refuse_opt_in();
		COMMIT`)
	s.Require().NoError(err)
	refusing := true
	allow := func() {
		if !refusing {
			return
		}
		refusing = false
		_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
			DROP TRIGGER channels_suite_refuse_opt_in ON opt_outs;
			DROP FUNCTION channels_suite_refuse_opt_in();
			COMMIT`)
		s.Require().NoError(err)
	}
	defer allow()

	body := s.delivery(writer, id, "START")
	status, answered := s.post(line.WebhookUrl, s.signed(body), body)
	s.Equal(http.StatusInternalServerError, status, "a START not recorded is the provider's to deliver again")
	s.Contains(string(answered), "something went wrong")
	s.NotContains(string(answered), "refused by the test", "the store's error stays in the log")
	optedOut, err := s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut)
	s.Contains(s.logged.String(), `level=ERROR msg="could not record a keyword" keyword=START channel=whatsapp`)
	allow()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "START"))
	optedOut, err = s.store.OptOuts.OptedOut(ctx, s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.False(optedOut, "the START delivered again is recorded")
	s.Equal("You are subscribed again. Reply STOP to unsubscribe.", s.reply(writer, 2))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, id, "START"))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "help"))
	s.Equal("Reply STOP to unsubscribe.", s.reply(writer, 3))
	s.Len(s.meta.to(writer), 3, "the START is confirmed once, however often it is delivered")
}

// A HELP or an ordinary message the store could not claim fails its delivery with a 500 the
// provider retries, saying nothing of the store, whose error goes to the log. A trigger
// refuses the line's deliveries.
func (s *ChannelsSuite) TestAMessageTheStoreCouldNotClaimIsA500WithoutTheStoresWords() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	ctx := context.Background()
	_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
		CREATE FUNCTION channels_suite_refuse_delivery() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'refused by the test'; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER channels_suite_refuse_delivery BEFORE INSERT ON channel_deliveries FOR EACH ROW
			WHEN (NEW.account_id = '`+line.Id+`') EXECUTE FUNCTION channels_suite_refuse_delivery();
		COMMIT`)
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.GlobalDB().ExecContext(ctx, `BEGIN;
			DROP TRIGGER channels_suite_refuse_delivery ON channel_deliveries;
			DROP FUNCTION channels_suite_refuse_delivery();
			COMMIT`)
		s.Require().NoError(err)
	}()

	for _, text := range []string{"HELP", "what time do you open?"} {
		writer := s.writer()
		body := s.delivery(writer, "msg-"+s.utils.uuid(), text)
		status, answered := s.post(line.WebhookUrl, s.signed(body), body)
		s.Equal(http.StatusInternalServerError, status, "%q not claimed is the provider's to deliver again", text)
		s.Contains(string(answered), "something went wrong", text)
		s.NotContains(string(answered), "refused by the test", text)
		s.NotContains(string(answered), "claim channel message", text)
		s.Empty(s.meta.to(writer), "%q not claimed is not answered", text)
	}
	s.Contains(s.logged.String(), `level=ERROR msg="could not claim a channel delivery" app_id=`+s.customerID())
	s.Contains(s.logged.String(), "refused by the test", "what the store said is kept in the log")
}

// A delivery of a STOP racing a slow first one waits for the first to record the opt-out, and
// both are answered as taken: the person is opted out and confirmed once.
func (s *ChannelsSuite) TestARedeliveryRacingASlowStopWaitsForIt() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()
	hold, holder := s.holdOptOuts("channels_suite_held_opt_out", false)

	first := s.postLater(line, s.delivery(writer, id, "Stop"))
	recording := s.waitingOn(holder)
	second := s.postLater(line, s.delivery(writer, id, "Stop"))
	s.waitingOn(recording)
	s.Require().NoError(hold.Rollback())

	s.Equal(http.StatusAccepted, (<-first).status)
	s.Equal(http.StatusAccepted, (<-second).status, "the redelivery is answered as taken once the first is recorded")
	optedOut, err := s.store.OptOuts.OptedOut(context.Background(), s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut)
	s.Contains(s.answer(writer), "unsubscribed")
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "help"))
	s.Equal("Reply STOP to unsubscribe.", s.reply(writer, 2))
	s.Len(s.meta.to(writer), 2, "the STOP is confirmed once")
}

// A delivery of a STOP racing a slow first one whose write then fails takes the message
// itself: the first is the provider's to deliver again, the second records the opt-out, and the
// person is confirmed once.
func (s *ChannelsSuite) TestARedeliveryRacingAStopThatFailsRecordsIt() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer, id := s.writer(), "msg-"+s.utils.uuid()
	hold, holder := s.holdOptOuts("channels_suite_held_opt_out", true)

	first := s.postLater(line, s.delivery(writer, id, "Stop"))
	recording := s.waitingOn(holder)
	second := s.postLater(line, s.delivery(writer, id, "Stop"))
	s.waitingOn(recording)
	s.Require().NoError(hold.Rollback())

	failed := <-first
	s.Equal(http.StatusInternalServerError, failed.status)
	s.Contains(string(failed.body), "something went wrong")
	s.Equal(http.StatusAccepted, (<-second).status)
	optedOut, err := s.store.OptOuts.OptedOut(context.Background(), s.customerID(), "+"+writer, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut, "the redelivery recorded the STOP the first could not")
	s.Contains(s.answer(writer), "unsubscribed")
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "help"))
	s.Equal("Reply STOP to unsubscribe.", s.reply(writer, 2))
	s.Len(s.meta.to(writer), 2, "the STOP is confirmed once")
}

func (s *ChannelsSuite) TestTextingStartAfterStopIsAnsweredAgain() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer := s.writer()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "STOP"))
	s.Require().NotEmpty(s.answer(writer))

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "START"))
	s.Eventually(func() bool { return len(s.meta.to(writer)) == 2 }, settleFor, 50*time.Millisecond)
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))

	s.Eventually(func() bool { return len(s.meta.to(writer)) == 3 }, settleFor, 50*time.Millisecond)
	s.Equal("Noted.", s.meta.to(writer)[2])
}

func (s *ChannelsSuite) TestADeliveryNotSignedWithTheLinesSecretIsRefused() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	body := s.delivery(s.writer(), "msg-"+s.utils.uuid(), "what is open?")

	header := http.Header{}
	header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString([]byte("guessed")))
	status, _ := s.post(line.WebhookUrl, header, body)

	s.Equal(http.StatusUnauthorized, status)
}

func (s *ChannelsSuite) TestADeliveryToALineNobodyConnectedIsGone() {
	status, _ := s.post(channels.HookPath+"never-connected", http.Header{}, []byte(`{}`))

	s.Equal(http.StatusGone, status)
}

func (s *ChannelsSuite) TestADisconnectedLineIsNotDeliveredToAgain() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")

	s.Require().Equal(http.StatusNoContent,
		s.serverClient.do(http.MethodDelete, "/v1/agents/channels/"+line.Id, nil, nil))

	status, _ := s.post(line.WebhookUrl, s.signed(s.delivery(s.writer(), "msg-x", "hello")),
		s.delivery(s.writer(), "msg-x", "hello"))
	s.Equal(http.StatusGone, status)
}

// An agent that reads somebody's own calendar has to know whose number is writing, and a
// phone number proves nothing by itself.
func (s *ChannelsSuite) TestANumberIsAskedForACodeUntilSomebodyClaimsIt() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, store.ChannelIdentityLink)
	writer := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))

	s.Contains(s.answer(writer), "code")
}

func (s *ChannelsSuite) TestTheNumberThatTextsACodeBecomesThatEndUser() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	owner := s.utils.uuid()
	writer := s.writer()

	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: owner}, &minted))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), minted.Code))
	s.Require().NotEmpty(s.answer(writer))

	held, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+writer)
	s.Require().NoError(err)
	s.Equal(owner, held.UserID)
}

// A code is never minted for a user id Stream keeps for itself.
func (s *ChannelsSuite) TestACodeIsNotMintedForAReservedUserID() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: "!anon"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "user_id may not start with !")
}

// A code somebody saw go by must not claim a second number.
func (s *ChannelsSuite) TestACodeAlreadySpentClaimsNothingMore() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	writer, other := s.writer(), s.writer()

	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &minted))
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), minted.Code))
	s.Require().NotEmpty(s.answer(writer))

	s.Require().Equal(http.StatusAccepted, s.deliver(line, other, "msg-"+s.utils.uuid(), minted.Code))

	s.Contains(s.answer(other), "code")
	_, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+other)
	s.ErrorIs(err, store.ErrNoContact)
}

// A code texted to another agent's line of the app is answered as a code nobody was given, so a
// guess cannot tell a live code from a dead one, and is not spent: the number still claims it on
// the line of the agent it was minted for.
func (s *ChannelsSuite) TestACodeTextedToAnotherAgentsLineStillClaimsTheNumberOnItsOwn() {
	ownNumber, otherNumber := s.number(), s.number()
	own, other := s.connect(ownNumber), s.connect(otherNumber)
	config := s.agentOn(ownNumber, store.ChannelIdentityLink)
	otherConfig := s.agentOn(otherNumber, store.ChannelIdentityLink)
	owner := s.utils.uuid()
	writer := s.writer()
	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: owner}, &minted))
	s.Regexp(`^[0-9]{6}$`, minted.Code)

	s.Require().Equal(http.StatusAccepted, s.deliver(other, writer, "msg-"+s.utils.uuid(), "no such code"))
	unknown := s.reply(writer, 1)
	s.Require().Equal(http.StatusAccepted, s.deliver(other, writer, "msg-"+s.utils.uuid(), minted.Code))
	s.Equal(unknown, s.reply(writer, 2))
	_, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), otherConfig.Id, "whatsapp", "+"+writer)
	s.Require().ErrorIs(err, store.ErrNoContact)

	s.Require().Equal(http.StatusAccepted, s.deliver(own, writer, "msg-"+s.utils.uuid(), minted.Code))
	s.Equal("This number is yours now. What can I do for you?", s.reply(writer, 3))
	held, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+writer)
	s.Require().NoError(err)
	s.Equal(owner, held.UserID)
}

// A guess that hits another agent's live code counts as a wrong one, as any other guess does.
func (s *ChannelsSuite) TestCodesOfAnotherAgentCountAsWrongCodes() {
	ownNumber, otherNumber := s.number(), s.number()
	s.connect(ownNumber)
	other := s.connect(otherNumber)
	config := s.agentOn(ownNumber, store.ChannelIdentityLink)
	s.agentOn(otherNumber, store.ChannelIdentityLink)
	guesser := s.writer()
	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &minted))
	for n := range 5 {
		s.Require().Equal(http.StatusAccepted, s.deliver(other, guesser, "msg-"+s.utils.uuid(), minted.Code))
		s.Contains(s.reply(guesser, n+1), "code")
	}

	s.Require().Equal(http.StatusAccepted, s.deliver(other, guesser, "msg-"+s.utils.uuid(), "no such code"))

	s.Contains(s.reply(guesser, 6), "Too many codes")
}

// A code is six digits, so a number that may text guesses without end finds one.
func (s *ChannelsSuite) TestANumberThatTextsWrongCodesCannotClaimEvenTheRightOne() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	guesser, owner := s.writer(), s.writer()
	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &minted))
	for n := range 5 {
		s.Require().Equal(http.StatusAccepted, s.deliver(line, guesser, "msg-"+s.utils.uuid(), "guess "+strconv.Itoa(n)))
		s.Contains(s.reply(guesser, n+1), "code")
	}

	s.Require().Equal(http.StatusAccepted, s.deliver(line, guesser, "msg-"+s.utils.uuid(), minted.Code))

	s.Contains(s.reply(guesser, 6), "Too many codes")
	_, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+guesser)
	s.ErrorIs(err, store.ErrNoContact)
	s.Require().Equal(http.StatusAccepted, s.deliver(line, owner, "msg-"+s.utils.uuid(), minted.Code))
	s.NotContains(s.reply(owner, 1), "Too many codes", "another number is not held to the guesser's count")
	_, err = s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+owner)
	s.NoError(err)
}

// Guessing from a new number each time is held to a count of the whole line.
func (s *ChannelsSuite) TestNumbersGuessingCodesOnOneLineTogetherCannotClaimOne() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &minted))
	for n := range 20 {
		guesser := s.writer()
		s.Require().Equal(http.StatusAccepted, s.deliver(line, guesser, "msg-"+s.utils.uuid(), "guess "+strconv.Itoa(n)))
		s.Contains(s.reply(guesser, 1), "code")
	}
	last := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, last, "msg-"+s.utils.uuid(), minted.Code))

	s.Contains(s.reply(last, 1), "Too many codes")
	_, err := s.store.ContactMapEntries.GetByAddress(context.Background(), s.customerID(), config.Id, "whatsapp", "+"+last)
	s.ErrorIs(err, store.ErrNoContact)
}

// Wrong codes texted at the same moment from many numbers are held to the line's count too:
// none of them may be tried on a count that the others have not yet added to.
func (s *ChannelsSuite) TestWrongCodesTextedAtOnceFromManyNumbersAreTriedNoMoreThanTheLineAllows() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, store.ChannelIdentityLink)
	guessers := make([]string, 60)
	for n := range guessers {
		guessers[n] = s.writer()
	}
	s.awayFromAWindowEdge()

	s.deliverAtOnce(line, guessers...)

	tried := 0
	for _, guesser := range guessers {
		if s.reply(guesser, 1) == tooManyCodes {
			continue
		}
		s.Equal(sendTheCode, s.reply(guesser, 1))
		tried++
	}
	s.Equal(20, tried, "the line's count of wrong codes")
}

// Wrong codes one number texts at the same moment are held to that number's count.
func (s *ChannelsSuite) TestWrongCodesTextedAtOnceFromOneNumberAreTriedNoMoreThanItsCount() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, store.ChannelIdentityLink)
	guesser := s.writer()
	s.awayFromAWindowEdge()

	s.deliverAtOnce(line, slices.Repeat([]string{guesser}, 10)...)

	s.reply(guesser, 10)
	s.Equal(append(slices.Repeat([]string{sendTheCode}, 5), slices.Repeat([]string{tooManyCodes}, 5)...),
		s.meta.to(guesser), "five tried, then the number is refused")
}

// A code that ties a number to somebody is no wrong code: it leaves the line as many wrong codes
// as it had before.
func (s *ChannelsSuite) TestAClaimedCodeLeavesTheLineItsCountOfWrongCodes() {
	number := s.number()
	line := s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	var minted ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &minted))
	guessers := make([]string, 19)
	for n := range guessers {
		guessers[n] = s.writer()
	}
	s.awayFromAWindowEdge()
	s.deliverAtOnce(line, guessers...)
	for _, guesser := range guessers {
		s.Require().Equal(sendTheCode, s.reply(guesser, 1))
	}
	owner := s.writer()

	s.Require().Equal(http.StatusAccepted, s.deliver(line, owner, "msg-"+s.utils.uuid(), minted.Code))

	s.Equal("This number is yours now. What can I do for you?", s.reply(owner, 1), "under the count, a code is claimed")
	twentieth, last := s.writer(), s.writer()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, twentieth, "msg-"+s.utils.uuid(), "no such code"))
	s.Equal(sendTheCode, s.reply(twentieth, 1), "the claim took none of the twenty")
	s.Require().Equal(http.StatusAccepted, s.deliver(line, last, "msg-"+s.utils.uuid(), "no such code"))
	s.Equal(tooManyCodes, s.reply(last, 1))
}

// A code the router could not try is no wrong code: the number is not held to it.
func (s *ChannelsSuite) TestACodeThatCouldNotBeTriedIsNotCountedAsAWrongOne() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, store.ChannelIdentityLink)
	writer := s.writer()
	s.awayFromAWindowEdge()
	s.failQueries(`DELETE FROM "channel_links"`)
	for n := range 5 {
		s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "123456"))
		s.Require().Equal(sendTheCode, s.reply(writer, n+1))
	}
	s.failing.value.Store(nil)

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "no such code"))

	s.Equal(sendTheCode, s.reply(writer, 6))
}

// Wrong codes of the window before count no more: the line takes codes again in the next one.
func (s *ChannelsSuite) TestWrongCodesOfTheWindowBeforeDoNotCount() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, store.ChannelIdentityLink)
	account, err := s.store.ChannelAccounts.GetByNumber(context.Background(), s.customerID(), "whatsapp", number)
	s.Require().NoError(err)
	guesser := s.writer()
	s.awayFromAWindowEdge()
	before := time.Now().UTC().Truncate(channels.LinkValidFor).Add(-channels.LinkValidFor)
	for n := range 20 {
		from := "+" + s.writer()
		if n < 5 {
			from = "+" + guesser
		}
		reserved, err := s.store.ChannelLinkFailures.Reserve(context.Background(), account.ID, from, before, before, 5, 20)
		s.Require().NoError(err)
		s.Require().True(reserved, "the window before takes all twenty")
	}

	s.Require().Equal(http.StatusAccepted, s.deliver(line, guesser, "msg-"+s.utils.uuid(), "no such code"))

	s.Equal(sendTheCode, s.reply(guesser, 1))
}

const (
	tooManyCodes = "Too many codes did not match. Try again in a few minutes."
	sendTheCode  = "Send me the code from your account to get started. It ties this number to you, so I can " +
		"answer about what is yours."
)

// deliverAtOnce posts a wrong code from each number to a line's webhook, all at the same moment,
// and waits until every delivery is taken.
func (s *ChannelsSuite) deliverAtOnce(line ChannelAccount, from ...string) {
	answered := make([]<-chan posted, len(from))
	for n, writer := range from {
		answered[n] = s.postLater(line, s.delivery(writer, "msg-"+s.utils.uuid(), "guess "+strconv.Itoa(n)))
	}
	for _, delivery := range answered {
		s.Require().Equal(http.StatusAccepted, (<-delivery).status)
	}
}

// awayFromAWindowEdge waits, when the window wrong codes are counted in ends within the next
// ten seconds, until the next one has begun: codes texted across an edge count in two windows.
func (s *ChannelsSuite) awayFromAWindowEdge() {
	s.Require().Eventually(func() bool {
		return time.Until(time.Now().Truncate(channels.LinkValidFor).Add(channels.LinkValidFor)) > 10*time.Second
	}, 15*time.Second, 100*time.Millisecond)
}

// A code the router cannot write is its own failure: the caller is told so, not what the store said.
func (s *ChannelsSuite) TestACodeThatCannotBeWrittenIsA500WithoutTheStoresWords() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)

	s.failQueries(`INSERT INTO "channel_links"`)
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()})

	s.Equal(http.StatusInternalServerError, status, string(payload))
	s.Contains(string(payload), "something went wrong")
	s.NotContains(string(payload), "channel link")
	s.NotContains(string(payload), "store:")
}

// Two people linking on one agent before either texts get two codes.
func (s *ChannelsSuite) TestTwoPeopleLinkingOnOneAgentGetTwoCodes() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	var first, second ChannelLink
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &first))
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()}, &second))
	s.NotEqual(first.Code, second.Code)
}

func (s *ChannelsSuite) TestAnAgentIdentifyingBySomebodysNumberHasNothingToLink() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, "")

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: s.utils.uuid()})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(body), "nothing to link")
}

func (s *ChannelsSuite) TestAnIdentityThatIsNeitherPhoneNorLinkIsRefused() {
	number := s.number()
	s.connect(number)

	status := s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name: "channelled-" + s.utils.uuid(),
		Mode: pointerTo(AgentModeText),
		Llm:  pointerTo("llm-flow"),
		Channels: &AgentChannels{
			Whatsapp: &ChannelLineRequest{Number: number},
			Identity: pointerTo(ChannelIdentity("whoever")),
		},
	}, nil)

	s.Equal(http.StatusBadRequest, status)
}

// A database that fails is the router's failure, and its text names the database's address.
func (s *ChannelsSuite) TestALineTheDatabaseCannotSaveIsAFailureWithNoDatabaseText() {
	number := s.number()
	s.failQueries(number)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/channels", ConnectChannelRequest{
		Kind:      string(channels.WhatsApp),
		Number:    number,
		AccountId: pointerTo(strings.TrimPrefix(number, "+")),
		Token:     pointerTo("meta-token"),
		Signing:   pointerTo(appSecret),
		Challenge: pointerTo(s.challenge()),
	})

	s.Equal(http.StatusInternalServerError, status)
	s.NotContains(string(body), "store:")
}

func (s *ChannelsSuite) TestACodeTheDatabaseCannotSaveIsAFailureWithNoDatabaseText() {
	number := s.number()
	s.connect(number)
	config := s.agentOn(number, store.ChannelIdentityLink)
	owner := s.utils.uuid()
	s.failQueries(owner)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/channels/links",
		LinkChannelRequest{ConfigId: config.Id, UserId: owner})

	s.Equal(http.StatusInternalServerError, status)
	s.NotContains(string(body), "store:")
}

// connect hands the router a WhatsApp line's credentials and returns where it will deliver.
func (s *ChannelsSuite) connect(number string) ChannelAccount {
	var line ChannelAccount
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/channels",
		ConnectChannelRequest{
			Kind:      string(channels.WhatsApp),
			Number:    number,
			AccountId: pointerTo(strings.TrimPrefix(number, "+")),
			Token:     pointerTo("meta-token"),
			Signing:   pointerTo(appSecret),
			Challenge: pointerTo(s.challenge()),
		}, &line))
	return line
}

// agentOn is a text agent answering on a number, identifying a sender the way named.
func (s *ChannelsSuite) agentOn(number, identity string) AgentConfig {
	return s.agentOnModel(number, identity, "noted/noted-model")
}

// agentOnModel is agentOn, with the model named.
func (s *ChannelsSuite) agentOnModel(number, identity, model string) AgentConfig {
	request := AgentConfigRequest{
		Name:     "channelled-" + s.utils.uuid(),
		Mode:     pointerTo(AgentModeText),
		Llm:      pointerTo(model),
		Channels: &AgentChannels{Whatsapp: &ChannelLineRequest{Number: number}},
	}
	if identity != "" {
		request.Channels.Identity = pointerTo(ChannelIdentity(identity))
	}
	var config AgentConfig
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/configs", request, &config))
	return config
}

// A message that arrives once the organization is suspended is not answered, and the router warns
// naming the customer, since the person loses the text until the organization pays.
func (s *ChannelsSuite) TestAMessageToASuspendedOrganizationIsNotAnsweredAndWarnsNamingTheCustomer() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	writer := s.writer()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))
	s.Require().Equal("Noted.", s.answer(writer))
	s.suspend(writer)

	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "and tomorrow?"))

	s.Require().Eventually(func() bool {
		return strings.Contains(s.logged.String(), `level=WARN msg="not answering a channel message" channel=whatsapp app_id=`+s.customerID())
	}, settleFor, 10*time.Millisecond, "the router never warned")
	s.NotContains(s.logged.String(), writer, "the log names no number")
	s.Len(s.meta.to(writer), 1, "only the answer to the first message")
}

// A reply the agent finishes after the organization was suspended is not written back, and the
// router warns naming the customer.
func (s *ChannelsSuite) TestAReplyOfASuspendedOrganizationIsNotWrittenBackAndWarnsNamingTheCustomer() {
	number := s.number()
	line := s.connect(number)
	s.agentOnModel(number, "", "holding/holding-model")
	writer := s.writer()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))
	s.Require().Equal("Held.", s.answer(writer))
	model := s.holding
	model.holds()
	defer model.answers()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "and tomorrow?"))
	s.Require().Eventually(func() bool { return len(model.replies()) >= 2 },
		settleFor, 10*time.Millisecond, "the second turn never began")
	s.suspend(writer)

	model.answers()

	s.Require().Eventually(func() bool {
		return strings.Contains(s.logged.String(), `level=WARN msg="not writing back on a channel" channel=whatsapp app_id=`+s.customerID())
	}, settleFor, 10*time.Millisecond, "the router never warned")
	s.NotContains(s.logged.String(), writer, "the log names no number")
	s.Len(s.meta.to(writer), 1, "only the answer to the first message")
}

// A message the sandbox holds back stays an Info line, as it was: only an organization that may not
// pay is a warning. The line says which rule held it and names no number.
func (s *ChannelsSuite) TestAMessageTheSandboxHoldsBackIsLoggedAsInfo() {
	s.holdSandbox(dlc.Sandbox{Enabled: true, Recipients: 2, MessagesPerDay: 1})
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")

	writer := s.writer()
	s.Require().Equal(http.StatusAccepted, s.deliver(line, writer, "msg-"+s.utils.uuid(), "what is open?"))

	s.Require().Eventually(func() bool {
		return strings.Contains(s.logged.String(), `level=INFO msg="not answering a channel message" channel=whatsapp reason=sandbox_recipient`)
	}, settleFor, 10*time.Millisecond, "the router never logged the refusal")
	s.NotContains(s.logged.String(), writer, "the refusal's text names the number, the log does not")
	s.NotContains(s.logged.String(), `level=WARN msg="not answering a channel message"`)
}

// suspend suspends the test's organization and waits until the gate refuses the customer a text to
// person for it: an organization's money is read after its first admission, which it admits.
func (s *ChannelsSuite) suspend(person string) {
	s.usage.Suspend(s.T(), s.app.organization.ID, "fraud review")
	s.Require().Eventually(func() bool {
		return errors.Is(s.gate.Allow(context.Background(), s.customerID(), dlc.WhatsApp, person), dlc.ErrUnpaid)
	}, 10*time.Second, 50*time.Millisecond, "the organization's money is read")
}

// challenge is the verify token every line in this suite is connected with.
func (s *ChannelsSuite) challenge() string { return "verify-" + s.app.key }

// number is a line nobody else in the run is using, since suites share the database. A
// UUIDv7 starts with the time, so it is the random tail that tells two of them apart.
func (s *ChannelsSuite) number() string { return "+1555" + digits(s.utils.uuid()) }

// writer is somebody's phone, as WhatsApp sends it: digits, no plus.
func (s *ChannelsSuite) writer() string { return "1347" + digits(s.utils.uuid()) }

// digits is the random tail of a UUID as seven decimal digits.
func digits(id string) string {
	tail := strings.ReplaceAll(id, "-", "")
	random, err := strconv.ParseUint(tail[len(tail)-8:], 16, 64)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%07d", random%10000000)
}

// posted is how a webhook answered a delivery: status 0 is one that got no answer.
type posted struct {
	status int
	body   []byte
}

// postLater posts a delivery to a line's webhook, signed as Meta signs it, and answers how it
// was answered once it is: for a delivery that waits on the database while the test goes on.
func (s *ChannelsSuite) postLater(line ChannelAccount, body []byte) <-chan posted {
	request, err := http.NewRequest(http.MethodPost, s.server.URL+line.WebhookUrl, bytes.NewReader(body))
	s.Require().NoError(err)
	request.Header = s.signed(body)
	request.Header.Set("Content-Type", "application/json")
	answered := make(chan posted, 1)
	go func() {
		response, err := (&http.Client{Timeout: 3 * settleFor}).Do(request)
		if err != nil {
			answered <- posted{}
			return
		}
		defer response.Body.Close()
		read := new(bytes.Buffer)
		_, _ = read.ReadFrom(response.Body)
		answered <- posted{status: response.StatusCode, body: read.Bytes()}
	}()
	return answered
}

// holdOptOuts makes the app's opt-out writes wait on the transaction it returns, open on the
// backend it names, until the test rolls it back, at the latest at the test's end. With
// refuseFirst, the first write let on is refused. name is the trigger's, apart from those of
// suites that run at the same time.
func (s *RouterSuite) holdOptOuts(name string, refuseFirst bool) (bun.Tx, int) {
	t, ctx := s.T(), context.Background()
	key := rand.Int64()
	_, err := s.store.GlobalDB().ExecContext(ctx, fmt.Sprintf(`BEGIN;
		CREATE SEQUENCE %[1]s_writes;
		CREATE FUNCTION %[1]s() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_advisory_xact_lock(%[2]d);
				IF %[3]t AND nextval('%[1]s_writes') = 1 THEN RAISE EXCEPTION 'refused by the test'; END IF;
				RETURN NEW;
			END $$ LANGUAGE plpgsql;
		CREATE TRIGGER %[1]s BEFORE INSERT ON opt_outs FOR EACH ROW
			WHEN (NEW.customer_id = '%[4]s') EXECUTE FUNCTION %[1]s();
		COMMIT`, name, key, refuseFirst, s.customerID()))
	s.Require().NoError(err)
	t.Cleanup(func() {
		_, err := s.store.GlobalDB().ExecContext(ctx, fmt.Sprintf(`BEGIN;
			DROP TRIGGER %[1]s ON opt_outs;
			DROP FUNCTION %[1]s();
			DROP SEQUENCE %[1]s_writes;
			COMMIT`, name))
		require.NoError(t, err)
	})
	hold, err := s.store.GlobalDB().BeginTx(ctx, nil)
	s.Require().NoError(err)
	// Cleanups run last first: the hold ends before the trigger it holds writes in is dropped.
	t.Cleanup(func() {
		if err := hold.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			require.NoError(t, err)
		}
	})
	var holder int
	s.Require().NoError(hold.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&holder))
	_, err = hold.ExecContext(ctx, "SELECT pg_advisory_xact_lock(?)", key)
	s.Require().NoError(err)
	return hold, holder
}

// waitingOn waits for the one backend that waits on a lock pid holds, and is it.
func (s *RouterSuite) waitingOn(pid int) int {
	var waiting []int
	s.Require().Eventually(func() bool {
		waiting = nil
		err := s.store.GlobalDB().NewRaw("SELECT pid FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))", pid).
			Scan(context.Background(), &waiting)
		return err == nil && len(waiting) > 0
	}, settleFor, 10*time.Millisecond, "nothing waits on backend %d", pid)
	s.Require().Len(waiting, 1)
	return waiting[0]
}

// deliver posts one message to a line's webhook, signed as Meta signs it.
func (s *ChannelsSuite) deliver(line ChannelAccount, from, id, text string) int {
	body := s.delivery(from, id, text)
	status, _ := s.post(line.WebhookUrl, s.signed(body), body)
	return status
}

func (s *ChannelsSuite) delivery(from, id, text string) []byte {
	raw, err := json.Marshal(map[string]any{"entry": []any{map[string]any{
		"changes": []any{map[string]any{"value": map[string]any{
			"metadata": map[string]any{"display_phone_number": "15556325550"},
			"contacts": []any{map[string]any{"wa_id": from, "profile": map[string]any{"name": "Thierry"}}},
			"messages": []any{map[string]any{
				"id": id, "from": from, "type": "text", "text": map[string]any{"body": text},
			}},
		}}},
	}}})
	s.Require().NoError(err)
	return raw
}

func (s *ChannelsSuite) signed(body []byte) http.Header {
	mac := hmac.New(sha256.New, []byte(appSecret))
	mac.Write(body)
	header := http.Header{}
	header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return header
}

// answer waits for what the agent wrote back to one number. The webhook answers before the
// model does, so the reply arrives at Meta rather than in the response.
func (s *ChannelsSuite) answer(to string) string {
	var written []string
	s.Require().Eventually(func() bool {
		written = s.meta.to(to)
		return len(written) > 0
	}, settleFor, 50*time.Millisecond, "the agent wrote nothing back to %s", to)
	return written[0]
}

// reply is the nth message the agent wrote back to a number, from 1, once there is one.
func (s *ChannelsSuite) reply(to string, n int) string {
	var written []string
	s.Require().Eventually(func() bool {
		written = s.meta.to(to)
		return len(written) >= n
	}, settleFor, 50*time.Millisecond, "the agent wrote %s fewer than %d messages", to, n)
	return written[n-1]
}

// post and get reach a webhook the way a provider does: no credentials, since the token in
// the path is what names the line. The suite's router has no public url, so what the API
// answered as the webhook URL is a path.
func (s *ChannelsSuite) post(path string, header http.Header, body []byte) (int, []byte) {
	request, err := http.NewRequest(http.MethodPost, s.server.URL+path, bytes.NewReader(body))
	s.Require().NoError(err)
	request.Header = header
	request.Header.Set("Content-Type", "application/json")
	return s.send(request)
}

func (s *ChannelsSuite) get(path string) (int, []byte) {
	request, err := http.NewRequest(http.MethodGet, s.server.URL+path, nil)
	s.Require().NoError(err)
	return s.send(request)
}

func (s *ChannelsSuite) send(request *http.Request) (int, []byte) {
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered := new(bytes.Buffer)
	_, err = answered.ReadFrom(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, answered.Bytes()
}

// metaAPI stands in for Meta's Cloud API, keeping the text of every message sent to it, and
// the link of every document.
type metaAPI struct {
	mu        sync.Mutex
	sent      map[string][]string
	documents map[string][]string
	// chats is the text of every message posted to a Linq chat, by the chat's id.
	chats map[string][]string
}

func (m *metaAPI) serve(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To   string `json:"to"`
		Text struct {
			Body string `json:"body"`
		} `json:"text"`
		Document struct {
			Link string `json:"link"`
		} `json:"document"`
		Message struct {
			Parts []struct {
				Value string `json:"value"`
			} `json:"parts"`
		} `json:"message"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	m.mu.Lock()
	if m.sent == nil {
		m.sent = map[string][]string{}
		m.documents = map[string][]string{}
		m.chats = map[string][]string{}
	}
	// Linq's POST /api/partner/v3/chats/{chat}/messages.
	if chat, ok := strings.CutPrefix(r.URL.Path, "/api/partner/v3/chats/"); ok && len(body.Message.Parts) > 0 {
		chat = strings.TrimSuffix(chat, "/messages")
		m.chats[chat] = append(m.chats[chat], body.Message.Parts[0].Value)
	} else if body.Document.Link != "" {
		m.documents[body.To] = append(m.documents[body.To], body.Document.Link)
	} else {
		m.sent[body.To] = append(m.sent[body.To], body.Text.Body)
	}
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprint(w, `{"messages":[{"id":"wamid.sent"}]}`)
}

func (m *metaAPI) to(number string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.sent[number]...)
}

// postedIn is the text of every message posted to a Linq chat, in order.
func (m *metaAPI) postedIn(chat string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.chats[chat]...)
}

// documentsTo is the link of every document sent to one number, in order.
func (m *metaAPI) documentsTo(number string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.documents[number]...)
}
