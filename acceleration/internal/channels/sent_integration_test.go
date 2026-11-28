//go:build integration

package channels

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// roundTrip is an http.RoundTripper that is a function.
type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

// SentSuite is whom the day's channel limit counts as reached when the service sends: the
// person written to, not the line or the customer. It runs a real gate over Redis and a real
// enforcer, with the provider stood in for by a local server.
type SentSuite struct {
	suite.Suite
	ctx     context.Context
	money   *policytest.Money
	redis   rueidis.Client
	service *Service
	owner   routing.Owner
	sealer  *auth.Sealer
}

func TestSentSuite(t *testing.T) { suite.Run(t, new(SentSuite)) }

func (s *SentSuite) SetupSuite() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR not set")
	}
	s.ctx = context.Background()
	s.money = policytest.Start(s.T(), "channels_sent")
	redis, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(redis.Close)
	s.redis = redis
	s.sealer, err = auth.NewSealer("sent-suite-kek")
	s.Require().NoError(err)
}

func (s *SentSuite) SetupTest() {
	s.owner = s.money.Owner(s.T(), 1_000_000_000)
	taken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	s.T().Cleanup(taken.Close)
	target, err := url.Parse(taken.URL)
	s.Require().NoError(err)
	provider := &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme, request.URL.Host = target.Scheme, target.Host
		return http.DefaultTransport.RoundTrip(request)
	})}
	gate := dlc.NewGate(s.money.Store, s.redis, dlc.Sandbox{}, slog.New(slog.DiscardHandler)).
		Charging(s.money.Enforcer).Limiting(config.LimitsEnforce)
	s.service = &Service{store: s.money.Store, secrets: s.sealer, transport: provider, gate: gate,
		logger: slog.New(slog.DiscardHandler)}
}

// reached is who the owner's organization reached by text today, as the counters say.
func (s *SentSuite) reached() []string {
	key := "channel:" + time.Now().UTC().Format("20060102") + ":{" + s.owner.OrganizationID + "}:sms:to"
	members, err := s.redis.Do(s.ctx, s.redis.B().Smembers().Key(key).Build()).AsStrSlice()
	s.Require().NoError(err)
	slices.Sort(members)
	s.T().Cleanup(func() { s.Require().NoError(s.redis.Do(s.ctx, s.redis.B().Del().Key(key).Build()).Error()) })
	return members
}

// Catches Notify counting its recipient as the line or nobody: the second message a day to the
// same person must not take another of the organization's recipients.
func (s *SentSuite) TestANoticeCountsTheUserItReachedAsReached() {
	line, writer := "+15550100"+uuid.NewString()[:2], "+13475550101"
	sealed, err := s.sealer.SealWithAAD(`{"token":"telnyx-key"}`, []byte(s.owner.CustomerID))
	s.Require().NoError(err)
	s.Require().NoError(s.money.Store.ChannelAccounts.Upsert(s.ctx, &store.ChannelAccount{
		CustomerID: s.owner.CustomerID, Kind: string(SMS), E164: line, Token: "line-" + uuid.NewString(),
		SecretsSealed: sealed, SecretsKEKVersion: s.sealer.CurrentVersion(),
	}))
	agentConfig := store.AgentConfig{CustomerID: s.owner.CustomerID, Name: "sent-" + uuid.NewString(),
		Channels: store.AgentAnswersIn{Lines: store.AgentChannels{SMS: &store.ChannelLine{Number: line}}}}
	s.Require().NoError(s.money.Store.AgentConfigs.Insert(s.ctx, &agentConfig))
	s.Require().NoError(s.money.Store.ContactMapEntries.Upsert(s.ctx, &store.ContactMapEntry{
		CustomerID: s.owner.CustomerID, ConfigID: agentConfig.ID, Kind: string(SMS), Address: writer, UserID: "user-1",
	}))

	err = s.service.Notify(s.ctx, Notice{CustomerID: s.owner.CustomerID, ConfigID: agentConfig.ID, Kind: SMS,
		UserID: "user-1", Text: "Your table is ready."})

	s.Require().NoError(err)
	s.Equal([]string{writer}, s.reached(), "the person written to, not the line, the customer or nobody")
}

// Catches a reply counting nobody, or the line, as the person reached: the reached set is who
// the reply went to.
func (s *SentSuite) TestAReplyCountsTheSenderItAnsweredAsReached() {
	line, writer := Account{Kind: SMS, E164: "+15550100999", Token: "telnyx-key"}, "+13475550102"
	provider, ok := For(SMS, s.service.transport)
	s.Require().True(ok)
	events := make(chan session.Event, 1)
	events <- agent.Responded{Text: "On my way."}
	close(events)

	s.service.send(s.ctx, nil, s.owner, line, provider, Message{Kind: SMS, From: writer, To: line.E164, Thread: writer}, events)

	s.Equal([]string{writer}, s.reached(), "the sender answered, not the line, the customer or nobody")
}

// Catches a STOP recorded under the sender as the provider wrote them, or read without the
// line's country: one from a national number must still refuse the next text to that number
// in E.164, and a START written another way lift it.
func (s *SentSuite) TestAStopFromASenderNotInE164RefusesTheirNumber() {
	account := s.smsLine()

	fresh, err := s.service.claim(s.ctx, account, Message{Kind: SMS, ID: uuid.NewString(), From: "3475550103", Text: "stop"})
	s.Require().NoError(err)
	s.Require().True(fresh)
	s.True(s.optedOut("+13475550103"))

	_, err = s.service.claim(s.ctx, account, Message{Kind: SMS, ID: uuid.NewString(), From: "(347) 555-0103", Text: "START"})
	s.Require().NoError(err)
	s.False(s.optedOut("+13475550103"))
}

// Catches a START from a sender whose STOP an older router stored as they wrote it missing
// that row before the backfill rewrites it, and a STOP then not refusing them.
func (s *SentSuite) TestAStartBeforeTheBackfillLiftsAStopStoredAsWritten() {
	account := s.smsLine()
	_, err := s.money.Store.GlobalDB().ExecContext(s.ctx, `INSERT INTO opt_outs (id, customer_id, recipient, channel, source, created_at)
		VALUES (?, ?, '13475550104', 'sms', 'keyword', now())`, uuid.NewString(), s.owner.CustomerID)
	s.Require().NoError(err)

	_, err = s.service.claim(s.ctx, account, Message{Kind: SMS, ID: uuid.NewString(), From: "13475550104", Text: "START"})
	s.Require().NoError(err)
	s.False(s.optedOut("13475550104"), "the row stored as written is lifted")

	_, err = s.service.claim(s.ctx, account, Message{Kind: SMS, ID: uuid.NewString(), From: "13475550104", Text: "STOP"})
	s.Require().NoError(err)
	s.True(s.optedOut("+13475550104"))
}

// smsLine is an SMS line of the owner's, on a number in the United States.
func (s *SentSuite) smsLine() store.ChannelAccount {
	sealed, err := s.sealer.SealWithAAD(`{"token":"telnyx-key"}`, []byte(s.owner.CustomerID))
	s.Require().NoError(err)
	account := store.ChannelAccount{
		CustomerID: s.owner.CustomerID, Kind: string(SMS), E164: fmt.Sprintf("+1202555%04d", rand.IntN(10000)),
		Token: "line-" + uuid.NewString(), SecretsSealed: sealed, SecretsKEKVersion: s.sealer.CurrentVersion(),
	}
	s.Require().NoError(s.money.Store.ChannelAccounts.Upsert(s.ctx, &account))
	return account
}

// optedOut is whether the owner's SMS opt-outs refuse recipient.
func (s *SentSuite) optedOut(recipient string) bool {
	blocked, err := s.money.Store.OptOuts.OptedOut(s.ctx, s.owner.CustomerID, recipient, string(SMS))
	s.Require().NoError(err)
	return blocked
}

// Catches a notice opening its line without the data keys, or without moving it. With sealing on,
// a notice on a line under the router's key moves it to the app's data key, and the next notice
// opens it there.
func (s *SentSuite) TestANoticeMovesItsLineToTheAppsDataKeyAndOpensItThere() {
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(s.money.Store, appkeystest.NewKMS(), options)
	s.Require().NoError(err)
	s.service.dataKeys = keys
	line, writer := "+15550100"+uuid.NewString()[:2], "+13475550101"
	account := store.ChannelAccount{CustomerID: s.owner.CustomerID, Kind: string(SMS), E164: line, Token: "line-" + uuid.NewString()}
	s.Require().NoError(Seal(s.ctx, s.sealer, nil, &account, Secrets{Token: "telnyx-key"}))
	s.Require().NoError(s.money.Store.ChannelAccounts.Upsert(s.ctx, &account))
	agentConfig := store.AgentConfig{CustomerID: s.owner.CustomerID, Name: "sent-" + uuid.NewString(),
		Channels: store.AgentAnswersIn{Lines: store.AgentChannels{SMS: &store.ChannelLine{Number: line}}}}
	s.Require().NoError(s.money.Store.AgentConfigs.Insert(s.ctx, &agentConfig))
	s.Require().NoError(s.money.Store.ContactMapEntries.Upsert(s.ctx, &store.ContactMapEntry{
		CustomerID: s.owner.CustomerID, ConfigID: agentConfig.ID, Kind: string(SMS), Address: writer, UserID: "user-1",
	}))
	notice := Notice{CustomerID: s.owner.CustomerID, ConfigID: agentConfig.ID, Kind: SMS, UserID: "user-1", Text: "Your table is ready."}

	s.Require().NoError(s.service.Notify(s.ctx, notice))
	moved, err := s.money.Store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal(new(1), moved.SecretsDataKeyVersion, "the notice moved the line to the app's data key")
	s.Zero(moved.SecretsKEKVersion)
	s.NoError(s.service.Reachable(s.ctx, notice), "the line opens under the data key")
}
