package limitnotice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// memory is a Store of its own: the first Insert of an organization, kind and key writes.
type memory struct {
	mu      sync.Mutex
	written map[string]store.Notice
	posted  map[string]bool
	fail    error
}

func newMemory() *memory {
	return &memory{written: map[string]store.Notice{}, posted: map[string]bool{}}
}

func (m *memory) Insert(_ context.Context, notice *store.Notice) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return false, m.fail
	}
	key := notice.OrganizationID + "/" + notice.Kind + notice.DedupeKey
	if _, ok := m.written[key]; ok {
		return false, nil
	}
	m.written[key] = *notice
	return true, nil
}

// Claim returns every notice written and not posted, as if each claim were stale.
func (m *memory) Claim(context.Context, time.Duration, int) ([]store.Notice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var unposted []store.Notice
	for key, notice := range m.written {
		if !m.posted[key] {
			unposted = append(unposted, notice)
		}
	}
	return unposted, nil
}

func (m *memory) MarkPosted(_ context.Context, notice store.Notice) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.posted[notice.OrganizationID+"/"+notice.Kind+notice.DedupeKey] = true
	return nil
}

func (m *memory) isPosted(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.posted[key]
}

// posts keeps every message posted.
type posts struct {
	mu   sync.Mutex
	sent []string
}

func (p *posts) Post(_ context.Context, text string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, text)
	return nil
}

func (p *posts) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

func TestTheFirstHitOfEachKindIsPostedOnce(t *testing.T) {
	db, slack := newMemory(), &posts{}
	notifier := New(NewSender(SenderOptions{Store: db, Poster: slack, Environment: "test", Logger: slog.New(slog.DiscardHandler)}), slog.New(slog.DiscardHandler))
	defer notifier.Close()

	hit := policy.LimitHit{OrganizationID: "org-1", AppID: "app-1", Kind: store.LimitNoCredit, Enforced: true, Message: "no credit"}
	for range 10 {
		notifier.Hit(hit)
	}
	other := hit
	other.Kind = store.LimitSpendLimit
	notifier.Hit(other)

	require.Eventually(t, func() bool {
		return db.isPosted("org-1/no_credit") && db.isPosted("org-1/spend_limit")
	}, 2*time.Second, 5*time.Millisecond)
	require.Len(t, slack.all(), 2, "once per kind, however many refusals")

	again := New(NewSender(SenderOptions{Store: db, Poster: slack, Environment: "test", Logger: slog.New(slog.DiscardHandler)}), slog.New(slog.DiscardHandler))
	defer again.Close()
	again.Hit(hit)
	time.Sleep(50 * time.Millisecond)
	require.Len(t, slack.all(), 2, "another router finds it written")
}

func TestTheSweepPostsTheNoticesNoRouterPostedInTheirKindsMessage(t *testing.T) {
	db, slack := newMemory(), &posts{}
	for _, made := range []func() (store.Notice, error){
		store.LimitNotice{OrganizationID: "org-1", Kind: store.LimitSpendHardLimit, Enforced: true, Message: "over the hard limit"}.Notice,
		store.CostAnomaly{OrganizationID: "org-2", Day: time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), SpentMicros: 50_000_000, BaselineMicros: 1_000_000}.Notice,
		store.BalanceNotice{OrganizationID: "org-3", Kind: store.NoticeBalanceLow, GrantedMicros: 10}.Notice,
	} {
		notice, err := made()
		require.NoError(t, err)
		_, err = db.Insert(context.Background(), &notice)
		require.NoError(t, err)
	}
	sender := NewSender(SenderOptions{Store: db, Poster: slack, Environment: "test", Logger: slog.New(slog.DiscardHandler)})
	sender.every = 5 * time.Millisecond
	sender.Start()
	defer sender.Close()

	require.Eventually(t, func() bool {
		return db.isPosted("org-1/spend_hard_limit") && db.isPosted("org-2/cost_anomaly2026-03-10")
	}, 2*time.Second, 5*time.Millisecond)
	sender.Close()

	sent := slack.all()
	require.Len(t, sent, 2, "a balance warning is the customer's webhook, not the channel's")
	require.False(t, db.isPosted("org-3/balance_low10"))
	require.True(t, slices.ContainsFunc(sent, func(text string) bool { return strings.Contains(text, "over the hard limit") }), sent)
	require.True(t, slices.ContainsFunc(sent, func(text string) bool { return strings.Contains(text, "org-2") }), sent)
}

func TestAHitThatCouldNotBeWrittenIsNotPosted(t *testing.T) {
	db, slack := newMemory(), &posts{}
	db.fail = errors.New("postgres is down")
	notifier := New(NewSender(SenderOptions{Store: db, Poster: slack, Environment: "test", Logger: slog.New(slog.DiscardHandler)}), slog.New(slog.DiscardHandler))
	defer notifier.Close()

	notifier.Hit(policy.LimitHit{OrganizationID: "org-1", Kind: store.LimitNoCredit, Enforced: true})
	time.Sleep(50 * time.Millisecond)

	require.Empty(t, slack.all())
}

func TestTheMessageSaysWhetherTheWorkWasRefused(t *testing.T) {
	notice := store.LimitNotice{
		OrganizationID: "org-1", Kind: store.LimitSpendHardLimit, AppID: "app-1", Tier: "free",
		Enforced: true, Message: "the organization's day spend hard limit of $5.00 is reached",
	}
	require.Equal(t, ":no_entry: Organization `org-1` was refused for the first time at Stream's spend hard limit."+
		"\nTier: free\nApp: `app-1`\n> the organization's day spend hard limit of $5.00 is reached", Message("prod", Links{}, notice))

	notice.Enforced = false
	require.True(t, strings.HasPrefix(Message("staging", Links{}, notice), "[STAGING] :eyes: Organization `org-1`"))
	require.Contains(t, Message("staging", Links{}, notice), "limits are in shadow, so the work was admitted")
}

func TestTheMessageLinksTheAppAndTheStaffPage(t *testing.T) {
	links := Links{App: "https://dash.example/organization/{organization}/{app}/agents/",
		Staff: "https://dash.example/internal/organizations/{organization}"}
	notice := store.LimitNotice{OrganizationID: "5380", Kind: store.LimitNoCredit, AppID: "1733675", Message: "no credit"}

	require.Contains(t, Message("staging", links, notice), "\nApp: `1733675`\n"+
		"<https://dash.example/organization/5380/1733675/agents/|App> · <https://dash.example/internal/organizations/5380|Staff page>\n> no credit")

	notice.AppID = ""
	require.Contains(t, Message("staging", links, notice), "\n<https://dash.example/internal/organizations/5380|Staff page>\n> no credit",
		"with no app, only what names none")
	require.NotContains(t, Message("staging", Links{}, notice), "<", "no templates, no links")

	notice.AppID = "a|b>c"
	require.Contains(t, Message("staging", links, notice), "/organization/5380/a%7Cb%3Ec/agents/|App>", "an id cannot break the link")
}

func TestOnlyProductionIsNotTagged(t *testing.T) {
	for environment, tag := range map[string]string{
		"prod": "", "production": "", "Production": "",
		"staging": "[STAGING] ", "benchmark": "[BENCHMARK] ", "": "[DEVELOPMENT] ",
	} {
		require.Equal(t, tag, EnvironmentTag(environment), environment)
	}
}

func TestSlackPostsToTheChannel(t *testing.T) {
	var got map[string]any
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/chat.postMessage", r.URL.Path)
		authorization = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &got))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	slack := NewSlack("xoxb-test", "C123")
	slack.baseURL = server.URL

	require.NoError(t, slack.Post(context.Background(), "hello"))
	require.Equal(t, "Bearer xoxb-test", authorization)
	require.Equal(t, "C123", got["channel"])
	require.Equal(t, "hello", got["text"])
}

func TestSlackSaysWhyItRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"not_in_channel"}`))
	}))
	defer server.Close()
	slack := NewSlack("xoxb-test", "C123")
	slack.baseURL = server.URL

	require.ErrorContains(t, slack.Post(context.Background(), "hello"), "not_in_channel")
}

func TestTheAnomalyMessageIsTaggedAndLinksOnlyTheStaffPage(t *testing.T) {
	links := Links{App: "https://dash.example/organization/{organization}/{app}/agents/",
		Staff: "https://dash.example/internal/organizations/{organization}"}
	anomaly := store.CostAnomaly{
		OrganizationID: "org-1", Day: time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), SpentMicros: 12_345_678, BaselineMicros: 2_000_000,
	}

	require.Equal(t, "[STAGING] :chart_with_upwards_trend: Organization `org-1` spent $12.35 on 2026-03-10 (UTC), "+
		"6.2x its median day of the 7 before ($2.00).\n<https://dash.example/internal/organizations/org-1|Staff page>",
		AnomalyMessage("staging", links, anomaly))
	require.NotContains(t, AnomalyMessage("staging", Links{Staff: "https://dash.example/{organization}/{app}"}, anomaly), "<",
		"a staff page naming an app links nothing: an anomaly has none")
}
