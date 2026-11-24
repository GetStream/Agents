//go:build integration

package ttsrouter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/breeze"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/s2pro"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// reservingSession is a session over a stub voice whose every character costs $1, for an
// organization with credit micros, reserving through the real enforcer. perSentence makes it a
// voice that takes each sentence as its own request.
func reservingSession(t *testing.T, money *policytest.Money, credit int64, perSentence bool) (*Session, *stubTTS, routing.Owner) {
	t.Helper()
	owner := money.Owner(t, credit)
	session, provider := reservingSessionOf(t, money, owner, perSentence)
	return session, provider, owner
}

// reservingSessionOf is a reservingSession for an owner that already has its credit.
func reservingSessionOf(t *testing.T, money *policytest.Money, owner routing.Owner, perSentence bool) (*Session, *stubTTS) {
	t.Helper()
	provider := newStubTTS()
	provider.perSentence = perSentence
	return reservingSessionOver(t, money, owner, provider), provider
}

// reservingSessionOver is a session over provider whose every character costs $1, for an owner
// that already has its credit.
func reservingSessionOver(t *testing.T, money *policytest.Money, owner routing.Owner, provider tts.TTS) *Session {
	t.Helper()
	config := routing.ProviderConfig{Provider: provider.Provider(), Model: provider.Model(), Price: routing.Price{PerMillionChars: 1_000_000}}
	prices, err := routing.Prices(routing.Config{routing.TTS: {Providers: []routing.ProviderConfig{config}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	session := newSession(context.Background(), provider, config, owner, money.Recorder(routing.TTS, prices), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// ownDeployments are the voices served by a deployment of ours, each started over a fake of it
// at url. The deployment speaks only flushed text and sends no final for a cancel of text it
// was never asked to say (deploy/breeze-tts-2/model/model.py, deploy/s2-pro/model/model.py).
var ownDeployments = map[string]func(t *testing.T, url string) tts.TTS{
	"breeze": func(t *testing.T, url string) tts.TTS {
		provider, err := breeze.New(breeze.Options{URL: url, APIKey: "test-key", DrainTimeout: 100 * time.Millisecond})
		require.NoError(t, err)
		return provider
	},
	"s2pro": func(t *testing.T, url string) tts.TTS {
		provider, err := s2pro.New(s2pro.Options{URL: url, APIKey: "test-key", DrainTimeout: 100 * time.Millisecond})
		require.NoError(t, err)
		return provider
	},
}

// fakeDeployment starts newProvider against a fake of its deployment and returns the
// deployment's side of the connection, once the handshake is done.
func fakeDeployment(t *testing.T, newProvider func(t *testing.T, url string) tts.TTS) (tts.TTS, *websocket.Conn) {
	t.Helper()
	conns := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"ready"}`)); err != nil {
			return
		}
		conns <- conn
		<-done
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(done) })
	provider := newProvider(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/websocket")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, provider.Start(ctx))
	select {
	case conn := <-conns:
		return provider, conn
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the voice did not connect")
		return nil, nil
	}
}

// framesUntilFlush reads what the voice sent the deployment up to its first flush, as
// type:id:text.
func framesUntilFlush(t *testing.T, conn *websocket.Conn) []string {
	t.Helper()
	var frames []string
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, raw, err := conn.ReadMessage()
		require.NoError(t, err)
		var frame struct{ Type, ID, Text string }
		require.NoError(t, json.Unmarshal(raw, &frame))
		frames = append(frames, frame.Type+":"+frame.ID+":"+frame.Text)
		if frame.Type == "flush" {
			return frames
		}
	}
}

// rowsOf waits for a customer's want rows and returns them.
func rowsOf(t *testing.T, money *policytest.Money, owner routing.Owner, want int) []store.Request {
	t.Helper()
	var rows []store.Request
	require.Eventually(t, func() bool {
		rows = nil
		require.NoError(t, money.Store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", owner.CustomerID).Order("id").Scan(context.Background()))
		return len(rows) == want
	}, 5*time.Second, 10*time.Millisecond, "%d rows", want)
	return rows
}

func TestClosingASessionReleasesWhatItsUtterancesHeld(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, _, owner := reservingSession(t, money, 1_000_000, false)

	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "its one character is held")

	require.NoError(t, session.Close())

	require.Eventually(t, func() bool { return money.Spend(t, owner) == nil }, 2*time.Second, 10*time.Millisecond,
		"an utterance that never completed holds nothing once the session is closed")
}

func TestClosingASessionNobodyReadsStillReleasesWhatItHeld(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 1_000_000, false)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	// The caller stopped reading: the provider's events fill every buffer between them.
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range 500 {
			provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "other-" + strconv.Itoa(i)})
		}
	}()
	require.Eventually(t, func() bool { return len(session.Events()) == cap(session.Events()) }, 2*time.Second, 5*time.Millisecond)
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits)

	require.NoError(t, session.Close())

	require.Eventually(t, func() bool { return money.Spend(t, owner) == nil }, 2*time.Second, 10*time.Millisecond,
		"what the unfinished utterance held is let go though nobody drains the events")
	<-sent
}

func TestClosingASessionReleasesEverySentenceOfAnId(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 9_000_000, true)
	other, _ := reservingSessionOf(t, money, owner, true)
	require.NoError(t, other.Synthesize(tts.Request{ID: "o1", Text: "yz"}))
	// Two sentences of u1, neither completed: the one cut by the barge-in and the one after.
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	require.NoError(t, session.Interrupt())
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "$10 is held of $9")

	require.NoError(t, session.Close())

	require.Eventually(t, func() bool { return money.Spend(t, owner) == nil }, 2*time.Second, 10*time.Millisecond,
		"both sentences' $8 is let go, and only the other session's $2 is still held")
}

func TestTextTheVoiceRefusedIsNoLongerHeld(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 2_000_000, false)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	require.NoError(t, money.Spend(t, owner), "$1 of $2 is held")

	provider.refuse = errors.New("the voice dropped the connection")
	require.Error(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}))

	require.NoError(t, money.Spend(t, owner), "what is held is the one character sent, not the two")
	provider.refuse = errors.New("still down")
	require.Error(t, session.Synthesize(tts.Request{ID: "u1", Text: "cd"}))
	require.NoError(t, money.Spend(t, owner))
}

// spoken passes on the sentence's start and end as the voice reports them, and waits until
// the session has settled it.
func spoken(t *testing.T, session *Session, provider *stubTTS, id string, characters int64) {
	t.Helper()
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: id, At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: id, Characters: characters})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, open := <-session.Events():
			if !open {
				t.Fatal("the session closed before the sentence settled")
			}
			if complete, ok := event.(tts.SynthesisComplete); ok && complete.SynthesisID == id {
				return
			}
		case <-deadline:
			t.Fatal("the sentence did not settle within 5s")
		}
	}
}

// voiceKinds is a voice that streams text into one utterance and one that takes each
// sentence as its own request, by whether it is per-sentence. Text for an id whose utterance
// settled is a new sentence on both.
var voiceKinds = map[string]bool{"streaming": false, "per-sentence": true}

func TestAnIdReusedAfterItsSentenceFinishedHoldsItsText(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 3_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
			spoken(t, session, provider, "u1", 1)
			require.NoError(t, money.Spend(t, owner), "$1 of $3 is spent")

			// The voice speaks it as a new sentence, so its 7 characters are held as a fresh
			// id's are: there is room for it, and none for anything after it.
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
			require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "the reused id's 7 characters are held")
			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u2", Text: "i", Final: true}), policy.ErrNoCredits)
			require.Len(t, provider.said, 2, "the sentence refused for credit is never sent")
		})
	}
}

func TestAnIdReusedAfterItsSentenceFinishedIsRefusedOnceNoCreditIsLeft(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 1_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
			spoken(t, session, provider, "u1", 1)

			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}), policy.ErrNoCredits)
			require.Len(t, provider.said, 1, "the refused sentence is not sent")
			session.mu.Lock()
			_, left := session.sentences["u1"]
			session.mu.Unlock()
			require.False(t, left, "nothing is left counted or held for the refused sentence")
			require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "$1 of $1 is spent")
		})
	}
}

// After a barge-in the voice takes the same id again before the interrupted sentence's
// completion has come back.
func TestAnIdReusedAfterItsFinalTextIsRefusedOnceNoCreditIsLeft(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, _ := reservingSession(t, money, 1_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
			provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
			require.NoError(t, session.Interrupt())

			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}), policy.ErrNoCredits,
				"the $1 of credit is held for the sentence sent")
			require.Len(t, provider.said, 1, "the refused sentence is not sent")
			session.mu.Lock()
			left := session.sentences["u1"]
			session.mu.Unlock()
			require.Len(t, left, 1, "only the sentence sent is waiting for its completion")
			require.EqualValues(t, 1, left[0].characters)
		})
	}
}

func TestAnIdReusedAfterABargeInCutItsUnfinishedSentenceIsRefusedOnceNoCreditIsLeft(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 2_000_000, perSentence)
			other, _ := reservingSessionOf(t, money, owner, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
			require.NoError(t, other.Synthesize(tts.Request{ID: "o1", Text: "y"}))
			provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
			require.NoError(t, session.Interrupt())

			if perSentence {
				// The voice throws away a sentence whose final text never came, and never
				// completes it.
				require.NoError(t, money.Spend(t, owner), "nothing is held for the sentence thrown away")
				require.NoError(t, other.Synthesize(tts.Request{ID: "o2", Text: "zz"}))
			}
			require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "no credit is left")
			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}), policy.ErrNoCredits,
				"the barge-in ended the sentence, so the text after it is a new one")
			require.Len(t, provider.said, 1, "the refused sentence is not sent")
		})
	}
}

// On a voice of our own deployment, text for an id cut by a barge-in before its final text is
// dropped there and billed as sent, and the id's text after it is spoken and billed on its own.
func TestAnUtteranceCutBeforeItsFinalTextIsBilledApartFromTheTextAfterIt(t *testing.T) {
	for name, newProvider := range ownDeployments {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			owner := money.Owner(t, 9_000_000)
			provider, conn := fakeDeployment(t, newProvider)
			session := reservingSessionOver(t, money, owner, provider)
			go func() {
				for range session.Events() {
				}
			}()

			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "abc"}))
			require.NoError(t, session.Interrupt())
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "defgh", Final: true}))
			frames := framesUntilFlush(t, conn)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"final","id":"u1"}`)))

			rows := rowsOf(t, money, owner, 2)
			require.EqualValues(t, 3, rows[0].Characters, "the cut text is billed as sent")
			require.EqualValues(t, 5, rows[1].Characters, "the text after the barge-in is billed in its own completion")
			require.Equal(t, []string{"text:u1:abc", "cancel:u1:", "text:u1:defgh", "flush:u1:"}, frames,
				"the deployment drops the cut text before the next text for the id")
			require.NoError(t, session.Close())
			// $8 of $9 is spent, so the last $1 is free only if nothing is still held.
			require.Eventually(t, func() bool { return money.Spend(t, owner) == nil }, 2*time.Second, 10*time.Millisecond,
				"nothing is held once the session is closed")
		})
	}
}

// On a voice of our own deployment the barge-in ends the sentence, so text for its id after it
// is a new sentence and is refused once no credit is left.
func TestAnIdReusedAfterABargeInOnOurOwnDeploymentIsRefusedOnceNoCreditIsLeft(t *testing.T) {
	for name, newProvider := range ownDeployments {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			owner := money.Owner(t, 3_000_000)
			provider, _ := fakeDeployment(t, newProvider)
			session := reservingSessionOver(t, money, owner, provider)
			go func() {
				for range session.Events() {
				}
			}()

			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "abc"}))
			require.NoError(t, session.Interrupt())
			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "defgh", Final: true}), policy.ErrNoCredits,
				"the barge-in ended the sentence, so the text after it is a new one")
		})
	}
}

// A streaming voice speaks text sent after an utterance's final text in that utterance, and
// completes it once.
func TestTextAfterAStreamingUtterancesFinalTextIsBilledInItsOneCompletion(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 10_000_000, false)
	go func() {
		for range session.Events() {
		}
	}()
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 8})
	rows := rowsOf(t, money, owner, 1)
	require.EqualValues(t, 8, rows[0].Characters, "the utterance is billed both texts")
	// Once it settled, the id is a new utterance with a completion of its own.
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "x", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 1})

	rows = rowsOf(t, money, owner, 2)
	require.EqualValues(t, 1, rows[1].Characters, "the new utterance is billed its own text")
	session.mu.Lock()
	left := len(session.sentences)
	session.mu.Unlock()
	require.Zero(t, left, "nothing is left waiting for a completion")
	// $9 of $10 is spent and nothing is still held for it, so one more character is the last.
	require.NoError(t, money.Spend(t, owner))
	require.NoError(t, session.Synthesize(tts.Request{ID: "u3", Text: "j", Final: true}))
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits)
}

func TestASentenceSpokenAgainAfterABargeInIsBilledOnItsOwn(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 9_000_000, true)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	require.NoError(t, session.Interrupt())
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
	go func() {
		for range session.Events() {
		}
	}()
	// The voice starts the new sentence before the interrupted one's completion comes back.
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 1, Interrupted: true})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 9})

	var rows []store.Request
	require.Eventually(t, func() bool {
		rows = nil
		require.NoError(t, money.Store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", owner.CustomerID).Order("id").Scan(context.Background()))
		return len(rows) == 2
	}, 5*time.Second, 10*time.Millisecond, "each sentence is a row")
	require.EqualValues(t, 1, rows[0].Characters)
	require.EqualValues(t, 7, rows[1].Characters, "the new sentence is billed what was sent")
	require.NotEqual(t, rows[0].UsageID, rows[1].UsageID)

	// $8 of $9 is spent and nothing is still held for it, so one more character is the last.
	require.NoError(t, money.Spend(t, owner))
	require.NoError(t, session.Synthesize(tts.Request{ID: "u3", Text: "j", Final: true}))
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits)
}

func TestMoreTextForAReusedIdsNewSentenceIsRefusedOnceNoCreditIsLeft(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, _ := reservingSession(t, money, 2_000_000, false)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
	spoken(t, session, provider, "u1", 1)
	// $1 is spent and the new sentence's first character holds the other.

	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}))
	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "cd"}), policy.ErrNoCredits,
		"the new sentence is speaking, and has no credit for more")
	require.Len(t, provider.said, 2, "the refused text is not sent")
}

// On a voice that takes each sentence as its own request, a final text the voice took ends
// the sentence; one it refused does not.
func TestASentenceWhoseFinalTextTheVoiceRefusedTakesItAgainAsItsOwn(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 4_000_000, true)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	provider.refuse = errors.New("the voice dropped the connection")
	require.Error(t, session.Synthesize(tts.Request{ID: "u1", Text: "bc", Final: true}))
	provider.refuse = nil

	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bc", Final: true}),
		"the final text never went out, so the sentence is still speaking")
	require.Len(t, provider.said, 2)
	spoken(t, session, provider, "u1", 3)
	rows := rowsOf(t, money, owner, 1)
	require.EqualValues(t, 3, rows[0].Characters, "it is one sentence")
}

func TestAnIdReusedAfterItsSentenceFinishedIsBilledOnceForWhatWasSent(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 9_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
			spoken(t, session, provider, "u1", 1)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
			require.Len(t, provider.said, 2, "the reused id is spoken")
			// The adapter counts 9; the row bills the 7 handed to it.
			spoken(t, session, provider, "u1", 9)

			var rows []store.Request
			require.Eventually(t, func() bool {
				rows = nil
				require.NoError(t, money.Store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", owner.CustomerID).Order("id").Scan(context.Background()))
				return len(rows) == 2
			}, 5*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 1, rows[0].Characters)
			require.EqualValues(t, 7, rows[1].Characters, "the reused id is billed what was sent")
			require.NotEqual(t, rows[0].UsageID, rows[1].UsageID, "each sentence is its own unit of work")

			// $8 of $9 is spent and nothing is still held for it, so one more character is the last.
			require.NoError(t, money.Spend(t, owner))
			require.NoError(t, session.Synthesize(tts.Request{ID: "u3", Text: "j", Final: true}))
			require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits)
		})
	}
}

// drain reads the session's events until it closes.
func drain(session *Session) {
	go func() {
		for range session.Events() {
		}
	}()
}

func TestAnUtterancesOwnTextCountsTowardTheCreditItsNextTextNeeds(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 3_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}))
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "c"}))

			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "d"}), policy.ErrNoCredits,
				"its own three characters hold all $3")
			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "e"}), policy.ErrNoCredits, "and stays refused")
			require.Len(t, provider.said, 3, "the refused text is not sent")

			// The caller ends it, and what was sent is spoken and billed.
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Final: true}))
			require.Equal(t, tts.Request{ID: "u1", Final: true}, provider.said[3])
			spoken(t, session, provider, "u1", 3)
			rows := rowsOf(t, money, owner, 1)
			require.EqualValues(t, 3, rows[0].Characters)
			require.EqualValues(t, 3_000_000, rows[0].CostMicros)
		})
	}
}

func TestTextAnUtteranceIsRefusedForCreditIsNotLeftHeld(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 3_000_000, false)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	other, _ := reservingSessionOf(t, money, owner, false)
	require.NoError(t, other.Synthesize(tts.Request{ID: "o1", Text: "xy"}))
	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh"}), policy.ErrNoCredits)

	require.NoError(t, other.Close())
	require.Eventually(t, func() bool { return money.Spend(t, owner) == nil }, 2*time.Second, 10*time.Millisecond,
		"one character is held of $3")
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}))
	require.NoError(t, money.Spend(t, owner), "two characters are held of $3")
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "c"}), "with room again the utterance goes on")
	require.ErrorIs(t, money.Spend(t, owner), policy.ErrNoCredits, "three are held, none of them twice")
	require.Len(t, provider.said, 3)
}

func TestAnUtteranceWhoseFinalTextIsRefusedForCreditStillEnds(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 1_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))

			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}), policy.ErrNoCredits)

			require.Equal(t, []tts.Request{{ID: "u1", Text: "a"}, {ID: "u1", Final: true}}, provider.said,
				"the voice is told the utterance is over, so it says and completes what it was sent")
			spoken(t, session, provider, "u1", 1)
			rows := rowsOf(t, money, owner, 1)
			require.EqualValues(t, 1, rows[0].Characters)
			session.mu.Lock()
			left := len(session.sentences)
			session.mu.Unlock()
			require.Zero(t, left, "nothing is left waiting for a completion")
		})
	}
}

// On a voice that takes each sentence as its own request, a sentence its refused final text
// ended takes no more, so text for its id after is a new sentence with a completion of its own.
func TestASentenceItsRefusedFinalTextEndedTakesNoMore(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 1_000_000, true)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "b", Final: true}), policy.ErrNoCredits)

	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "c", Final: true}), policy.ErrNoCredits)
	require.Len(t, provider.said, 2, "the sentence is not ended twice")
	spoken(t, session, provider, "u1", 1)
	rows := rowsOf(t, money, owner, 1)
	require.EqualValues(t, 1, rows[0].Characters)
}

func TestAnUtteranceThatWasSentNoTextIsNotEndedByARefusal(t *testing.T) {
	money := policytest.Start(t, "ttsrouter_reserve")
	session, provider, owner := reservingSession(t, money, 1_000_000, false)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1"}))
	other, _ := reservingSessionOf(t, money, owner, false)
	require.NoError(t, other.Synthesize(tts.Request{ID: "o1", Text: "x"}))

	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}), policy.ErrNoCredits)

	require.Equal(t, []tts.Request{{ID: "u1"}}, provider.said, "there is nothing to say")
	session.mu.Lock()
	left := len(session.sentences)
	session.mu.Unlock()
	require.Zero(t, left)
}

func TestASuspensionRefusesMoreTextOfAnUtterance(t *testing.T) {
	for name, perSentence := range voiceKinds {
		t.Run(name, func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve")
			session, provider, owner := reservingSession(t, money, 9_000_000, perSentence)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))

			money.Usage.Suspend(t, owner.OrganizationID, "a test")
			require.Eventually(t, func() bool {
				_, err := money.Enforcer.Admit(t.Context(), owner)
				return errors.Is(err, policy.ErrSuspended)
			}, 5*time.Second, 5*time.Millisecond, "the suspension is read")

			require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}), policy.ErrSuspended)
			require.Len(t, provider.said, 1)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Final: true}), "what was sent is still said")
			spoken(t, session, provider, "u1", 1)
			rows := rowsOf(t, money, owner, 1)
			require.EqualValues(t, 1, rows[0].Characters, "and billed")
			require.False(t, provider.closed, "the session stays open")
		})
	}
}

func TestMoneyThatCannotBeCheckedLetsAnUtteranceFinish(t *testing.T) {
	redis, sharing := policytest.SharedHolds(t)
	money := policytest.Start(t, "ttsrouter_reserve", sharing, func(settings *policy.Settings) { settings.FlushMaxLag = 200 * time.Millisecond })
	session, provider, owner := reservingSession(t, money, 9_000_000, false)
	drain(session)
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))

	redis.Stop()
	require.Eventually(t, func() bool {
		return errors.Is(money.Spend(t, owner), policy.ErrSpendCheckUnavailable)
	}, 3*time.Second, 10*time.Millisecond, "holds go unshared")
	require.ErrorIs(t, session.Synthesize(tts.Request{ID: "u2", Text: "x"}), policy.ErrSpendCheckUnavailable, "a new utterance is refused")

	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "bc"}))
	require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "d", Final: true}))
	require.Len(t, provider.said, 3, "the utterance already speaking is finished")
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 4})
	require.EqualValues(t, 4, rowsOf(t, money, owner, 1)[0].Characters, "and billed all of it")
}

// unenforced is an owner with no credit on a router that does not enforce its limits.
func unenforced(t *testing.T, money *policytest.Money) routing.Owner {
	t.Helper()
	app, organization := "app-"+uuid.NewString(), "org-"+uuid.NewString()
	money.Usage.Suspend(t, organization, "a test")
	owner := routing.Owner{CustomerID: app, Attribution: routing.Attribution{OrganizationID: organization}}
	money.Enforcer.Join(t.Context(), app, organization, usage.DefaultTier)
	return owner
}

func TestARouterThatDoesNotEnforceItsLimitsRefusesNoMoreText(t *testing.T) {
	for _, mode := range []config.LimitsMode{config.LimitsOff, config.LimitsShadow} {
		t.Run(string(mode), func(t *testing.T) {
			money := policytest.Start(t, "ttsrouter_reserve", func(settings *policy.Settings) { settings.Mode = mode })
			owner := unenforced(t, money)
			session, provider := reservingSessionOf(t, money, owner, false)
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "a"}))
			if mode == config.LimitsShadow {
				require.Eventually(t, func() bool {
					_, read := money.Enforcer.Balance(owner.CustomerID, owner.OrganizationID)
					return read
				}, 5*time.Second, 5*time.Millisecond, "the organization's standing is read")
			}

			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "b"}))
			require.NoError(t, session.Synthesize(tts.Request{ID: "u1", Text: "c", Final: true}))
			require.Len(t, provider.said, 3)
			spoken(t, session, provider, "u1", 3)
			require.EqualValues(t, 3, rowsOf(t, money, owner, 1)[0].Characters)
		})
	}
}

func BenchmarkSynthesizeDelta(b *testing.B) {
	money := policytest.Start(b, "ttsrouter_reserve")
	owner := money.Owner(b, 1_000_000_000_000)
	config := routing.ProviderConfig{Provider: "stub", Model: "stub-model", Price: routing.Price{PerMillionChars: 1}}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	prices, err := routing.Prices(routing.Config{routing.TTS: {Providers: []routing.ProviderConfig{config}}}, discard)
	require.NoError(b, err)
	session := newSession(context.Background(), mutedTTS{newStubTTS()}, config, owner, money.Recorder(routing.TTS, prices), discard)
	b.Cleanup(func() { _ = session.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := session.Synthesize(tts.Request{ID: "u1", Text: "a sentence of a reply, as long as most"}); err != nil {
			b.Fatal(err)
		}
	}
}

// mutedTTS keeps nothing of what it is sent.
type mutedTTS struct{ *stubTTS }

func (mutedTTS) Synthesize(tts.Request) error { return nil }
