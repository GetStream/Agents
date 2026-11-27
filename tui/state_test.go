package tui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/sdks/go/stream"
)

// failingSession wraps a real session so Respond or Interrupt can return a chosen error.
type failingSession struct {
	Session
	respondErr   error
	interruptErr error
}

func (s failingSession) Respond(text string) error {
	if s.respondErr != nil {
		return s.respondErr
	}
	return s.Session.Respond(text)
}

func (s failingSession) Interrupt() error {
	if s.interruptErr != nil {
		return s.interruptErr
	}
	return s.Session.Interrupt()
}

func TestSubmittingAfterTheSessionEndsDoesNotWedgeTheConversation(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{Open: backend.opener(t)})
	attach(t, m, backend.session(t, ""), stream.ConversationPage{})

	m.Update(received{generation: m.generation, ok: false})
	if m.session != nil {
		t.Fatal("the ended session is still attached")
	}
	if m.State().Busy {
		t.Fatal("the ended session is still marked busy")
	}

	// Pressing Enter after the session ends must not mark the conversation busy, append a
	// pending question, discard the draft, or send respond to the dead session.
	settle(m, ask(m, "are you still there?"))
	if m.State().Busy {
		t.Error("submitting after the session ended wedged the conversation in busy")
	}
	if len(m.messages) != 0 {
		t.Errorf("submitting after the session ended appended %+v", m.messages)
	}
	if m.activeQuestionID != "" {
		t.Errorf("submitting after the session ended set activeQuestionID to %q", m.activeQuestionID)
	}
	if m.input.Value() != "are you still there?" {
		t.Errorf("submitting after the session ended changed the composer to %q", m.input.Value())
	}
	if sent := backend.commands(t); len(sent) != 0 {
		t.Errorf("submitting after the session ended sent %v", sent)
	}

	// Resuming recovers to a ready session where asking works again.
	settle(m, ask(m, "/resume session-42"))
	if m.session == nil || m.State().Busy || m.State().Connecting {
		t.Fatalf("resuming left session=%v busy=%v connecting=%v", m.session != nil, m.State().Busy, m.State().Connecting)
	}
	settle(m, ask(m, "back online?"))
	if !m.State().Busy {
		t.Error("asking after resuming did not start a turn")
	}
	backend.commands(t, "respond")
}

func TestAnOlderPageFromAReplacedConversationIsDropped(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: backend.opener(t),
		History: func(_ context.Context, _ string, before string) (stream.ConversationPage, error) {
			switch before {
			case "old-cursor":
				return stream.ConversationPage{
					Messages: []stream.ConversationMessage{{ID: "old-msg", Role: "user", Text: "from the previous conversation"}},
					Before:   "older-still",
				}, nil
			case "new-cursor":
				return stream.ConversationPage{
					Messages: []stream.ConversationMessage{{ID: "new-earlier", Role: "user", Text: "earlier in the new conversation"}},
					Before:   "new-cursor-2",
				}, nil
			default:
				return stream.ConversationPage{
					Messages: []stream.ConversationMessage{{ID: "new-msg", Role: "user", Text: "from the new conversation"}},
					Before:   "new-cursor",
				}, nil
			}
		},
	})
	attach(t, m, backend.session(t, ""), stream.ConversationPage{
		Messages: []stream.ConversationMessage{{ID: "first", Role: "user", Text: "first conversation"}},
		Before:   "old-cursor",
	})

	// Capture the /older command before switching conversations, then deliver its result after /new.
	staleOlder := ask(m, "/older")
	settle(m, ask(m, "/new"))

	if len(m.messages) != 1 || m.messages[0].ID != "new-msg" || m.before != "new-cursor" {
		t.Fatalf("new conversation opened with messages=%+v before=%q", m.messages, m.before)
	}

	feed(m, step(staleOlder, 3*time.Second))

	if len(m.messages) != 1 || m.messages[0].ID != "new-msg" {
		t.Errorf("a stale /older corrupted the new conversation: %+v", m.messages)
	}
	if m.before != "new-cursor" {
		t.Errorf("a stale /older overwrote the cursor with %q", m.before)
	}
	if m.status != "Ready" {
		t.Errorf("a stale /older overwrote the status with %q", m.status)
	}

	// A current-generation /older still loads into the new conversation.
	settle(m, ask(m, "/older"))
	if len(m.messages) != 2 || m.messages[0].ID != "new-earlier" || m.before != "new-cursor-2" {
		t.Errorf("current-generation /older did not load: messages=%+v before=%q", m.messages, m.before)
	}
}

func TestAStaleOlderErrorFromAReplacedConversationIsDropped(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: backend.opener(t),
		History: func(_ context.Context, _ string, before string) (stream.ConversationPage, error) {
			if before == "old-cursor" {
				return stream.ConversationPage{}, errors.New("old history failed")
			}
			return stream.ConversationPage{}, nil
		},
	})
	attach(t, m, backend.session(t, ""), stream.ConversationPage{Before: "old-cursor"})

	staleOlder := ask(m, "/older")
	settle(m, ask(m, "/new"))
	feed(m, step(staleOlder, 3*time.Second))

	if m.statusFailed || m.status != "Ready" {
		t.Errorf("a stale /older error overwrote status=%q failed=%v", m.status, m.statusFailed)
	}
}

func TestAFailingCommandDoesNotClearBusyWhileAnAnswerIsStreaming(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: backend.opener(t),
		Commands: []Command{{
			Name: "fail",
			Run:  func(context.Context, []string) (string, error) { return "", errors.New("command failed") },
		}},
	})
	settle(m, m.open(""))

	settle(m, ask(m, "in-flight question"))
	settle(m, ask(m, "/fail"))

	if !m.State().Busy {
		t.Error("a failing slash command cleared busy while an answer was streaming")
	}
	if !m.statusFailed || m.status != "command failed" {
		t.Errorf("the status line says %q failed=%v", m.status, m.statusFailed)
	}
	if len(m.messages) != 1 || m.messages[0].ID != pendingID || m.activeQuestionID != pendingID {
		t.Errorf("a failing slash command altered messages=%+v activeQuestionID=%q", m.messages, m.activeQuestionID)
	}
}

func TestAFailingInterruptDoesNotClearBusyWhileAnAnswerIsStreaming(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: func(context.Context, string) (Session, error) {
			return failingSession{
				Session:      backend.session(t, ""),
				interruptErr: errors.New("interrupt failed"),
			}, nil
		},
	})
	settle(m, m.open(""))

	settle(m, ask(m, "in-flight question"))
	settle(m, send(m, "esc"))

	if !m.State().Busy {
		t.Error("a failing interrupt cleared busy while an answer was streaming")
	}
	if !m.statusFailed || m.status != "interrupt failed" {
		t.Errorf("the status line says %q failed=%v", m.status, m.statusFailed)
	}
	if len(m.messages) != 1 || m.messages[0].ID != pendingID {
		t.Errorf("a failing interrupt altered messages=%+v", m.messages)
	}
}

func TestAFailingOnOpenDoesNotClearBusyWhileAnAnswerIsStreaming(t *testing.T) {
	backend := newRouter(t)
	release := make(chan struct{})
	m := newModel(t, Options{
		Open: backend.opener(t),
		OnOpen: func(context.Context, Session) error {
			<-release
			return errors.New("on-open failed")
		},
	})
	m.open("")
	_, openCmd := m.Update(opened{generation: m.generation, session: backend.session(t, "")})

	settle(m, ask(m, "in-flight question"))
	close(release)
	for _, msg := range step(openCmd, silence) {
		if _, isActed := msg.(acted); isActed {
			m.Update(msg)
		}
	}

	if !m.State().Busy {
		t.Error("a failing OnOpen cleared busy while an answer was streaming")
	}
	if !m.statusFailed || m.status != "on-open failed" {
		t.Errorf("the status line says %q failed=%v", m.status, m.statusFailed)
	}
}

func TestAFailingRespondClearsBusyAndRollsBackThePendingQuestion(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: func(context.Context, string) (Session, error) {
			return failingSession{
				Session:    backend.session(t, ""),
				respondErr: errors.New("send failed"),
			}, nil
		},
	})
	settle(m, m.open(""))

	settle(m, ask(m, "will fail to send"))

	if m.State().Busy {
		t.Error("a failing Respond left the conversation busy")
	}
	if len(m.messages) != 0 {
		t.Errorf("a failing Respond left phantom messages: %+v", m.messages)
	}
	if m.activeQuestionID != "" {
		t.Errorf("a failing Respond left activeQuestionID=%q", m.activeQuestionID)
	}
	if !m.statusFailed || m.status != "send failed" {
		t.Errorf("the status line says %q failed=%v", m.status, m.statusFailed)
	}
}

func TestAStaleActionFromAReplacedConversationIsIgnored(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{
		Open: backend.opener(t),
		Commands: []Command{{
			Name: "slow",
			Run:  func(context.Context, []string) (string, error) { return "stale status", nil },
		}},
	})
	attach(t, m, failingSession{
		Session:    backend.session(t, ""),
		respondErr: errors.New("stale respond failed"),
	}, stream.ConversationPage{})

	// Capture an in-flight command and an in-flight failing Respond from generation 1.
	staleCmd := ask(m, "/slow")
	staleRespond := ask(m, "question on old session")
	oldGen := m.generation

	// Session 1 ends, then user opens a fresh conversation (generation 2) and asks a question.
	m.Update(received{generation: oldGen, ok: false})
	settle(m, ask(m, "/new"))
	settle(m, ask(m, "question on new session"))

	// Now deliver the stale command and stale failing Respond from generation 1.
	feed(m, step(staleCmd, 3*time.Second))
	feed(m, step(staleRespond, 3*time.Second))

	if !m.State().Busy {
		t.Error("a stale Respond error cleared busy on the new conversation")
	}
	if len(m.messages) != 1 || m.messages[0].Text != "question on new session" {
		t.Errorf("a stale Respond error altered the new conversation's messages: %+v", m.messages)
	}
	if m.activeQuestionID != pendingID {
		t.Errorf("a stale Respond error cleared activeQuestionID on the new conversation: %q", m.activeQuestionID)
	}
	if m.statusFailed || m.status != "Thinking…" {
		t.Errorf("a stale action overwrote status=%q failed=%v", m.status, m.statusFailed)
	}
}

func TestAFailingRespondAfterDisconnectDoesNotReviveTheSession(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{Open: backend.opener(t)})
	attach(t, m, failingSession{
		Session:    backend.session(t, ""),
		respondErr: errors.New("connection closed"),
	}, stream.ConversationPage{})

	respCmd := ask(m, "question during disconnect")
	m.Update(received{generation: m.generation, ok: false})
	feed(m, step(respCmd, 3*time.Second))

	if m.session != nil || m.phase != phaseDisconnected || m.State().Busy {
		t.Fatalf("after disconnect + respond error: session=%v phase=%v busy=%v", m.session != nil, m.phase, m.State().Busy)
	}
	if len(m.messages) != 0 || m.activeQuestionID != "" {
		t.Errorf("pending question was not rolled back: messages=%+v activeQuestionID=%q", m.messages, m.activeQuestionID)
	}
	// Still disconnected: pressing Enter must not submit.
	settle(m, ask(m, "should not submit"))
	if m.State().Busy || len(m.messages) != 0 {
		t.Errorf("conversation accepted a question while disconnected: busy=%v messages=%+v", m.State().Busy, m.messages)
	}
}

func TestAStaleOpenedSessionIsClosed(t *testing.T) {
	backend := newRouter(t)
	m := newModel(t, Options{Open: backend.opener(t)})
	attach(t, m, backend.session(t, ""), stream.ConversationPage{})
	stale := m.generation

	settle(m, ask(m, "/new"))
	current := m.session

	closed := make(chan struct{})
	staleSession := leaving{Session: backend.session(t, ""), closed: closed}
	_, cmd := m.Update(opened{generation: stale, session: staleSession})
	settle(m, cmd)

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("the discarded stale session was never closed")
	}
	if m.session != current {
		t.Error("the current session was replaced by a stale opened message")
	}
	if strings.Contains(m.status, "error") || m.statusFailed {
		t.Errorf("closing a stale session unexpectedly changed status=%q failed=%v", m.status, m.statusFailed)
	}
}
