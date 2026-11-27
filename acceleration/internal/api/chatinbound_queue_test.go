//go:build integration

package api

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	chatinboundtasks "github.com/GetStream/Vision-Agents/acceleration/internal/chatinbound/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// ChatInboundQueueSuite exercises async Stream Chat inbound message dispatching on the
// owning shard's events worker.
type ChatInboundQueueSuite struct {
	RouterSuite
	chatInbounds
}

func TestChatInboundQueueSuite(t *testing.T) {
	runSuite(t, new(ChatInboundQueueSuite))
}

func (s *ChatInboundQueueSuite) SetupSuite() {
	s.imageHTTP = &http.Client{}
	s.chatInbounds.keys(&s.RouterSuite)
	s.deploymentKnown = true
	s.database = "chatqueue"
	s.RouterSuite.SetupSuite()
}

func (s *ChatInboundQueueSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.channelID = "chat-" + s.utils.uuid()
	s.answering()
}

func (s *ChatInboundQueueSuite) answering() {
	s.chatInbounds.answering(&s.RouterSuite)
}

func (s *ChatInboundQueueSuite) handed(worker <-chan dispatch.Message) dispatch.Message {
	select {
	case message := <-worker:
		return message
	case <-time.After(settleFor):
		s.FailNow("the message never reached the worker")
		return dispatch.Message{}
	}
}

func (s *ChatInboundQueueSuite) queueTask(body string) chatinboundtasks.DispatchMessage {
	return chatinboundtasks.DispatchMessage{
		StreamAppPK: suiteStreamApp,
		MessageID:   "queued-" + s.utils.uuid(),
		Deployment:  true,
		Body:        []byte(body),
	}
}

func (s *ChatInboundQueueSuite) inAppMode() string {
	customer := streamapp.CustomerOf(suiteStreamApp)
	s.setApps(customer, func(apps *suiteApps) { apps.perApp = true })
	s.T().Cleanup(func() {
		s.setApps(customer, func(apps *suiteApps) {
			apps.perApp = false
			delete(apps.waiting, customer)
			delete(apps.readOnly, customer)
		})
	})
	return customer
}

func (s *ChatInboundQueueSuite) deafWorkers(logger *slog.Logger) *node.Workers {
	listener := httptest.NewUnstartedServer(nil)
	s.T().Cleanup(listener.Close)
	directory, bus := s.nodeDirectory(listener, s.otherShard, logger), s.workBus(logger)
	var cancel context.CancelFunc
	workers, err := redisready.Retry(s.redisAddr, func() (*node.Workers, error) {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		workers, err := node.NewWorkers(ctx, dispatch.NewPool(), directory, bus, logger)
		if err != nil {
			cancel()
		}
		return workers, err
	})
	s.Require().NoError(err)
	redis := s.live.Redis()
	pattern := "*replies:" + listener.Listener.Addr().String()
	subscribed, err := redis.Do(context.Background(), redis.B().PubsubChannels().Pattern(pattern).Build()).AsStrSlice()
	s.Require().NoError(err)
	s.Require().Len(subscribed, 1, "the node hears its peers' replies on no channel named so")
	cancel()
	s.Require().Eventually(func() bool {
		subscribed, err := redis.Do(context.Background(), redis.B().PubsubChannels().Pattern(pattern).Build()).AsStrSlice()
		return err == nil && len(subscribed) == 0
	}, settleFor, 20*time.Millisecond, "the node still hears its peers' replies")
	return workers
}

// loggedText is a concurrency-safe buffer for capturing slog output in integration tests.
type loggedText struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *loggedText) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *loggedText) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
