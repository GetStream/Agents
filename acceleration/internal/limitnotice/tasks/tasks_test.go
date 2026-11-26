package tasks_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	lntasks "github.com/GetStream/Vision-Agents/acceleration/internal/limitnotice/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type fakeSender struct {
	sent  []store.Notice
	swept int
}

func (f *fakeSender) SendNotice(_ context.Context, n store.Notice) error {
	f.sent = append(f.sent, n)
	return nil
}

func (f *fakeSender) SweepOnce(_ context.Context) error {
	f.swept++
	return nil
}

type fakeChecker struct {
	checked int
}

func (f *fakeChecker) CheckOnce(_ context.Context) error {
	f.checked++
	return nil
}

func TestLimitNoticeTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	sender := &fakeSender{}
	checker := &fakeChecker{}
	lntasks.Register(tr.Registry(), lntasks.Deps{Sender: sender, Checker: checker})

	_, err := async.Enqueue(context.Background(), tr, lntasks.PostNotice{
		OrganizationID: "org-1",
		Kind:           store.LimitSpendLimit,
		Payload:        json.RawMessage(`{"limit":100}`),
	})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, lntasks.SweepNotices{})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, lntasks.CheckAnomalies{})
	require.NoError(t, err)

	require.Len(t, sender.sent, 1)
	assert.Equal(t, "org-1", sender.sent[0].OrganizationID)
	assert.Equal(t, 1, sender.swept)
	assert.Equal(t, 1, checker.checked)
}
