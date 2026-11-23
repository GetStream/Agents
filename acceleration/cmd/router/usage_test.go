package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

func TestAUsageTokenWithoutTheServiceWarnsAndShipsNothing(t *testing.T) {
	var logs bytes.Buffer
	settings := config.Defaults()
	settings.Usage.Token = "the-ingest-token"

	client, err := newUsageClient(settings, nil, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, err)
	require.Nil(t, client)
	require.Contains(t, logs.String(), "level=WARN")
	require.Contains(t, logs.String(), "usage token is set without the usage service's URL")
	require.NotContains(t, logs.String(), "the-ingest-token")
}

func TestAStorageRunRecordsTheLastHourToHaveEnded(t *testing.T) {
	ended := time.Date(2026, 10, 23, 13, 0, 0, 0, time.UTC)
	for _, now := range []time.Time{ended.Add(time.Hour + 17*time.Minute), ended.Add(time.Hour + 59*time.Minute)} {
		hour, err := storageHourOf("", now)
		require.NoError(t, err)
		require.Equal(t, ended, hour, "the run at %s and its retries within the hour bill the same hour", now)
	}

	hour, err := storageHourOf("2026-10-23T09:30:00+02:00", time.Now())
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 10, 23, 7, 0, 0, 0, time.UTC), hour, "--hour names the hour it falls in")

	_, err = storageHourOf("yesterday", time.Now())
	require.ErrorContains(t, err, "RFC 3339")
}

// encodedRow is a row of another customer's, as a lost-row line's row_hex holds it.
func encodedRow(t *testing.T) (uuid.UUID, string) {
	other := store.Request{UsageID: uuid.Must(uuid.NewV7()), CustomerID: "app-other", APIKey: "key-other", Provider: "openai", Model: "gpt"}
	row, err := json.Marshal(other)
	require.NoError(t, err)
	return other.UsageID, hex.EncodeToString(row)
}

func TestALostRowIsReadFromItsOwnFieldAndNotFromTheErrorAfterIt(t *testing.T) {
	lost := store.Request{UsageID: uuid.Must(uuid.NewV7()), CustomerID: "app-1", APIKey: "key-a", Provider: "openai", Model: "gpt"}
	row, err := json.Marshal(lost)
	require.NoError(t, err)
	_, other := encodedRow(t)
	var log bytes.Buffer
	// As the writer logs a row Postgres refused, with the refusal after it.
	newLogger(&log, slog.LevelInfo).Error("usage row lost", "reason", "invalid", "usage_id", lost.UsageID,
		"row_hex", hex.EncodeToString(row), "error", errors.New("refused row_hex="+other+" here"))

	rows, unreadable, err := lostRows(&log, false)

	require.NoError(t, err)
	require.Zero(t, unreadable)
	require.Len(t, rows, 1)
	require.Equal(t, lost.UsageID, rows[0].UsageID)
	require.Equal(t, "key-a", rows[0].APIKey)
}

func TestALineWithTheRowAsJSONIsUnreadableWhateverItsTextHolds(t *testing.T) {
	_, other := encodedRow(t)
	lost := store.Request{
		UsageID: uuid.Must(uuid.NewV7()), CustomerID: "app-1", APIKey: "key-a", Provider: "openai", Model: "gpt",
		Tags: map[string]string{"note": "x row_hex=" + other + " y"},
	}
	row, err := json.Marshal(lost)
	require.NoError(t, err)
	var log bytes.Buffer
	// As a router from before row_hex logged a lost row.
	newLogger(&log, slog.LevelInfo).Error("usage row lost", "reason", "shutdown", "usage_id", lost.UsageID, "row", string(row))
	require.Contains(t, log.String(), " row_hex=")

	rows, unreadable, err := lostRows(&log, false)

	require.NoError(t, err)
	require.Empty(t, rows, "the row in a tag is not read as the lost row")
	require.Equal(t, 1, unreadable)
}
