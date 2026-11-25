package store

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAgentLogValidate(t *testing.T) {
	valid := func() AgentLog {
		return AgentLog{CustomerID: "acme", Source: "agent", Severity: "warn", EventType: "turn"}
	}
	log := valid()
	require.NoError(t, log.Validate())

	for field, broken := range map[string]func(*AgentLog){
		"customer_id": func(l *AgentLog) { l.CustomerID = "" },
		"source":      func(l *AgentLog) { l.Source = "" },
		"severity":    func(l *AgentLog) { l.Severity = "debug" },
		"event_type":  func(l *AgentLog) { l.EventType = "" },
	} {
		log := valid()
		broken(&log)
		var invalid *ValidationError
		require.True(t, errors.As(log.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}

func TestALogCursorOfTheSharedDatabaseAloneIsTheBareIdEveryCursorWasBefore(t *testing.T) {
	bare := base64.RawURLEncoding.EncodeToString([]byte("42"))

	require.Equal(t, bare, AgentLogMark{0: 42}.Cursor())
	mark, err := ParseAgentLogCursor(bare)
	require.NoError(t, err)
	require.Equal(t, AgentLogMark{0: 42}, mark)
}

func TestALogCursorCarriesTheMarkOfEveryRange(t *testing.T) {
	mark := AgentLogMark{0: 5, 1: 1<<logRangeBits + 9, 3: 3<<logRangeBits + 1}

	read, err := ParseAgentLogCursor(mark.Cursor())
	require.NoError(t, err)
	require.Equal(t, mark, read)
}

func TestALogCursorNamingARangeTwiceIsRefused(t *testing.T) {
	_, err := ParseAgentLogCursor(base64.RawURLEncoding.EncodeToString([]byte("5,7")))
	require.Error(t, err)
}

// TestPagesOfLogsMissNoRowIngestedOutOfItsIdsOrder pages through two databases, one holding a
// row ingested before the one numbered before it, as Insert stamps a row before it takes the
// lock that numbers it. A page cut by ingestion time alone would leave id 3 behind its mark.
func TestPagesOfLogsMissNoRowIngestedOutOfItsIdsOrder(t *testing.T) {
	at := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	shared := []AgentLog{{ID: 3, IngestedAt: at.Add(time.Second)}, {ID: 2, IngestedAt: at.Add(5 * time.Second)}, {ID: 1, IngestedAt: at}}
	shard := []AgentLog{{ID: 1<<logRangeBits + 1, IngestedAt: at.Add(4 * time.Second)}}
	below := func(rows []AgentLog, mark AgentLogMark) []AgentLog {
		var kept []AgentLog
		for _, row := range rows {
			if bound, ok := mark[logRange(row.ID)]; !ok || row.ID < bound {
				kept = append(kept, row)
			}
		}
		return kept
	}

	var read []int64
	var mark AgentLogMark
	for range 5 {
		page := interleave([][]AgentLog{below(shared, mark), below(shard, mark)}, mark, false, 2)
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			read = append(read, row.ID)
		}
		mark = page[len(page)-1].Mark
	}
	require.ElementsMatch(t, []int64{3, 2, 1, 1<<logRangeBits + 1}, read)
	require.Len(t, read, 4)
}
