package routing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
)

// asked is work that used one request.
type asked struct{}

func (asked) Used() pricing.Quantities { return pricing.Quantities{pricing.Requests: 1} }

func TestACallWithNoRecorderIsStillAnswered(t *testing.T) {
	result, err := Call(context.Background(), Meter{}, func(context.Context) (asked, error) { return asked{}, nil })

	require.NoError(t, err)
	require.Equal(t, asked{}, result)
}

func TestAStatTakesWhatTheWorkUsedIntoItsColumnsAndMeters(t *testing.T) {
	var stat Stat
	stat.Use(pricing.Quantities{
		pricing.InputTokens:           3,
		pricing.CachedInputTokens:     11,
		pricing.CacheWriteInputTokens: 2_386,
		pricing.OutputTokens:          22,
		pricing.Requests:              1,
		pricing.OutputAudioTokens:     40,
		pricing.Seconds:               0,
	})

	require.Equal(t, Usage{InputTokens: 2_400, CachedInputTokens: 11, OutputTokens: 22, Requests: 1}, stat.Usage)
	require.Equal(t, pricing.Quantities{pricing.CacheWriteInputTokens: 2_386, pricing.OutputAudioTokens: 40}, stat.Meters)
}

// hooked is every call a Meter.Recorded hook was told of: the stat and what its row costs.
type hooked struct {
	stats []Stat
	costs []int64
}

func (t *hooked) hook(stat Stat, costMicros int64) {
	t.stats, t.costs = append(t.stats, stat), append(t.costs, costMicros)
}

// toldMeter is a meter of a model priced at 2 US dollars a million input tokens, whose
// recorder writes nothing, with a hook that keeps what it is told.
func toldMeter(t *testing.T) (Meter, *hooked) {
	config := ProviderConfig{Provider: "vendor", Model: "told", Price: Price{PerMillionInputTokens: 2}}
	logger := slog.New(slog.DiscardHandler)
	prices, err := Prices(Config{LLM: {Providers: []ProviderConfig{config}}}, logger)
	require.NoError(t, err)
	recorder := NewRecorder(LLM, nil, nil, prices, logger)
	t.Cleanup(recorder.Close)
	hook := &hooked{}
	return Meter{Recorder: recorder, Config: config, Owner: Owner{CustomerID: "acme", CallID: "default:c1"},
		Code: func(error) string { return "vendor_refused" }, Recorded: hook.hook}, hook
}

// inputTokens is work that read n input tokens.
type inputTokens int64

func (n inputTokens) Used() pricing.Quantities {
	return pricing.Quantities{pricing.InputTokens: int64(n)}
}

func TestARecordedHookIsToldWhatTheCallUsedAndWhatItsRowCosts(t *testing.T) {
	meter, hook := toldMeter(t)

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 1_000, nil })

	require.NoError(t, err)
	require.Len(t, hook.stats, 1, "one call is told once")
	stat := hook.stats[0]
	require.True(t, stat.Success)
	require.Empty(t, stat.ErrorCode)
	require.EqualValues(t, 1_000, stat.InputTokens)
	require.Equal(t, "default:c1", stat.CallID)
	require.Equal(t, []int64{2_000}, hook.costs, "a thousand tokens at 2 US dollars a million")
}

func TestARecordedHookIsToldOfAFailureWithItsCodeAndNoCost(t *testing.T) {
	meter, hook := toldMeter(t)

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) {
		return 1_000, errors.New("vendor said no")
	})

	require.Error(t, err)
	require.Len(t, hook.stats, 1)
	require.False(t, hook.stats[0].Success)
	require.Equal(t, "vendor_refused", hook.stats[0].ErrorCode)
	require.Equal(t, "vendor said no", hook.stats[0].ErrorMessage)
	require.Zero(t, hook.stats[0].InputTokens, "a failure is recorded without quantities")
	require.Equal(t, []int64{0}, hook.costs)
}

func TestARecordedHookIsToldOfAPanicBeforeItCarriesOn(t *testing.T) {
	meter, hook := toldMeter(t)

	require.PanicsWithValue(t, "vendor fell over", func() {
		_, _ = Call(context.Background(), meter, func(context.Context) (inputTokens, error) { panic("vendor fell over") })
	})

	require.Len(t, hook.stats, 1)
	require.Equal(t, "panic", hook.stats[0].ErrorCode)
}

func TestARecordedHookIsNotToldOfWorkTheMeterOmits(t *testing.T) {
	meter, hook := toldMeter(t)
	meter.Omit = func(used pricing.Quantities) bool { return used[pricing.InputTokens] == 0 }

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 0, nil })
	require.NoError(t, err)
	_, err = Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 5, nil })
	require.NoError(t, err)

	require.Len(t, hook.stats, 1, "only the work with a row is told")
	require.EqualValues(t, 5, hook.stats[0].InputTokens)
}

func TestARecordedHookOfAMeterWithNoRecorderIsToldItCostsNothing(t *testing.T) {
	hook := &hooked{}

	_, err := Call(context.Background(), Meter{Recorded: hook.hook}, func(context.Context) (inputTokens, error) { return 1_000, nil })

	require.NoError(t, err)
	require.Len(t, hook.stats, 1, "a router built without a recorder still logs its line")
	require.Equal(t, []int64{0}, hook.costs)
}

func TestACallWithNoRecordedHookLogsNothing(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })
	meter, _ := toldMeter(t)
	meter.Recorded = nil
	recorder := NewRecorder(LLM, nil, nil, meter.Recorder.pricer, logger)
	t.Cleanup(recorder.Close)
	meter.Recorder = recorder

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 1_000, nil })
	require.NoError(t, err)
	_, err = Call(context.Background(), meter, func(context.Context) (inputTokens, error) {
		return 0, errors.New("vendor said no")
	})
	require.Error(t, err)

	require.Empty(t, logs.String(), "the other callers of Call log no line of their own")
}
