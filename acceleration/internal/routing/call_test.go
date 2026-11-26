package routing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
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

func TestAStatCountsHourLongCacheWritesAsPartOfThePrompt(t *testing.T) {
	var stat Stat
	used := pricing.Quantities{
		pricing.InputTokens:             3,
		pricing.CacheWriteInputTokens:   386,
		pricing.CacheWrite1hInputTokens: 2_000,
	}
	stat.Use(used)

	require.Equal(t, int64(2_389), stat.Usage.InputTokens)
	row := store.Request{InputTokens: stat.Usage.InputTokens, Meters: stat.Meters}
	require.Equal(t, used, row.Quantities(), "the row it is stored as is priced as the work was")
}

// hooked is every call a Meter.Recorded hook was told of: the stat and what its row costs.
type hooked struct {
	stats []Stat
	costs []int64
}

func (t *hooked) hook(stat Stat, costMicros int64) {
	t.stats, t.costs = append(t.stats, stat), append(t.costs, costMicros)
}

// inputTokens is work that read n input tokens.
type inputTokens int64

func (n inputTokens) Used() pricing.Quantities {
	return pricing.Quantities{pricing.InputTokens: int64(n)}
}

// CallHookSuite calls vendors through Call with a Meter.Recorded hook that keeps what it is told,
// on a meter of a model priced at 2 US dollars a million input tokens, whose recorder writes
// nothing.
type CallHookSuite struct {
	suite.Suite
	meter Meter
	hook  *hooked
}

func TestCallHookSuite(t *testing.T) {
	suite.Run(t, new(CallHookSuite))
}

func (s *CallHookSuite) SetupTest() {
	config := ProviderConfig{Provider: "vendor", Model: "told", Price: Price{PerMillionInputTokens: 2}}
	logger := slog.New(slog.DiscardHandler)
	prices, err := Prices(Config{LLM: {Providers: []ProviderConfig{config}}}, logger)
	s.Require().NoError(err)
	recorder := NewRecorder(LLM, nil, nil, prices, logger)
	s.T().Cleanup(recorder.Close)
	s.hook = &hooked{}
	s.meter = Meter{Recorder: recorder, Config: config, Owner: Owner{CustomerID: "acme", CallID: "default:c1"},
		Code: func(error) string { return "vendor_refused" }, Recorded: s.hook.hook}
}

func (s *CallHookSuite) TestARecordedHookIsToldWhatTheCallUsedAndWhatItsRowCosts() {
	meter, hook := s.meter, s.hook

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 1_000, nil })

	s.Require().NoError(err)
	s.Len(hook.stats, 1, "one call is told once")
	stat := hook.stats[0]
	s.True(stat.Success)
	s.Empty(stat.ErrorCode)
	s.EqualValues(1_000, stat.InputTokens)
	s.Equal("default:c1", stat.CallID)
	s.Equal([]int64{2_000}, hook.costs, "a thousand tokens at 2 US dollars a million")
}

func (s *CallHookSuite) TestARecordedHookIsToldOfAFailureWithItsCodeAndNoCost() {
	meter, hook := s.meter, s.hook

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) {
		return 1_000, errors.New("vendor said no")
	})

	s.Require().Error(err)
	s.Len(hook.stats, 1)
	s.False(hook.stats[0].Success)
	s.Equal("vendor_refused", hook.stats[0].ErrorCode)
	s.Equal("vendor said no", hook.stats[0].ErrorMessage)
	s.Zero(hook.stats[0].InputTokens, "a failure is recorded without quantities")
	s.Equal([]int64{0}, hook.costs)
}

func (s *CallHookSuite) TestARecordedHookIsToldOfAPanicBeforeItCarriesOn() {
	meter, hook := s.meter, s.hook

	s.PanicsWithValue("vendor fell over", func() {
		_, _ = Call(context.Background(), meter, func(context.Context) (inputTokens, error) { panic("vendor fell over") })
	})

	s.Len(hook.stats, 1)
	s.Equal("panic", hook.stats[0].ErrorCode)
}

func (s *CallHookSuite) TestARecordedHookIsNotToldOfWorkTheMeterOmits() {
	meter, hook := s.meter, s.hook
	meter.Omit = func(used pricing.Quantities) bool { return used[pricing.InputTokens] == 0 }

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 0, nil })
	s.Require().NoError(err)
	_, err = Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 5, nil })
	s.Require().NoError(err)

	s.Len(hook.stats, 1, "only the work with a row is told")
	s.EqualValues(5, hook.stats[0].InputTokens)
}

func (s *CallHookSuite) TestARecordedHookOfAMeterWithNoRecorderIsToldItCostsNothing() {
	hook := &hooked{}

	_, err := Call(context.Background(), Meter{Recorded: hook.hook}, func(context.Context) (inputTokens, error) { return 1_000, nil })

	s.Require().NoError(err)
	s.Len(hook.stats, 1, "a router built without a recorder still logs its line")
	s.Equal([]int64{0}, hook.costs)
}

func (s *CallHookSuite) TestACallWithNoRecordedHookLogsNothing() {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	previous := slog.Default()
	slog.SetDefault(logger)
	s.T().Cleanup(func() { slog.SetDefault(previous) })
	meter := s.meter
	meter.Recorded = nil
	recorder := NewRecorder(LLM, nil, nil, meter.Recorder.pricer, logger)
	s.T().Cleanup(recorder.Close)
	meter.Recorder = recorder

	_, err := Call(context.Background(), meter, func(context.Context) (inputTokens, error) { return 1_000, nil })
	s.Require().NoError(err)
	_, err = Call(context.Background(), meter, func(context.Context) (inputTokens, error) {
		return 0, errors.New("vendor said no")
	})
	s.Require().Error(err)

	s.Empty(logs.String(), "the other callers of Call log no line of their own")
}
