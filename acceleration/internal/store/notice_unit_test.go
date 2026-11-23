package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNoticeValidate(t *testing.T) {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	for _, made := range []func() (Notice, error){
		LimitNotice{OrganizationID: "org-1", Kind: LimitSpendLimit, Message: "over"}.Notice,
		BalanceNotice{OrganizationID: "org-1", Kind: NoticeBalanceLow, GrantedMicros: 10}.Notice,
		CostAnomaly{OrganizationID: "org-1", Day: day, SpentMicros: 5}.Notice,
	} {
		notice, err := made()
		require.NoError(t, err)
		require.NoError(t, notice.Validate(), notice.Kind)
	}

	for name, test := range map[string]struct {
		notice Notice
		field  string
	}{
		"no organization":             {Notice{Kind: LimitNoCredit, Payload: []byte(`{}`)}, "organization_id"},
		"an unknown kind":             {Notice{OrganizationID: "org-1", Kind: "suspended", Payload: []byte(`{}`)}, "kind"},
		"a money limit with a key":    {Notice{OrganizationID: "org-1", Kind: LimitNoCredit, DedupeKey: "1", Payload: []byte(`{}`)}, "dedupe_key"},
		"a balance keyed by no grant": {Notice{OrganizationID: "org-1", Kind: NoticeBalanceExhausted, DedupeKey: "ten", Payload: []byte(`{}`)}, "dedupe_key"},
		"an anomaly keyed by no day":  {Notice{OrganizationID: "org-1", Kind: NoticeCostAnomaly, DedupeKey: "10/03/2026", Payload: []byte(`{}`)}, "dedupe_key"},
		"a payload that is a list":    {Notice{OrganizationID: "org-1", Kind: LimitNoCredit, Payload: []byte(`[]`)}, "payload"},
		"no payload":                  {Notice{OrganizationID: "org-1", Kind: LimitNoCredit}, "payload"},
	} {
		var invalid *ValidationError
		require.True(t, errors.As(test.notice.Validate(), &invalid), name)
		require.Equal(t, test.field, invalid.Field, name)
	}
}

func TestANoticeIsReadBackInItsKindsShape(t *testing.T) {
	limit := LimitNotice{OrganizationID: "org-1", Kind: LimitSpendHardLimit, AppID: "app-1", Tier: "pro", Enforced: true, Message: "over"}
	notice, err := limit.Notice()
	require.NoError(t, err)
	read, err := notice.LimitNotice()
	require.NoError(t, err)
	require.Equal(t, limit, read)

	anomaly := CostAnomaly{OrganizationID: "org-1", Day: time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), SpentMicros: 500, BaselineMicros: 100}
	notice, err = anomaly.Notice()
	require.NoError(t, err)
	require.Equal(t, "2026-03-10", notice.DedupeKey)
	back, err := notice.CostAnomaly()
	require.NoError(t, err)
	require.Equal(t, anomaly, back)

	balance, err := BalanceNotice{OrganizationID: "org-1", Kind: NoticeBalanceLow, GrantedMicros: 10, BalanceMicros: 4, ThresholdMicros: 5}.Notice()
	require.NoError(t, err)
	require.Equal(t, "10", balance.DedupeKey, "a grant re-arms the warning")
	require.JSONEq(t, `{"balance_micros": 4, "threshold_micros": 5}`, string(balance.Payload))
	warning, err := balance.BalanceNotice()
	require.NoError(t, err)
	require.Equal(t, BalanceNotice{OrganizationID: "org-1", Kind: NoticeBalanceLow, GrantedMicros: 10, BalanceMicros: 4, ThresholdMicros: 5}, warning)
}
