//go:build integration

package store

import (
	"errors"
	"fmt"
	"time"
)

// START lifts what STOP did, and the record of both stays.
func (s *StoreSuite) TestAnOptOutFromEveryChannelBlocksEachUntilRevoked() {
	customerID := newID()
	first := OptOut{CustomerID: customerID, Recipient: "+15551230000", Channel: OptOutAll, Source: "api"}
	again := first
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &first))
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &again))
	s.Equal(first.ID, again.ID, "asking twice keeps the first")

	blocked, err := s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551230000", "sms")
	s.Require().NoError(err)
	s.True(blocked)

	s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "+15551230000", "sms"))
	blocked, err = s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551230000", "voice")
	s.Require().NoError(err)
	s.False(blocked)
	s.True(errors.Is(s.store.OptOuts.Revoke(s.ctx, customerID, first.ID), ErrUnknownOptOut), "already revoked")
}

func (s *StoreSuite) TestOptOutQueryBudgets() {
	customerID := newID()
	for i := range 4 {
		s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &OptOut{
			CustomerID: customerID, Recipient: fmt.Sprintf("+1555123000%d", i), Channel: "sms", Source: "api",
		}))
	}
	optOut := OptOut{CustomerID: customerID, Recipient: "+15551239999", Channel: "sms", Source: "api"}
	s.Equal(2, s.countQueries(func() { s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &optOut)) }),
		"Insert writes, then reads back whichever opt-out was kept")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.OptOuts.Select(s.ctx, customerID, 10, nil)
		s.Require().NoError(err)
		s.Len(listed, 5)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		opted, err := s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551239999", "sms")
		s.Require().NoError(err)
		s.True(opted)
	}), "OptedOut")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.OptOuts.Revoke(s.ctx, customerID, optOut.ID)) }), "Revoke")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "+15551230000", "sms"))
	}), "RevokeAll")
}

// Catches an opt-out written, matched or lifted under the number as it was given: each of
// Insert, OptedOut and RevokeAll reads it in E.164, and an iMessage email handle as it is.
func (s *StoreSuite) TestAnOptOutIsKeptAndMatchedInE164HoweverItsNumberWasWritten() {
	customerID := newID()
	optOut := OptOut{CustomerID: customerID, Recipient: "12025550123", Channel: "sms", Source: "keyword"}
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &optOut))
	s.Equal("+12025550123", optOut.Recipient)

	for _, dialled := range []string{"+12025550123", "1 (202) 555-0123"} {
		blocked, err := s.store.OptOuts.OptedOut(s.ctx, customerID, dialled, "sms")
		s.Require().NoError(err)
		s.True(blocked, dialled)
	}
	s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "1 202 555 0123", "sms"))
	s.False(s.optedOut(customerID, "+12025550123", "sms"), "a START from the number written another way lifts it")

	handle := OptOut{CustomerID: customerID, Recipient: "jane@icloud.com", Channel: "imessage", Source: "keyword"}
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &handle))
	s.Equal("jane@icloud.com", handle.Recipient)
	s.True(s.optedOut(customerID, "jane@icloud.com", "imessage"))
}

// Catches a row an older router stored as the sender wrote it no longer matching once the new
// one reads recipients in E.164, before the backfill has run: a STOP from them still refuses
// them, and a START written the same way still lifts it.
func (s *StoreSuite) TestAnOptOutStoredAsWrittenStillMatchesBeforeTheBackfill() {
	customerID := s.storeRawOptOuts(OptOut{ID: "a", Recipient: "12025550131", Channel: "sms"})

	s.True(s.optedOut(customerID, "12025550131", "sms"))
	s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "12025550131", "sms"))
	s.False(s.optedOut(customerID, "12025550131", "sms"))
}

// Catches the backfill leaving a number not in E.164 unmatched, counting a row it only trimmed
// as rewritten, or touching a revoked row's time.
func (s *StoreSuite) TestNormalizeMakesAnOptOutStoredBeforeE164MatchTheNumberDialled() {
	s.normalize(100) // what other tests left, so the counts below are this test's
	revoked := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	customerID := s.storeRawOptOuts(
		OptOut{ID: "a", Recipient: "12025550123", Channel: "sms"},
		OptOut{ID: "b", Recipient: " +12025550130", Channel: "sms"},
		OptOut{ID: "c", Recipient: "12025550133", Channel: "voice", RevokedAt: &revoked},
		OptOut{ID: "d", Recipient: "447911123456", Channel: "whatsapp"},
	)
	s.Require().False(s.optedOut(customerID, "+12025550123", "sms"), "before the backfill the stored number does not match")

	done := s.normalize(100)

	s.True(s.optedOut(customerID, "+12025550123", "sms"), "a dial to the number in E.164 is refused")
	s.Equal(map[string]string{"a": "sms +12025550123", "b": "sms +12025550130", "d": "whatsapp +447911123456"},
		s.liveOptOuts(customerID))
	rows := s.optOutRows(customerID)
	s.Equal("+12025550133", rows["c"].Recipient, "a revoked row is rewritten too")
	s.True(revoked.Equal(*rows["c"].RevokedAt), "and keeps when it was revoked")
	s.Equal(3, done.Rewritten, "a, c and d; b was only trimmed")
	s.Zero(done.Revoked)
}

// Catches the backfill breaking the one live opt-out per recipient and channel: beside one
// already in E.164, two in one batch, or three at once, which it must count once each; and
// revoking again a row revoked already.
func (s *StoreSuite) TestNormalizeRevokesARowItWouldMakeASecondLiveOne() {
	s.normalize(100) // what other tests left, so the count below is this test's
	revoked := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	customerID := s.storeRawOptOuts(
		OptOut{ID: "a", Recipient: "+12025550124", Channel: "sms"},
		OptOut{ID: "b", Recipient: "12025550124", Channel: "sms"},
		OptOut{ID: "c", Recipient: "12025550124", Channel: "sms", RevokedAt: &revoked},
		OptOut{ID: "d", Recipient: "12025550125", Channel: "voice"},
		OptOut{ID: "e", Recipient: "1 (202) 555-0125", Channel: "voice"},
		OptOut{ID: "f", Recipient: "+12025550126", Channel: "voice"},
		OptOut{ID: "g", Recipient: "12025550126", Channel: "voice"},
		OptOut{ID: "h", Recipient: "1 202 555 0126", Channel: "voice"},
	)

	done := s.normalize(100)

	s.Equal(map[string]string{"a": "sms +12025550124", "d": "voice +12025550125", "f": "voice +12025550126"},
		s.liveOptOuts(customerID))
	rows := s.optOutRows(customerID)
	s.Len(rows, 8, "no row is dropped")
	for _, id := range []string{"b", "e", "g", "h"} {
		s.NotNil(rows[id].RevokedAt, id)
	}
	s.Equal("+12025550124", rows["b"].Recipient, "a row revoked as a duplicate is kept, in E.164")
	s.True(revoked.Equal(*rows["c"].RevokedAt), "a revoked row beside a live one keeps when it was revoked")
	s.Equal(4, done.Revoked, "b, e, g and h, each once")
}

// Catches the backfill bringing back an opt-out the person lifted: a START since recorded in
// E.164 lifted the row kept there, not the one stored as written. One lifted before the row
// was recorded lifts nothing of it.
func (s *StoreSuite) TestNormalizeRevokesALegacyRowTheRecipientLiftedSince() {
	recorded := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	later, earlier := recorded.Add(30*time.Minute), recorded.Add(-30*time.Minute)
	customerID := s.storeRawOptOuts(
		OptOut{ID: "a", Recipient: "+12025550127", Channel: "sms", RevokedAt: &later},
		OptOut{ID: "b", Recipient: "12025550127", Channel: "sms"},
		OptOut{ID: "c", Recipient: "+12025550128", Channel: "sms", RevokedAt: &earlier},
		OptOut{ID: "d", Recipient: "12025550128", Channel: "sms"},
	)

	s.normalize(100)

	s.Equal(map[string]string{"d": "sms +12025550128"}, s.liveOptOuts(customerID))
	s.False(s.optedOut(customerID, "+12025550127", "sms"), "lifted after the STOP")
	s.True(s.optedOut(customerID, "+12025550128", "sms"), "lifted before the STOP")
}

// Catches the backfill rewriting, or stalling on, a row it cannot read: a national number that
// names no country is kept as written and reported, an email handle is kept and counted, and
// the batches go on past both.
func (s *StoreSuite) TestNormalizeKeepsWhatItCannotReadAndGoesOn() {
	customerID := s.storeRawOptOuts(
		OptOut{ID: "a", Recipient: "(202) 555-0129", Channel: "sms"},
		OptOut{ID: "b", Recipient: "jane@icloud.com", Channel: "imessage"},
		OptOut{ID: "c", Recipient: "12025550129", Channel: "voice"},
	)
	var unread []string
	done, err := s.store.OptOuts.Normalize(s.ctx, 1, func(page []OptOut) {
		for _, optOut := range page {
			if optOut.CustomerID == customerID {
				unread = append(unread, optOut.ID)
			}
		}
	})
	s.Require().NoError(err)

	s.Equal([]string{customerID + "-a"}, unread, "what looks like a number is reported")
	s.Equal(map[string]string{"a": "sms (202) 555-0129", "b": "imessage jane@icloud.com", "c": "voice +12025550129"},
		s.liveOptOuts(customerID))
	s.Positive(done.Handles, "the handle is counted")
}

// Catches a second run changing anything.
func (s *StoreSuite) TestNormalizeTwiceChangesNothingTheSecondTime() {
	customerID := s.storeRawOptOuts(
		OptOut{ID: "a", Recipient: "12025550134", Channel: "sms"},
		OptOut{ID: "b", Recipient: "+12025550134", Channel: "voice"},
		OptOut{ID: "c", Recipient: "12025550134", Channel: "voice"},
	)
	first := s.normalize(100)
	s.Require().Equal(map[string]string{"a": "sms +12025550134", "b": "voice +12025550134"}, s.liveOptOuts(customerID))
	s.Require().Positive(first.Rewritten)

	again := s.normalize(100)
	s.Zero(again.Rewritten)
	s.Zero(again.Revoked)
	s.Equal(map[string]string{"a": "sms +12025550134", "b": "voice +12025550134"}, s.liveOptOuts(customerID))
}

// Catches a STOP recorded while the backfill rewrites a row to the same recipient failing the
// backfill on the one-live-row index: the batch is read again, and the row stored as written
// is revoked beside the STOP's.
func (s *StoreSuite) TestNormalizeTriesABatchAgainWhenAStopTakesItsRecipient() {
	customerID := s.storeRawOptOuts(OptOut{ID: "a", Recipient: "12025550135", Channel: "sms"})
	stop, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer stop.Rollback() //nolint:errcheck // committed below
	_, err = stop.NewInsert().Model(&OptOut{ID: customerID + "-stop", CustomerID: customerID, Recipient: "+12025550135",
		Channel: "sms", Source: "keyword", CreatedAt: time.Now().UTC()}).Exec(s.ctx)
	s.Require().NoError(err)

	finished := make(chan error, 1)
	go func() {
		_, err := s.store.OptOuts.Normalize(s.ctx, 100, func([]OptOut) {})
		finished <- err
	}()
	s.Require().Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.global.NewRaw(
			"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'").
			Scan(s.ctx, &waiting))
		return waiting > 0
	}, 10*time.Second, 10*time.Millisecond, "the backfill waits on the STOP's row")
	s.Require().NoError(stop.Commit())

	s.Require().NoError(<-finished)
	s.Equal(map[string]string{"stop": "sms +12025550135"}, s.liveOptOuts(customerID))
}

// storeRawOptOuts stores opt-outs as an older router left them, recipients as written, under
// a customer of their own, each id prefixed with it; it returns the customer.
func (s *StoreSuite) storeRawOptOuts(rows ...OptOut) string {
	customerID := newID()
	recorded := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	for i := range rows {
		rows[i].ID = customerID + "-" + rows[i].ID
		rows[i].CustomerID, rows[i].Source, rows[i].CreatedAt = customerID, "api", recorded
	}
	_, err := s.store.global.NewInsert().Model(&rows).Exec(s.ctx)
	s.Require().NoError(err)
	return customerID
}

// normalize runs the backfill in batches of batch rows.
func (s *StoreSuite) normalize(batch int) NormalizedOptOuts {
	done, err := s.store.OptOuts.Normalize(s.ctx, batch, func([]OptOut) {})
	s.Require().NoError(err)
	return done
}

// optedOut is OptOuts.OptedOut, failing the test on an error.
func (s *StoreSuite) optedOut(customerID, recipient, channel string) bool {
	blocked, err := s.store.OptOuts.OptedOut(s.ctx, customerID, recipient, channel)
	s.Require().NoError(err)
	return blocked
}

// optOutRows are a customer's opt-outs, live and revoked, by the id storeRawOptOuts was given.
func (s *StoreSuite) optOutRows(customerID string) map[string]OptOut {
	var rows []OptOut
	s.Require().NoError(s.store.global.NewSelect().Model(&rows).Where("customer_id = ?", customerID).Scan(s.ctx))
	byID := map[string]OptOut{}
	for _, row := range rows {
		byID[row.ID[len(customerID)+1:]] = row
	}
	return byID
}

// liveOptOuts are a customer's live opt-outs, as "channel recipient" by short id.
func (s *StoreSuite) liveOptOuts(customerID string) map[string]string {
	live := map[string]string{}
	for id, row := range s.optOutRows(customerID) {
		if row.RevokedAt == nil {
			live[id] = row.Channel + " " + row.Recipient
		}
	}
	return live
}
