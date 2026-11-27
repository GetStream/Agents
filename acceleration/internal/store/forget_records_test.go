//go:build integration

package store

import (
	"slices"
	"strings"
	"time"
)

// forgetUser runs the shared database's step of forgetting app's user, as forgotten, and checks
// that a dry run counted what it changed.
func (s *StoreSuite) forgetUser(app, user string) UserSettings {
	counted, err := s.store.CountUserSettings(s.ctx, app, user)
	s.Require().NoError(err)
	counts, err := s.store.ForgetUserSettings(s.ctx, app, user, "forgotten", &AuditEntry{
		CustomerID: app, ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().NoError(err)
	s.Equal(counted, counts, "a dry run counts what the forget changes")
	return counts
}

// countOf counts the rows of the shared database that query, with its arguments, picks.
func (s *StoreSuite) countOf(query string, args ...any) int {
	var count int
	s.Require().NoError(s.store.global.QueryRowContext(s.ctx, "SELECT count(*) FROM "+query, args...).Scan(&count))
	return count
}

// contactRow is a contact map row as it is now, by its id.
func (s *StoreSuite) contactRow(id string) ContactMapEntry {
	var row ContactMapEntry
	s.Require().NoError(s.store.global.NewSelect().Model(&row).Where("id = ?", id).Scan(s.ctx))
	return row
}

func (s *StoreSuite) TestForgettingAUserDeletesTheirPhoneLinksAndTheNumberKeepsNoneOfTheirHistory() {
	s.agentConfigs()
	chat := s.made(streamUser("anya", "agent:omni-chat"))
	sms := s.made(contact("+15550100", "agent:omni-sms"))
	s.Require().NoError(s.link("anya", "+15550100"))
	s.Require().Equal(s.omniOf(chat), s.omniOf(sms), "the link joined the number's row to the user's")
	s.Require().NoError(s.link("grace", "+15550101"))
	s.Require().NoError(s.store.UserPhones.Link(s.ctx, &UserPhone{CustomerID: "other-app", StreamAppPK: 4242, UserID: "anya", Phone: "+15550100"}))
	s.Require().Equal(1, s.countOf("user_phone_history WHERE customer_id = 'acme-app' AND user_id = 'anya'"))

	counts := s.forgetUser("acme-app", "anya")

	s.EqualValues(1, counts.Phones)
	_, _, linked, err := s.store.UserPhones.Linked(s.ctx, "acme-app", 4242, "anya")
	s.Require().NoError(err)
	s.False(linked)
	s.Zero(s.countOf("user_phone_history WHERE customer_id = 'acme-app' AND user_id = 'anya'"))
	s.NotEqual(s.contactRow(chat.ID).ConversationID, s.contactRow(sms.ID).ConversationID, "the number keeps none of the user's history")
	phone, _, linked, err := s.store.UserPhones.Linked(s.ctx, "acme-app", 4242, "grace")
	s.Require().NoError(err)
	s.True(linked, "another user's link stays")
	s.Equal("+15550101", phone)
	_, _, linked, err = s.store.UserPhones.Linked(s.ctx, "other-app", 4242, "anya")
	s.Require().NoError(err)
	s.True(linked, "the same user id of another app is another user")
}

func (s *StoreSuite) TestForgettingAUserUnlinksTheContactMapRowsThatNameThem() {
	s.agentConfigs()
	sender := func(customer, number, user string) *ContactMapEntry {
		row := &ContactMapEntry{CustomerID: customer, ConfigID: "agent-one", Kind: "sms", Address: number, ConversationID: "conversation-" + number, UserID: user}
		s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, row))
		return row
	}
	theirs := sender("acme-app", "+15550100", "anya")
	graces := sender("acme-app", "+15550101", "grace")
	elsewhere := sender("other-app", "+15550100", "anya")
	chat := s.made(streamUser("anya", "agent:omni-chat"))
	own := s.made(streamUserOf(0, "anya", "agent:omni-own"))
	// Ids that end, or start, like theirs are other users.
	tanya := s.made(streamUser("tanya", "agent:omni-tanya"))
	prefixed := s.made(streamUser("anya-2", "agent:omni-anya-2"))
	// A Slack user of that id in a workspace is someone else: only a Stream user's address is the app's id.
	slack := contact("T1:anya", "agent:omni-slack")
	slack.Kind = ContactSlack
	s.made(slack)

	counts := s.forgetUser("acme-app", "anya")

	s.EqualValues(3, counts.Contacts)
	_, err := s.store.ContactMapEntries.GetByUser(s.ctx, "acme-app", "agent-one", "sms", "anya")
	s.ErrorIs(err, ErrNoContact)
	unlinked := s.contactRow(theirs.ID)
	s.Empty(unlinked.UserID)
	s.Equal("conversation-+15550100", unlinked.ConversationID, "the number's conversation stays the number's")
	s.Equal("4242:forgotten", s.contactRow(chat.ID).Address)
	s.Equal("agent:omni-chat", s.contactRow(chat.ID).ConversationID)
	s.Equal("0:forgotten", s.contactRow(own.ID).Address)
	s.Equal("grace", s.contactRow(graces.ID).UserID, "another user's row stays linked")
	s.Equal("4242:tanya", s.contactRow(tanya.ID).Address)
	s.Equal("4242:anya-2", s.contactRow(prefixed.ID).Address)
	s.Equal("T1:anya", s.contactRow(slack.ID).Address)
	s.Equal("anya", s.contactRow(elsewhere.ID).UserID, "the same user id of another app is another user")
}

func (s *StoreSuite) TestForgettingAUserDeletesTheirChannelLinkCodes() {
	code := func(customer, user, value string) {
		s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &ChannelLink{
			CustomerID: customer, ConfigID: "agent-one", UserID: user, ExpiresAt: time.Now().Add(time.Hour),
		}, func() (string, error) { return value, nil }))
	}
	code("acme-app", "anya", "111111")
	code("acme-app", "anya", "222222")
	code("acme-app", "grace", "333333")
	code("other-app", "anya", "444444")

	counts := s.forgetUser("acme-app", "anya")

	s.EqualValues(2, counts.ChannelLinks)
	_, err := s.store.ChannelLinks.Claim(s.ctx, "acme-app", "agent-one", "111111")
	s.ErrorIs(err, ErrUnknownChannelLink)
	s.Zero(s.countOf("channel_links WHERE customer_id = 'acme-app' AND user_id = 'anya'"))
	s.Equal(1, s.countOf("channel_links WHERE customer_id = 'acme-app' AND user_id = 'grace'"))
	s.Equal(1, s.countOf("channel_links WHERE customer_id = 'other-app' AND user_id = 'anya'"))
}

func (s *StoreSuite) TestForgettingAUserDeletesTheirProfileAndEmptiesItsChanges() {
	for _, app := range []string{"acme-app", "other-app"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, app, time.Hour, time.Second))
	}
	for _, user := range []User{
		{CustomerID: "acme-app", ID: "anya", Kind: UserKindAuthenticated, Name: "Anya Lahey", Email: "anya@example.com"},
		{CustomerID: "acme-app", ID: "guest-1", Kind: UserKindGuest},
		{CustomerID: "acme-app", ID: "grace", Kind: UserKindAuthenticated, Name: "Grace"},
		{CustomerID: "other-app", ID: "anya", Kind: UserKindAuthenticated, Name: "Anya Lahey"},
		{CustomerID: "other-app", ID: "guest-1", Kind: UserKindGuest},
	} {
		s.Require().NoError(s.store.Users.Insert(s.ctx, &user))
	}
	_, err := s.store.global.ExecContext(s.ctx, "UPDATE users SET claimed_by = 'anya', claimed_at = now() WHERE id = 'guest-1'")
	s.Require().NoError(err)

	counts := s.forgetUser("acme-app", "anya")

	s.EqualValues(1, counts.Profiles)
	s.EqualValues(1, counts.Claims)
	s.Zero(s.countOf("users WHERE customer_id = 'acme-app' AND id = 'anya'"))
	s.Equal(1, s.countOf("users WHERE customer_id = 'acme-app' AND id = 'guest-1' AND claimed_by = 'forgotten'"),
		"the guest they were stays claimed")
	s.Equal(1, s.countOf("users WHERE customer_id = 'acme-app' AND id = 'grace'"))
	s.Equal(1, s.countOf("users WHERE customer_id = 'other-app' AND id = 'anya'"))
	var naming []string
	for _, change := range s.recordedOf("acme-app", "users") {
		if strings.Contains(string(change.Payload)+string(change.Key), "anya") {
			naming = append(naming, change.Op+" "+string(change.Key)+" "+string(change.Payload))
		}
	}
	s.Equal([]string{`delete {"id": "anya", "customer_id": "acme-app"} `}, naming,
		"only the profile's delete names them, by its key, for a follower to delete its copy by")
	s.NotEmpty(s.payloadsNaming("acme-app", "grace"), "another user's changes stay")
	s.NotEmpty(s.payloadsNaming("other-app", "anya"), "the same user id of another app is another user")
	claims := 0
	for _, change := range s.recordedOf("other-app", "users") {
		if strings.Contains(string(change.Payload), `"claimed_by": "anya"`) {
			claims++
		}
	}
	s.Equal(1, claims, "a guest of another app claimed by the same id keeps its claim's change")
}

func (s *StoreSuite) TestARouterRecordsAForgottenUserAgainWhenTheyComeBack() {
	router := s.replica()
	// Another replica saw them, so the routers' Redis says their profile is written.
	s.Require().NoError(s.store.Users.Seen(s.ctx, "acme-app", "anya", UserKindGuest))
	s.Require().Equal(1, s.countOf("users WHERE customer_id = 'acme-app' AND id = 'anya'"))

	s.forgetUser("acme-app", "anya")

	s.Require().NoError(router.Users.Seen(s.ctx, "acme-app", "anya", UserKindGuest))
	s.Equal(1, s.countOf("users WHERE customer_id = 'acme-app' AND id = 'anya'"), "the profile is written again, not taken as held")
}

func (s *StoreSuite) TestForgettingAUserDeletesTheLogsNamingThemAndEmptiesTheirChanges() {
	s.inSmallBatches()
	for _, app := range []string{"acme-app", "other-app"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, app, time.Hour, time.Second))
	}
	log := func(customer, user, message string) *AgentLog {
		entry := &AgentLog{CustomerID: customer, UserID: user, Source: "agent", Severity: "info", EventType: "turn", Message: message}
		s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, entry))
		return entry
	}
	// First, so a batch that took any app's logs would find it first.
	log("other-app", "anya", "said hello")
	log("acme-app", "anya", "said hello")
	log("acme-app", "anya", "said goodbye")
	gone := log("acme-app", "anya", "said hello before")
	_, err := s.store.shard.ExecContext(s.ctx, "DELETE FROM agent_logs WHERE id = ?", gone.ID)
	s.Require().NoError(err)
	log("acme-app", "grace", "said hello")
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &Campaign{CustomerID: "acme-app", Name: "Renewals", ConfigID: "config-1",
		FromNumber: "+15550100", UserID: "anya"}))

	deleted, err := s.store.AgentLogs.DeleteUser(s.ctx, "acme-app", "anya")

	s.Require().NoError(err)
	s.EqualValues(2, deleted, "two batches of one")
	countLogs := func(where string) int {
		var count int
		s.Require().NoError(s.store.shard.QueryRowContext(s.ctx, "SELECT count(*) FROM agent_logs WHERE "+where).Scan(&count))
		return count
	}
	s.Zero(countLogs("customer_id = 'acme-app' AND user_id = 'anya'"))
	s.Equal(1, countLogs("customer_id = 'acme-app' AND user_id = 'grace'"))
	s.Equal(1, countLogs("customer_id = 'other-app' AND user_id = 'anya'"))
	if !s.store.Sharded() {
		s.Empty(slices.DeleteFunc(s.payloadsNaming("acme-app", "anya"), func(change DataChange) bool { return change.Table != "agent_logs" }),
			"the changes of every log of theirs, the one deleted before included")
		s.Len(s.payloadsNaming("acme-app", "anya"), 1, "another table's change is another step's")
		s.Len(s.recordedOf("acme-app", "agent_logs"), 7, "each change keeps its number: four inserts, three deletes")
		s.NotEmpty(s.payloadsNaming("acme-app", "grace"), "another user's changes stay")
		s.NotEmpty(s.payloadsNaming("other-app", "anya"), "the same user id of another app is another user")
	}
	left, err := s.store.AgentLogs.CountUser(s.ctx, "acme-app", "anya")
	s.Require().NoError(err)
	s.Zero(left)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemOnTheirCallLegs() {
	s.inSmallBatches()
	leg := func(customer, user string) *CallLeg {
		row := &CallLeg{CustomerID: customer, UserID: user, CallType: "phone", CallID: "call-" + newID(), Vendor: "twilio",
			Direction: Outbound, LocalNumber: "+15550100", RemoteNumber: "+15550199", DialedAt: s.base}
		_, err := s.store.shard.NewInsert().Model(row).Exec(s.ctx)
		s.Require().NoError(err)
		return row
	}
	theirs := []*CallLeg{leg("acme-app", "anya"), leg("acme-app", "anya")}
	graces := leg("acme-app", "grace")
	elsewhere := leg("other-app", "anya")
	counted, err := s.store.CallLegs.CountUser(s.ctx, "acme-app", "anya")
	s.Require().NoError(err)

	renamed, err := s.store.CallLegs.RenameUser(s.ctx, "acme-app", "anya", "forgotten")

	s.Require().NoError(err)
	s.EqualValues(2, renamed)
	s.EqualValues(2, counted)
	userOf := func(row *CallLeg) string {
		var user string
		s.Require().NoError(s.store.shard.NewSelect().Model((*CallLeg)(nil)).Column("user_id").Where("id = ?", row.ID).Scan(s.ctx, &user))
		return user
	}
	for _, row := range theirs {
		s.Equal("forgotten", userOf(row))
	}
	s.Equal("grace", userOf(graces))
	s.Equal("anya", userOf(elsewhere), "the same user id of another app is another user")
}

func (s *DataMoveSuite) TestAMoveFollowingAForgottenUserEndsWithoutTheirProfile() {
	s.capture("acme")
	s.Require().NoError(s.source.Users.Insert(s.ctx, &User{CustomerID: "acme", ID: "anya", Kind: UserKindAuthenticated, Name: "Anya"}))
	cursor := s.move("acme", "acme")
	s.Require().NoError(s.source.Users.Insert(s.ctx, &User{CustomerID: "acme", ID: "grace", Kind: UserKindAuthenticated, Name: "Grace"}))
	_, err := s.source.ForgetUserSettings(s.ctx, "acme", "anya", "forgotten", &AuditEntry{
		CustomerID: "acme", ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().NoError(err)
	users := func() []string {
		var ids []string
		s.Require().NoError(s.destination.GlobalDB().NewSelect().Model((*User)(nil)).Column("id").
			Where("customer_id = 'acme'").Order("id").Scan(s.ctx, &ids))
		return ids
	}

	changes, _, err := s.source.Changes(s.ctx, "acme", cursor, 100)
	s.Require().NoError(err)
	s.Require().NoError(s.destination.ApplyChanges(s.ctx, "acme", changes))
	s.Equal([]string{"grace"}, users(), "the follower deleted its copy")

	every, _, err := s.source.Changes(s.ctx, "acme", 0, 100)
	s.Require().NoError(err)
	s.Require().NoError(s.destination.ApplyChanges(s.ctx, "acme", every), "a follower starting over replays the emptied changes")
	s.Equal([]string{"grace"}, users())
}
