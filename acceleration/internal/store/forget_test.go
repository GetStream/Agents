//go:build integration

package store

import (
	"encoding/json"
	"slices"
	"strings"
	"time"
)

// recordedFor is every change recorded for a customer, as following their move reads them.
func (s *StoreSuite) recordedFor(customerID string) []DataChange {
	changes, _, err := s.store.Changes(s.ctx, customerID, 0, 1000)
	s.Require().NoError(err)
	return changes
}

// payloadsNaming are the recorded changes of a customer whose row names a user.
func (s *StoreSuite) payloadsNaming(customerID, userID string) []DataChange {
	var naming []DataChange
	for _, change := range s.recordedFor(customerID) {
		if strings.Contains(string(change.Payload), `"`+userID+`"`) {
			naming = append(naming, change)
		}
	}
	return naming
}

// recordedOf are the recorded changes of one table of a customer's.
func (s *StoreSuite) recordedOf(customerID, table string) []DataChange {
	return slices.DeleteFunc(s.recordedFor(customerID), func(change DataChange) bool { return change.Table != table })
}

// seqs are the numbers of changes.
func seqs(changes []DataChange) []int64 {
	numbers := make([]int64, 0, len(changes))
	for _, change := range changes {
		numbers = append(numbers, change.Seq)
	}
	return numbers
}

// forget runs the shared database's step of forgetting jlahey of app.
func (s *StoreSuite) forget() {
	_, err := s.store.ForgetUserSettings(s.ctx, "app", "jlahey", "forgotten", &AuditEntry{
		CustomerID: "app", ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().NoError(err)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesRecordedOfTheirCampaigns() {
	for _, app := range []string{"app", "app2"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, app, time.Hour, time.Second))
	}
	reused := Campaign{CustomerID: "app2", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &reused))
	// The other app's campaign is gone and the app's holds its id, so only the customer tells
	// their changes apart: an id is unique among the rows that exist, not among those recorded.
	s.Require().NoError(s.store.Campaigns.Delete(s.ctx, "app2", reused.ID))
	_, err := s.store.global.ExecContext(s.ctx,
		"INSERT INTO campaigns (id, customer_id, name, config_id, from_number, user_id) VALUES (?, 'app', 'Renewals', 'config-1', '+15550100', 'jlahey')", reused.ID)
	s.Require().NoError(err)
	other := Campaign{CustomerID: "app", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550100", UserID: "rlafleur"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &other))
	before := seqs(s.recordedFor("app"))
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the campaign's insert was recorded")

	s.forget()

	s.Empty(s.payloadsNaming("app", "jlahey"))
	s.Len(s.payloadsNaming("app", "forgotten"), 2, "the insert renamed, and the rewrite recorded under the replacement")
	s.NotEmpty(s.payloadsNaming("app", "rlafleur"), "another user's changes stay")
	s.NotEmpty(s.payloadsNaming("app2", "jlahey"), "the same user id of another app is another user")
	s.Subset(seqs(s.recordedFor("app")), before, "every change keeps its number")
}

func (s *StoreSuite) TestForgettingAUserOfOneAppLeavesAnotherAppsFollowerWhereItWas() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	theirs := Campaign{CustomerID: "app", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &theirs))
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "other", time.Hour, time.Second))
	cursor, err := s.store.ExportCustomer(s.ctx, "other", func(string, json.RawMessage) error { return nil })
	s.Require().NoError(err)
	// The user's changes are then on both sides of the follower's cursor, and the oldest kept.
	later := Campaign{CustomerID: "app", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &later))
	unread := Campaign{CustomerID: "other", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550101", UserID: "rlafleur"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &unread))

	s.forget()

	changes, _, err := s.store.Changes(s.ctx, "other", cursor, 100)
	s.Require().NoError(err, "forgetting a user of another app takes no change of this one's")
	s.Len(changes, 1)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesOfTheRequestsRewrittenAlone() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	request := func(user string) *Request {
		recorded := &Request{Modality: "stt", CustomerID: "app", UserID: user, Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
		s.Require().NoError(s.store.Requests.Insert(s.ctx, recorded))
		return recorded
	}
	request("jlahey")
	request("rlafleur")
	s.opened("one", "app", s.base, nil)

	_, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")
	s.Require().NoError(err)
	// A request in flight when its rows were rewritten, which forget-user waits out and rewrites
	// on a later run: until then its row names the user, and so does its change.
	late := request("jlahey")
	s.forget()

	var naming []string
	for _, change := range s.recordedOf("app", "requests") {
		if strings.Contains(string(change.Payload), `"jlahey"`) {
			naming = append(naming, string(change.Key))
		}
	}
	s.Require().Len(naming, 1, "only the late request's change names the user")
	s.Contains(naming[0], late.UsageID.String())
	s.Len(slices.DeleteFunc(s.recordedOf("app", "requests"), func(change DataChange) bool {
		return !strings.Contains(string(change.Payload), `"forgotten"`)
	}), 2, "the rewritten request's insert renamed, and its rewrite")
	s.NotEmpty(s.payloadsNaming("app", "rlafleur"), "another user's changes stay")
	s.NotEmpty(s.recordedOf("app", "agent_sessions")[0].Payload, "a table forget leaves alone keeps its changes")
	s.Contains(string(s.recordedOf("app", "agent_sessions")[0].Payload), `"jlahey"`)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesOfACampaignDeletedBefore() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	gone := Campaign{CustomerID: "app", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &gone))
	s.Require().NoError(s.store.Campaigns.Delete(s.ctx, "app", gone.ID))
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the campaign's insert was recorded")

	s.forget()

	s.Empty(s.payloadsNaming("app", "jlahey"))
}

func (s *StoreSuite) TestRenamingAUserRenamesThemInTheChangesOfARequestDeletedBefore() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	gone := &Request{Modality: "stt", CustomerID: "app", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
	s.Require().NoError(s.store.Requests.Insert(s.ctx, gone))
	_, err := s.store.shard.ExecContext(s.ctx, "DELETE FROM requests WHERE usage_id = ?", gone.UsageID)
	s.Require().NoError(err)
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the request's insert was recorded")

	_, err = s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")
	s.Require().NoError(err)

	s.Empty(s.payloadsNaming("app", "jlahey"))
}
