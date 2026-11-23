package store

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// UserSettings are the shared database's rows that name one end user of an app: their own
// policy and their user-level limits, which forgetting them deletes, and their resets, grants
// and the campaigns they scheduled, whose id it replaces.
type UserSettings struct {
	Policies  int64 `bun:"policies"`
	Limits    int64 `bun:"limits"`
	Resets    int64 `bun:"resets"`
	Grants    int64 `bun:"grants"`
	Campaigns int64 `bun:"campaigns"`
}

// Changes are the counts as an audit entry records them.
func (u UserSettings) Changes() []AuditChange {
	return []AuditChange{
		{Field: "policies_deleted", After: u.Policies}, {Field: "limits_deleted", After: u.Limits},
		{Field: "resets", After: u.Resets}, {Field: "grants", After: u.Grants}, {Field: "campaigns", After: u.Campaigns},
	}
}

// UserOrganizations are the organizations an app's end user may have counters and holds under:
// those of the requests on every shard, and of the limits, resets and grants, naming any of
// userIDs, and the app's own. A run finishing one that stopped names the replacement too, since
// the rows it already rewrote no longer name the user.
func (s *Store) UserOrganizations(ctx context.Context, appID string, userIDs ...string) ([]string, error) {
	shards, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]string, error) {
		var found []string
		err := db.NewSelect().Model((*Request)(nil)).
			Distinct().
			Column("r.organization_id").
			Where("r.customer_id = ? AND r.user_id IN (?)", appID, bun.In(userIDs)).
			Scan(ctx, &found)
		return found, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: user organizations: %w", err))
	}
	var shared []string
	users := bun.In(userIDs)
	err = s.global.NewSelect().Model((*AppOrganization)(nil)).Column("organization_id").Where("app_id = ?", appID).
		Union(s.global.NewSelect().Model((*App)(nil)).Column("organization_id").Where("id = ?", appID)).
		Union(s.global.NewSelect().Model((*Limit)(nil)).Column("organization_id").
			Where("level = ? AND app_id = ? AND user_id IN (?)", LevelUser, appID, users)).
		Union(s.global.NewSelect().Model((*UsageReset)(nil)).Column("organization_id").Where("app_id = ? AND user_id IN (?)", appID, users)).
		Union(s.global.NewSelect().Model((*LimitGrant)(nil)).Column("organization_id").Where("app_id = ? AND user_id IN (?)", appID, users)).
		Scan(ctx, &shared)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: user organizations: %w", err))
	}
	organizations := map[string]bool{}
	for _, found := range append(shards, shared) {
		for _, organization := range found {
			if organization != "" {
				organizations[organization] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(organizations)), nil
}

// CountUserSettings counts the shared database's rows that name an app's end user.
func (s *Store) CountUserSettings(ctx context.Context, appID, userID string) (UserSettings, error) {
	var counts UserSettings
	err := s.global.NewRaw(`SELECT
		(SELECT count(*) FROM policies WHERE scope = ? AND scope_id = ?) AS policies,
		(SELECT count(*) FROM limits WHERE level = ? AND app_id = ? AND user_id = ?) AS limits,
		(SELECT count(*) FROM usage_resets WHERE app_id = ? AND user_id = ?) AS resets,
		(SELECT count(*) FROM limit_grants WHERE app_id = ? AND user_id = ?) AS grants,
		(SELECT count(*) FROM campaigns WHERE customer_id = ? AND user_id = ?) AS campaigns`,
		ScopeUser, UserScopeID(appID, userID), LevelUser, appID, userID, appID, userID, appID, userID, appID, userID).
		Scan(ctx, &counts)
	if err != nil {
		return UserSettings{}, stack.Wrap(fmt.Errorf("store: count user settings: %w", err))
	}
	return counts, nil
}

// dataChange is a row of data_changes, the changes recorded for a customer who is moving.
type dataChange struct {
	bun.BaseModel `bun:"table:data_changes,alias:dc"`

	Seq int64 `bun:"seq,pk"`
}

// renamedChanges replaces an end user's id with replacement in the recorded changes of a
// table's rows that name them in user_id, deleted rows' included.
// Changes are rewritten rather than deleted: a follower is refused a cursor below the oldest
// change kept of any customer (Changes), so dropping one would make every move copy again.
func renamedChanges(db bun.IDB, customerID, table, userID, replacement string) *bun.UpdateQuery {
	return db.NewUpdate().Model((*dataChange)(nil)).
		Set("payload = jsonb_set(payload, '{user_id}', to_jsonb(?::text))", replacement).
		Where("customer_id = ? AND table_name = ? AND payload->>'user_id' = ?", customerID, table, userID)
}

// forgottenCopies turns the recorded inserts and updates of a customer's copies of hidden
// messages into deletes that carry nothing, for the caller to narrow to the copies it deletes.
// They keep their numbers (renamedChanges), and the copy's own delete change stays after them.
func forgottenCopies(db bun.IDB, customerID string) *bun.UpdateQuery {
	return db.NewUpdate().Model((*dataChange)(nil)).Set("op = ?", ChangeDelete).Set("payload = NULL").
		Where("customer_id = ? AND table_name = 'agent_branch_messages' AND op <> ?", customerID, ChangeDelete)
}

// ForgetUserSettings deletes an app's end user's own policy and user-level limits, so they fall
// back to the app's, replaces their id with replacement on their resets, grants and campaigns,
// and records entry, with these counts added to its changes, in the same transaction. The
// changes recorded of their campaigns, deleted ones' included, name the replacement too
// (renamedChanges). A campaign's contacts and their attempts stay: they are the app's data
// about third parties, not the user's.
func (s *Store) ForgetUserSettings(ctx context.Context, appID, userID, replacement string, entry *AuditEntry) (UserSettings, error) {
	var counts UserSettings
	err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, change := range []struct {
			count *int64
			exec  func(context.Context, ...any) (sql.Result, error)
		}{
			{&counts.Policies, tx.NewDelete().Model((*Policy)(nil)).Where("scope = ? AND scope_id = ?", ScopeUser, UserScopeID(appID, userID)).Exec},
			{&counts.Limits, tx.NewDelete().Model((*Limit)(nil)).Where("level = ? AND app_id = ? AND user_id = ?", LevelUser, appID, userID).Exec},
			{&counts.Resets, tx.NewUpdate().Model((*UsageReset)(nil)).Set("user_id = ?", replacement).Where("app_id = ? AND user_id = ?", appID, userID).Exec},
			{&counts.Grants, tx.NewUpdate().Model((*LimitGrant)(nil)).Set("user_id = ?", replacement).Where("app_id = ? AND user_id = ?", appID, userID).Exec},
			{&counts.Campaigns, tx.NewUpdate().With("renamed", renamedChanges(tx, appID, "campaigns", userID, replacement)).
				Model((*Campaign)(nil)).Set("user_id = ?", replacement).Where("customer_id = ? AND user_id = ?", appID, userID).Exec},
		} {
			result, err := change.exec(ctx)
			if err != nil {
				return err
			}
			if *change.count, err = result.RowsAffected(); err != nil {
				return err
			}
		}
		entry.Changes = append(entry.Changes, counts.Changes()...)
		return recordAuditEntry(ctx, tx, entry)
	})
	// Dropped whatever the outcome, since a commit that fails can still have landed.
	s.Policies.forget(ctx, ScopeUser, UserScopeID(appID, userID))
	if err != nil {
		return UserSettings{}, stack.Wrap(fmt.Errorf("store: forget user settings: %w", err))
	}
	return counts, nil
}

// UserTasks are what forgetting an app's end user does to their scheduled tasks: the tasks it
// deletes, with their runs, and the changes recorded of those rows that it redacts.
type UserTasks struct {
	Tasks   int64 `bun:"tasks"`
	Changes int64 `bun:"changes"`
}

// AuditChanges are the counts as an audit entry records them.
func (u UserTasks) AuditChanges() []AuditChange {
	return []AuditChange{{Field: "tasks_deleted", After: u.Tasks}, {Field: "task_changes_redacted", After: u.Changes}}
}

// userTaskChanges picks, as dc, the recorded inserts and updates of an app's end user's tasks
// and runs, by what their payload holds, taking the app and then the user twice. A run is the user's when the latest change
// of a task by its id before the run's names them, or, with none kept, when the task by that id
// here is theirs: an id freed by a delete can be another user's task later.
const userTaskChanges = `dc.customer_id = ? AND (
	(dc.table_name = 'scheduled_tasks' AND dc.payload->>'user_id' = ?)
	OR (dc.table_name = 'scheduled_task_runs' AND COALESCE(
		(SELECT t.payload->>'user_id' FROM data_changes AS t
		WHERE t.customer_id = dc.customer_id AND t.table_name = 'scheduled_tasks' AND t.payload->>'id' = dc.payload->>'task_id' AND t.seq < dc.seq
		ORDER BY t.seq DESC LIMIT 1),
		(SELECT st.user_id FROM scheduled_tasks AS st WHERE st.customer_id = dc.customer_id AND st.id = dc.payload->>'task_id')) = ?))`

// CountUserTasks counts what ForgetUserTasks would change.
func (s *Store) CountUserTasks(ctx context.Context, appID, userID string) (UserTasks, error) {
	var counts UserTasks
	err := s.global.NewRaw(`SELECT
		(SELECT count(*) FROM scheduled_tasks WHERE customer_id = ? AND user_id = ?) AS tasks,
		(SELECT count(*) FROM data_changes AS dc WHERE `+userTaskChanges+`) AS changes`,
		appID, userID, appID, userID, userID).
		Scan(ctx, &counts)
	if err != nil {
		return UserTasks{}, stack.Wrap(fmt.Errorf("store: count user tasks: %w", err))
	}
	return counts, nil
}

// ForgetUserTasks deletes an app's end user's scheduled tasks, and their runs by cascade, so
// none runs for them again. The changes recorded of those rows hold the user's id and words, so
// each becomes a delete of its row, with no payload, under its own number: deleting it would
// raise the oldest number kept, and expire the cursors of other apps' moves.
func (s *Store) ForgetUserTasks(ctx context.Context, appID, userID string) (UserTasks, error) {
	var counts UserTasks
	err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// A write to a task that commits while this waits on its row recorded a change the
		// redaction would not see: waiting here first lets the next statement see it.
		var locked []string
		if err := tx.NewSelect().Model((*ScheduledTask)(nil)).Column("id").
			Where("customer_id = ? AND user_id = ?", appID, userID).For("UPDATE").Scan(ctx, &locked); err != nil {
			return err
		}
		redacted, err := tx.NewUpdate().TableExpr("data_changes AS dc").Set("op = ?", ChangeDelete).Set("payload = NULL").
			Where(userTaskChanges, appID, userID, userID).Exec(ctx)
		if err != nil {
			return err
		}
		if counts.Changes, err = redacted.RowsAffected(); err != nil {
			return err
		}
		deleted, err := tx.NewDelete().Model((*ScheduledTask)(nil)).Where("customer_id = ? AND user_id = ?", appID, userID).Exec(ctx)
		if err != nil {
			return err
		}
		counts.Tasks, err = deleted.RowsAffected()
		return err
	})
	if err != nil {
		return UserTasks{}, stack.Wrap(fmt.Errorf("store: forget user tasks: %w", err))
	}
	return counts, nil
}

// forgottenUserTables are the tables with a user_id column that forgetting a user reaches: the
// requests of every shard (Requests.RenameUser), the tasks ForgetUserTasks deletes and the rows
// ForgetUserSettings deletes or rewrites.
var forgottenUserTables = []string{"requests", "limits", "usage_resets", "limit_grants", "campaigns", "scheduled_tasks"}

// keptUserTables are the tables with a user_id column that forgetting a user leaves as they are,
// each with why.
var keptUserTables = map[string]string{
	"agent_sessions": "the user's conversations, which the app deletes one by one (DELETE /v1/agents/sessions/{id}), " +
		"their tasks with them",
	"agent_logs": "the app's log of what its agents did; nothing deletes or rewrites it, forget included",
	"call_legs":  "the legs of phone calls as billed, with the numbers on them; forget does not rewrite them",
	"channel_links": "one-time codes that tie a number texting in to the user, spent once (used_at) " +
		"or expired (expires_at), and kept after",
	"contact_map": "the addresses the app's agents reach people at and the conversation each carries on, " +
		"one linked to the user among them; forget does not clear the link",
	"user_phones":        "the number the app linked to the user, which it unlinks (PUT /v1/agents/users/{user_id})",
	"user_phone_history": "when a link of user_phones began, deleted with it",
}
