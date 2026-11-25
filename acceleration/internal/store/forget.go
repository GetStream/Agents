package store

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// UserSettings are the shared database's rows that name one end user of an app: their own
// policy, their user-level limits, their phone links, their channel link codes and their
// profile, which forgetting them deletes; the contact map rows linked to them or naming them,
// which it unlinks; and their resets, grants, the campaigns they scheduled and the guests they
// claimed, whose id it replaces.
type UserSettings struct {
	Policies     int64 `bun:"policies"`
	Limits       int64 `bun:"limits"`
	Resets       int64 `bun:"resets"`
	Grants       int64 `bun:"grants"`
	Campaigns    int64 `bun:"campaigns"`
	Phones       int64 `bun:"phones"`
	Contacts     int64 `bun:"contacts"`
	ChannelLinks int64 `bun:"channel_links"`
	Profiles     int64 `bun:"profiles"`
	Claims       int64 `bun:"claims"`
}

// Changes are the counts as an audit entry records them.
func (u UserSettings) Changes() []AuditChange {
	return []AuditChange{
		{Field: "policies_deleted", After: u.Policies}, {Field: "limits_deleted", After: u.Limits},
		{Field: "resets", After: u.Resets}, {Field: "grants", After: u.Grants}, {Field: "campaigns", After: u.Campaigns},
		{Field: "phones_unlinked", After: u.Phones}, {Field: "contacts_unlinked", After: u.Contacts},
		{Field: "channel_links_deleted", After: u.ChannelLinks}, {Field: "profiles_deleted", After: u.Profiles},
		{Field: "claims", After: u.Claims},
	}
}

// streamUserAddressee is the user id a contact map row cm names in its address, when it is a Stream
// user's (splitStreamUserAddress): what follows the first colon.
const streamUserAddressee = "substr(cm.address, strpos(cm.address, ':') + 1)"

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
		(SELECT count(*) FROM campaigns WHERE customer_id = ? AND user_id = ?) AS campaigns,
		(SELECT count(*) FROM user_phones WHERE customer_id = ? AND user_id = ?) AS phones,
		(SELECT count(*) FROM contact_map AS cm WHERE cm.customer_id = ? AND (cm.user_id = ? OR cm.kind = ? AND `+streamUserAddressee+` = ?)) AS contacts,
		(SELECT count(*) FROM channel_links WHERE customer_id = ? AND user_id = ?) AS channel_links,
		(SELECT count(*) FROM users WHERE customer_id = ? AND id = ?) AS profiles,
		(SELECT count(*) FROM users WHERE customer_id = ? AND claimed_by = ?) AS claims`,
		ScopeUser, UserScopeID(appID, userID), LevelUser, appID, userID, appID, userID, appID, userID, appID, userID,
		appID, userID, appID, userID, ContactStreamUser, userID, appID, userID, appID, userID, appID, userID).
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

// forgottenChanges turns the recorded inserts and updates of a customer's rows of tables into
// deletes that carry nothing, for the caller to narrow to the rows it forgets. They keep their
// numbers (renamedChanges), and a row's own delete change stays after them.
func forgottenChanges(db bun.IDB, customerID string, tables ...string) *bun.UpdateQuery {
	return db.NewUpdate().Model((*dataChange)(nil)).Set("op = ?", ChangeDelete).Set("payload = NULL").
		Where("customer_id = ? AND table_name IN (?) AND op <> ?", customerID, bun.In(tables), ChangeDelete)
}

// ForgetUserSettings deletes an app's end user's own policy and user-level limits, so they fall
// back to the app's, replaces their id with replacement on their resets, grants and campaigns,
// and records entry, with these counts added to its changes, in the same transaction. The
// changes recorded of their campaigns, deleted ones' included, name the replacement too
// (renamedChanges). A campaign's contacts and their attempts stay: they are the app's data
// about third parties, not the user's.
//
// In the same transaction it deletes their phone links, each as Unlink does under the link's
// lock, so the number keeps none of their history (detachLinked); unlinks the contact map rows
// linked to them, and gives their Stream user rows the replacement's address, so a message of
// theirs later starts afresh; deletes their channel link codes; and deletes their profile
// (users), and replaces their id on the guests they claimed, so a claimed guest stays claimed.
// The recorded changes of their profile become deletes with no payload under the replacement's
// key, all but its deletes, which a follower needs to delete its copy; those of the guests name
// the replacement.
//
// It outlives ctx, its statements sharing the store's write timeout, and drops the user's cached
// policy and profile whatever the commit answered, and again after a COMMIT whose answer was
// lost (detachedWrite).
func (s *Store) ForgetUserSettings(ctx context.Context, appID, userID, replacement string, entry *AuditEntry) (UserSettings, error) {
	var counts UserSettings
	err := s.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		// The links first, as Link and Unlink take the link's lock before they touch contact_map.
		var links []UserPhone
		if err := tx.NewSelect().Model(&links).Where("customer_id = ? AND user_id = ?", appID, userID).Order("stream_app_pk").Scan(ctx); err != nil {
			return err
		}
		for _, link := range links {
			if err := lockUntilCommit(ctx, tx, link.lockKey(), userLinkLockSeed); err != nil {
				return err
			}
			var had []string
			if _, err := tx.NewDelete().Model((*UserPhone)(nil)).
				Where("customer_id = ? AND stream_app_pk = ? AND user_id = ?", link.CustomerID, link.StreamAppPK, link.UserID).
				Returning("phone").Exec(ctx, &had); err != nil {
				return err
			}
			if len(had) == 0 {
				continue
			}
			counts.Phones++
			if err := s.UserPhones.detachLinked(ctx, tx, link, had[0]); err != nil {
				return err
			}
		}
		// A data-modifying WITH runs to the end whether or not the statement reads it.
		claimed := tx.NewUpdate().Model((*dataChange)(nil)).
			Set("payload = jsonb_set(payload, '{claimed_by}', to_jsonb(?::text))", replacement).
			Where("customer_id = ? AND table_name = 'users' AND payload->>'claimed_by' = ?", appID, userID)
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
			{&counts.Contacts, tx.NewUpdate().Model((*ContactMapEntry)(nil)).
				Set("user_id = NULLIF(cm.user_id, ?)", userID).
				Set("address = CASE WHEN cm.kind = ? AND "+streamUserAddressee+" = ? THEN split_part(cm.address, ':', 1) || ':' || ? ELSE cm.address END",
					ContactStreamUser, userID, replacement).
				Set("updated_at = ?", time.Now().UTC()).
				Where("cm.customer_id = ? AND (cm.user_id = ? OR cm.kind = ? AND "+streamUserAddressee+" = ?)", appID, userID, ContactStreamUser, userID).Exec},
			{&counts.ChannelLinks, tx.NewDelete().Model((*ChannelLink)(nil)).Where("customer_id = ? AND user_id = ?", appID, userID).Exec},
			{&counts.Claims, tx.NewUpdate().With("renamed", claimed).
				Model((*User)(nil)).Set("claimed_by = ?", replacement).Where("customer_id = ? AND claimed_by = ?", appID, userID).Exec},
			{&counts.Profiles, tx.NewDelete().Model((*User)(nil)).Where("customer_id = ? AND id = ?", appID, userID).Exec},
		} {
			result, err := change.exec(ctx)
			if err != nil {
				return err
			}
			if *change.count, err = result.RowsAffected(); err != nil {
				return err
			}
		}
		// After the profile's delete, which records a change of its own that a follower deletes
		// its copy by. The rest are deletes of a row by the replacement's id, which no follower
		// holds.
		if _, err := forgottenChanges(tx, appID, "users").Set("key = jsonb_set(key, '{id}', to_jsonb(?::text))", replacement).
			Where("key->>'id' = ?", userID).Exec(ctx); err != nil {
			return err
		}
		entry.Changes = append(entry.Changes, counts.Changes()...)
		return recordAuditEntry(ctx, tx, entry)
	}, func() []string {
		return append(s.Policies.keys(ctx, ScopeUser, UserScopeID(appID, userID)), userKey(appID, userID))
	})
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
	counts, err := deleteTasks(ctx, s.global, appID, func(q bun.QueryBuilder) bun.QueryBuilder {
		return q.Where("user_id = ?", userID)
	}, false, userTaskChanges, appID, userID, userID)
	if err != nil {
		return UserTasks{}, stack.Wrap(fmt.Errorf("store: forget user tasks: %w", err))
	}
	return counts, nil
}

// deleteTasks deletes the app's tasks that tasks narrows, and their runs by cascade, and makes
// each change that changes picks, as dc, a delete of its row with no payload under its own number,
// in one transaction. changes may read the ids deleted from the CTE deleted.
//
// onlyLocked deletes just the tasks the lock found, and nothing when it found none, for a set a
// write can move a task into after the lock (SetSession), whose runs would not be locked.
// Otherwise tasks is read again by the delete, so a task made after the lock goes with the
// changes that name it.
func deleteTasks(ctx context.Context, db bun.IDB, appID string, tasks func(bun.QueryBuilder) bun.QueryBuilder,
	onlyLocked bool, changes string, args ...any) (UserTasks, error) {
	var counts UserTasks
	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// A write to a task or a run that commits while this waits on its row recorded a change
		// the redaction would not see: waiting here first lets the next statement see it. Some
		// run writes lock only the run (TakeCall), so the runs are locked too, after their
		// tasks, as Claim and Finish lock them.
		var locked []string
		if err := tx.NewSelect().Model((*ScheduledTask)(nil)).Column("id").
			Where("customer_id = ?", appID).ApplyQueryBuilder(tasks).Order("id").For("UPDATE").Scan(ctx, &locked); err != nil {
			return err
		}
		if onlyLocked && len(locked) == 0 {
			return nil
		}
		var runs []string
		if err := tx.NewSelect().Model((*ScheduledTaskRun)(nil)).Column("id").
			Where("customer_id = ? AND task_id IN (?)", appID, bun.In(locked)).Order("id").For("UPDATE").Scan(ctx, &runs); err != nil {
			return err
		}
		// One statement, so the delete and the redaction read one snapshot: a task that commits
		// between two statements would be deleted with its changes kept. The redaction's reads of
		// the live tasks read that snapshot, from before the delete.
		redacted := tx.NewUpdate().Model((*dataChange)(nil)).Set("op = ?", ChangeDelete).Set("payload = NULL").
			Where(changes, args...).Returning("1")
		deleted := tx.NewDelete().Model((*ScheduledTask)(nil)).Where("customer_id = ?", appID).Returning("id")
		if onlyLocked {
			deleted = deleted.Where("id IN (?)", bun.In(locked))
		} else {
			deleted = deleted.ApplyQueryBuilder(tasks)
		}
		return tx.NewSelect().With("deleted", deleted).With("redacted", redacted).
			ColumnExpr("(SELECT count(*) FROM deleted) AS tasks").
			ColumnExpr("(SELECT count(*) FROM redacted) AS changes").
			Scan(ctx, &counts)
	})
	return counts, err
}
