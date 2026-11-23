-- Squash cleanup: rewrites a database's migration tracking from the history squashed into each
-- set's initial migration (20270110000000_initial.sql) to that initial migration alone, the
-- "fake apply". Run it once on every database, after every router using it runs a release with
-- the squashed migrations: a release from before would run the history again on the rewritten
-- tracking. There is no going back to such a release once it has run.
--
--   psql "$DSN" -v set=global|shard|both [-v apply=1] [-v drop_strays=1] [-v show=1] -f cleanup.sql
--
-- set is the migration sets the database holds: global for the shared database, shard for a
-- region's own, both for a deployment of one database. Without apply=1 every step runs and is
-- rolled back. In one transaction, under the router's migrate lock, it:
--
--   1. refuses a database whose schema is not exactly the initial migrations', by a fingerprint
--      of its catalogue taken from a database the initial migrations made (show=1 prints the
--      lines fingerprinted, to diff against that database's);
--   2. refuses a tracking table that does not record exactly the squashed history: one short
--      of it, or holding a version of a branch's own;
--   3. leaves each tracking table recording the initial migration only;
--   4. drops goose_db_version, which tracked the single history from before the split;
--   5. with apply=1, recreates limits_level_check as the initial migration writes it: a
--      database made before Postgres 18 keeps the nesting of its OR chain, which a new one
--      flattens, though they check the same;
--   6. with drop_strays=1, drops the empty tables of the other set a database kept from before
--      the split, and the functions only they used. One holding rows is left for a person.
--
-- A database it already cleaned is checked and left as it is.

\set ON_ERROR_STOP on
\set QUIET on

\if :{?set}
\else
  \echo 'usage: psql "$DSN" -v set=global|shard|both [-v apply=1] [-v drop_strays=1] [-v show=1] -f cleanup.sql'
  SELECT 1/0 AS set_is_required;
\endif
\if :{?apply}
\else
  \set apply 0
\endif
\if :{?drop_strays}
\else
  \set drop_strays 0
\endif
\if :{?show}
\else
  \set show 0
\endif

\set initial 20270110000000

\set global_versions '{20270110000000,20270111000000,20270112000000,20270112000100,20270112000200,20270112000300,20270113000000,20270113000001,20270113000002,20270115000100,20270115000200,20270115000300,20270115000400,20270115000500,20270116000000,20270116000002,20270116000003,20270117000300,20270117000400,20270119000000,20270122000000,20270122000100,20270123021810,20270124093000}'
\set shard_versions '{20270110000000,20270114000000,20270116000001,20270116000004,20270117000100,20270117000400,20270118000000,20270118000100,20270124000000,20270124000100,20270126000000}'

\set global_tables '{activity_digests,agent_configs,agent_logs,api_keys,app_organizations,apps,audit_log,business_profiles,campaign_attempts,campaign_contacts,campaigns,channel_accounts,channel_deliveries,channel_link_failures,channel_links,channel_thread_inbound,channel_thread_members,channel_thread_messages,channel_thread_waiting,channel_threads,channel_turns,chat_inbound,connection_event_subscriptions,connector_audit,connector_authorization_attempts,connector_broken_revisions,connector_config_tokens,connector_connections,connector_definitions,connector_event_deliveries,connector_event_destinations,connector_invocations,connector_oauth_clients,connector_tool_pins,contact_map,custom_models,customer_activity,data_change_capture,data_change_starts,data_changes,data_move_cursors,deliveries_seen,dlc_review_logs,dlc_use_cases,episodes,image_jobs,knowledge_documents,knowledge_namespaces,knowledge_urls,limit_grant_reconciles,limit_grant_uses,limit_grants,limits,notices,opt_outs,organization_tiers,organizations,phone_numbers,policies,price_changes,price_list_entries,provider_keys,router_configs,sandbox_recipients,scheduled_task_runs,scheduled_tasks,simulation_cases,simulation_runs,simulations,sip_trunks,skills,stream_app_tokens,stream_fallback_uses,stream_trunks,telephony_suspensions,usage_resets,user_phone_history,user_phones,users,voice_bindings,voice_samples,voices}'
\set global_sequences '{agent_logs_id_seq,campaign_contacts_seq_seq,channel_thread_inbound_seq_seq,chat_inbound_seq_seq,data_changes_seq_seq,limit_grants_id_seq,limits_id_seq,phone_numbers_id_seq,price_entries_id_seq,usage_resets_id_seq}'
\set global_functions '{price_changes_guard,price_entries_guard,record_data_change}'

\set shard_tables '{agent_branch_messages,agent_branch_switches,agent_response_items,agent_responses,agent_session_tools,agent_sessions,call_bridges,call_legs,call_participants,call_resources,calls,conversation_documents,data_change_capture,data_change_starts,data_changes,recordings,requests,stats_daily,stats_hourly,stats_tags_daily,stats_tags_hourly,turn_latency_hourly,turn_stats_hourly}'
\set shard_sequences '{call_legs_id_seq,call_participants_id_seq,call_resources_id_seq,data_changes_seq_seq,stt_requests_id_seq}'
\set shard_functions '{record_data_change}'

-- The fingerprint of each kind of database the initial migrations make, on Postgres 18.
\set global_fingerprint '33b23c834d6849661a29da3c6fd22682'
\set shard_fingerprint 'fffce33c2a9db8243625ebe3e3e6644a'
\set both_fingerprint 'e7edc5427addf775bf3a35331b0b0f09'

\set limits_level_check 'CHECK ((((subject = ''organization''::text) AND (level = ''organization''::text)) OR ((subject = ''app''::text) AND (level = ''app''::text)) OR ((subject = ''api_key''::text) AND (level = ''organization''::text)) OR ((subject = ''api_key''::text) AND (level = ''app''::text)) OR ((subject = ''api_key''::text) AND (level = ''api_key''::text)) OR ((subject = ''user''::text) AND (level = ''organization''::text)) OR ((subject = ''user''::text) AND (level = ''app''::text)) OR ((subject = ''user''::text) AND (level = ''user''::text)) OR ((subject = ''category''::text) AND (level = ''organization''::text)) OR ((subject = ''app_category''::text) AND (level = ''app''::text))))'

BEGIN;
-- A router waiting on a lock this holds waits with it; this waits on nobody for long.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '5min';

CREATE TEMP TABLE squash_sets ON COMMIT DROP AS
SELECT name, 'goose_' || name || '_version' AS tracker, versions::bigint[] AS versions
FROM (VALUES ('global', :'global_versions'), ('shard', :'shard_versions')) AS sets (name, versions)
WHERE :'set' IN (name, 'both');

CREATE TEMP TABLE squash_plan ON COMMIT DROP AS
SELECT
    ARRAY(SELECT DISTINCT unnest(CASE WHEN :'set' IN ('global', 'both') THEN :'global_tables'::text[] END
        || CASE WHEN :'set' IN ('shard', 'both') THEN :'shard_tables'::text[] END)) AS tables,
    ARRAY(SELECT DISTINCT unnest(CASE WHEN :'set' IN ('global', 'both') THEN :'global_sequences'::text[] END
        || CASE WHEN :'set' IN ('shard', 'both') THEN :'shard_sequences'::text[] END)) AS sequences,
    ARRAY(SELECT DISTINCT unnest(CASE WHEN :'set' IN ('global', 'both') THEN :'global_functions'::text[] END
        || CASE WHEN :'set' IN ('shard', 'both') THEN :'shard_functions'::text[] END)) AS functions,
    CASE :'set' WHEN 'global' THEN :'global_fingerprint' WHEN 'shard' THEN :'shard_fingerprint' WHEN 'both' THEN :'both_fingerprint' END
        AS fingerprint,
    :apply::int = 1 AS apply,
    :drop_strays::int = 1 AS drop_strays,
    :'limits_level_check'::text AS limits_level_check;

DO $$
BEGIN
    IF (SELECT fingerprint IS NULL FROM squash_plan) THEN
        RAISE EXCEPTION 'set must be global, shard or both';
    END IF;
END $$;

DO $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('router.migrate', 0));
END $$;

-- 5. Before the fingerprint, so that it is taken of what the initial migration writes.
DO $$
DECLARE
    plan squash_plan;
    held text;
BEGIN
    SELECT * INTO plan FROM squash_plan;
    SELECT pg_get_constraintdef(oid) INTO held FROM pg_constraint
    WHERE conname = 'limits_level_check' AND conrelid = to_regclass('public.limits');
    IF NOT plan.apply OR held IS NULL OR held = plan.limits_level_check THEN
        RETURN;
    END IF;
    EXECUTE 'ALTER TABLE limits DROP CONSTRAINT limits_level_check, ADD CONSTRAINT limits_level_check ' || plan.limits_level_check;
    RAISE NOTICE 'recreated limits_level_check as the initial migration writes it';
END $$;

-- 1. What the set's objects are, each as one line. Neither owners nor the data_changes_recorded
-- triggers the router attaches as it starts are the migrations'. A CHECK is compared without
-- its parentheses, which only the nesting of an OR chain made before Postgres 18 differs in.
CREATE TEMP TABLE squash_described ON COMMIT DROP AS
WITH plan AS (SELECT * FROM squash_plan),
tables AS (
    SELECT c.oid, c.relname, c.relkind, c.relpersistence, c.reloptions
    FROM pg_class c, plan
    WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND c.relname = ANY (plan.tables)
)
SELECT format('table %s %s %s %s', relname, relkind, relpersistence, coalesce(array_to_string(reloptions, ','), '')) AS line
FROM tables
UNION ALL
SELECT format('missing %s', expected)
FROM plan, unnest(plan.tables || plan.sequences) AS expected
WHERE to_regclass('public.' || expected) IS NULL
UNION ALL
SELECT format('column %s %s %s %s %s %s %s %s %s', t.relname,
    row_number() OVER (PARTITION BY t.oid ORDER BY a.attnum), a.attname, format_type(a.atttypid, a.atttypmod),
    a.attnotnull, a.attidentity, a.attgenerated, a.attcollation::regcollation, coalesce(pg_get_expr(d.adbin, d.adrelid), ''))
FROM tables t
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum
UNION ALL
SELECT format('constraint %s %s %s %s %s %s', t.relname, co.conname, co.contype, co.convalidated, co.condeferrable,
    CASE co.contype WHEN 'c' THEN translate(pg_get_constraintdef(co.oid), '()', '') ELSE pg_get_constraintdef(co.oid) END)
FROM tables t
JOIN pg_constraint co ON co.conrelid = t.oid
UNION ALL
SELECT format('index %s %s', pg_get_indexdef(i.indexrelid), i.indisvalid)
FROM tables t
JOIN pg_index i ON i.indrelid = t.oid
UNION ALL
SELECT format('trigger %s %s', pg_get_triggerdef(tg.oid), tg.tgenabled)
FROM tables t
JOIN pg_trigger tg ON tg.tgrelid = t.oid AND NOT tg.tgisinternal AND tg.tgname <> 'data_changes_recorded'
UNION ALL
SELECT format('function %s %s', p.proname, md5(pg_get_functiondef(p.oid)))
FROM pg_proc p, plan
WHERE p.pronamespace = 'public'::regnamespace AND p.proname = ANY (plan.functions)
UNION ALL
SELECT format('sequence %s %s %s %s %s %s %s %s %s', c.relname, format_type(s.seqtypid, NULL), s.seqstart, s.seqincrement,
    s.seqmin, s.seqmax, s.seqcache, s.seqcycle, coalesce(owner.name, ''))
FROM pg_sequence s
JOIN pg_class c ON c.oid = s.seqrelid
CROSS JOIN plan
LEFT JOIN LATERAL (
    SELECT r.relname || '.' || a.attname AS name
    FROM pg_depend dep
    JOIN pg_class r ON r.oid = dep.refobjid
    JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = dep.refobjsubid
    WHERE dep.classid = 'pg_class'::regclass AND dep.objid = c.oid AND dep.deptype = 'a'
) owner ON true
WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY (plan.sequences);

\if :show
SELECT line FROM squash_described ORDER BY line;
\endif

DO $$
DECLARE
    expected text := (SELECT fingerprint FROM squash_plan);
    held text := (SELECT md5(string_agg(line, E'\n' ORDER BY line)) FROM squash_described);
BEGIN
    IF held <> expected THEN
        RAISE EXCEPTION 'the schema is not the initial migrations'': fingerprint % where % was expected; rerun with -v show=1 and diff the lines against a database the initial migrations made', held, expected;
    END IF;
    RAISE NOTICE 'the schema is the initial migrations'' (%)', held;
END $$;

-- 2 and 3.
DO $$
DECLARE
    plan squash_plan;
    s squash_sets;
    recorded bigint[];
    missing bigint[];
    extra bigint[];
BEGIN
    SELECT * INTO plan FROM squash_plan;
    FOR s IN SELECT * FROM squash_sets ORDER BY name LOOP
        IF to_regclass('public.' || s.tracker) IS NULL THEN
            RAISE EXCEPTION '% does not exist: this database never ran the % set', s.tracker, s.name;
        END IF;
        EXECUTE format('SELECT coalesce(array_agg(DISTINCT version_id ORDER BY version_id), ''{}'') FROM %I WHERE is_applied AND version_id <> 0', s.tracker)
            INTO recorded;
        IF recorded = ARRAY[20270110000000::bigint] THEN
            RAISE NOTICE '% already records the initial migration alone', s.tracker;
            CONTINUE;
        END IF;
        missing := ARRAY(SELECT unnest(s.versions) EXCEPT SELECT unnest(recorded) ORDER BY 1);
        extra := ARRAY(SELECT unnest(recorded) EXCEPT SELECT unnest(s.versions) ORDER BY 1);
        IF missing <> '{}' THEN
            RAISE EXCEPTION '% has not applied %, of the history squashed into the initial migration', s.tracker, missing;
        END IF;
        IF extra <> '{}' THEN
            RAISE EXCEPTION '% records %, which the squashed history does not hold: a branch migrated this database', s.tracker, extra;
        END IF;
        EXECUTE format('DELETE FROM %I WHERE version_id <> 0', s.tracker);
        EXECUTE format('INSERT INTO %I (version_id, is_applied) VALUES (20270110000000, true)', s.tracker);
        RAISE NOTICE '% recorded % versions; it now records the initial migration alone', s.tracker, cardinality(recorded);
    END LOOP;
END $$;

-- 4. The ledger keeps its own versions in a goose_db_version too, numbered from 1, in a database
-- of its own; one holding such a version is not the router's to drop.
DO $$
BEGIN
    IF to_regclass('public.goose_db_version') IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM goose_db_version WHERE version_id BETWEEN 1 AND 20000000000000) THEN
        RAISE NOTICE 'left goose_db_version: it holds versions that are not the router''s';
        RETURN;
    END IF;
    DROP TABLE goose_db_version;
    RAISE NOTICE 'dropped goose_db_version, the single history''s tracking from before the split';
END $$;

-- 6. A stray another stray points at is dropped on a later pass, once the one pointing is gone.
DO $$
DECLARE
    plan squash_plan;
    strays text[];
    kept text[];
    stray text;
    empty boolean;
    dropped int := 0;
BEGIN
    SELECT * INTO plan FROM squash_plan;
    IF NOT plan.drop_strays THEN
        RETURN;
    END IF;
    strays := ARRAY(
        SELECT relname FROM pg_class
        WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p')
          AND relname <> ALL (plan.tables) AND relname NOT LIKE 'goose\_%'
        ORDER BY relname);
    LOOP
        kept := '{}';
        FOREACH stray IN ARRAY strays LOOP
            EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM %I)', stray) INTO empty;
            IF NOT empty THEN
                RAISE NOTICE 'left stray %: it holds rows', stray;
                CONTINUE;
            END IF;
            BEGIN
                EXECUTE format('DROP TABLE %I', stray);
                dropped := dropped + 1;
            EXCEPTION WHEN dependent_objects_still_exist THEN
                kept := kept || stray;
            END;
        END LOOP;
        EXIT WHEN kept = '{}' OR cardinality(kept) = cardinality(strays);
        strays := kept;
    END LOOP;
    IF kept <> '{}' THEN
        RAISE NOTICE 'left strays %: a table holding rows points at them', kept;
    END IF;
    FOR stray IN
        SELECT c.relname FROM pg_class c
        WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'S'
          AND c.relname <> ALL (plan.sequences) AND c.relname NOT LIKE 'goose\_%'
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('a', 'i'))
    LOOP
        EXECUTE format('DROP SEQUENCE %I', stray);
        RAISE NOTICE 'dropped stray sequence %', stray;
    END LOOP;
    FOR stray IN
        SELECT p.oid::regprocedure::text FROM pg_proc p
        WHERE p.pronamespace = 'public'::regnamespace AND p.proname <> ALL (plan.functions)
    LOOP
        BEGIN
            EXECUTE format('DROP FUNCTION %s', stray);
            RAISE NOTICE 'dropped stray function %', stray;
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE NOTICE 'left stray function %: a trigger left with its table uses it', stray;
        END;
    END LOOP;
    RAISE NOTICE 'dropped % empty stray tables', dropped;
END $$;

\if :apply
COMMIT;
\echo 'committed'
\else
ROLLBACK;
\echo 'dry run: rolled back; rerun with -v apply=1 to commit'
\endif
