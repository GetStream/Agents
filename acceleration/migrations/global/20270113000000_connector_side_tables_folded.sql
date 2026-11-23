-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Each of these tables held one row per row of its parent, read with the parent every time it
-- was read. As columns they cost no second read or write. The client and the last validation
-- are still read without unsealing the credentials, and the audit credential and the argument
-- shapes are a few names and fingerprints, not a payload worth keeping apart.

ALTER TABLE connector_connections
    ADD COLUMN client_registration text NOT NULL DEFAULT '',
    ADD COLUMN client_id text NOT NULL DEFAULT '',
    ADD COLUMN client_updated_at timestamp with time zone,
    ADD COLUMN validation_revision integer NOT NULL DEFAULT 0,
    ADD COLUMN validation_status text NOT NULL DEFAULT '',
    ADD COLUMN validation_code text NOT NULL DEFAULT '',
    ADD COLUMN validation_error text NOT NULL DEFAULT '',
    ADD COLUMN validation_checked_at timestamp with time zone;

ALTER TABLE connector_audit ADD COLUMN credential jsonb;

ALTER TABLE connector_invocations ADD COLUMN arguments jsonb;

UPDATE connector_connections AS cc
SET client_registration = ccc.registration, client_id = ccc.client_id, client_updated_at = ccc.updated_at
FROM connector_connection_clients AS ccc
WHERE ccc.connection_id = cc.id;

UPDATE connector_connections AS cc
SET validation_revision = ccv.revision, validation_status = ccv.status, validation_code = ccv.code,
    validation_error = ccv.error, validation_checked_at = ccv.checked_at
FROM connector_connection_validations AS ccv
WHERE ccv.connection_id = cc.id;

-- The keys are store.ConnectorAuditCredential's; an expiry the provider did not give is left out.
UPDATE connector_audit AS ca
SET credential = jsonb_strip_nulls(jsonb_build_object(
    'access_fingerprint', cac.access_fingerprint,
    'previous_access_fingerprint', cac.previous_access_fingerprint,
    'refresh_fingerprint', cac.refresh_fingerprint,
    'previous_refresh_fingerprint', cac.previous_refresh_fingerprint,
    'rotated', cac.rotated,
    'access_expires_at', cac.access_expires_at,
    'refresh_expires_at', cac.refresh_expires_at))
FROM connector_audit_credentials AS cac
WHERE cac.audit_id = ca.id;

UPDATE connector_invocations AS ci
SET arguments = cia.shape
FROM connector_invocation_arguments AS cia
WHERE cia.invocation_id = ci.id;

DROP TABLE connector_connection_clients;
DROP TABLE connector_connection_validations;
DROP TABLE connector_audit_credentials;
DROP TABLE connector_invocation_arguments;

-- +goose Down

CREATE TABLE connector_connection_clients (
    connection_id text PRIMARY KEY REFERENCES connector_connections(id) ON DELETE CASCADE,
    registration text NOT NULL,
    client_id text NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE connector_connection_validations (
    connection_id text PRIMARY KEY REFERENCES connector_connections(id) ON DELETE CASCADE,
    revision integer NOT NULL,
    status text NOT NULL,
    code text NOT NULL DEFAULT '',
    error text NOT NULL DEFAULT '',
    checked_at timestamp with time zone NOT NULL
);

CREATE TABLE connector_audit_credentials (
    audit_id text PRIMARY KEY REFERENCES connector_audit(id) ON DELETE CASCADE,
    access_fingerprint text NOT NULL DEFAULT '',
    previous_access_fingerprint text NOT NULL DEFAULT '',
    refresh_fingerprint text NOT NULL DEFAULT '',
    previous_refresh_fingerprint text NOT NULL DEFAULT '',
    rotated boolean NOT NULL DEFAULT false,
    access_expires_at timestamp with time zone,
    refresh_expires_at timestamp with time zone
);

CREATE TABLE connector_invocation_arguments (
    invocation_id text PRIMARY KEY REFERENCES connector_invocations(id) ON DELETE CASCADE,
    shape jsonb NOT NULL
);

INSERT INTO connector_connection_clients (connection_id, registration, client_id, updated_at)
SELECT id, client_registration, client_id, client_updated_at FROM connector_connections
WHERE client_updated_at IS NOT NULL;

INSERT INTO connector_connection_validations (connection_id, revision, status, code, error, checked_at)
SELECT id, validation_revision, validation_status, validation_code, validation_error, validation_checked_at
FROM connector_connections WHERE validation_checked_at IS NOT NULL;

INSERT INTO connector_audit_credentials (audit_id, access_fingerprint, previous_access_fingerprint,
    refresh_fingerprint, previous_refresh_fingerprint, rotated, access_expires_at, refresh_expires_at)
SELECT id, coalesce(credential ->> 'access_fingerprint', ''), coalesce(credential ->> 'previous_access_fingerprint', ''),
    coalesce(credential ->> 'refresh_fingerprint', ''), coalesce(credential ->> 'previous_refresh_fingerprint', ''),
    coalesce((credential ->> 'rotated')::boolean, false), (credential ->> 'access_expires_at')::timestamptz,
    (credential ->> 'refresh_expires_at')::timestamptz
FROM connector_audit WHERE credential IS NOT NULL;

INSERT INTO connector_invocation_arguments (invocation_id, shape)
SELECT id, arguments FROM connector_invocations WHERE arguments IS NOT NULL;

ALTER TABLE connector_connections
    DROP COLUMN client_registration,
    DROP COLUMN client_id,
    DROP COLUMN client_updated_at,
    DROP COLUMN validation_revision,
    DROP COLUMN validation_status,
    DROP COLUMN validation_code,
    DROP COLUMN validation_error,
    DROP COLUMN validation_checked_at;

ALTER TABLE connector_audit DROP COLUMN credential;

ALTER TABLE connector_invocations DROP COLUMN arguments;
