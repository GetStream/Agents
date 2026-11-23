-- +goose Up
SET LOCAL lock_timeout = '5s';

-- One table for every notice an organization is sent once (store.Notice): the first router to
-- write an organization's row of a kind and key is the one that sends it. dedupe_key is what
-- makes a kind's notice due again: nothing for a money limit, which is noticed once ever, the
-- credit granted for a balance warning, the UTC day for a cost anomaly. payload is the kind's
-- own fields. A notice queued as webhook deliveries is posted as it is written.
CREATE TABLE notices (
    organization_id TEXT NOT NULL CHECK (organization_id <> ''),
    kind TEXT NOT NULL CHECK (kind IN (
        'no_credit', 'spend_hard_limit', 'spend_limit', 'balance_low', 'balance_exhausted', 'cost_anomaly')),
    dedupe_key TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, kind, dedupe_key)
);

CREATE INDEX notices_unposted ON notices (claimed_at) WHERE posted_at IS NULL;

INSERT INTO notices (organization_id, kind, dedupe_key, payload, created_at, claimed_at, posted_at)
SELECT organization_id, kind, '',
       jsonb_build_object('app_id', app_id, 'tier', tier, 'enforced', enforced, 'message', message),
       created_at, claimed_at, posted_at
FROM limit_notices
UNION ALL
SELECT organization_id, kind, granted_micros::text,
       jsonb_build_object('balance_micros', balance_micros, 'threshold_micros', threshold_micros),
       created_at, created_at, created_at
FROM balance_notices
UNION ALL
SELECT organization_id, 'cost_anomaly', to_char(day, 'YYYY-MM-DD'),
       jsonb_build_object('spent_micros', spent_micros, 'baseline_micros', baseline_micros),
       created_at, claimed_at, posted_at
FROM cost_anomaly_notices;

DROP TABLE limit_notices, balance_notices, cost_anomaly_notices;

-- +goose Down
CREATE TABLE limit_notices (
    organization_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind = ANY (ARRAY['no_credit', 'spend_hard_limit', 'spend_limit'])),
    app_id TEXT NOT NULL DEFAULT '',
    tier TEXT NOT NULL DEFAULT '',
    enforced BOOLEAN NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, kind)
);
CREATE INDEX limit_notices_unposted ON limit_notices (claimed_at) WHERE posted_at IS NULL;

CREATE TABLE balance_notices (
    organization_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind = ANY (ARRAY['balance_low', 'balance_exhausted'])),
    granted_micros BIGINT NOT NULL,
    balance_micros BIGINT NOT NULL,
    threshold_micros BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, kind, granted_micros)
);

CREATE TABLE cost_anomaly_notices (
    organization_id TEXT NOT NULL,
    day DATE NOT NULL,
    spent_micros BIGINT NOT NULL,
    baseline_micros BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, day)
);
CREATE INDEX cost_anomaly_notices_unposted ON cost_anomaly_notices (claimed_at) WHERE posted_at IS NULL;

INSERT INTO limit_notices (organization_id, kind, app_id, tier, enforced, message, created_at, claimed_at, posted_at)
SELECT organization_id, kind, COALESCE(payload->>'app_id', ''), COALESCE(payload->>'tier', ''),
       COALESCE((payload->>'enforced')::boolean, false), COALESCE(payload->>'message', ''),
       created_at, claimed_at, posted_at
FROM notices WHERE kind IN ('no_credit', 'spend_hard_limit', 'spend_limit');

INSERT INTO balance_notices (organization_id, kind, granted_micros, balance_micros, threshold_micros, created_at)
SELECT organization_id, kind, dedupe_key::bigint, (payload->>'balance_micros')::bigint, (payload->>'threshold_micros')::bigint,
       created_at
FROM notices WHERE kind IN ('balance_low', 'balance_exhausted');

INSERT INTO cost_anomaly_notices (organization_id, day, spent_micros, baseline_micros, created_at, claimed_at, posted_at)
SELECT organization_id, dedupe_key::date, (payload->>'spent_micros')::bigint, (payload->>'baseline_micros')::bigint,
       created_at, claimed_at, posted_at
FROM notices WHERE kind = 'cost_anomaly';

DROP TABLE notices;
