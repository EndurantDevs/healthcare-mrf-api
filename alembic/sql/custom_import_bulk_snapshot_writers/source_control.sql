-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE TABLE __CONTROL__.source_bulk_completion (
    batch_id uuid PRIMARY KEY,
    transaction_id xid8 NOT NULL,
    attempted_count integer NOT NULL CHECK (attempted_count BETWEEN 1 AND 100000),
    build_id bigint NOT NULL,
    stream_slot smallint NOT NULL,
    first_source bigint NOT NULL,
    after_source bigint NOT NULL,
    input_sha256 bytea NOT NULL CHECK (octet_length(input_sha256)=32),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(batch_id,transaction_id)
);

-- statement boundary --

CREATE TABLE __CONTROL__.source_bulk_authorization (
    batch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    opened_by name NOT NULL,
    build_id bigint NOT NULL REFERENCES __CONTROL__.custom_import_build_attempt(build_id),
    dataset_id bigint NOT NULL,
    definition_revision_id bigint NOT NULL,
    schema_revision_id bigint NOT NULL,
    execution_id bigint NOT NULL,
    capture_bundle_id bigint NOT NULL,
    stream_slot smallint NOT NULL,
    fence bigint NOT NULL,
    token_hash bytea NOT NULL CHECK (octet_length(token_hash)=32),
    expected_count integer NOT NULL CHECK (expected_count BETWEEN 1 AND 100000),
    source_byte_limit bigint NOT NULL CHECK (source_byte_limit BETWEEN 1 AND 268435456),
    first_pack integer NOT NULL,
    first_part integer NOT NULL,
    first_row bigint NOT NULL,
    first_source bigint NOT NULL,
    accepting boolean NOT NULL DEFAULT true,
    UNIQUE(batch_id,transaction_id,accepting),
    FOREIGN KEY(batch_id,transaction_id) REFERENCES __CONTROL__.source_bulk_completion(batch_id,transaction_id)
        DEFERRABLE INITIALLY DEFERRED
);

-- statement boundary --

CREATE INDEX source_bulk_open_build_idx ON __CONTROL__.source_bulk_authorization(build_id) WHERE accepting;

-- statement boundary --

CREATE FUNCTION __CONTROL__.source_bulk_canonical(doc jsonb) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $fn$
    SELECT CASE jsonb_typeof(doc)
    WHEN 'object' THEN '{'||coalesce((SELECT string_agg(to_jsonb(key)::text||':'||
        __CONTROL__.source_bulk_canonical(value),',' ORDER BY key COLLATE "C") FROM jsonb_each(doc)),'')||'}'
    WHEN 'array' THEN '['||coalesce((SELECT string_agg(__CONTROL__.source_bulk_canonical(value),','
        ORDER BY ordinal) FROM jsonb_array_elements(doc) WITH ORDINALITY x(value,ordinal)),'')||']'
    ELSE doc::text END
$fn$;

-- statement boundary --

CREATE FUNCTION __CONTROL__.source_bulk_digest(domain text, document text) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $fn$
    SELECT sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')
        ||convert_to(domain,'UTF8')||decode('00','hex')||convert_to(document,'UTF8'))
$fn$;
