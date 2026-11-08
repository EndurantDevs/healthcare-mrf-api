# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fenced incremental capture with protected logical retained-byte accounting.

The migration role must be a protected owner: writers must neither own these
relations/functions nor inherit or SET ROLE to their owner or alter triggers.
Admission must verify that boundary separately; schema installation cannot
establish deployment role membership. No existing payloads are scanned here.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

from alembic import op

revision = "20261002000000_custom_import_segmented_capture"
down_revision = "20260929040000_reference_source_generation_guard"
branch_labels = None
depends_on = None

_V2 = "custom-import/parquet-parts/v2"
_BUNDLE = "custom_import_capture_bundle"
_CAPTURE = "custom_import_capture"
_PART = "custom_import_capture_parquet_part"
_USAGE = "custom_import_capture_usage"
_COUNTERS = (
    "committed_part_count",
    "committed_byte_count",
    "committed_decoded_byte_count",
    "committed_arrow_byte_count",
    "committed_record_count",
    "committed_manifest_byte_count",
)
_BUDGETS = (
    "maximum_parts",
    "maximum_compressed_bytes",
    "maximum_decoded_bytes",
    "maximum_arrow_bytes",
    "maximum_records",
    "maximum_manifest_bytes",
)
_PART_COUNTER_INCREMENTS = (
    "1",
    "NEW.byte_count",
    "NEW.decoded_byte_count",
    "NEW.arrow_byte_count",
    "NEW.record_count",
    "octet_length(NEW.canonical_capture_manifest)",
)
_PAYLOAD_DOMAIN = "custom-import/parquet-parts/v1\0".encode().hex()
_MANIFEST_DOMAIN = "custom-import/parquet-manifest-accounting/v1\0".encode().hex()


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _t(schema: str, name: str) -> str:
    return f"{_q(schema)}.{_q(name)}"


def _legacy():
    path = Path(__file__).with_name("20260922000000_custom_import_durable_parquet_capture.py")
    spec = importlib.util.spec_from_file_location("durable_capture_schema", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _function(
    schema: str, name: str, body: str, *, definer: bool = True, signature: str = "", returns: str = "trigger"
) -> None:
    op.execute(f"""CREATE FUNCTION {_t(schema, name)}({signature}) RETURNS {returns}
        LANGUAGE plpgsql SECURITY {"DEFINER" if definer else "INVOKER"}
        SET search_path = pg_catalog AS $body$ {body} $body$""")
    # Trigger invocation does not require caller EXECUTE permission.
    types = ", ".join(field.strip().split()[-1] for field in signature.split(",") if field.strip())
    op.execute(f"REVOKE ALL ON FUNCTION {_t(schema, name)}({types}) FROM PUBLIC")


def _trigger(
    schema: str, table: str, name: str, events: str, function: str, *, deferred: bool = False, statement: bool = False
) -> None:
    op.execute(f"""CREATE {"CONSTRAINT " if deferred else ""}TRIGGER {_q(name)}
        {events} ON {_t(schema, table)} {"DEFERRABLE INITIALLY DEFERRED" if deferred else ""}
        FOR EACH {"STATEMENT" if statement else "ROW"} EXECUTE FUNCTION {_t(schema, function)}()""")
    op.execute(f"ALTER TABLE {_t(schema, table)} ENABLE ALWAYS TRIGGER {_q(name)}")


_BUNDLE_SCHEMA_SQL = """ALTER TABLE {bundle}
        ALTER COLUMN canonical_manifest DROP NOT NULL, ALTER COLUMN manifest_sha256 DROP NOT NULL,
        ALTER COLUMN sealed_at DROP NOT NULL,
        ADD COLUMN payload_contract varchar(63), ADD COLUMN capture_state varchar(16) NOT NULL DEFAULT 'sealed',
        ADD COLUMN producing_execution_id bigint, ADD COLUMN producing_fence bigint,
        ADD COLUMN producing_token_sha256 bytea, ADD COLUMN request_identity_sha256 bytea,
        ADD COLUMN source_binding_revision_id bigint, ADD COLUMN source_binding_sha256 bytea,
        ADD COLUMN source_request_sha256 bytea, ADD COLUMN statement_sha256 bytea,
        ADD COLUMN canonical_policy text, ADD COLUMN policy_sha256 bytea,
        ADD COLUMN acquisition_started_at timestamptz, ADD COLUMN acquisition_deadline_at timestamptz,
        ADD CONSTRAINT custom_import_capture_bundle_producer_fkey FOREIGN KEY
            (producing_execution_id, dataset_id, definition_revision_id, schema_revision_id)
            REFERENCES {execution_table}
            (execution_id, dataset_id, definition_revision_id, schema_revision_id),
        ADD CONSTRAINT custom_import_capture_bundle_attempt_key UNIQUE (producing_execution_id, producing_fence),
        ADD CONSTRAINT custom_import_capture_bundle_lifecycle_check CHECK (
            (payload_contract IS NULL AND capture_state = 'sealed'
             AND producing_execution_id IS NULL AND producing_fence IS NULL AND producing_token_sha256 IS NULL
             AND request_identity_sha256 IS NULL AND source_binding_revision_id IS NULL AND source_binding_sha256 IS NULL
             AND source_request_sha256 IS NULL AND statement_sha256 IS NULL
             AND canonical_policy IS NULL AND policy_sha256 IS NULL
             AND acquisition_started_at IS NULL AND acquisition_deadline_at IS NULL)
            OR (payload_contract IS NOT NULL AND payload_contract = '{payload_contract}' AND capture_state IN ('pending', 'sealed')
                AND producing_execution_id IS NOT NULL AND producing_fence IS NOT NULL AND producing_fence > 0
                AND octet_length(producing_token_sha256) = 32 AND producing_token_sha256 IS NOT NULL
                AND octet_length(request_identity_sha256) = 32 AND request_identity_sha256 IS NOT NULL
                AND octet_length(source_request_sha256) = 32 AND source_request_sha256 IS NOT NULL
                AND octet_length(statement_sha256) = 32 AND statement_sha256 IS NOT NULL
                AND octet_length(policy_sha256) = 32 AND policy_sha256 IS NOT NULL
                AND octet_length(canonical_policy) BETWEEN 2 AND 16384
                AND canonical_policy IS NOT NULL AND acquisition_started_at IS NOT NULL
                AND acquisition_deadline_at > acquisition_started_at AND acquisition_deadline_at IS NOT NULL
                AND ((source_binding_revision_id IS NULL AND source_binding_sha256 IS NULL)
                     OR (source_binding_revision_id IS NOT NULL AND source_binding_sha256 IS NOT NULL
                         AND octet_length(source_binding_sha256) = 32))))"""


_CAPTURE_SCHEMA_SQL = """ALTER TABLE {capture}
        ALTER COLUMN content_sha256 DROP NOT NULL, ALTER COLUMN byte_count DROP NOT NULL,
        ALTER COLUMN canonical_manifest DROP NOT NULL, ALTER COLUMN manifest_sha256 DROP NOT NULL,
        ALTER COLUMN sealed_at DROP NOT NULL,
        ADD COLUMN capture_state varchar(16) NOT NULL DEFAULT 'sealed', ADD COLUMN eof_at timestamptz,
        ADD COLUMN manifest_set_sha256 bytea,
        DROP CONSTRAINT custom_import_capture_payload_shape_check,
        ADD CONSTRAINT custom_import_capture_payload_shape_check CHECK (
            (payload_contract IS NULL AND payload_part_count IS NULL AND payload_set_sha256 IS NULL
                AND capture_state = 'sealed' AND eof_at IS NULL AND manifest_set_sha256 IS NULL)
            OR (payload_contract IS NOT NULL AND payload_contract = 'custom-import/parquet-parts/v1' AND capture_state = 'sealed'
                AND byte_count BETWEEN 1 AND 67108864 AND payload_part_count BETWEEN 1 AND 4096
                AND payload_part_count IS NOT NULL AND payload_set_sha256 IS NOT NULL
                AND octet_length(payload_set_sha256) = 32 AND eof_at IS NULL AND manifest_set_sha256 IS NULL)
            OR (payload_contract IS NOT NULL AND payload_contract = '{payload_contract}' AND capture_state IN ('pending', 'sealed') AND
                ((capture_state = 'pending' AND payload_part_count IS NULL AND payload_set_sha256 IS NULL
                    AND manifest_set_sha256 IS NULL)
                 OR (capture_state = 'sealed' AND payload_part_count BETWEEN 1 AND 131072
                    AND payload_part_count IS NOT NULL AND payload_set_sha256 IS NOT NULL
                    AND octet_length(payload_set_sha256) = 32 AND manifest_set_sha256 IS NOT NULL
                    AND octet_length(manifest_set_sha256) = 32 AND eof_at IS NOT NULL))))"""


_PART_SCHEMA_SQL = """ALTER TABLE {part}
        ADD COLUMN canonical_capture_manifest text, ADD COLUMN capture_manifest_sha256 bytea,
        ADD COLUMN decoded_byte_count bigint, ADD COLUMN arrow_byte_count bigint, ADD COLUMN record_count bigint,
        DROP CONSTRAINT custom_import_capture_parquet_part_shape_check,
        ADD CONSTRAINT custom_import_capture_parquet_part_shape_check CHECK (
            part_ordinal BETWEEN 1 AND 131072 AND byte_count BETWEEN 1 AND 67108864
            AND octet_length(payload) = byte_count AND octet_length(payload_sha256) = 32
            AND payload_sha256 = pg_catalog.sha256(payload)),
        ADD CONSTRAINT custom_import_capture_parquet_manifest_shape_check CHECK (
            (canonical_capture_manifest IS NULL AND capture_manifest_sha256 IS NULL
                AND decoded_byte_count IS NULL AND arrow_byte_count IS NULL AND record_count IS NULL)
            OR (canonical_capture_manifest IS NOT NULL AND capture_manifest_sha256 IS NOT NULL
                AND octet_length(canonical_capture_manifest) BETWEEN 2 AND 2097152
                AND capture_manifest_sha256 = pg_catalog.sha256(convert_to(canonical_capture_manifest, 'UTF8'))
                AND decoded_byte_count IS NOT NULL AND decoded_byte_count BETWEEN 1 AND 268435456
                AND arrow_byte_count IS NOT NULL AND arrow_byte_count BETWEEN 0 AND 268435456
                AND record_count IS NOT NULL AND record_count BETWEEN 0 AND 1000000))"""


def _schema_changes(schema: str) -> None:
    """Add pending capture headers, protected counters and retained usage."""

    bundle = _t(schema, _BUNDLE)
    capture = _t(schema, _CAPTURE)
    part = _t(schema, _PART)
    usage = _t(schema, _USAGE)
    op.execute(
        _BUNDLE_SCHEMA_SQL.format(
            bundle=bundle, execution_table=_t(schema, "custom_import_execution"), payload_contract=_V2
        )
    )
    op.execute(_CAPTURE_SCHEMA_SQL.format(capture=capture, payload_contract=_V2))
    for table in (bundle, capture):
        op.execute(
            f"ALTER TABLE {table} "
            + ", ".join(f"ADD COLUMN {name} bigint NOT NULL DEFAULT 0 CHECK ({name} >= 0)" for name in _COUNTERS)
        )
        finals = ["canonical_manifest", "manifest_sha256", "sealed_at"]
        if table == capture:
            finals += ["content_sha256", "byte_count"]
        nulls = " AND ".join(f"{name} IS NULL" for name in finals)
        present = " AND ".join(f"{name} IS NOT NULL" for name in finals)
        suffix = "bundle" if table == bundle else "stream"
        op.execute(f"""ALTER TABLE {table} ADD CONSTRAINT custom_import_capture_{suffix}_final_shape_check
            CHECK ((capture_state = 'pending' AND {nulls}) OR (capture_state = 'sealed' AND {present}))""")
    op.execute(_PART_SCHEMA_SQL.format(part=part))
    op.execute(f"""CREATE TABLE {usage} (
        dataset_id bigint PRIMARY KEY REFERENCES {_t(schema, "custom_import_dataset")}(dataset_id),
        retained_bytes bigint NOT NULL CONSTRAINT custom_import_capture_usage_shape_check CHECK (retained_bytes >= 0))""")
    op.execute(f"REVOKE ALL ON TABLE {usage} FROM PUBLIC")


_CANONICAL_JSON_SQL = """
    DECLARE result text; duplicate boolean;
    BEGIN
        CASE json_typeof(document)
        WHEN 'object' THEN
            SELECT '{{' || coalesce(string_agg(to_jsonb(entry.key)::text || ':' || {canonical}(entry.value),
                ',' ORDER BY entry.key COLLATE "C"),'') || '}}',
                count(*) <> count(DISTINCT entry.key COLLATE "C") INTO result, duplicate FROM json_each(document) entry;
            IF duplicate THEN RAISE EXCEPTION 'custom_import_capture_duplicate_json_key'; END IF;
            RETURN result;
        WHEN 'array' THEN
            SELECT '[' || coalesce(string_agg({canonical}(entry.value), ',' ORDER BY entry.ordinality),'') || ']'
                INTO result FROM json_array_elements(document) WITH ORDINALITY entry(value, ordinality);
            RETURN result;
        WHEN 'string' THEN RETURN to_jsonb(document #>> '{{}}')::text;
        ELSE RETURN btrim(document::text);
        END CASE;
    END;
    """


_POLICY_VALIDATION_SQL = """
    DECLARE
        policy jsonb; raw_policy json; section jsonb; key text; value json; ceiling bigint; minimum bigint; category text;
        maxima jsonb := '{{"maximum_compressed_bytes": 67108864,"maximum_decoded_bytes": 268435456,
            "maximum_record_bytes": 1048576,"maximum_records": 1000000,"maximum_fields_per_record": 1024,
            "read_chunk_bytes": 67108864}}'::jsonb;
    BEGIN
        IF document IS NULL OR octet_length(document) NOT BETWEEN 2 AND 16384
           OR digest IS DISTINCT FROM sha256(convert_to('custom-import/segmented-capture-policy/v1:' || document, 'UTF8')) THEN
            RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
        END IF;
        raw_policy := document::json;
        policy := raw_policy::jsonb;
        IF document IS DISTINCT FROM {canonical}(raw_policy) THEN
            RAISE EXCEPTION 'custom_import_segmented_policy_not_canonical';
        END IF;
        IF jsonb_typeof(policy) IS DISTINCT FROM 'object' OR
           NOT (policy ?& ARRAY['contract','part_limits','stream_budget','bundle_budget','maximum_part_arrow_bytes',
               'maximum_part_manifest_bytes','maximum_dataset_retained_bytes','acquisition_deadline_seconds']) OR
           (SELECT count(*) FROM jsonb_object_keys(policy)) <> 8 OR
           policy->>'contract' IS DISTINCT FROM 'custom-import/segmented-capture-policy/v1' THEN
            RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
        END IF;
        FOR category IN SELECT unnest(ARRAY['part_limits','stream_budget','bundle_budget']) LOOP
            section := policy->category;
            IF jsonb_typeof(section) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(section)) <> 6
               OR (category <> 'part_limits' AND NOT section ?& {budget_keys})
               OR (category = 'part_limits' AND NOT section ?& ARRAY(SELECT jsonb_object_keys(maxima))) THEN
                RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
            END IF;
            -- json preserves integer token spelling; jsonb normalizes exponent notation.
            FOR key, value IN SELECT * FROM json_each(raw_policy->category) LOOP
                ceiling := CASE WHEN category = 'part_limits' THEN (maxima->>key)::bigint
                    WHEN key = 'maximum_parts' THEN 131072 ELSE 9223372036854775807 END;
                IF json_typeof(value) <> 'number' OR value::text !~ '^[1-9][0-9]*$'
                   OR (value::text)::numeric > ceiling THEN
                    RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
                END IF;
                IF category = 'stream_budget' THEN
                    minimum := CASE key WHEN 'maximum_parts' THEN 1
                        WHEN 'maximum_arrow_bytes' THEN (policy->>'maximum_part_arrow_bytes')::bigint
                        WHEN 'maximum_manifest_bytes' THEN (policy->>'maximum_part_manifest_bytes')::bigint
                        ELSE (policy->'part_limits'->>key)::bigint END;
                    IF (value::text)::bigint < minimum THEN RAISE EXCEPTION 'custom_import_segmented_policy_invalid'; END IF;
                ELSIF category = 'bundle_budget' AND (value::text)::bigint < (policy->'stream_budget'->>key)::bigint THEN
                    RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
                END IF;
            END LOOP;
        END LOOP;
        FOR key, ceiling IN SELECT * FROM (VALUES ('maximum_part_arrow_bytes',268435456::bigint),
            ('maximum_part_manifest_bytes',2097152::bigint), ('maximum_dataset_retained_bytes',9223372036854775807::bigint),
            ('acquisition_deadline_seconds',86400::bigint)) AS limits(key, ceiling) LOOP
            value := raw_policy->key;
            IF json_typeof(value) <> 'number' OR value::text !~ '^[1-9][0-9]*$' OR (value::text)::numeric > ceiling THEN
                RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
            END IF;
        END LOOP;
        IF (policy->'part_limits'->>'maximum_decoded_bytes')::bigint < (policy->'part_limits'->>'maximum_record_bytes')::bigint
           OR (policy->'bundle_budget'->>'maximum_compressed_bytes')::numeric
              + (policy->'bundle_budget'->>'maximum_manifest_bytes')::numeric > (policy->>'maximum_dataset_retained_bytes')::bigint THEN
            RAISE EXCEPTION 'custom_import_segmented_policy_invalid';
        END IF;
        RETURN policy;
    END;
    """


def _policy_function(schema: str) -> None:
    """Install canonical JSON and strict segmented-policy validation."""

    canonical = _t(schema, "canonical_custom_import_capture_json")
    # Match UTF-8, sorted-key application canonical JSON without changing number spelling.
    _function(
        schema,
        "canonical_custom_import_capture_json",
        _CANONICAL_JSON_SQL.format(canonical=canonical),
        definer=False,
        signature="document json",
        returns="text",
    )
    budget_keys = "ARRAY[" + ",".join(f"'{key}'" for key in _BUDGETS) + "]"
    _function(
        schema,
        "validate_custom_import_segmented_policy",
        _POLICY_VALIDATION_SQL.format(canonical=canonical, budget_keys=budget_keys),
        signature="document text, digest bytea",
        returns="jsonb",
    )


def _authority_function(schema: str) -> None:
    bundle = _t(schema, _BUNDLE)
    _function(
        schema,
        "require_custom_import_segmented_authority",
        f"""
    DECLARE producer {_t(schema, "custom_import_execution")}%ROWTYPE;
        lease {_t(schema, "custom_import_lease")}%ROWTYPE; binding bytea;
    BEGIN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'custom_import_segmented_requires_read_committed';
        END IF;
        PERFORM 1 FROM {_t(schema, "custom_import_dataset")} WHERE dataset_id = target.dataset_id FOR UPDATE;
        SELECT * INTO producer FROM {_t(schema, "custom_import_execution")}
            WHERE execution_id = target.producing_execution_id FOR UPDATE;
        SELECT * INTO lease FROM {_t(schema, "custom_import_lease")}
            WHERE execution_id = target.producing_execution_id FOR UPDATE;
        IF producer.execution_id IS NULL OR producer.state <> 'running'
           OR (producer.dataset_id, producer.definition_revision_id, producer.schema_revision_id)
                IS DISTINCT FROM (target.dataset_id, target.definition_revision_id, target.schema_revision_id)
           OR producer.request_identity_sha256 IS DISTINCT FROM target.request_identity_sha256
           OR producer.source_binding_revision_id IS DISTINCT FROM target.source_binding_revision_id
           OR (producer.capture_bundle_id IS NOT NULL AND producer.capture_bundle_id <> target.capture_bundle_id)
           OR lease.fence IS DISTINCT FROM target.producing_fence
           OR lease.token_sha256 IS DISTINCT FROM target.producing_token_sha256
           OR lease.expires_at IS NULL OR lease.expires_at <= clock_timestamp()
           OR target.acquisition_deadline_at IS NULL OR target.acquisition_deadline_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'custom_import_segmented_producing_authority_lost';
        END IF;
        IF target.source_binding_revision_id IS NOT NULL THEN
            SELECT binding_sha256 INTO binding FROM {_t(schema, "custom_import_source_binding_revision")}
                WHERE source_binding_revision_id = target.source_binding_revision_id
                  AND dataset_id = target.dataset_id AND definition_revision_id = target.definition_revision_id
                  AND schema_revision_id = target.schema_revision_id;
            IF binding IS NULL OR binding IS DISTINCT FROM target.source_binding_sha256 THEN
                RAISE EXCEPTION 'custom_import_segmented_source_binding_mismatch';
            END IF;
        ELSIF target.source_binding_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'custom_import_segmented_source_binding_mismatch';
        END IF;
        RETURN;
    END;
    """,
        signature=f"target {bundle}",
        returns="void",
    )


def _progress_functions(schema: str) -> None:
    counters = "ARRAY[" + ",".join(f"'{name}'" for name in _COUNTERS) + "]"
    tables = ",".join(f"'{_t(schema, name)}'::regclass" for name in (_BUNDLE, _CAPTURE))
    account = f"'{_t(schema, 'account_custom_import_capture_parts')}()'::regprocedure"
    _function(
        schema,
        "guard_custom_import_segmented_progress",
        f"""
    DECLARE key text; owner_name name;
    BEGIN
        IF TG_RELID NOT IN ({tables}) OR TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW' OR TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'custom_import_segmented_progress_context_invalid';
        END IF;
        IF (SELECT bool_and(to_jsonb(NEW)->item.name = to_jsonb(OLD)->item.name)
            FROM unnest({counters}) AS item(name)) THEN RETURN NEW; END IF;
        SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid = {account};
        IF pg_trigger_depth() <> 2 OR current_user <> owner_name
           OR to_jsonb(NEW) - {counters} IS DISTINCT FROM to_jsonb(OLD) - {counters}
           OR OLD.capture_state <> 'pending' OR NEW.committed_part_count <> OLD.committed_part_count + 1 THEN
            RAISE EXCEPTION 'custom_import_segmented_progress_protected';
        END IF;
        FOREACH key IN ARRAY {counters} LOOP
            IF (to_jsonb(NEW)->>key)::bigint < (to_jsonb(OLD)->>key)::bigint THEN
                RAISE EXCEPTION 'custom_import_segmented_progress_protected';
            END IF;
        END LOOP;
        RETURN NEW;
    END;
    """,
        definer=False,
    )
    _function(
        schema,
        "guard_custom_import_capture_usage",
        f"""
    DECLARE owner_name name;
    BEGIN
        SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid = {account};
        IF TG_RELID <> '{_t(schema, _USAGE)}'::regclass OR TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW'
           OR TG_OP NOT IN ('INSERT','UPDATE') OR pg_trigger_depth() <> 2 OR current_user <> owner_name THEN
            RAISE EXCEPTION 'custom_import_capture_usage_protected';
        END IF;
        IF TG_OP = 'UPDATE' AND (NEW.dataset_id <> OLD.dataset_id OR NEW.retained_bytes <= OLD.retained_bytes) THEN
            RAISE EXCEPTION 'custom_import_capture_usage_protected';
        END IF;
        RETURN NEW;
    END;
    """,
        definer=False,
    )


_PART_ACCOUNTING_SQL = """
    DECLARE target {bundle}%ROWTYPE; current_capture {capture}%ROWTYPE; existing {part}%ROWTYPE;
        retained bigint; policy jsonb; manifest jsonb; stream record;
    BEGIN
        IF TG_RELID <> '{part}'::regclass OR TG_OP <> 'INSERT' OR TG_LEVEL <> 'ROW'
           OR TG_WHEN NOT IN ('BEFORE','AFTER') THEN RAISE EXCEPTION 'custom_import_capture_accounting_context_invalid'; END IF;
        SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id;
        IF target.capture_bundle_id IS NULL THEN RAISE EXCEPTION 'custom_import_capture_parent_missing'; END IF;
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'custom_import_segmented_requires_read_committed';
        END IF;
        PERFORM 1 FROM {dataset_table} WHERE dataset_id = target.dataset_id FOR UPDATE;
        IF TG_WHEN = 'AFTER' THEN
            IF target.payload_contract = '{payload_contract}' THEN
                PERFORM {authority_function}(target);
                -- AFTER events are queued for a multi-row INSERT. Recheck each actual
                -- charge against preceding charges, not the stale BEFORE-row counters.
                SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id FOR UPDATE;
                SELECT * INTO current_capture FROM {capture}
                    WHERE capture_bundle_id = NEW.capture_bundle_id AND stream_slot = NEW.stream_slot FOR UPDATE;
                policy := target.canonical_policy::jsonb;
                IF {quotas} THEN RAISE EXCEPTION 'custom_import_segmented_quota_exceeded'; END IF;
                SELECT retained_bytes INTO retained FROM {usage} WHERE dataset_id = target.dataset_id;
                IF retained::numeric + NEW.byte_count + octet_length(NEW.canonical_capture_manifest)
                    > (policy->>'maximum_dataset_retained_bytes')::bigint THEN
                    RAISE EXCEPTION 'custom_import_segmented_retained_quota_exceeded';
                END IF;
            END IF;
            UPDATE {usage} SET retained_bytes = retained_bytes + NEW.byte_count
                + coalesce(octet_length(NEW.canonical_capture_manifest), 0) WHERE dataset_id = target.dataset_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_capture_usage_missing'; END IF;
            IF target.payload_contract = '{payload_contract}' THEN
                UPDATE {bundle} SET {updates} WHERE capture_bundle_id = NEW.capture_bundle_id;
                UPDATE {capture} SET {updates} WHERE capture_bundle_id = NEW.capture_bundle_id AND stream_slot = NEW.stream_slot;
            END IF;
            RETURN NULL;
        END IF;
        -- Seed before insertion: AFTER row triggers can already see a whole multi-row INSERT.
        IF NOT EXISTS (SELECT 1 FROM {usage} WHERE dataset_id = target.dataset_id) THEN
            INSERT INTO {usage}(dataset_id, retained_bytes)
                SELECT target.dataset_id, coalesce(sum(p.byte_count::numeric + coalesce(octet_length(p.canonical_capture_manifest),0)),0)
                FROM {part} p JOIN {bundle} b USING (capture_bundle_id) WHERE b.dataset_id = target.dataset_id;
        END IF;
        IF target.payload_contract IS NULL THEN
            IF NEW.part_ordinal > 4096 OR NEW.canonical_capture_manifest IS NOT NULL
                OR NEW.capture_manifest_sha256 IS NOT NULL OR NEW.decoded_byte_count IS NOT NULL
                OR NEW.arrow_byte_count IS NOT NULL OR NEW.record_count IS NOT NULL THEN
                RAISE EXCEPTION 'custom_import_capture_legacy_part_shape_invalid';
            END IF;
            RETURN NEW;
        END IF;
        PERFORM {authority_function}(target);
        SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id FOR UPDATE;
        SELECT * INTO current_capture FROM {capture}
            WHERE capture_bundle_id = NEW.capture_bundle_id AND stream_slot = NEW.stream_slot FOR UPDATE;
        IF target.capture_state <> 'pending' OR current_capture.capture_state IS DISTINCT FROM 'pending'
           OR current_capture.eof_at IS NOT NULL THEN RAISE EXCEPTION 'custom_import_segmented_append_closed'; END IF;
        SELECT * INTO existing FROM {part} WHERE capture_bundle_id = NEW.capture_bundle_id
            AND stream_slot = NEW.stream_slot AND part_ordinal = NEW.part_ordinal;
        IF FOUND THEN
            IF (existing.byte_count, existing.payload, existing.payload_sha256, existing.canonical_capture_manifest,
                existing.capture_manifest_sha256, existing.decoded_byte_count, existing.arrow_byte_count, existing.record_count)
               IS NOT DISTINCT FROM (NEW.byte_count, NEW.payload, NEW.payload_sha256, NEW.canonical_capture_manifest,
                NEW.capture_manifest_sha256, NEW.decoded_byte_count, NEW.arrow_byte_count, NEW.record_count) THEN RETURN NULL; END IF;
            RAISE EXCEPTION 'custom_import_segmented_part_collision';
        END IF;
        IF NEW.part_ordinal <> current_capture.committed_part_count + 1
           OR NEW.canonical_capture_manifest IS NULL OR NEW.capture_manifest_sha256 IS NULL
           OR NEW.decoded_byte_count IS NULL OR NEW.arrow_byte_count IS NULL OR NEW.record_count IS NULL THEN
            RAISE EXCEPTION 'custom_import_segmented_part_shape_invalid';
        END IF;
        policy := target.canonical_policy::jsonb;
        IF NEW.byte_count > (policy->'part_limits'->>'maximum_compressed_bytes')::bigint
           OR NEW.decoded_byte_count > (policy->'part_limits'->>'maximum_decoded_bytes')::bigint
           OR NEW.arrow_byte_count > (policy->>'maximum_part_arrow_bytes')::bigint
           OR NEW.record_count > (policy->'part_limits'->>'maximum_records')::bigint
           OR octet_length(NEW.canonical_capture_manifest) > (policy->>'maximum_part_manifest_bytes')::bigint
           OR {quotas} THEN RAISE EXCEPTION 'custom_import_segmented_quota_exceeded'; END IF;
        SELECT retained_bytes INTO retained FROM {usage} WHERE dataset_id = target.dataset_id;
        IF retained::numeric + NEW.byte_count + octet_length(NEW.canonical_capture_manifest)
            > (policy->>'maximum_dataset_retained_bytes')::bigint THEN
            RAISE EXCEPTION 'custom_import_segmented_retained_quota_exceeded';
        END IF;
        SELECT stream_id, decoder, compression INTO stream FROM {source_stream_table}
            WHERE definition_revision_id = target.definition_revision_id AND stream_slot = NEW.stream_slot;
        manifest := NEW.canonical_capture_manifest::jsonb;
        IF NEW.canonical_capture_manifest IS DISTINCT FROM
            {canonical_json_function}(NEW.canonical_capture_manifest::json) THEN
            RAISE EXCEPTION 'custom_import_segmented_manifest_not_canonical';
        END IF;
        IF jsonb_typeof(manifest) IS DISTINCT FROM 'object'
           OR NOT manifest ?& ARRAY['stream_id','format','compression','source_snapshot_token','stream_sha256',
               'compressed_bytes','decoded_bytes','compressed_sha256','decoded_sha256','capture_sha256']
           OR (SELECT count(*) FROM jsonb_object_keys(manifest)) <> 10
           OR EXISTS (SELECT 1 FROM json_each(NEW.canonical_capture_manifest::json) item WHERE
               (item.key IN ('compressed_bytes','decoded_bytes') AND item.value::text !~ '^[1-9][0-9]*$')
               OR (item.key NOT IN ('compressed_bytes','decoded_bytes') AND json_typeof(item.value) <> 'string'))
           OR manifest->>'stream_id' IS DISTINCT FROM stream.stream_id
           OR stream.decoder IS DISTINCT FROM 'parquet' OR stream.compression IS DISTINCT FROM 'none'
           OR manifest->>'format' IS DISTINCT FROM 'parquet' OR manifest->>'compression' IS DISTINCT FROM 'none'
           OR manifest->>'source_snapshot_token' IS DISTINCT FROM target.snapshot_token
           OR manifest->'compressed_bytes' IS DISTINCT FROM to_jsonb(NEW.byte_count)
           OR manifest->'decoded_bytes' IS DISTINCT FROM to_jsonb(NEW.decoded_byte_count)
           OR manifest->>'compressed_sha256' IS DISTINCT FROM encode(NEW.payload_sha256,'hex')
           OR manifest->>'decoded_sha256' IS DISTINCT FROM encode(NEW.payload_sha256,'hex')
           OR NEW.decoded_byte_count <> NEW.byte_count
           OR coalesce(manifest->>'stream_sha256','') !~ '^[0-9a-f]{{64}}$'
           OR coalesce(manifest->>'capture_sha256','') !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION 'custom_import_segmented_manifest_invalid';
        END IF;
        RETURN NEW;
    END;
    """


def _account_function(schema: str) -> None:
    """Install fenced part accounting and retained-byte quota enforcement."""

    bundle = _t(schema, _BUNDLE)
    capture = _t(schema, _CAPTURE)
    part = _t(schema, _PART)
    usage = _t(schema, _USAGE)
    updates = ", ".join(
        f"{key} = {key} + {increment}"
        for key, increment in zip(
            _COUNTERS,
            _PART_COUNTER_INCREMENTS,
            strict=True,
        )
    )
    quotas = " OR ".join(
        f"current_capture.{key}::numeric + {increment} > (policy->'stream_budget'->>'{limit}')::bigint OR "
        f"target.{key}::numeric + {increment} > (policy->'bundle_budget'->>'{limit}')::bigint"
        for key, limit, increment in zip(
            _COUNTERS,
            _BUDGETS,
            _PART_COUNTER_INCREMENTS,
            strict=True,
        )
    )
    _function(
        schema,
        "account_custom_import_capture_parts",
        _PART_ACCOUNTING_SQL.format(
            bundle=bundle,
            capture=capture,
            part=part,
            dataset_table=_t(schema, "custom_import_dataset"),
            payload_contract=_V2,
            authority_function=_t(schema, "require_custom_import_segmented_authority"),
            quotas=quotas,
            usage=usage,
            updates=updates,
            source_stream_table=_t(schema, "custom_import_source_stream"),
            canonical_json_function=_t(schema, "canonical_custom_import_capture_json"),
        ),
    )


_CAPTURE_HEADER_SQL = """
    DECLARE target {bundle}%ROWTYPE; policy jsonb; totals record; key text; receipt jsonb;
        mutable text[]; declared bigint; count_streams bigint; timeout_ms bigint; remaining_ms bigint; stream_receipts jsonb;
        observed_at timestamptz;
    BEGIN
        IF TG_RELID NOT IN ('{bundle}'::regclass, '{capture}'::regclass) OR TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW'
           OR TG_OP NOT IN ('INSERT','UPDATE','DELETE') THEN RAISE EXCEPTION 'custom_import_segmented_header_context_invalid'; END IF;
        IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'custom_import_immutable_row'; END IF;
        IF TG_OP = 'UPDATE' THEN
            IF OLD.capture_state = 'sealed' OR OLD.payload_contract IS DISTINCT FROM '{payload_contract}' THEN
                RAISE EXCEPTION 'custom_import_immutable_row';
            END IF;
            IF to_jsonb(NEW) - {counters} = to_jsonb(OLD) - {counters} AND to_jsonb(NEW) <> to_jsonb(OLD) THEN
                -- The separate SECURITY INVOKER progress guard authenticates this nested write.
                RETURN NEW;
            END IF;
        ELSE
            FOREACH key IN ARRAY {counters} LOOP
                IF (to_jsonb(NEW)->>key)::bigint <> 0 THEN RAISE EXCEPTION 'custom_import_segmented_initial_progress_invalid'; END IF;
            END LOOP;
            IF NEW.payload_contract IS DISTINCT FROM '{payload_contract}' THEN
                IF NEW.capture_state <> 'sealed' THEN RAISE EXCEPTION 'custom_import_segmented_version_invalid'; END IF;
                IF TG_RELID = '{capture}'::regclass AND EXISTS (
                    SELECT 1 FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id AND payload_contract = '{payload_contract}'
                ) THEN RAISE EXCEPTION 'custom_import_segmented_version_invalid'; END IF;
                RETURN NEW;
            END IF;
            IF NEW.capture_state <> 'pending' OR NEW.canonical_manifest IS NOT NULL OR NEW.manifest_sha256 IS NOT NULL THEN
                RAISE EXCEPTION 'custom_import_segmented_must_begin_pending';
            END IF;
            NEW.sealed_at := NULL;
        END IF;
        IF TG_RELID = '{bundle}'::regclass THEN
            IF TG_OP = 'INSERT' THEN
                policy := {policy_function}(NEW.canonical_policy, NEW.policy_sha256);
                PERFORM 1 FROM {dataset_table} WHERE dataset_id = NEW.dataset_id FOR UPDATE;
                SELECT started_at INTO NEW.acquisition_started_at FROM {execution_table}
                    WHERE execution_id = NEW.producing_execution_id FOR UPDATE;
                PERFORM 1 FROM {lease_table} WHERE execution_id = NEW.producing_execution_id FOR UPDATE;
                observed_at := clock_timestamp();
                IF NEW.acquisition_started_at IS NULL OR NEW.acquisition_started_at > observed_at THEN
                    RAISE EXCEPTION 'custom_import_segmented_acquisition_start_invalid';
                END IF;
                NEW.acquisition_deadline_at := NEW.acquisition_started_at
                    + (policy->>'acquisition_deadline_seconds')::bigint * interval '1 second';
                IF NEW.acquisition_deadline_at <= observed_at THEN
                    RAISE EXCEPTION 'custom_import_segmented_producing_authority_lost';
                END IF;
                IF NEW.snapshot_token_sha256 IS DISTINCT FROM sha256(convert_to(NEW.snapshot_token, 'UTF8')) THEN
                    RAISE EXCEPTION 'custom_import_segmented_snapshot_invalid';
                END IF;
            END IF;
            target := NEW;
        ELSE
            SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id;
        END IF;
        IF TG_OP = 'UPDATE' THEN
            -- UPDATE locks its tuple before a row trigger. Ordered callers already own
            -- the dataset lock; unordered SQL must retry instead of waiting in reverse order.
            PERFORM 1 FROM {dataset_table} WHERE dataset_id = target.dataset_id FOR UPDATE NOWAIT;
        END IF;
        PERFORM {authority_function}(target);
        IF TG_RELID = '{capture}'::regclass THEN
            SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id FOR UPDATE;
            IF target.capture_state <> 'pending' OR target.payload_contract IS DISTINCT FROM '{payload_contract}' THEN
                RAISE EXCEPTION 'custom_import_segmented_append_closed';
            END IF;
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF TG_RELID = '{capture}'::regclass THEN
                IF NEW.eof_at IS NOT NULL OR NEW.content_sha256 IS NOT NULL
                   OR NEW.byte_count IS NOT NULL OR NEW.payload_part_count IS NOT NULL OR NEW.payload_set_sha256 IS NOT NULL
                   OR NEW.manifest_set_sha256 IS NOT NULL THEN RAISE EXCEPTION 'custom_import_segmented_must_begin_pending'; END IF;
            END IF;
            RETURN NEW;
        END IF;
        mutable := ARRAY['capture_state','canonical_manifest','manifest_sha256','sealed_at'];
        IF TG_RELID = '{capture}'::regclass THEN
            mutable := mutable || ARRAY['content_sha256','byte_count','payload_part_count','payload_set_sha256','manifest_set_sha256','eof_at'];
        END IF;
        IF to_jsonb(NEW) - mutable IS DISTINCT FROM to_jsonb(OLD) - mutable THEN
            RAISE EXCEPTION 'custom_import_segmented_identity_immutable';
        END IF;
        IF TG_RELID = '{capture}'::regclass AND NEW.capture_state = 'pending' THEN
            IF to_jsonb(NEW) - ARRAY['eof_at'] IS DISTINCT FROM to_jsonb(OLD) - ARRAY['eof_at']
               OR NEW.eof_at IS NULL OR (OLD.eof_at IS NOT NULL AND OLD.eof_at IS DISTINCT FROM NEW.eof_at) THEN
                RAISE EXCEPTION 'custom_import_segmented_eof_invalid';
            END IF;
            IF OLD.eof_at IS NULL THEN NEW.eof_at := clock_timestamp(); END IF;
            RETURN NEW;
        END IF;
        IF NEW.capture_state <> 'sealed' OR NEW.canonical_manifest IS NULL
           OR octet_length(NEW.canonical_manifest) NOT BETWEEN 2 AND 2097152
           OR NEW.manifest_sha256 IS DISTINCT FROM sha256(convert_to(NEW.canonical_manifest, 'UTF8')) THEN
            RAISE EXCEPTION 'custom_import_segmented_seal_invalid';
        END IF;
        receipt := NEW.canonical_manifest::jsonb;
        IF NEW.canonical_manifest IS DISTINCT FROM {canonical_json_function}(NEW.canonical_manifest::json)
           OR receipt->>'contract_version' IS DISTINCT FROM '{payload_contract}'
           OR receipt->>'policy_sha256' IS DISTINCT FROM encode(target.policy_sha256,'hex')
           OR (receipt ? 'source_request_sha256' AND receipt->>'source_request_sha256' IS DISTINCT FROM encode(target.source_request_sha256,'hex'))
           OR (receipt ? 'statement_sha256' AND receipt->>'statement_sha256' IS DISTINCT FROM encode(target.statement_sha256,'hex'))
           OR (receipt ? 'source_binding_sha256' AND receipt->>'source_binding_sha256' IS DISTINCT FROM encode(target.source_binding_sha256,'hex')) THEN
            RAISE EXCEPTION 'custom_import_segmented_receipt_identity_mismatch';
        END IF;
        IF EXISTS (SELECT 1 FROM json_each(NEW.canonical_manifest::json) item
            WHERE item.key IN ('part_count','byte_count','decoded_byte_count','arrow_byte_count','record_count','manifest_byte_count','stream_count')
              AND item.value::text !~ '^(0|[1-9][0-9]*)$') THEN
            RAISE EXCEPTION 'custom_import_segmented_receipt_integer_required';
        END IF;
        -- Existing statement timeout is a caller prerequisite; do not pretend changing it inside a running statement arms a timer.
        timeout_ms := CASE WHEN current_setting('statement_timeout') = '0' THEN 0
            ELSE extract(epoch FROM current_setting('statement_timeout')::interval) * 1000 END;
        SELECT floor(extract(epoch FROM least(target.acquisition_deadline_at, expires_at) - clock_timestamp()) * 1000)
            INTO remaining_ms FROM {lease_table} WHERE execution_id = target.producing_execution_id;
        IF timeout_ms <= 0 OR timeout_ms > remaining_ms THEN RAISE EXCEPTION 'custom_import_segmented_seal_timeout_required'; END IF;
        IF TG_RELID = '{capture}'::regclass THEN
            IF OLD.eof_at IS NULL OR NEW.eof_at IS DISTINCT FROM OLD.eof_at OR OLD.committed_part_count NOT BETWEEN 1 AND 131072 THEN
                RAISE EXCEPTION 'custom_import_segmented_eof_required';
            END IF;
            SELECT count(*) AS committed_part_count, coalesce(sum(byte_count),0) AS committed_byte_count,
                coalesce(sum(decoded_byte_count),0) AS committed_decoded_byte_count,
                coalesce(sum(arrow_byte_count),0) AS committed_arrow_byte_count,
                coalesce(sum(record_count),0) AS committed_record_count,
                coalesce(sum(octet_length(canonical_capture_manifest)),0) AS committed_manifest_byte_count,
                min(part_ordinal) AS first_ordinal, max(part_ordinal) AS last_ordinal,
                sha256(decode('{payload_domain}','hex') || coalesce(string_agg(
                    int4send(part_ordinal) || int8send(byte_count) || payload_sha256, decode('','hex') ORDER BY part_ordinal),decode('','hex'))) AS payload_hash,
                sha256(decode('{manifest_domain}','hex') || coalesce(string_agg(
                    int4send(part_ordinal) || int8send(octet_length(canonical_capture_manifest)::bigint) || capture_manifest_sha256
                    || int8send(decoded_byte_count) || int8send(arrow_byte_count) || int8send(record_count),
                    decode('','hex') ORDER BY part_ordinal),decode('','hex'))) AS manifest_hash
              INTO totals FROM {part} WHERE capture_bundle_id = NEW.capture_bundle_id AND stream_slot = NEW.stream_slot;
            IF {count_checks} OR totals.first_ordinal <> 1 OR totals.last_ordinal <> NEW.committed_part_count
               OR NEW.byte_count IS DISTINCT FROM totals.committed_byte_count
               OR NEW.payload_part_count IS DISTINCT FROM totals.committed_part_count
               OR NEW.payload_set_sha256 IS DISTINCT FROM totals.payload_hash
               OR NEW.content_sha256 IS DISTINCT FROM totals.payload_hash
               OR NEW.manifest_set_sha256 IS DISTINCT FROM totals.manifest_hash THEN
                RAISE EXCEPTION 'custom_import_segmented_part_totals_mismatch';
            END IF;
            receipt := NEW.canonical_manifest::jsonb;
            IF receipt->'part_count' IS DISTINCT FROM to_jsonb(NEW.committed_part_count)
               OR receipt->'byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_byte_count)
               OR receipt->'decoded_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_decoded_byte_count)
               OR receipt->'arrow_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_arrow_byte_count)
               OR receipt->'record_count' IS DISTINCT FROM to_jsonb(NEW.committed_record_count)
               OR receipt->'manifest_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_manifest_byte_count)
               OR receipt->>'payload_set_sha256' IS DISTINCT FROM encode(NEW.payload_set_sha256,'hex')
               OR receipt->>'manifest_set_sha256' IS DISTINCT FROM encode(NEW.manifest_set_sha256,'hex')
               OR receipt->>'policy_sha256' IS DISTINCT FROM encode(target.policy_sha256,'hex') THEN
                RAISE EXCEPTION 'custom_import_segmented_receipt_mismatch';
            END IF;
        ELSE
            SELECT count(*), {totals} INTO totals FROM {capture} WHERE capture_bundle_id = NEW.capture_bundle_id;
            count_streams := totals.count;
            SELECT count(*) INTO declared FROM {source_stream_table}
                WHERE definition_revision_id = NEW.definition_revision_id;
            IF count_streams <> NEW.stream_count OR declared <> NEW.stream_count OR {count_checks}
               OR EXISTS (SELECT 1 FROM {capture} WHERE capture_bundle_id = NEW.capture_bundle_id
                   AND (capture_state <> 'sealed' OR eof_at IS NULL OR payload_contract IS DISTINCT FROM '{payload_contract}')) THEN
                RAISE EXCEPTION 'custom_import_segmented_stream_coverage_incomplete';
            END IF;
            SELECT jsonb_agg(jsonb_build_object('stream_slot', stream_slot, 'manifest_sha256', encode(manifest_sha256,'hex'))
                ORDER BY stream_slot) INTO stream_receipts FROM {capture} WHERE capture_bundle_id = NEW.capture_bundle_id;
            IF receipt->'streams' IS DISTINCT FROM stream_receipts
               OR {canonical_json_function}(NEW.canonical_manifest::json->'streams')
                  IS DISTINCT FROM {canonical_json_function}(stream_receipts::json)
               OR receipt->'stream_count' IS DISTINCT FROM to_jsonb(NEW.stream_count)
               OR receipt->'part_count' IS DISTINCT FROM to_jsonb(NEW.committed_part_count)
               OR receipt->'byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_byte_count)
               OR receipt->'decoded_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_decoded_byte_count)
               OR receipt->'arrow_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_arrow_byte_count)
               OR receipt->'record_count' IS DISTINCT FROM to_jsonb(NEW.committed_record_count)
               OR receipt->'manifest_byte_count' IS DISTINCT FROM to_jsonb(NEW.committed_manifest_byte_count) THEN
                RAISE EXCEPTION 'custom_import_segmented_bundle_receipt_mismatch';
            END IF;
        END IF;
        PERFORM {authority_function}(target);
        NEW.sealed_at := clock_timestamp();
        RETURN NEW;
    END;
    """


def _header_function(schema: str) -> None:
    """Install immutable identity, EOF and atomic-seal guards."""

    bundle = _t(schema, _BUNDLE)
    capture = _t(schema, _CAPTURE)
    part = _t(schema, _PART)
    counters = "ARRAY[" + ",".join(f"'{name}'" for name in _COUNTERS) + "]"
    count_checks = " OR ".join(f"NEW.{name} <> totals.{name}" for name in _COUNTERS)
    totals = ", ".join(f"coalesce(sum({name}),0) AS {name}" for name in _COUNTERS)
    _function(
        schema,
        "guard_custom_import_segmented_header",
        _CAPTURE_HEADER_SQL.format(
            bundle=bundle,
            capture=capture,
            payload_contract=_V2,
            counters=counters,
            policy_function=_t(schema, "validate_custom_import_segmented_policy"),
            dataset_table=_t(schema, "custom_import_dataset"),
            execution_table=_t(schema, "custom_import_execution"),
            lease_table=_t(schema, "custom_import_lease"),
            authority_function=_t(schema, "require_custom_import_segmented_authority"),
            canonical_json_function=_t(schema, "canonical_custom_import_capture_json"),
            payload_domain=_PAYLOAD_DOMAIN,
            manifest_domain=_MANIFEST_DOMAIN,
            part=part,
            count_checks=count_checks,
            totals=totals,
            source_stream_table=_t(schema, "custom_import_source_stream"),
        ),
    )


def _consumer_functions(schema: str) -> None:
    bundle = _t(schema, _BUNDLE)
    consumers = (
        "custom_import_execution",
        "custom_import_generation",
        "custom_import_generation_seal",
        "custom_import_no_change_seal",
    )
    allowed = ",".join(f"'{_t(schema, name)}'::regclass" for name in consumers)
    _function(
        schema,
        "guard_custom_import_segmented_consumer",
        f"""
    BEGIN
        IF TG_RELID NOT IN ({allowed}) OR TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW' OR TG_OP NOT IN ('INSERT','UPDATE') THEN
            RAISE EXCEPTION 'custom_import_segmented_consumer_context_invalid';
        END IF;
        IF NEW.capture_bundle_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id AND capture_state = 'sealed'
              AND dataset_id = NEW.dataset_id AND definition_revision_id = NEW.definition_revision_id AND schema_revision_id = NEW.schema_revision_id
        ) THEN RAISE EXCEPTION 'custom_import_segmented_capture_not_sealed'; END IF;
        IF TG_RELID = '{_t(schema, "custom_import_execution")}'::regclass AND TG_OP = 'UPDATE' THEN
            IF OLD.capture_bundle_id IS NOT NULL AND OLD.capture_bundle_id IS DISTINCT FROM NEW.capture_bundle_id THEN
                RAISE EXCEPTION 'custom_import_capture_binding_immutable';
            END IF;
        END IF;
        RETURN NEW;
    END;
    """,
    )
    _function(
        schema,
        "guard_custom_import_segmented_bound",
        f"""
    DECLARE target {bundle}%ROWTYPE;
    BEGIN
        IF TG_RELID NOT IN ('{bundle}'::regclass, '{_t(schema, _CAPTURE)}'::regclass)
           OR TG_WHEN <> 'AFTER' OR TG_LEVEL <> 'ROW' OR TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'custom_import_segmented_bound_context_invalid';
        END IF;
        SELECT * INTO target FROM {bundle} WHERE capture_bundle_id = NEW.capture_bundle_id;
        IF target.capture_state <> 'sealed' OR NOT EXISTS (
            SELECT 1 FROM {_t(schema, "custom_import_execution")} WHERE execution_id = target.producing_execution_id
                AND capture_bundle_id = target.capture_bundle_id
        ) THEN RAISE EXCEPTION 'custom_import_segmented_seal_requires_atomic_binding'; END IF;
        PERFORM {_t(schema, "require_custom_import_segmented_authority")}(target);
        RETURN NULL;
    END;
    """,
    )


def upgrade() -> None:
    """Install schema and guards; retained usage is initialized at first subsequent part INSERT."""
    schema = _schema()
    _schema_changes(schema)
    _policy_function(schema)
    _authority_function(schema)
    _progress_functions(schema)
    _account_function(schema)
    _header_function(schema)
    _consumer_functions(schema)
    _legacy_capture_guards(schema)
    _header_triggers(schema)
    _capture_triggers(schema)


def _legacy_capture_guards(schema: str) -> None:
    """Adapt legacy guards and retain their always-enabled trigger behavior."""

    legacy = _legacy()
    for builder in (legacy._part_insert_guard_function_sql, legacy._complete_guard_function_sql):
        sql = builder(schema).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
        skip = f"""BEGIN
            IF EXISTS (SELECT 1 FROM {_t(schema, _BUNDLE)} WHERE capture_bundle_id = NEW.capture_bundle_id
                AND payload_contract = '{_V2}') THEN RETURN NEW; END IF;"""
        op.execute(sql.replace("BEGIN", skip, 1))
    for table, trigger in (
        (_PART, legacy._PART_INSERT_TRIGGER),
        (_PART, legacy._PART_COMPLETE_TRIGGER),
        (_CAPTURE, legacy._CAPTURE_COMPLETE_TRIGGER),
    ):
        op.execute(f"ALTER TABLE {_t(schema, table)} ENABLE ALWAYS TRIGGER {_q(trigger)}")


def _header_triggers(schema: str) -> None:
    """Protect capture headers, progress and deferred atomic binding."""

    for table in (_BUNDLE, _CAPTURE):
        op.execute(f"DROP TRIGGER {_q(table + '_immutable_row_guard')} ON {_t(schema, table)}")
        _trigger(
            schema,
            table,
            table + "_segmented_header",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_custom_import_segmented_header",
        )
        _trigger(
            schema, table, table + "_segmented_progress", "BEFORE UPDATE", "guard_custom_import_segmented_progress"
        )
        # Only terminal updates need deferred checks; counter updates must remain cheap.
        op.execute(f"""CREATE CONSTRAINT TRIGGER {_q(table + "_segmented_bound")} AFTER UPDATE ON {_t(schema, table)}
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            WHEN (OLD.capture_state = 'pending' AND NEW.capture_state = 'sealed')
            EXECUTE FUNCTION {_t(schema, "guard_custom_import_segmented_bound")}()""")
        op.execute(f"ALTER TABLE {_t(schema, table)} ENABLE ALWAYS TRIGGER {_q(table + '_segmented_bound')}")


def _capture_triggers(schema: str) -> None:
    """Register accounting, retention and capture-consumer guards."""

    _trigger(
        schema,
        _PART,
        "a_custom_import_capture_part_account_before",
        "BEFORE INSERT",
        "account_custom_import_capture_parts",
    )
    _trigger(
        schema, _PART, "custom_import_capture_part_account_after", "AFTER INSERT", "account_custom_import_capture_parts"
    )
    _trigger(
        schema,
        _USAGE,
        "custom_import_capture_usage_guard",
        "BEFORE INSERT OR UPDATE OR DELETE",
        "guard_custom_import_capture_usage",
    )
    _trigger(
        schema,
        _USAGE,
        "custom_import_capture_usage_truncate_guard",
        "BEFORE TRUNCATE",
        "guard_custom_import_capture_usage",
        statement=True,
    )
    for table in (_BUNDLE, _CAPTURE, _PART):
        _trigger(
            schema,
            table,
            table + "_segmented_truncate",
            "BEFORE TRUNCATE",
            "guard_custom_import_immutable_row",
            statement=True,
        )
    op.execute(
        f"ALTER TABLE {_t(schema, _PART)} ENABLE ALWAYS TRIGGER custom_import_capture_parquet_part_immutable_row_guard"
    )
    for table in (
        "custom_import_execution",
        "custom_import_generation",
        "custom_import_generation_seal",
        "custom_import_no_change_seal",
    ):
        _trigger(
            schema,
            table,
            "a_" + table + "_segmented_consumer",
            "BEFORE INSERT OR UPDATE OF capture_bundle_id",
            "guard_custom_import_segmented_consumer",
        )


def downgrade() -> None:
    """Retain versioned capture authority; removal requires an explicit retention procedure."""
    raise RuntimeError("segmented capture schema requires an explicit retention-aware downgrade")
