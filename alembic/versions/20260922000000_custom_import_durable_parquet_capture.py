# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist immutable bounded Parquet parts for custom-import replay.

Revision ID: 20260922000000_custom_import_durable_parquet_capture
Revises: 20260921000000_provider_quality_result_generation
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260922000000_custom_import_durable_parquet_capture"
down_revision = "20260921000000_provider_quality_result_generation"
branch_labels = None
depends_on = None


_PAYLOAD_CONTRACT = "custom-import/parquet-parts/v1"
_MAX_PART_BYTES = 64 * 1024 * 1024
_MAX_PART_COUNT = 4_096
_MAX_BUNDLE_BYTES = 128 * 1024 * 1024
_MAX_BUNDLE_PART_COUNT = 8_192
_PAYLOAD_SET_DOMAIN_HEX = "637573746f6d2d696d706f72742f706172717565742d70617274732f763100"
_CAPTURE_TABLE = "custom_import_capture"
_BUNDLE_TABLE = "custom_import_capture_bundle"
_PART_TABLE = "custom_import_capture_parquet_part"
_STREAM_TABLE = "custom_import_source_stream"
_BUNDLE_SNAPSHOT_INDEX = "custom_import_capture_bundle_snapshot_digest_idx"
_PART_INSERT_GUARD = "guard_custom_import_capture_parquet_part_insert"
_COMPLETE_GUARD = "guard_custom_import_capture_parquet_complete"
_PART_INSERT_TRIGGER = "custom_import_capture_parquet_part_insert_guard"
_PART_IMMUTABLE_TRIGGER = "custom_import_capture_parquet_part_immutable_row_guard"
_CAPTURE_COMPLETE_TRIGGER = "custom_import_capture_durable_complete_guard"
_PART_COMPLETE_TRIGGER = "custom_import_capture_parquet_part_complete_guard"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, object_name: str) -> str:
    return f"{_quote(schema)}.{_quote(object_name)}"


def _capture_payload_columns_sql(schema: str) -> str:
    capture = _qualified(schema, _CAPTURE_TABLE)
    return f"""
    ALTER TABLE {capture}
        ADD COLUMN payload_contract VARCHAR(63),
        ADD COLUMN payload_part_count INTEGER,
        ADD COLUMN payload_set_sha256 BYTEA,
        ADD CONSTRAINT custom_import_capture_payload_shape_check CHECK (
            (payload_contract IS NULL AND payload_part_count IS NULL AND payload_set_sha256 IS NULL)
            OR (
                payload_contract IS NOT NULL
                AND payload_contract = '{_PAYLOAD_CONTRACT}'
                AND byte_count BETWEEN 1 AND {_MAX_PART_BYTES}
                AND payload_part_count IS NOT NULL
                AND payload_part_count BETWEEN 1 AND {_MAX_PART_COUNT}
                AND payload_set_sha256 IS NOT NULL
                AND octet_length(payload_set_sha256) = 32
            )
        )
    """


def _part_table_sql(schema: str) -> str:
    capture = _qualified(schema, _CAPTURE_TABLE)
    part = _qualified(schema, _PART_TABLE)
    return f"""
    CREATE TABLE {part} (
        capture_bundle_id BIGINT NOT NULL,
        stream_slot SMALLINT NOT NULL,
        part_ordinal INTEGER NOT NULL,
        byte_count BIGINT NOT NULL,
        payload BYTEA NOT NULL,
        payload_sha256 BYTEA NOT NULL,
        sealed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT transaction_timestamp(),
        CONSTRAINT custom_import_capture_parquet_part_pkey
            PRIMARY KEY (capture_bundle_id, stream_slot, part_ordinal),
        CONSTRAINT custom_import_capture_parquet_part_capture_fkey
            FOREIGN KEY (capture_bundle_id, stream_slot)
            REFERENCES {capture} (capture_bundle_id, stream_slot) ON DELETE RESTRICT,
        CONSTRAINT custom_import_capture_parquet_part_shape_check CHECK (
            part_ordinal BETWEEN 1 AND {_MAX_PART_COUNT}
            AND byte_count BETWEEN 1 AND {_MAX_PART_BYTES}
            AND octet_length(payload) = byte_count
            AND octet_length(payload_sha256) = 32
            AND payload_sha256 = pg_catalog.sha256(payload)
        )
    )
    """


def _bundle_snapshot_index_sql(schema: str) -> str:
    bundle = _qualified(schema, _BUNDLE_TABLE)
    return f"""
    CREATE INDEX {_quote(_BUNDLE_SNAPSHOT_INDEX)}
        ON {bundle} (dataset_id, definition_revision_id, schema_revision_id, snapshot_token_sha256)
    """


def _part_insert_guard_function_sql(schema: str) -> str:
    capture = _qualified(schema, _CAPTURE_TABLE)
    part = _qualified(schema, _PART_TABLE)
    source_stream = _qualified(schema, _STREAM_TABLE)
    function = _qualified(schema, _PART_INSERT_GUARD)
    return f"""
    CREATE FUNCTION {function}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        parent_contract text;
        parent_part_count integer;
        source_decoder text;
        source_compression text;
    BEGIN
        SELECT capture.payload_contract, capture.payload_part_count, stream.decoder, stream.compression
          INTO parent_contract, parent_part_count, source_decoder, source_compression
          FROM {capture} AS capture
          JOIN {source_stream} AS stream
            ON stream.definition_revision_id = capture.definition_revision_id
           AND stream.dataset_id = capture.dataset_id
           AND stream.schema_revision_id = capture.schema_revision_id
           AND stream.stream_slot = capture.stream_slot
         WHERE capture.capture_bundle_id = NEW.capture_bundle_id
           AND capture.stream_slot = NEW.stream_slot
         FOR KEY SHARE OF capture;
        IF parent_contract IS DISTINCT FROM '{_PAYLOAD_CONTRACT}'
           OR parent_part_count IS NULL
           OR source_decoder IS DISTINCT FROM 'parquet'
           OR source_compression IS DISTINCT FROM 'none' THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_part_parent_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF EXISTS (
            SELECT 1
              FROM {part} AS existing_part
             WHERE existing_part.capture_bundle_id = NEW.capture_bundle_id
               AND existing_part.stream_slot = NEW.stream_slot
               AND existing_part.part_ordinal = parent_part_count
        ) THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_part_already_complete'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $function$
    """


def _complete_guard_function_sql(schema: str) -> str:
    bundle = _qualified(schema, _BUNDLE_TABLE)
    capture = _qualified(schema, _CAPTURE_TABLE)
    part = _qualified(schema, _PART_TABLE)
    source_stream = _qualified(schema, _STREAM_TABLE)
    function = _qualified(schema, _COMPLETE_GUARD)
    return f"""
    CREATE FUNCTION {function}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        target_bundle_id bigint;
        first_stream_slot smallint;
        declared_stream_count smallint;
        persisted_stream_count bigint;
        capture_count bigint;
        payload_capture_count bigint;
        complete_payload_capture_count bigint;
        parts_incomplete boolean;
        payload_set_digest_mismatch boolean;
    BEGIN
        target_bundle_id := NEW.capture_bundle_id;
        IF TG_TABLE_NAME = '{_CAPTURE_TABLE}' THEN
            IF EXISTS (
                SELECT 1
                  FROM {part} AS part
                 WHERE part.capture_bundle_id = target_bundle_id
                   AND part.stream_slot = NEW.stream_slot
                   AND part.part_ordinal = 1
            ) THEN
                RETURN NULL;
            END IF;
        ELSE
            SELECT min(capture.stream_slot) INTO first_stream_slot
              FROM {capture} AS capture
             WHERE capture.capture_bundle_id = target_bundle_id
               AND capture.payload_contract = '{_PAYLOAD_CONTRACT}';
            IF NEW.stream_slot IS DISTINCT FROM first_stream_slot THEN
                RETURN NULL;
            END IF;
        END IF;
        SELECT bundle.stream_count INTO declared_stream_count
          FROM {bundle} AS bundle
         WHERE bundle.capture_bundle_id = target_bundle_id;
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;

        SELECT count(*) INTO persisted_stream_count
          FROM {source_stream} AS stream
          JOIN {bundle} AS bundle
            ON bundle.dataset_id = stream.dataset_id
           AND bundle.definition_revision_id = stream.definition_revision_id
           AND bundle.schema_revision_id = stream.schema_revision_id
         WHERE bundle.capture_bundle_id = target_bundle_id;
        SELECT count(*),
               count(*) FILTER (
                   WHERE capture.payload_contract IS NOT NULL
                      OR capture.payload_part_count IS NOT NULL
                      OR capture.payload_set_sha256 IS NOT NULL
               ),
               count(*) FILTER (
                   WHERE capture.payload_contract = '{_PAYLOAD_CONTRACT}'
                     AND capture.payload_part_count BETWEEN 1 AND {_MAX_PART_COUNT}
                     AND octet_length(capture.payload_set_sha256) = 32
                     AND capture.byte_count BETWEEN 1 AND {_MAX_PART_BYTES}
               )
          INTO capture_count, payload_capture_count, complete_payload_capture_count
          FROM {capture} AS capture
         WHERE capture.capture_bundle_id = target_bundle_id;

        IF payload_capture_count = 0 THEN
            RETURN NULL;
        END IF;
        IF capture_count <> declared_stream_count
           OR capture_count <> persisted_stream_count
           OR payload_capture_count <> declared_stream_count
           OR complete_payload_capture_count <> declared_stream_count THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_bundle_incomplete'
                USING ERRCODE = '23514';
        END IF;
        SELECT bool_or(
                   parts.part_count <> capture.payload_part_count
                   OR parts.part_bytes <> capture.byte_count
                   OR parts.first_ordinal IS DISTINCT FROM 1
                   OR parts.last_ordinal IS DISTINCT FROM capture.payload_part_count
               ),
               bool_or(parts.payload_set_sha256 IS DISTINCT FROM capture.payload_set_sha256)
          INTO parts_incomplete, payload_set_digest_mismatch
          FROM {capture} AS capture
          LEFT JOIN LATERAL (
              SELECT count(*) AS part_count,
                     coalesce(sum(part.byte_count), 0) AS part_bytes,
                     min(part.part_ordinal) AS first_ordinal,
                     max(part.part_ordinal) AS last_ordinal,
                     pg_catalog.sha256(
                         pg_catalog.decode('{_PAYLOAD_SET_DOMAIN_HEX}', 'hex')
                         || coalesce(
                             pg_catalog.string_agg(
                                 pg_catalog.int4send(part.part_ordinal)
                                 || pg_catalog.int8send(part.byte_count)
                                 || part.payload_sha256,
                                 pg_catalog.decode('', 'hex') ORDER BY part.part_ordinal
                             ),
                             pg_catalog.decode('', 'hex')
                         )
                     ) AS payload_set_sha256
                FROM {part} AS part
               WHERE part.capture_bundle_id = capture.capture_bundle_id
                 AND part.stream_slot = capture.stream_slot
          ) AS parts ON true
         WHERE capture.capture_bundle_id = target_bundle_id;
        IF coalesce(parts_incomplete, true) THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_parts_incomplete'
                USING ERRCODE = '23514';
        END IF;
        IF coalesce(payload_set_digest_mismatch, true) THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_payload_set_digest_mismatch'
                USING ERRCODE = '23514';
        END IF;
        IF (SELECT count(*) FROM {part} WHERE capture_bundle_id = target_bundle_id) > {_MAX_BUNDLE_PART_COUNT}
           OR (
               SELECT coalesce(sum(byte_count), 0)
                 FROM {part}
                WHERE capture_bundle_id = target_bundle_id
           ) > {_MAX_BUNDLE_BYTES} THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_bundle_limit_exceeded'
                USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
    END;
    $function$
    """


def _part_insert_trigger_sql(schema: str) -> str:
    return f"""
    CREATE TRIGGER {_quote(_PART_INSERT_TRIGGER)}
    BEFORE INSERT ON {_qualified(schema, _PART_TABLE)}
    FOR EACH ROW EXECUTE FUNCTION {_qualified(schema, _PART_INSERT_GUARD)}()
    """


def _part_immutable_trigger_sql(schema: str) -> str:
    return f"""
    CREATE TRIGGER {_quote(_PART_IMMUTABLE_TRIGGER)}
    BEFORE UPDATE OR DELETE ON {_qualified(schema, _PART_TABLE)}
    FOR EACH ROW EXECUTE FUNCTION {_qualified(schema, "guard_custom_import_immutable_row")}()
    """


def _complete_trigger_sql(schema: str, table_name: str, trigger_name: str) -> str:
    when = f"NEW.payload_contract = '{_PAYLOAD_CONTRACT}'" if table_name == _CAPTURE_TABLE else "NEW.part_ordinal = 1"
    return f"""
    CREATE CONSTRAINT TRIGGER {_quote(trigger_name)}
    AFTER INSERT ON {_qualified(schema, table_name)}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN ({when}) EXECUTE FUNCTION {_qualified(schema, _COMPLETE_GUARD)}()
    """


def _revoke_function_sql(schema: str, function_name: str) -> str:
    return f"REVOKE ALL ON FUNCTION {_qualified(schema, function_name)}() FROM PUBLIC"


def _drop_trigger_sql(schema: str, trigger_name: str, table_name: str) -> str:
    return f"DROP TRIGGER IF EXISTS {_quote(trigger_name)} ON {_qualified(schema, table_name)}"


def _drop_bundle_snapshot_index_sql(schema: str) -> str:
    return f"DROP INDEX IF EXISTS {_qualified(schema, _BUNDLE_SNAPSHOT_INDEX)}"


def _downgrade_lock_sql(schema: str) -> str:
    return (
        f"LOCK TABLE {_qualified(schema, _CAPTURE_TABLE)}, {_qualified(schema, _PART_TABLE)} IN ACCESS EXCLUSIVE MODE"
    )


def _downgrade_payload_guard_sql(schema: str) -> str:
    capture = _qualified(schema, _CAPTURE_TABLE)
    part = _qualified(schema, _PART_TABLE)
    return f"""
    DO $block$
    BEGIN
        IF EXISTS (SELECT 1 FROM {part})
           OR EXISTS (
               SELECT 1 FROM {capture}
                WHERE payload_contract IS NOT NULL
                   OR payload_part_count IS NOT NULL
                   OR payload_set_sha256 IS NOT NULL
           ) THEN
            RAISE EXCEPTION 'custom_import_capture_parquet_downgrade_blocked'
                USING ERRCODE = 'P0001';
        END IF;
    END;
    $block$
    """


def _drop_capture_payload_columns_sql(schema: str) -> str:
    capture = _qualified(schema, _CAPTURE_TABLE)
    return f"""
    ALTER TABLE {capture}
        DROP CONSTRAINT IF EXISTS custom_import_capture_payload_shape_check,
        DROP COLUMN IF EXISTS payload_set_sha256,
        DROP COLUMN IF EXISTS payload_part_count,
        DROP COLUMN IF EXISTS payload_contract
    """


def upgrade() -> None:
    """Add schema-only bounded durable payload storage without backfilling rows."""

    schema = _schema()
    op.execute(_capture_payload_columns_sql(schema))
    op.execute(_part_table_sql(schema))
    op.execute(_bundle_snapshot_index_sql(schema))
    op.execute(_part_insert_guard_function_sql(schema))
    op.execute(_revoke_function_sql(schema, _PART_INSERT_GUARD))
    op.execute(_complete_guard_function_sql(schema))
    op.execute(_revoke_function_sql(schema, _COMPLETE_GUARD))
    op.execute(_part_insert_trigger_sql(schema))
    op.execute(_part_immutable_trigger_sql(schema))
    op.execute(_complete_trigger_sql(schema, _CAPTURE_TABLE, _CAPTURE_COMPLETE_TRIGGER))
    op.execute(_complete_trigger_sql(schema, _PART_TABLE, _PART_COMPLETE_TRIGGER))


def downgrade() -> None:
    """Remove only empty durable payload storage and retain legacy captures."""

    schema = _schema()
    op.execute(_downgrade_lock_sql(schema))
    op.execute(_downgrade_payload_guard_sql(schema))
    op.execute(_drop_bundle_snapshot_index_sql(schema))
    op.execute(_drop_trigger_sql(schema, _CAPTURE_COMPLETE_TRIGGER, _CAPTURE_TABLE))
    op.execute(_drop_trigger_sql(schema, _PART_COMPLETE_TRIGGER, _PART_TABLE))
    op.execute(_drop_trigger_sql(schema, _PART_IMMUTABLE_TRIGGER, _PART_TABLE))
    op.execute(_drop_trigger_sql(schema, _PART_INSERT_TRIGGER, _PART_TABLE))
    op.execute(f"DROP FUNCTION IF EXISTS {_qualified(schema, _COMPLETE_GUARD)}()")
    op.execute(f"DROP FUNCTION IF EXISTS {_qualified(schema, _PART_INSERT_GUARD)}()")
    op.execute(f"DROP TABLE IF EXISTS {_qualified(schema, _PART_TABLE)}")
    op.execute(_drop_capture_payload_columns_sql(schema))
