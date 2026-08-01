# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rendered PostgreSQL guards for legacy V3 reconciliation."""

from __future__ import annotations


_SOURCE_ATTEMPT_GUARD_TEMPLATE = """
CREATE OR REPLACE FUNCTION {function_name}(
    requested_source_file_import_id text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    normalized_source_id text;
BEGIN
    normalized_source_id := btrim(requested_source_file_import_id);
    IF normalized_source_id IS NULL
       OR normalized_source_id = ''
       OR length(normalized_source_id) > 64
       OR normalized_source_id IS DISTINCT FROM requested_source_file_import_id
    THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_ID_INVALID'
            USING ERRCODE = '22023';
    END IF;
    PERFORM pg_advisory_xact_lock(
        hashtextextended('{lock_namespace}:' || normalized_source_id, 0)
    );
    IF EXISTS (
        SELECT 1 FROM {audit}
         WHERE source_file_import_id = normalized_source_id
    ) THEN
        RAISE EXCEPTION 'PTG2_LEGACY_V3_ATTEMPT_RECONCILED'
            USING ERRCODE = 'P0001';
    END IF;
END;
$$
"""

_COMMON_ATTEMPT_GUARD_TEMPLATE = """
CREATE OR REPLACE FUNCTION {guard}(
    requested_snapshot_id text,
    requested_internal_run_id text,
    allow_reconciled boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    locked_snapshot_id text;
    locked_run_id text;
    run_generation text;
    fence_state text;
    matching_snapshot_count integer;
    actual_run_id text;
BEGIN
    locked_snapshot_id := NULLIF(requested_snapshot_id, '');
    locked_run_id := NULLIF(requested_internal_run_id, '');
    IF EXISTS (
        SELECT 1 FROM {legacy_audit}
         WHERE snapshot_id = locked_snapshot_id
            OR internal_run_id = locked_run_id
    ) THEN
        RAISE EXCEPTION 'PTG2_LEGACY_V3_ATTEMPT_RECONCILED'
            USING ERRCODE = 'P0001';
    END IF;
    IF locked_snapshot_id IS NOT NULL THEN
        SELECT import_run_id INTO actual_run_id
          FROM {snapshot}
         WHERE snapshot_id = locked_snapshot_id;
        IF actual_run_id IS NOT NULL THEN
            IF locked_run_id IS NOT NULL
               AND actual_run_id IS DISTINCT FROM locked_run_id THEN
                RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_PAIR_CHANGED'
                    USING ERRCODE = 'P0001';
            END IF;
            locked_run_id := actual_run_id;
        END IF;
    END IF;
    -- Source-attempt authority is acquired at start/retry/ensure/finalize and
    -- worker-start boundaries. This writer guard retains the predecessor
    -- lifecycle order and checks the immutable legacy fence by coordinates.
    PERFORM pg_advisory_xact_lock(hashtext('ptg2_source_pointer_gc_v1'));
    IF EXISTS (
        SELECT 1 FROM {legacy_audit}
         WHERE snapshot_id = locked_snapshot_id
            OR internal_run_id = locked_run_id
    ) THEN
        RAISE EXCEPTION 'PTG2_LEGACY_V3_ATTEMPT_RECONCILED'
            USING ERRCODE = 'P0001';
    END IF;
    IF locked_snapshot_id IS NULL AND locked_run_id IS NOT NULL THEN
        SELECT internal_run.options::jsonb->>'storage_generation', fence.state
          INTO run_generation, fence_state
          FROM {internal_run} AS internal_run
          LEFT JOIN {fence} AS fence
            ON fence.internal_run_id = internal_run.import_run_id
         WHERE internal_run.import_run_id = locked_run_id;
        IF run_generation IS DISTINCT FROM 'shared_blocks_v4'
           AND fence_state IS NULL THEN
            RETURN;
        END IF;
        SELECT COUNT(*), MIN(snapshot_id)
          INTO matching_snapshot_count, locked_snapshot_id
          FROM {snapshot}
         WHERE import_run_id = locked_run_id;
        IF matching_snapshot_count = 0 THEN
            RETURN;
        END IF;
        IF matching_snapshot_count <> 1 THEN
            RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_AMBIGUOUS_RUN'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
    IF locked_snapshot_id IS NULL THEN
        RETURN;
    END IF;
    IF locked_run_id IS NULL THEN
        SELECT import_run_id INTO locked_run_id
          FROM {snapshot}
         WHERE snapshot_id = locked_snapshot_id;
    END IF;
    IF locked_run_id IS NULL THEN
        RETURN;
    END IF;
    SELECT internal_run.options::jsonb->>'storage_generation', fence.state
      INTO run_generation, fence_state
      FROM {internal_run} AS internal_run
      LEFT JOIN {fence} AS fence
        ON fence.snapshot_id = locked_snapshot_id
       AND fence.internal_run_id = internal_run.import_run_id
     WHERE internal_run.import_run_id = locked_run_id;
    IF run_generation IS DISTINCT FROM 'shared_blocks_v4'
       AND fence_state IS NULL THEN
        RETURN;
    END IF;
    SELECT import_run_id INTO actual_run_id
      FROM {snapshot}
     WHERE snapshot_id = locked_snapshot_id
     FOR UPDATE;
    IF actual_run_id IS NULL THEN
        RETURN;
    END IF;
    IF actual_run_id IS DISTINCT FROM locked_run_id THEN
        RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_PAIR_CHANGED'
            USING ERRCODE = 'P0001';
    END IF;
    SELECT options::jsonb->>'storage_generation' INTO run_generation
      FROM {internal_run}
     WHERE import_run_id = locked_run_id
     FOR UPDATE;
    IF run_generation IS DISTINCT FROM 'shared_blocks_v4' THEN
        RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_GENERATION_CHANGED'
            USING ERRCODE = 'P0001';
    END IF;
    INSERT INTO {fence} (snapshot_id, internal_run_id, state)
    VALUES (locked_snapshot_id, locked_run_id, 'active')
    ON CONFLICT (snapshot_id) DO NOTHING;
    SELECT state INTO fence_state
      FROM {fence}
     WHERE snapshot_id = locked_snapshot_id
       AND internal_run_id = locked_run_id
     FOR UPDATE;
    IF fence_state IS NULL THEN
        RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_PAIR_CONFLICT'
            USING ERRCODE = 'P0001';
    END IF;
    IF fence_state = 'reconciled' AND NOT allow_reconciled THEN
        RAISE EXCEPTION 'PTG2_STALE_METADATA_FENCE'
            USING ERRCODE = 'P0001';
    END IF;
END;
$$
"""


def source_attempt_guard_sql(
    *,
    function_name: str,
    audit: str,
    lock_namespace: str,
) -> str:
    """Render the exact shared source-attempt advisory guard."""

    return _SOURCE_ATTEMPT_GUARD_TEMPLATE.format(
        function_name=function_name,
        audit=audit,
        lock_namespace=lock_namespace,
    )


def common_attempt_guard_sql(
    *,
    guard: str,
    legacy_audit: str,
    snapshot: str,
    internal_run: str,
    fence: str,
) -> str:
    """Render V4 behavior plus the coordinate-first legacy fence."""

    return _COMMON_ATTEMPT_GUARD_TEMPLATE.format(
        guard=guard,
        legacy_audit=legacy_audit,
        snapshot=snapshot,
        internal_run=internal_run,
        fence=fence,
    )


__all__ = ["common_attempt_guard_sql", "source_attempt_guard_sql"]
