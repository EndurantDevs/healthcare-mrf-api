"""Add durable PTG V4 attempt fencing and immutable reconciliation audit.

Revision ID: 20260724100000_ptg2_v4_attempt_fence; revises the V4 map pack.
"""

from __future__ import annotations

from collections import namedtuple
import os

from alembic import op
from db.migration_ptg2_v4_attempt_audit import (
    install_attempt_audit_guard,
)
from db.migration_ptg2_v4_attempt_catalog import (
    adopt_or_validate_attempt_tables,
)
from db.migration_ptg2_v4_attempt_fence import (
    assert_attempt_downgrade_safe,
    install_lifecycle_lock_layer,
)


revision = "20260724100000_ptg2_v4_attempt_fence"
down_revision = "20260723100000_ptg2_v4_snapshot_map_pack"
branch_labels = None
depends_on = None


_MigrationAttachment = namedtuple(
    "_MigrationAttachment",
    (
        "name", "table_name", "snapshot_columns",
        "run_columns", "statement_trigger",
    ),
    defaults=((), (), True),
)


ATTEMPT_ATTACHMENTS = (
    _MigrationAttachment("layout_bindings", "ptg2_v3_snapshot_binding", ("snapshot_id",)),
    _MigrationAttachment("snapshot_scope", "ptg2_v3_snapshot_scope", ("snapshot_id",)),
    _MigrationAttachment("snapshot_plan_scope", "ptg2_v3_snapshot_plan_scope", ("snapshot_id",)),
    _MigrationAttachment("snapshot_source", "ptg2_v3_snapshot_source", ("snapshot_id",)),
    _MigrationAttachment("candidate_attestation", "ptg2_v3_candidate_audit_attestation", ("snapshot_id",)),
    _MigrationAttachment("snapshot_pin", "ptg2_snapshot_pin", ("snapshot_id",)),
    _MigrationAttachment("plan_release_binding", "plan_release_snapshot_binding", ("snapshot_id",)),
    _MigrationAttachment("plan_month", "ptg2_plan_month", ("snapshot_id",)),
    _MigrationAttachment("artifact_manifest", "ptg2_artifact_manifest", ("snapshot_id",), ("import_run_id",)),
    _MigrationAttachment("allowed_amount_plan", "ptg2_allowed_amount_plan", ("snapshot_id",), (), False),
    _MigrationAttachment("allowed_amount_item", "ptg2_allowed_amount_item", ("snapshot_id",), (), False),
    _MigrationAttachment("allowed_amount_payment", "ptg2_allowed_amount_payment", ("snapshot_id",), (), False),
    _MigrationAttachment("allowed_amount_provider_payment", "ptg2_allowed_amount_provider_payment", ("snapshot_id",), (), False),
    _MigrationAttachment("current_snapshot", "ptg2_current_snapshot", ("snapshot_id", "previous_snapshot_id")),
    _MigrationAttachment("current_source_snapshot", "ptg2_current_source_snapshot", ("snapshot_id", "previous_snapshot_id")),
    _MigrationAttachment("current_plan_source", "ptg2_current_plan_source", ("snapshot_id", "previous_snapshot_id")),
    _MigrationAttachment("import_job", "ptg2_import_job", (), ("import_run_id",)),
    _MigrationAttachment("source_catalog", "ptg2_source_catalog", (), ("import_run_id",), False),
    _MigrationAttachment("serving_rate", "ptg2_serving_rate", ("snapshot_id",), (), False),
    _MigrationAttachment("serving_rate_compact", "ptg2_serving_rate_compact", ("snapshot_id",), (), False),
    _MigrationAttachment("price_set_stage", "ptg2_price_set_stage", ("snapshot_id",), (), False),
    _MigrationAttachment("serving_rate_stage", "ptg2_serving_rate_stage", ("snapshot_id",), (), False),
    _MigrationAttachment("manifest_stage_registration", "ptg2_v4_attempt_stage", ("snapshot_id",), ("internal_run_id",)),
)
ATTEMPT_STATE_TABLES = (
    _MigrationAttachment("snapshot_state", "ptg2_snapshot", ("snapshot_id",), ("import_run_id",)),
    _MigrationAttachment("run_state", "ptg2_import_run", (), ("import_run_id",)),
)

def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _function(schema: str, name: str) -> str:
    return f"{_q(schema)}.{_q(name)}"


def _coordinate_select(
    attachment: _MigrationAttachment,
    row_alias: str,
) -> str:
    selections: list[str] = []
    if attachment.snapshot_columns and attachment.run_columns:
        selections.append(
            "SELECT "
            f"{_q(attachment.snapshot_columns[0])}::text AS snapshot_id, "
            f"{_q(attachment.run_columns[0])}::text AS internal_run_id "
            f"FROM {row_alias}"
        )
        remaining_snapshot_columns = attachment.snapshot_columns[1:]
        remaining_run_columns = attachment.run_columns[1:]
    else:
        remaining_snapshot_columns = attachment.snapshot_columns
        remaining_run_columns = attachment.run_columns
    for snapshot_column in remaining_snapshot_columns:
        selections.append(
            f"SELECT {_q(snapshot_column)}::text AS snapshot_id, "
            "NULL::text AS internal_run_id "
            f"FROM {row_alias}"
        )
    for run_column in remaining_run_columns:
        selections.append(
            "SELECT NULL::text AS snapshot_id, "
            f"{_q(run_column)}::text AS internal_run_id "
            f"FROM {row_alias}"
        )
    return " UNION ".join(selections)


def _trigger_function_sql(
    schema: str,
    attachment: _MigrationAttachment,
) -> str:
    function_name = f"guard_{attachment.table_name}_attempt"
    guard_function = _function(schema, "guard_ptg2_v4_attempt")
    new_select = _coordinate_select(attachment, "attempt_new_rows")
    old_select = _coordinate_select(attachment, "attempt_old_rows")
    return f"""
        CREATE OR REPLACE FUNCTION {_function(schema, function_name)}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            coordinate record;
        BEGIN
            IF TG_OP IN ('INSERT', 'UPDATE') THEN
                FOR coordinate IN
                    SELECT DISTINCT snapshot_id, internal_run_id
                      FROM ({new_select}) AS coordinates
                     WHERE snapshot_id IS NOT NULL
                        OR internal_run_id IS NOT NULL
                LOOP
                    PERFORM {guard_function}(
                        coordinate.snapshot_id,
                        coordinate.internal_run_id
                    );
                END LOOP;
            END IF;
            IF TG_OP IN ('DELETE', 'UPDATE') THEN
                FOR coordinate IN
                    SELECT DISTINCT snapshot_id, internal_run_id
                      FROM ({old_select}) AS coordinates
                     WHERE snapshot_id IS NOT NULL
                        OR internal_run_id IS NOT NULL
                LOOP
                    PERFORM {guard_function}(
                        coordinate.snapshot_id,
                        coordinate.internal_run_id
                    );
                END LOOP;
            END IF;
            RETURN NULL;
        END;
        $$;
    """


def _create_attachment_triggers(
    schema: str,
    attachment: _MigrationAttachment,
) -> None:
    table = _qt(schema, attachment.table_name)
    function_name = f"guard_{attachment.table_name}_attempt"
    op.execute(_trigger_function_sql(schema, attachment))
    for operation, transition in (
        ("insert", "REFERENCING NEW TABLE AS attempt_new_rows"),
        (
            "update",
            "REFERENCING OLD TABLE AS attempt_old_rows "
            "NEW TABLE AS attempt_new_rows",
        ),
        ("delete", "REFERENCING OLD TABLE AS attempt_old_rows"),
    ):
        trigger_name = f"{attachment.table_name}_attempt_{operation}_guard"
        op.execute(
            f"""
            DROP TRIGGER IF EXISTS {_q(trigger_name)} ON {table};
            CREATE TRIGGER {_q(trigger_name)}
            AFTER {operation.upper()} ON {table}
            {transition}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {_function(schema, function_name)}();
            """
        )


def _drop_attachment_triggers(
    schema: str,
    attachment: _MigrationAttachment,
) -> None:
    table = _qt(schema, attachment.table_name)
    for operation in ("insert", "update", "delete"):
        trigger_name = f"{attachment.table_name}_attempt_{operation}_guard"
        op.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} ON {table}"
        )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_function(schema, f'guard_{attachment.table_name}_attempt')}()"
    )


def _create_fence_tables(schema: str) -> None:
    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    fence = _qt(schema, "ptg2_v4_attempt_fence")
    stage = _qt(schema, "ptg2_v4_attempt_stage")
    op.execute(
        f"""
        CREATE TABLE {fence} (
            snapshot_id varchar(96) NOT NULL,
            internal_run_id varchar(96) NOT NULL,
            fence_nonce uuid NOT NULL DEFAULT gen_random_uuid(),
            state varchar(16) NOT NULL DEFAULT 'active',
            target_digest varchar(64),
            plan_digest varchar(64),
            marker_digest varchar(64),
            marker jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            reconciled_at timestamptz,
            CONSTRAINT {_q('ptg2_v4_attempt_fence_pkey')}
                PRIMARY KEY (snapshot_id),
            CONSTRAINT {_q('ptg2_v4_attempt_fence_internal_run_id_key')}
                UNIQUE (internal_run_id),
            CONSTRAINT {_q('ptg2_v4_attempt_fence_nonce_key')}
                UNIQUE (fence_nonce),
            CONSTRAINT {_q('ptg2_v4_attempt_fence_snapshot_fkey')}
                FOREIGN KEY (snapshot_id) REFERENCES {snapshot}(snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_attempt_fence_run_fkey')}
                FOREIGN KEY (internal_run_id)
                REFERENCES {internal_run}(import_run_id) ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_attempt_fence_pair_key')}
                UNIQUE (snapshot_id, internal_run_id),
            CONSTRAINT {_q('ptg2_v4_attempt_fence_state_check')}
                CHECK (state IN ('active', 'reconciled')),
            CONSTRAINT {_q('ptg2_v4_attempt_fence_audit_shape_check')}
                CHECK (
                    (
                        state = 'active'
                        AND target_digest IS NULL
                        AND plan_digest IS NULL
                        AND marker_digest IS NULL
                        AND marker IS NULL
                        AND reconciled_at IS NULL
                    )
                    OR (
                        state = 'reconciled'
                        AND target_digest IS NOT NULL
                        AND target_digest ~ '^[0-9a-f]{{64}}$'
                        AND plan_digest IS NOT NULL
                        AND plan_digest ~ '^[0-9a-f]{{64}}$'
                        AND marker_digest IS NOT NULL
                        AND marker_digest ~ '^[0-9a-f]{{64}}$'
                        AND COALESCE(jsonb_typeof(marker) = 'object', false)
                        AND reconciled_at IS NOT NULL
                    )
                )
        );

        CREATE TABLE {stage} (
            snapshot_id varchar(96) NOT NULL,
            internal_run_id varchar(96) NOT NULL,
            table_name varchar(63) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_attempt_stage_pkey')}
                PRIMARY KEY (snapshot_id, table_name),
            CONSTRAINT {_q('ptg2_v4_attempt_stage_fence_fkey')}
                FOREIGN KEY (snapshot_id, internal_run_id)
                REFERENCES {fence}(snapshot_id, internal_run_id)
                ON DELETE CASCADE
        );
        """
    )


def _lifecycle_guarded_tables() -> tuple[str, ...]:
    return tuple(
        attachment.table_name
        for attachment in (*ATTEMPT_STATE_TABLES, *ATTEMPT_ATTACHMENTS)
        if attachment.statement_trigger
    )


def _create_lifecycle_lock_layer(schema: str) -> None:
    """Acquire the lifecycle lock before attachment statements take row locks."""

    install_lifecycle_lock_layer(
        op,
        schema,
        _lifecycle_guarded_tables(),
    )


def _drop_lifecycle_lock_layer(schema: str) -> None:
    for table_name in reversed(_lifecycle_guarded_tables()):
        trigger_name = f"{table_name}_attempt_lifecycle_lock"
        op.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_function(schema, 'lock_ptg2_v4_attempt_lifecycle')}()"
    )


def _assert_one_snapshot_per_v4_run(schema: str) -> None:
    """Fail clearly before the fence enforces the existing pair invariant."""
    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {snapshot} AS snapshot
                  JOIN {internal_run} AS internal_run
                    ON internal_run.import_run_id = snapshot.import_run_id
                 WHERE COALESCE(internal_run.options::jsonb
                                ->>'storage_generation', '') = 'shared_blocks_v4'
                 GROUP BY internal_run.import_run_id
                HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'PTG2_ATTEMPT_FENCE_V4_RUN_MULTIPLE_SNAPSHOTS'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $$;
        """
    )


def _create_guard_function(schema: str) -> None:
    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    fence = _qt(schema, "ptg2_v4_attempt_fence")
    guard = _function(schema, "guard_ptg2_v4_attempt")
    op.execute(
        f"""
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
            PERFORM pg_advisory_xact_lock(
                hashtext('ptg2_source_pointer_gc_v1')
            );
            locked_snapshot_id := NULLIF(requested_snapshot_id, '');
            locked_run_id := NULLIF(requested_internal_run_id, '');

            IF locked_snapshot_id IS NOT NULL THEN
                SELECT import_run_id
                  INTO actual_run_id
                  FROM {snapshot}
                 WHERE snapshot_id = locked_snapshot_id;
                IF actual_run_id IS NOT NULL THEN
                    IF locked_run_id IS NOT NULL
                       AND actual_run_id IS DISTINCT FROM locked_run_id THEN
                        RAISE EXCEPTION
                            'PTG2_ATTEMPT_FENCE_PAIR_CHANGED'
                            USING ERRCODE = 'P0001';
                    END IF;
                    locked_run_id := actual_run_id;
                END IF;
            END IF;
            IF locked_snapshot_id IS NULL AND locked_run_id IS NOT NULL THEN
                SELECT internal_run.options::jsonb->>'storage_generation',
                       fence.state
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
                    RAISE EXCEPTION
                        'PTG2_ATTEMPT_FENCE_AMBIGUOUS_RUN'
                        USING ERRCODE = 'P0001';
                END IF;
            END IF;
            IF locked_snapshot_id IS NULL THEN
                RETURN;
            END IF;
            IF locked_run_id IS NULL THEN
                SELECT import_run_id
                  INTO locked_run_id
                  FROM {snapshot}
                 WHERE snapshot_id = locked_snapshot_id;
            END IF;
            IF locked_run_id IS NULL THEN
                RETURN;
            END IF;

            SELECT internal_run.options::jsonb->>'storage_generation',
                   fence.state
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

            SELECT import_run_id
              INTO actual_run_id
              FROM {snapshot}
             WHERE snapshot_id = locked_snapshot_id
             FOR UPDATE;
            IF actual_run_id IS NULL THEN
                RETURN;
            END IF;
            IF actual_run_id IS DISTINCT FROM locked_run_id THEN
                RAISE EXCEPTION
                    'PTG2_ATTEMPT_FENCE_PAIR_CHANGED'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT options::jsonb->>'storage_generation'
              INTO run_generation
              FROM {internal_run}
             WHERE import_run_id = locked_run_id
             FOR UPDATE;
            IF run_generation IS DISTINCT FROM 'shared_blocks_v4' THEN
                RAISE EXCEPTION
                    'PTG2_ATTEMPT_FENCE_GENERATION_CHANGED'
                    USING ERRCODE = 'P0001';
            END IF;

            INSERT INTO {fence} (snapshot_id, internal_run_id, state)
            VALUES (locked_snapshot_id, locked_run_id, 'active')
            ON CONFLICT (snapshot_id) DO NOTHING;
            SELECT state
              INTO fence_state
              FROM {fence}
             WHERE snapshot_id = locked_snapshot_id
               AND internal_run_id = locked_run_id
             FOR UPDATE;
            IF fence_state IS NULL THEN
                RAISE EXCEPTION
                    'PTG2_ATTEMPT_FENCE_PAIR_CONFLICT'
                    USING ERRCODE = 'P0001';
            END IF;
            IF fence_state = 'reconciled' AND NOT allow_reconciled THEN
                RAISE EXCEPTION
                    'PTG2_STALE_METADATA_FENCE'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $$;
        """
    )


def _create_audit_guard(schema: str) -> None:
    install_attempt_audit_guard(op, schema)


def upgrade() -> None:
    """Install the V4 attempt fence, registry triggers, and immutable audit."""

    schema = _schema()
    adoption_status = adopt_or_validate_attempt_tables(op, schema)
    _assert_one_snapshot_per_v4_run(schema)
    if adoption_status in {"absent", "offline"}:
        _create_fence_tables(schema)
    _create_lifecycle_lock_layer(schema)
    op.execute(
        f"""
        INSERT INTO {_qt(schema, 'ptg2_v4_attempt_fence')}
            (snapshot_id, internal_run_id, state)
        SELECT snapshot.snapshot_id, internal_run.import_run_id, 'active'
          FROM {_qt(schema, 'ptg2_snapshot')} AS snapshot
          JOIN {_qt(schema, 'ptg2_import_run')} AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
         WHERE COALESCE(
                   internal_run.options::jsonb->>'storage_generation',
                   ''
               ) = 'shared_blocks_v4'
        ON CONFLICT (snapshot_id) DO NOTHING
        """
    )
    _create_guard_function(schema)
    _create_audit_guard(schema)
    for attachment in (*ATTEMPT_STATE_TABLES, *ATTEMPT_ATTACHMENTS):
        if attachment.statement_trigger:
            _create_attachment_triggers(schema, attachment)


def downgrade() -> None:
    """Remove attempt fencing while preserving pre-existing PTG tables."""

    schema = _schema()
    assert_attempt_downgrade_safe(op, schema)
    for attachment in reversed(
        (*ATTEMPT_STATE_TABLES, *ATTEMPT_ATTACHMENTS)
    ):
        if attachment.statement_trigger:
            _drop_attachment_triggers(schema, attachment)
    _drop_lifecycle_lock_layer(schema)
    fence = _qt(schema, "ptg2_v4_attempt_fence")
    op.execute(
        f"DROP TRIGGER IF EXISTS "
        f"{_q('ptg2_v4_attempt_fence_audit_guard')} ON {fence}"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_function(schema, 'guard_ptg2_v4_attempt_audit')}()"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_function(schema, 'guard_ptg2_v4_attempt')}"
        "(text, text, boolean)"
    )
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v4_attempt_stage')}"
    )
    op.execute(f"DROP TABLE IF EXISTS {fence}")
