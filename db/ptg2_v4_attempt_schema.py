# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared schema contract for immutable PTG V4 attempt authority."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


ATTEMPT_FENCE_TABLE = "ptg2_v4_attempt_fence"
ATTEMPT_IMPORT_JOB_TABLE = "ptg2_import_job"
ATTEMPT_IMPORT_JOB_PRIMARY_KEY = "ptg2_import_job_pkey"
ATTEMPT_IMPORT_JOB_INDEXES = (
    ("ptg2_import_job_run_idx", "import_run_id"),
    ("ptg2_import_job_status_idx", "status"),
    ("ptg2_import_job_type_idx", "source_type"),
)
ATTEMPT_STAGE_TABLE = "ptg2_v4_attempt_stage"
ATTEMPT_STAGE_RUN_INDEX = "ptg2_v4_attempt_stage_run_idx"
LIFECYCLE_GUARDED_TABLES = (
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_snapshot_pin",
    "plan_release_snapshot_binding",
    "ptg2_plan_month",
    "ptg2_artifact_manifest",
    "ptg2_current_snapshot",
    "ptg2_current_source_snapshot",
    "ptg2_current_plan_source",
    "ptg2_import_job",
    "ptg2_v4_attempt_stage",
    "ptg2_snapshot",
    "ptg2_import_run",
)

FENCE_STATE_CHECK_SQL = "state IN ('active', 'reconciled')"
FENCE_LEGACY_AUDIT_SHAPE_CHECK_SQL = """
state = 'active'
OR (
    target_digest ~ '^[0-9a-f]{64}$'
    AND plan_digest ~ '^[0-9a-f]{64}$'
    AND marker_digest ~ '^[0-9a-f]{64}$'
    AND marker IS NOT NULL
    AND reconciled_at IS NOT NULL
)
""".strip()
FENCE_AUDIT_SHAPE_CHECK_SQL = """
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
    AND target_digest ~ '^[0-9a-f]{64}$'
    AND plan_digest IS NOT NULL
    AND plan_digest ~ '^[0-9a-f]{64}$'
    AND marker_digest IS NOT NULL
    AND marker_digest ~ '^[0-9a-f]{64}$'
    AND COALESCE(jsonb_typeof(marker) = 'object', false)
    AND reconciled_at IS NOT NULL
)
""".strip()

FENCE_LEGACY_COLUMNS = frozenset(
    {
        "snapshot_id",
        "internal_run_id",
        "state",
        "target_digest",
        "plan_digest",
        "marker_digest",
        "marker",
        "created_at",
        "reconciled_at",
    }
)
ATTEMPT_IMPORT_JOB_COLUMNS = frozenset(
    {
        "import_job_id",
        "import_run_id",
        "source_catalog_id",
        "source_type",
        "status",
        "attempts",
        "lease_owner",
        "lease_expires_at",
        "heartbeat_at",
        "error",
        "payload",
        "created_at",
        "updated_at",
    }
)
FENCE_FINAL_COLUMNS = FENCE_LEGACY_COLUMNS | {"fence_nonce"}
STAGE_FINAL_COLUMNS = frozenset(
    {"snapshot_id", "internal_run_id", "table_name", "created_at"}
)

FENCE_BASE_CONSTRAINTS = frozenset(
    {
        "ptg2_v4_attempt_fence_pkey",
        "ptg2_v4_attempt_fence_internal_run_id_key",
        "ptg2_v4_attempt_fence_snapshot_fkey",
        "ptg2_v4_attempt_fence_run_fkey",
        "ptg2_v4_attempt_fence_pair_key",
        "ptg2_v4_attempt_fence_state_check",
    }
)
FENCE_LEGACY_AUDIT_CONSTRAINT = (
    "ptg2_v4_attempt_fence_reconciled_check"
)
FENCE_AUDIT_CONSTRAINT = "ptg2_v4_attempt_fence_audit_shape_check"
FENCE_NONCE_CONSTRAINT = "ptg2_v4_attempt_fence_nonce_key"
FENCE_FINAL_CONSTRAINTS = FENCE_BASE_CONSTRAINTS | {
    FENCE_AUDIT_CONSTRAINT,
    FENCE_NONCE_CONSTRAINT,
}
STAGE_FINAL_CONSTRAINTS = frozenset(
    {
        "ptg2_v4_attempt_stage_pkey",
        "ptg2_v4_attempt_stage_fence_fkey",
    }
)


def _fence_columns(*, include_nonce: bool) -> list:
    """Return fresh columns for one supported fence shape."""

    elements: list = [
        sa.Column("snapshot_id", sa.String(96), nullable=False),
        sa.Column("internal_run_id", sa.String(96), nullable=False),
    ]
    if include_nonce:
        elements.append(
            sa.Column(
                "fence_nonce",
                postgresql.UUID(as_uuid=True),
                nullable=False,
                server_default=sa.text("gen_random_uuid()"),
            )
        )
    elements.extend(
        (
            sa.Column(
                "state",
                sa.String(16),
                nullable=False,
                server_default=sa.text("'active'::character varying"),
            ),
            sa.Column("target_digest", sa.String(64)),
            sa.Column("plan_digest", sa.String(64)),
            sa.Column("marker_digest", sa.String(64)),
            sa.Column("marker", postgresql.JSONB()),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column("reconciled_at", sa.DateTime(timezone=True)),
        )
    )
    return elements


def _fence_constraints(
    schema_name: str,
    *,
    include_nonce: bool,
    include_nonce_constraint: bool,
    legacy_audit_constraint: bool,
) -> list:
    """Return fresh named constraints for one supported fence shape."""

    elements: list = [
            sa.PrimaryKeyConstraint(
                "snapshot_id",
                name="ptg2_v4_attempt_fence_pkey",
            ),
            sa.UniqueConstraint(
                "internal_run_id",
                name="ptg2_v4_attempt_fence_internal_run_id_key",
            ),
            sa.UniqueConstraint(
                "snapshot_id",
                "internal_run_id",
                name="ptg2_v4_attempt_fence_pair_key",
            ),
            sa.ForeignKeyConstraint(
                ["snapshot_id"],
                [f"{schema_name}.ptg2_snapshot.snapshot_id"],
                name="ptg2_v4_attempt_fence_snapshot_fkey",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["internal_run_id"],
                [f"{schema_name}.ptg2_import_run.import_run_id"],
                name="ptg2_v4_attempt_fence_run_fkey",
                ondelete="CASCADE",
            ),
            sa.CheckConstraint(
                FENCE_STATE_CHECK_SQL,
                name="ptg2_v4_attempt_fence_state_check",
            ),
            sa.CheckConstraint(
                (
                    FENCE_LEGACY_AUDIT_SHAPE_CHECK_SQL
                    if legacy_audit_constraint
                    else FENCE_AUDIT_SHAPE_CHECK_SQL
                ),
                name=(
                    FENCE_LEGACY_AUDIT_CONSTRAINT
                    if legacy_audit_constraint
                    else FENCE_AUDIT_CONSTRAINT
                ),
            ),
    ]
    if include_nonce and include_nonce_constraint:
        elements.append(
            sa.UniqueConstraint(
                "fence_nonce",
                name=FENCE_NONCE_CONSTRAINT,
            )
        )
    return elements


def fence_table_elements(
    schema_name: str,
    *,
    include_nonce: bool = True,
    include_nonce_constraint: bool = True,
    legacy_audit_constraint: bool = False,
) -> tuple:
    """Return fresh SQLAlchemy objects for one supported fence shape."""

    return tuple(
        _fence_columns(include_nonce=include_nonce)
        + _fence_constraints(
            schema_name,
            include_nonce=include_nonce,
            include_nonce_constraint=include_nonce_constraint,
            legacy_audit_constraint=legacy_audit_constraint,
        )
    )


def import_job_table_elements() -> tuple:
    """Return fresh SQLAlchemy objects for the migration-owned job table."""

    return (
        sa.Column("import_job_id", sa.String(96), nullable=False),
        sa.Column("import_run_id", sa.String(96)),
        sa.Column("source_catalog_id", sa.String(96)),
        sa.Column("source_type", sa.String(64)),
        sa.Column("status", sa.String(32)),
        sa.Column("attempts", sa.Integer()),
        sa.Column("lease_owner", sa.String()),
        sa.Column("lease_expires_at", sa.DateTime()),
        sa.Column("heartbeat_at", sa.DateTime()),
        sa.Column("error", sa.Text()),
        sa.Column("payload", postgresql.JSON()),
        sa.Column("created_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime()),
        sa.PrimaryKeyConstraint(
            "import_job_id",
            name=ATTEMPT_IMPORT_JOB_PRIMARY_KEY,
        ),
    )


def stage_table_elements(schema_name: str) -> tuple:
    """Return fresh SQLAlchemy objects for the exact stage registry."""

    return (
        sa.Column("snapshot_id", sa.String(96), nullable=False),
        sa.Column("internal_run_id", sa.String(96), nullable=False),
        sa.Column("table_name", sa.String(63), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "table_name",
            name="ptg2_v4_attempt_stage_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "internal_run_id"],
            [
                f"{schema_name}.ptg2_v4_attempt_fence.snapshot_id",
                f"{schema_name}.ptg2_v4_attempt_fence.internal_run_id",
            ],
            name="ptg2_v4_attempt_stage_fence_fkey",
            ondelete="CASCADE",
        ),
    )


__all__ = [
    "ATTEMPT_FENCE_TABLE",
    "ATTEMPT_IMPORT_JOB_COLUMNS",
    "ATTEMPT_IMPORT_JOB_INDEXES",
    "ATTEMPT_IMPORT_JOB_PRIMARY_KEY",
    "ATTEMPT_IMPORT_JOB_TABLE",
    "ATTEMPT_STAGE_RUN_INDEX",
    "ATTEMPT_STAGE_TABLE",
    "FENCE_AUDIT_CONSTRAINT",
    "FENCE_AUDIT_SHAPE_CHECK_SQL",
    "FENCE_BASE_CONSTRAINTS",
    "FENCE_FINAL_COLUMNS",
    "FENCE_FINAL_CONSTRAINTS",
    "FENCE_LEGACY_AUDIT_CONSTRAINT",
    "FENCE_LEGACY_AUDIT_SHAPE_CHECK_SQL",
    "FENCE_LEGACY_COLUMNS",
    "FENCE_NONCE_CONSTRAINT",
    "FENCE_STATE_CHECK_SQL",
    "LIFECYCLE_GUARDED_TABLES",
    "STAGE_FINAL_COLUMNS",
    "STAGE_FINAL_CONSTRAINTS",
    "fence_table_elements",
    "import_job_table_elements",
    "stage_table_elements",
]
