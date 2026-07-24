# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""ORM parity contracts for migration-owned PTG V4 attempt tables."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.models import (
    PTG2ArtifactManifest,
    PTG2CurrentPlanSource,
    PTG2CurrentSnapshot,
    PTG2CurrentSourceSnapshot,
    PTG2ImportJob,
    PTG2Snapshot,
    PTG2SourceCatalog,
    PTG2V4AttemptFence,
    PTG2V4AttemptStage,
)
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_IMPORT_JOB_INDEXES,
    ATTEMPT_IMPORT_JOB_TABLE,
    ATTEMPT_STAGE_RUN_INDEX,
    ATTEMPT_STAGE_TABLE,
    fence_table_elements,
    import_job_table_elements,
    stage_table_elements,
)
from process.ptg_parts.table_setup import PTG2_MODEL_CLASSES


SCHEMA = PTG2V4AttemptFence.__table__.schema
ATTEMPT_INDEX_OWNERS = (
    *(
        (PTG2ImportJob, index_name, column_name)
        for index_name, column_name in ATTEMPT_IMPORT_JOB_INDEXES
    ),
    (PTG2Snapshot, "ptg2_snapshot_attempt_run_idx", "import_run_id"),
    (
        PTG2SourceCatalog,
        "ptg2_source_catalog_attempt_run_idx",
        "import_run_id",
    ),
    (
        PTG2ArtifactManifest,
        "ptg2_artifact_manifest_attempt_run_idx",
        "import_run_id",
    ),
    (
        PTG2CurrentSnapshot,
        "ptg2_current_snapshot_attempt_previous_idx",
        "previous_snapshot_id",
    ),
    (
        PTG2CurrentSourceSnapshot,
        "ptg2_current_source_attempt_previous_idx",
        "previous_snapshot_id",
    ),
    (
        PTG2CurrentPlanSource,
        "ptg2_current_plan_attempt_previous_idx",
        "previous_snapshot_id",
    ),
    (
        PTG2V4AttemptStage,
        ATTEMPT_STAGE_RUN_INDEX,
        "internal_run_id",
    ),
)


def _normalized_sql(value: object) -> str:
    return " ".join(str(value).split())


def _constraint_signature(constraint: sa.Constraint) -> tuple:
    columns = tuple(column.name for column in constraint.columns)
    if isinstance(constraint, sa.CheckConstraint):
        return ("check", constraint.name, _normalized_sql(constraint.sqltext))
    if isinstance(constraint, sa.ForeignKeyConstraint):
        return (
            "foreign_key",
            constraint.name,
            columns,
            tuple(element.target_fullname for element in constraint.elements),
            constraint.ondelete,
            constraint.onupdate,
        )
    if isinstance(constraint, sa.PrimaryKeyConstraint):
        return ("primary_key", constraint.name, columns)
    if isinstance(constraint, sa.UniqueConstraint):
        return ("unique", constraint.name, columns)
    raise AssertionError(f"unsupported constraint: {constraint!r}")


def _table_signature(table: sa.Table) -> tuple:
    dialect = postgresql.dialect()
    columns = tuple(
        (
            column.name,
            column.type.compile(dialect=dialect),
            column.nullable,
            (
                None
                if column.server_default is None
                else _normalized_sql(column.server_default.arg)
            ),
        )
        for column in table.columns
    )
    constraints = frozenset(
        _constraint_signature(constraint)
        for constraint in table.constraints
    )
    indexes = frozenset(
        (
            index.name,
            tuple(column.name for column in index.columns),
            index.unique,
        )
        for index in table.indexes
    )
    return columns, constraints, indexes


def test_attempt_models_match_the_canonical_schema_metadata() -> None:
    expected_metadata = sa.MetaData()
    expected_fence = sa.Table(
        ATTEMPT_FENCE_TABLE,
        expected_metadata,
        *fence_table_elements(SCHEMA),
        schema=SCHEMA,
    )
    expected_stage = sa.Table(
        ATTEMPT_STAGE_TABLE,
        expected_metadata,
        *stage_table_elements(SCHEMA),
        sa.Index(ATTEMPT_STAGE_RUN_INDEX, "internal_run_id"),
        schema=SCHEMA,
    )
    expected_import_job = sa.Table(
        ATTEMPT_IMPORT_JOB_TABLE,
        expected_metadata,
        *import_job_table_elements(),
        *(
            sa.Index(index_name, column_name)
            for index_name, column_name in ATTEMPT_IMPORT_JOB_INDEXES
        ),
        schema=SCHEMA,
    )

    assert _table_signature(PTG2V4AttemptFence.__table__) == (
        _table_signature(expected_fence)
    )
    assert _table_signature(PTG2V4AttemptStage.__table__) == (
        _table_signature(expected_stage)
    )
    assert _table_signature(PTG2ImportJob.__table__) == (
        _table_signature(expected_import_job)
    )


def test_attempt_indexes_are_owned_by_orm_metadata_and_runtime_ensures() -> None:
    for model, index_name, column_name in ATTEMPT_INDEX_OWNERS:
        indexes_by_name = {
            index.name: index for index in model.__table__.indexes
        }
        assert tuple(
            column.name for column in indexes_by_name[index_name].columns
        ) == (column_name,)
        assert {
            "index_elements": (column_name,),
            "name": index_name,
        } in model.__my_additional_indexes__


def test_migration_owned_attempt_models_are_not_runtime_created() -> None:
    assert PTG2V4AttemptFence not in PTG2_MODEL_CLASSES
    assert PTG2V4AttemptStage not in PTG2_MODEL_CLASSES
    assert PTG2ImportJob not in PTG2_MODEL_CLASSES
