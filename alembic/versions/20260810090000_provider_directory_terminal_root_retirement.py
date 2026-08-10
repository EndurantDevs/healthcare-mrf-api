# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal one exact legacy terminal Provider Directory acquisition root.

Revision ID: 20260810090000_provider_directory_terminal_root_retirement
Revises: 20260810080000_provider_directory_uhc_flex_practitioner_publication
"""

from __future__ import annotations

import os

from alembic import op

from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as guards


revision = "20260810090000_provider_directory_terminal_root_retirement"
down_revision = (
    "20260810080000_provider_directory_uhc_flex_practitioner_publication"
)
branch_labels = None
depends_on = None


_DATASET = "provider_directory_endpoint_dataset"
_IMPORT_RUN = "import_run"
_TRIGGER_PREFIX = "pd_trr_"
_PARENT_TRIGGERS = (
    "pd_trr_dataset_row",
    "pd_trr_dataset_truncate",
)
_IMPORT_RUN_TRIGGERS = ("pd_trr_import_run_row", "pd_trr_import_run_truncate")


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _function_signatures(schema: str) -> tuple[str, ...]:
    names = (
        (evidence.EVIDENCE_FUNCTION, "text"),
        (evidence.RELATION_EVIDENCE_FUNCTION, "text,text"),
        (guards.VALID_FUNCTION, "text"),
        (guards.MARKER_FUNCTION, "text,jsonb"),
        (guards.ELIGIBLE_FUNCTION, "text,integer"),
        (guards.RUN_RETIRED_FUNCTION, "text"),
        (guards.PARENT_GUARD, ""),
        (guards.CHILD_GUARD, ""),
        (guards.IMPORT_RUN_GUARD, ""),
    )
    return tuple(
        f"{guards._qf(schema, name)}({arguments})" for name, arguments in names
    )


def _adoption_fence_sql(schema: str) -> str:
    dataset = guards._qf(schema, _DATASET)
    signatures = ", ".join(
        f"pg_catalog.to_regprocedure({guards._ql(signature)})"
        for signature in _function_signatures(schema)
    )
    return f"""
    DO $migration$
    BEGIN
        LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE row.status = {guards._ql(guards.STATUS)}
                OR COALESCE(row.publication_metadata_json::jsonb, '{{}}'::jsonb)
                     ? {guards._ql(guards.MARKER)}
        ) OR EXISTS (
            SELECT 1 FROM pg_catalog.unnest(ARRAY[{signatures}]::regprocedure[])
             AS installed(signature) WHERE installed.signature IS NOT NULL
        ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger AS trigger
            JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = {guards._ql(schema)}
               AND trigger.tgname LIKE {guards._ql(_TRIGGER_PREFIX + '%')}
               AND NOT trigger.tgisinternal
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _downgrade_fence_sql(schema: str) -> str:
    dataset = guards._qf(schema, _DATASET)
    return f"""
    DO $migration$
    BEGIN
        LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE row.status = {guards._ql(guards.STATUS)}
                OR COALESCE(row.publication_metadata_json::jsonb, '{{}}'::jsonb)
                     ? {guards._ql(guards.MARKER)}
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _function_sqls(schema: str) -> tuple[str, ...]:
    return (
        evidence.relation_evidence_function_sql(schema),
        evidence.evidence_function_sql(schema),
        guards.eligible_function_sql(schema),
        guards.marker_function_sql(schema),
        guards.valid_function_sql(schema),
        guards.run_retired_function_sql(schema),
        guards.parent_guard_function_sql(schema),
        guards.child_guard_function_sql(schema),
        guards.import_run_guard_function_sql(schema),
    )


def _create_trigger(relation: str, name: str, clause: str, function: str) -> None:
    op.execute(
        f"CREATE {clause} ON {relation} FOR EACH ROW EXECUTE FUNCTION {function}();"
    )
    op.execute(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {guards._q(name)};")


def _create_triggers(schema: str) -> None:
    dataset = guards._qf(schema, _DATASET)
    parent = guards._qf(schema, guards.PARENT_GUARD)
    _create_trigger(
        dataset, _PARENT_TRIGGERS[0],
        f"TRIGGER {guards._q(_PARENT_TRIGGERS[0])} BEFORE INSERT OR UPDATE OR DELETE",
        parent,
    )
    op.execute(
        f"CREATE TRIGGER {guards._q(_PARENT_TRIGGERS[1])} BEFORE TRUNCATE "
        f"ON {dataset} FOR EACH STATEMENT EXECUTE FUNCTION {parent}();"
    )
    op.execute(
        f"ALTER TABLE {dataset} ENABLE ALWAYS TRIGGER {guards._q(_PARENT_TRIGGERS[1])};"
    )
    _create_child_triggers(schema)
    _create_import_run_triggers(schema)


def _create_child_triggers(schema: str) -> None:
    child_guard = guards._qf(schema, guards.CHILD_GUARD)
    for table_name, suffix in guards.CHILD_TRIGGER_SUFFIXES.items():
        relation = guards._qf(schema, table_name)
        row_name = f"pd_trr_{suffix}_row"
        truncate_name = f"pd_trr_{suffix}_truncate"
        _create_trigger(
            relation, row_name,
            f"TRIGGER {guards._q(row_name)} BEFORE INSERT OR UPDATE OR DELETE",
            child_guard,
        )
        op.execute(
            f"CREATE TRIGGER {guards._q(truncate_name)} BEFORE TRUNCATE ON "
            f"{relation} FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();"
        )
        op.execute(
            f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {guards._q(truncate_name)};"
        )


def _create_import_run_triggers(schema: str) -> None:
    relation = guards._qf(schema, _IMPORT_RUN)
    function = guards._qf(schema, guards.IMPORT_RUN_GUARD)
    _create_trigger(
        relation, _IMPORT_RUN_TRIGGERS[0],
        f"TRIGGER {guards._q(_IMPORT_RUN_TRIGGERS[0])} BEFORE INSERT OR UPDATE OR DELETE",
        function,
    )
    op.execute(
        f"CREATE TRIGGER {guards._q(_IMPORT_RUN_TRIGGERS[1])} BEFORE TRUNCATE ON "
        f"{relation} FOR EACH STATEMENT EXECUTE FUNCTION {function}();"
    )
    op.execute(
        f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "
        f"{guards._q(_IMPORT_RUN_TRIGGERS[1])};"
    )


def _drop_triggers(schema: str) -> None:
    relations = {
        _DATASET: _PARENT_TRIGGERS,
        _IMPORT_RUN: _IMPORT_RUN_TRIGGERS,
        **{
            table: (f"pd_trr_{suffix}_row", f"pd_trr_{suffix}_truncate")
            for table, suffix in guards.CHILD_TRIGGER_SUFFIXES.items()
        },
    }
    for table_name, trigger_names in relations.items():
        relation = guards._qf(schema, table_name)
        for trigger_name in trigger_names:
            op.execute(
                f"DROP TRIGGER IF EXISTS {guards._q(trigger_name)} ON {relation};"
            )


def _drop_functions(schema: str) -> None:
    signatures = _function_signatures(schema)
    drop_order = (6, 7, 8, 2, 3, 4, 5, 0, 1)
    for index in drop_order:
        signature = signatures[index]
        op.execute(f"DROP FUNCTION IF EXISTS {signature};")


def upgrade() -> None:
    schema = _schema()
    op.execute(_adoption_fence_sql(schema))
    for statement in _function_sqls(schema):
        op.execute(statement)
    for signature in _function_signatures(schema):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")
    _create_triggers(schema)


def downgrade() -> None:
    schema = _schema()
    op.execute(_downgrade_fence_sql(schema))
    _drop_triggers(schema)
    _drop_functions(schema)
