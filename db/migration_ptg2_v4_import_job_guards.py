# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration-owned writer guards for PTG V4 import jobs."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from db.migration_expression_adoption import _normalized_expression
from db.ptg2_v4_attempt_schema import ATTEMPT_IMPORT_JOB_TABLE


_LIFECYCLE_FUNCTION = "lock_ptg2_v4_attempt_lifecycle"
_GUARD_FUNCTION = "guard_ptg2_import_job_attempt"
_GUARD_BODY = """
DECLARE
    coordinate record;
BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        FOR coordinate IN
            SELECT DISTINCT snapshot_id, internal_run_id
              FROM (
                SELECT NULL::text AS snapshot_id,
                       "import_run_id"::text AS internal_run_id
                  FROM attempt_new_rows
              ) AS coordinates
             WHERE snapshot_id IS NOT NULL
                OR internal_run_id IS NOT NULL
        LOOP
            PERFORM guard_ptg2_v4_attempt(
                coordinate.snapshot_id,
                coordinate.internal_run_id
            );
        END LOOP;
    END IF;
    IF TG_OP IN ('DELETE', 'UPDATE') THEN
        FOR coordinate IN
            SELECT DISTINCT snapshot_id, internal_run_id
              FROM (
                SELECT NULL::text AS snapshot_id,
                       "import_run_id"::text AS internal_run_id
                  FROM attempt_old_rows
              ) AS coordinates
             WHERE snapshot_id IS NOT NULL
                OR internal_run_id IS NOT NULL
        LOOP
            PERFORM guard_ptg2_v4_attempt(
                coordinate.snapshot_id,
                coordinate.internal_run_id
            );
        END LOOP;
    END IF;
    RETURN NULL;
END;
""".strip()
EXPECTED_IMPORT_JOB_TRIGGER_CONTRACT_BY_NAME = {
    f"{ATTEMPT_IMPORT_JOB_TABLE}_attempt_lifecycle_lock": (
        30,
        _LIFECYCLE_FUNCTION,
        None,
        None,
    ),
    f"{ATTEMPT_IMPORT_JOB_TABLE}_attempt_insert_guard": (
        4,
        _GUARD_FUNCTION,
        None,
        "attempt_new_rows",
    ),
    f"{ATTEMPT_IMPORT_JOB_TABLE}_attempt_update_guard": (
        16,
        _GUARD_FUNCTION,
        "attempt_old_rows",
        "attempt_new_rows",
    ),
    f"{ATTEMPT_IMPORT_JOB_TABLE}_attempt_delete_guard": (
        8,
        _GUARD_FUNCTION,
        "attempt_old_rows",
        None,
    ),
}


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def _connection(operations: Any) -> sa.engine.Connection | None:
    get_bind = getattr(operations, "get_bind", None)
    connection = get_bind() if callable(get_bind) else None
    return (
        connection
        if isinstance(connection, sa.engine.Connection)
        else None
    )


def _qualified_guard_body(schema: str) -> str:
    return _GUARD_BODY.replace(
        "PERFORM guard_ptg2_v4_attempt(",
        f"PERFORM {_q(schema)}.{_q('guard_ptg2_v4_attempt')}(",
    )


def install_import_job_attempt_guards(
    operations: Any,
    schema: str,
) -> None:
    """Install exact transition-table guards for import-job writes."""

    table = _qt(schema, ATTEMPT_IMPORT_JOB_TABLE)
    guard_function = f"{_q(schema)}.{_q(_GUARD_FUNCTION)}"
    operations.execute(
        f"""
        CREATE OR REPLACE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        {_qualified_guard_body(schema)}
        $$
        """
    )
    for operation, transition in (
        ("insert", "REFERENCING NEW TABLE AS attempt_new_rows"),
        (
            "update",
            "REFERENCING OLD TABLE AS attempt_old_rows "
            "NEW TABLE AS attempt_new_rows",
        ),
        ("delete", "REFERENCING OLD TABLE AS attempt_old_rows"),
    ):
        trigger_name = (
            f"{ATTEMPT_IMPORT_JOB_TABLE}_attempt_{operation}_guard"
        )
        operations.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} ON {table}"
        )
        operations.execute(
            f"""
            CREATE TRIGGER {_q(trigger_name)}
            AFTER {operation.upper()} ON {table}
            {transition}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {guard_function}()
            """
        )


def _import_job_trigger_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> dict[str, dict[str, Any]]:
    trigger_result = connection.execute(
        sa.text(
            """
            SELECT trigger_record.tgname,
                   trigger_record.tgtype,
                   trigger_record.tgenabled,
                   trigger_record.tgoldtable,
                   trigger_record.tgnewtable,
                   function_record.proname,
                   function_schema.nspname AS function_schema,
                   language_record.lanname,
                   function_record.pronargs,
                   function_record.prorettype::regtype::text AS return_type,
                   function_record.prosrc
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
              JOIN pg_namespace AS function_schema
                ON function_schema.oid = function_record.pronamespace
              JOIN pg_language AS language_record
                ON language_record.oid = function_record.prolang
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table_name
               AND NOT trigger_record.tgisinternal
            """
        ),
        {"schema": schema, "table_name": ATTEMPT_IMPORT_JOB_TABLE},
    )
    return {
        str(trigger_record["tgname"]): dict(trigger_record)
        for trigger_record in trigger_result.mappings()
    }


def _validate_import_job_trigger_contract(
    trigger_by_name: dict[str, dict[str, Any]],
    schema: str,
) -> None:
    expected_by_name = EXPECTED_IMPORT_JOB_TRIGGER_CONTRACT_BY_NAME
    if set(trigger_by_name) != set(expected_by_name):
        raise RuntimeError(
            f"ptg2_v4_import_job_trigger_set_unknown:{schema}"
        )
    expected_guard_body = _normalized_expression(
        _qualified_guard_body(schema)
    )
    for trigger_name, (
        trigger_type,
        function_name,
        old_table,
        new_table,
    ) in expected_by_name.items():
        trigger_record = trigger_by_name[trigger_name]
        if (
            int(trigger_record["tgtype"]) != trigger_type
            or trigger_record["tgenabled"] not in {"O", b"O"}
            or trigger_record["proname"] != function_name
            or trigger_record["function_schema"] != schema
            or trigger_record["lanname"] != "plpgsql"
            or int(trigger_record["pronargs"]) != 0
            or trigger_record["return_type"] != "trigger"
            or trigger_record["tgoldtable"] != old_table
            or trigger_record["tgnewtable"] != new_table
        ):
            raise RuntimeError(
                f"ptg2_v4_import_job_trigger_unknown:{trigger_name}"
            )
        if (
            function_name == _GUARD_FUNCTION
            and _normalized_expression(trigger_record["prosrc"])
            != expected_guard_body
        ):
            raise RuntimeError(
                f"ptg2_v4_import_job_guard_function_unknown:{schema}"
            )


def validate_import_job_attempt_guards(
    operations: Any,
    schema: str,
) -> None:
    """Require exactly one lifecycle and three attachment guards."""

    connection = _connection(operations)
    if connection is None:
        return
    trigger_by_name = _import_job_trigger_catalog(connection, schema)
    _validate_import_job_trigger_contract(trigger_by_name, schema)
