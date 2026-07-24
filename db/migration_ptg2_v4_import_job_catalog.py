# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration-owned catalog adoption for PTG V4 import jobs."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import has_matching_index
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_IMPORT_JOB_COLUMNS,
    ATTEMPT_IMPORT_JOB_INDEXES,
    ATTEMPT_IMPORT_JOB_PRIMARY_KEY,
    ATTEMPT_IMPORT_JOB_TABLE,
    import_job_table_elements,
)


_LIFECYCLE_LOCK_KEY = "ptg2_source_pointer_gc_v1"
EXPECTED_PHYSICAL_COLUMNS_BY_NAME = {
    "import_job_id": (
        1,
        "character varying(96)",
        True,
        None,
        "",
        "",
    ),
    "import_run_id": (
        2,
        "character varying(96)",
        False,
        None,
        "",
        "",
    ),
    "source_catalog_id": (
        3,
        "character varying(96)",
        False,
        None,
        "",
        "",
    ),
    "source_type": (
        4,
        "character varying(64)",
        False,
        None,
        "",
        "",
    ),
    "status": (
        5,
        "character varying(32)",
        False,
        None,
        "",
        "",
    ),
    "attempts": (6, "integer", False, None, "", ""),
    "lease_owner": (
        7,
        "character varying",
        False,
        None,
        "",
        "",
    ),
    "lease_expires_at": (
        8,
        "timestamp without time zone",
        False,
        None,
        "",
        "",
    ),
    "heartbeat_at": (
        9,
        "timestamp without time zone",
        False,
        None,
        "",
        "",
    ),
    "error": (10, "text", False, None, "", ""),
    "payload": (11, "json", False, None, "", ""),
    "created_at": (
        12,
        "timestamp without time zone",
        False,
        None,
        "",
        "",
    ),
    "updated_at": (
        13,
        "timestamp without time zone",
        False,
        None,
        "",
        "",
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


def _is_import_job_table_present(
    connection: sa.engine.Connection,
    schema: str,
) -> bool:
    return bool(
        connection.execute(
            sa.text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{schema}.{ATTEMPT_IMPORT_JOB_TABLE}"},
        ).scalar_one()
    )


def _has_import_job_table_under_lock(
    connection: sa.engine.Connection,
    schema: str,
) -> bool:
    """Lock parents before deciding whether the child relation exists."""

    connection.execute(
        sa.text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": _LIFECYCLE_LOCK_KEY},
    )
    connection.exec_driver_sql(
        f"LOCK TABLE {_qt(schema, 'ptg2_snapshot')}, "
        f"{_qt(schema, 'ptg2_import_run')} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    has_import_job_table = _is_import_job_table_present(connection, schema)
    if has_import_job_table:
        connection.exec_driver_sql(
            f"LOCK TABLE {_qt(schema, ATTEMPT_IMPORT_JOB_TABLE)} "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    return has_import_job_table


def _offline_table_sql(schema: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, ATTEMPT_IMPORT_JOB_TABLE)} (
            import_job_id varchar(96) NOT NULL,
            import_run_id varchar(96),
            source_catalog_id varchar(96),
            source_type varchar(64),
            status varchar(32),
            attempts integer,
            lease_owner varchar,
            lease_expires_at timestamp without time zone,
            heartbeat_at timestamp without time zone,
            error text,
            payload json,
            created_at timestamp without time zone,
            updated_at timestamp without time zone,
            CONSTRAINT {_q(ATTEMPT_IMPORT_JOB_PRIMARY_KEY)}
                PRIMARY KEY (import_job_id)
        )
    """


def _offline_index_sql(
    schema: str,
    index_name: str,
    column_name: str,
) -> str:
    return (
        f"CREATE INDEX IF NOT EXISTS {_q(index_name)} "
        f"ON {_qt(schema, ATTEMPT_IMPORT_JOB_TABLE)} "
        f"({_q(column_name)})"
    )


def emit_offline_import_job_catalog(
    operations: Any,
    schema: str,
) -> None:
    """Render the exact catalog through execute-only offline recorders."""

    operations.execute(_offline_table_sql(schema))
    for index_name, column_name in ATTEMPT_IMPORT_JOB_INDEXES:
        operations.execute(
            _offline_index_sql(schema, index_name, column_name)
        )


def _constraint_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> dict[str, tuple[str, tuple[str, ...]]]:
    constraint_result = connection.execute(
        sa.text(
            """
            SELECT constraint_record.conname,
                   constraint_record.contype::text AS constraint_type,
                   ARRAY(
                       SELECT attribute_record.attname
                         FROM unnest(constraint_record.conkey)
                              WITH ORDINALITY AS key_record(attnum, ordinal)
                         JOIN pg_attribute AS attribute_record
                           ON attribute_record.attrelid =
                              constraint_record.conrelid
                          AND attribute_record.attnum = key_record.attnum
                        ORDER BY key_record.ordinal
                   ) AS columns
              FROM pg_constraint AS constraint_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = constraint_record.conrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table_name
               AND constraint_record.contype <> 'n'
            """
        ),
        {"schema": schema, "table_name": ATTEMPT_IMPORT_JOB_TABLE},
    )
    return {
        str(constraint_record["conname"]): (
            str(constraint_record["constraint_type"]),
            tuple(constraint_record["columns"] or ()),
        )
        for constraint_record in constraint_result.mappings()
    }


def _physical_column_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> dict[str, tuple[int, str, bool, str | None, str, str]]:
    column_result = connection.execute(
        sa.text(
            """
            SELECT attribute_record.attname,
                   attribute_record.attnum,
                   format_type(
                       attribute_record.atttypid,
                       attribute_record.atttypmod
                   ) AS formatted_type,
                   attribute_record.attnotnull,
                   attribute_record.attidentity::text AS identity_kind,
                   attribute_record.attgenerated::text AS generated_kind,
                   pg_get_expr(
                       default_record.adbin,
                       default_record.adrelid
                   ) AS default_expression
              FROM pg_attribute AS attribute_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = attribute_record.attrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              LEFT JOIN pg_attrdef AS default_record
                ON default_record.adrelid = attribute_record.attrelid
               AND default_record.adnum = attribute_record.attnum
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table_name
               AND attribute_record.attnum > 0
               AND NOT attribute_record.attisdropped
            """
        ),
        {"schema": schema, "table_name": ATTEMPT_IMPORT_JOB_TABLE},
    )
    return {
        str(column_record["attname"]): (
            int(column_record["attnum"]),
            str(column_record["formatted_type"]),
            bool(column_record["attnotnull"]),
            (
                None
                if column_record["default_expression"] is None
                else str(column_record["default_expression"])
            ),
            str(column_record["identity_kind"]),
            str(column_record["generated_kind"]),
        )
        for column_record in column_result.mappings()
    }


def _index_names(
    connection: sa.engine.Connection,
    schema: str,
) -> frozenset[str]:
    result = connection.execute(
        sa.text(
            """
            SELECT index_record.relname
              FROM pg_index AS index_membership
              JOIN pg_class AS relation_record
                ON relation_record.oid = index_membership.indrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              JOIN pg_class AS index_record
                ON index_record.oid = index_membership.indexrelid
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table_name
            """
        ),
        {"schema": schema, "table_name": ATTEMPT_IMPORT_JOB_TABLE},
    )
    return frozenset(str(record[0]) for record in result)


def _is_schema_relation_present(
    connection: sa.engine.Connection,
    schema: str,
    relation_name: str,
) -> bool:
    return bool(
        connection.execute(
            sa.text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{schema}.{relation_name}"},
        ).scalar_one()
    )


def _validate_exact_catalog(
    operations: Any,
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    _validate_physical_column_catalog(operations, connection, schema)
    _validate_primary_key_catalog(connection, schema)
    _adopt_or_validate_index_catalog(operations, connection, schema)


def _validate_physical_column_catalog(
    operations: Any,
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    physical_columns = _physical_column_catalog(connection, schema)
    if (
        frozenset(physical_columns) != ATTEMPT_IMPORT_JOB_COLUMNS
        or physical_columns != EXPECTED_PHYSICAL_COLUMNS_BY_NAME
    ):
        raise RuntimeError(
            f"ptg2_v4_import_job_columns_unknown:{schema}"
        )
    create_table_or_validate(
        operations,
        ATTEMPT_IMPORT_JOB_TABLE,
        *import_job_table_elements(),
        schema=schema,
    )


def _validate_primary_key_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    constraints = _constraint_catalog(connection, schema)
    if constraints != {
        ATTEMPT_IMPORT_JOB_PRIMARY_KEY: (
            "p",
            ("import_job_id",),
        )
    }:
        raise RuntimeError(
            f"ptg2_v4_import_job_constraints_unknown:{schema}"
        )


def _adopt_or_validate_index_catalog(
    operations: Any,
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    expected_index_names = {
        ATTEMPT_IMPORT_JOB_PRIMARY_KEY,
        *(index_name for index_name, _column in ATTEMPT_IMPORT_JOB_INDEXES),
    }
    existing_index_names = _index_names(connection, schema)
    if existing_index_names - expected_index_names:
        raise RuntimeError(
            f"ptg2_v4_import_job_index_set_unknown:{schema}"
    )
    for index_name, column_name in ATTEMPT_IMPORT_JOB_INDEXES:
        if index_name in existing_index_names:
            if not has_matching_index(
                operations,
                index_name,
                ATTEMPT_IMPORT_JOB_TABLE,
                (column_name,),
                schema=schema,
            ):
                raise RuntimeError(
                    f"ptg2_v4_import_job_index_unknown:"
                    f"{schema}.{index_name}"
                )
            continue
        if _is_schema_relation_present(connection, schema, index_name):
            raise RuntimeError(
                f"ptg2_v4_import_job_index_name_collision:"
                f"{schema}.{index_name}"
            )
        operations.create_index(
            index_name,
            ATTEMPT_IMPORT_JOB_TABLE,
            [column_name],
            schema=schema,
        )
    if _index_names(connection, schema) != expected_index_names:
        raise RuntimeError(
            f"ptg2_v4_import_job_index_set_unknown:{schema}"
        )


def adopt_or_create_import_job(
    operations: Any,
    schema: str,
    *,
    emit_offline: bool = True,
) -> str:
    """Create/adopt under lifecycle, snapshot/run, then job locks."""

    connection = _connection(operations)
    if connection is None:
        if emit_offline:
            emit_offline_import_job_catalog(operations, schema)
        return "offline"
    has_import_job_table = _has_import_job_table_under_lock(
        connection,
        schema,
    )
    if not has_import_job_table:
        operations.create_table(
            ATTEMPT_IMPORT_JOB_TABLE,
            *import_job_table_elements(),
            schema=schema,
        )
        for index_name, column_name in ATTEMPT_IMPORT_JOB_INDEXES:
            operations.create_index(
                index_name,
                ATTEMPT_IMPORT_JOB_TABLE,
                [column_name],
                schema=schema,
            )
    _validate_exact_catalog(operations, connection, schema)
    return "adopted" if has_import_job_table else "created"


def validate_import_job_catalog(
    operations: Any,
    schema: str,
) -> None:
    """Validate the exact table under its migration lifecycle lock."""

    connection = _connection(operations)
    if connection is None:
        return
    if not _is_import_job_table_present(connection, schema):
        raise RuntimeError(f"ptg2_v4_import_job_missing:{schema}")
    _validate_exact_catalog(operations, connection, schema)
