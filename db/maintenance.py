# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Tuple

import click
from sqlalchemy import inspect, text

from db.connection import Base, db
from db.migration_index_adoption import has_matching_index
from db.models import ProviderDirectoryPaginationCheckpoint
from db.provider_directory_schema import (
    ensure_provider_directory_pagination_root_identity,
)

logger = logging.getLogger(__name__)

EXACT_RUNTIME_INDEXES = {
    "provider_directory_dataset_insurance_plan_active_lookup_idx",
}


class _BoundMigrationOperations:
    """Expose a synchronous SQLAlchemy connection to migration validators."""

    def __init__(self, database_connection: Any):
        self._database_connection = database_connection

    def get_bind(self) -> Any:
        """Return the synchronous connection used by the runtime schema pass."""
        return self._database_connection


async def sync_structure(add_columns: bool = True, add_indexes: bool = True) -> Dict[str, List[str]]:
    """
    Ensure tables, columns, and indexes defined in SQLAlchemy metadata exist in the database.
    """
    await db.connect()
    results: Dict[str, List[str]] = {
        "tables": [],
        "columns": [],
        "indexes": [],
        "constraints": [],
        "skipped_columns": [],
        "retired": [],
    }

    async with db.engine.begin() as conn:
        await conn.run_sync(
            _sync_structure,
            results,
            add_columns,
            add_indexes,
        )

    return results


def _sync_structure(
    sync_conn,
    sync_summary_by_kind: Dict[str, List[str]],
    add_columns: bool,
    add_indexes: bool,
) -> None:
    metadata = Base.metadata
    inspector = inspect(sync_conn)
    model_map = _model_by_table_fullname()
    managed_schemas = set(_managed_schemas())

    for table in metadata.sorted_tables:
        schema = table.schema
        if not _should_manage_table_schema(schema, managed_schemas):
            continue
        table_name = table.name
        fullname = table.fullname

        if not inspector.has_table(table_name, schema=schema):
            table.create(bind=sync_conn, checkfirst=True)
            sync_summary_by_kind["tables"].append(fullname)
            continue

        if add_columns:
            _ensure_columns(sync_conn, inspector, table, sync_summary_by_kind)

        if add_indexes:
            model = model_map.get(fullname)
            if model is not None:
                _ensure_indexes(
                    sync_conn,
                    inspector,
                    table,
                    model,
                    sync_summary_by_kind,
                )

    checkpoint_schema = ProviderDirectoryPaginationCheckpoint.__table__.schema
    if checkpoint_schema in managed_schemas:
        ensure_provider_directory_pagination_root_identity(
            sync_conn,
            checkpoint_schema,
            sync_summary_by_kind,
        )


def _model_by_table_fullname() -> Dict[str, Any]:
    mapping: Dict[str, Any] = {}
    for mapper in Base.registry.mappers:
        cls = mapper.class_
        table = getattr(cls, "__table__", None)
        if table is not None:
            mapping[table.fullname] = cls
    return mapping


def _ensure_columns(sync_conn, inspector, table, results: Dict[str, List[str]]) -> None:
    schema = table.schema
    table_name = table.name
    fullname = table.fullname
    existing_columns = {col["name"] for col in inspector.get_columns(table_name, schema=schema)}

    for column in table.columns:
        if column.name in existing_columns:
            continue
        ddl = _compile_column_definition(column, sync_conn.dialect)
        if ddl is None:
            warning = f"{fullname}.{column.name}"
            results["skipped_columns"].append(warning)
            logger.warning("Skipping column %s without default because it is non-nullable.", warning)
            continue
        schema_prefix = f'"{schema}".' if schema else ""
        add_stmt = text(f'ALTER TABLE {schema_prefix}"{table_name}" ADD COLUMN {ddl}')
        sync_conn.execute(add_stmt)
        results["columns"].append(f"{fullname}.{column.name}")


def _compile_column_definition(column, dialect) -> str | None:
    col_type = column.type.compile(dialect=dialect)
    pieces = [f'"{column.name}" {col_type}']

    if column.computed is not None:
        persisted = " STORED" if column.computed.persisted is not False else ""
        pieces.append(
            f"GENERATED ALWAYS AS ({column.computed.sqltext}){persisted}"
        )
        return " ".join(pieces)

    default = None
    if column.server_default is not None:
        default = str(column.server_default.arg)
    elif column.default is not None and getattr(column.default, "is_scalar", False):
        default = repr(column.default.arg)

    if default is not None:
        pieces.append(f"DEFAULT {default}")

    if not column.nullable:
        if default is None:
            return None
        pieces.append("NOT NULL")

    return " ".join(pieces)


def _ensure_indexes(sync_conn, inspector, table, model, results: Dict[str, List[str]]) -> None:
    schema = table.schema
    table_name = table.name
    fullname = table.fullname
    existing_indexes = {idx["name"] for idx in inspector.get_indexes(table_name, schema=schema)}

    for spec in _iter_index_specs(model):
        name = spec["name"]
        if name in existing_indexes:
            if name in EXACT_RUNTIME_INDEXES:
                has_matching_index(
                    _BoundMigrationOperations(sync_conn),
                    name,
                    table_name,
                    spec["columns"],
                    schema=schema or "public",
                    unique=bool(spec.get("unique", False)),
                    postgresql_include=spec.get("include") or (),
                    postgresql_where=(
                        text(spec["where"]) if spec.get("where") else None
                    ),
                    postgresql_using=spec.get("using"),
                )
            continue
        schema_prefix = f'"{schema}".' if schema else ""
        index_sql = _build_index_sql(name, schema_prefix, table_name, spec)
        sync_conn.execute(text(index_sql))
        results["indexes"].append(f"{fullname}.{name}")


def _iter_index_specs(model) -> Iterable[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    primary_elements = _normalize_index_columns(getattr(model, "__my_index_elements__", None))
    if primary_elements and primary_elements != _primary_key_columns(model):
        specs.append(
            {
                "name": getattr(model, "__primary_index_name__", f"{model.__tablename__}_idx_primary"),
                "columns": primary_elements,
                "unique": True,
                "using": None,
            }
        )
    for extra in getattr(model, "__my_additional_indexes__", []) or []:
        columns = _normalize_index_columns(extra.get("index_elements", ()))
        if not columns:
            continue
        specs.append(
            {
                "name": extra.get("name") or f"{model.__tablename__}_{'_'.join(columns)}_idx",
                "columns": columns,
                "unique": extra.get("unique", False),
                "using": extra.get("using"),
                "where": extra.get("where"),
                "include": _normalize_index_columns(extra.get("include", ())),
            }
        )
    return specs


def _primary_key_columns(model) -> Tuple[str, ...]:
    table = getattr(model, "__table__", None)
    if table is None:
        return ()
    return tuple(column.name for column in table.primary_key.columns)


def _build_index_sql(name: str, schema_prefix: str, table_name: str, spec: Dict[str, Any]) -> str:
    columns = _normalize_index_columns(spec.get("columns") or ())
    column_clause = ", ".join(columns)
    using_clause = f" USING {spec['using']}" if spec.get("using") else ""
    unique_clause = "UNIQUE " if spec.get("unique") else ""
    where_clause = f" WHERE {spec['where']}" if spec.get("where") else ""
    include_columns = _normalize_index_columns(spec.get("include") or ())
    include_clause = (
        f" INCLUDE ({', '.join(include_columns)})" if include_columns else ""
    )
    qualified_table = f'{schema_prefix}"{table_name}"'
    return (
        f"CREATE {unique_clause}INDEX IF NOT EXISTS {name} "
        f"ON {qualified_table}{using_clause} ({column_clause})"
        f"{include_clause}{where_clause};"
    )


def _normalize_index_columns(columns: Any) -> Tuple[str, ...]:
    if columns is None:
        return ()
    if isinstance(columns, str):
        text_value = columns.strip()
        return (text_value,) if text_value else ()
    if isinstance(columns, (tuple, list)):
        return tuple(str(item).strip() for item in columns if str(item).strip())
    try:
        return tuple(str(item).strip() for item in columns if str(item).strip())
    except TypeError:
        text_value = str(columns).strip()
        return (text_value,) if text_value else ()


def _managed_schemas() -> Tuple[str, ...]:
    configured = str(os.getenv("HLTHPRT_MANAGED_SCHEMAS") or "").strip()
    if configured:
        schemas = tuple(part.strip() for part in configured.split(",") if part.strip())
        if schemas:
            return schemas
    default_schema = str(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").strip() or "mrf"
    return (default_schema,)


def _should_manage_table_schema(schema: str | None, managed_schemas: set[str]) -> bool:
    if schema is None:
        return "public" in managed_schemas
    return schema in managed_schemas


def render_sync_summary(results: Dict[str, List[str]]) -> None:
    if results.get("retired"):
        click.echo(f"Retired objects: {', '.join(results['retired'])}")
    if results["tables"]:
        click.echo(f"Created tables: {', '.join(results['tables'])}")
    if results["columns"]:
        click.echo(f"Added columns: {', '.join(results['columns'])}")
    if results["indexes"]:
        click.echo(f"Added indexes: {', '.join(results['indexes'])}")
    if results["constraints"]:
        click.echo(f"Reconciled constraints: {', '.join(results['constraints'])}")
    if results["skipped_columns"]:
        click.echo(
            "Skipped non-nullable columns without defaults: "
            + ", ".join(results["skipped_columns"])
        )
