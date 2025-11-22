# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Tuple

import click
from importlib import import_module
from sqlalchemy import inspect, text

from db.connection import Base, db

import_module("db.models")

logger = logging.getLogger(__name__)


async def sync_structure(add_columns: bool = True, add_indexes: bool = True) -> Dict[str, List[str]]:
    """
    Ensure tables, columns, and indexes defined in SQLAlchemy metadata exist in the database.
    """
    await db.connect()
    results: Dict[str, List[str]] = {"tables": [], "columns": [], "indexes": [], "skipped_columns": []}

    async with db.engine.begin() as conn:
        await conn.run_sync(
            _sync_structure,
            results,
            add_columns,
            add_indexes,
        )

    return results


def _sync_structure(sync_conn, results: Dict[str, List[str]], add_columns: bool, add_indexes: bool) -> None:
    metadata = Base.metadata
    inspector = inspect(sync_conn)
    model_map = _model_by_table_fullname()

    for table in metadata.sorted_tables:
        schema = table.schema
        table_name = table.name
        fullname = table.fullname

        if not inspector.has_table(table_name, schema=schema):
            table.create(bind=sync_conn, checkfirst=True)
            results["tables"].append(fullname)
            continue

        if add_columns:
            _ensure_columns(sync_conn, inspector, table, results)

        if add_indexes:
            model = model_map.get(fullname)
            if model is not None:
                _ensure_indexes(sync_conn, inspector, table, model, results)


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
            continue
        schema_prefix = f'"{schema}".' if schema else ""
        index_sql = _build_index_sql(name, schema_prefix, table_name, spec)
        sync_conn.execute(text(index_sql))
        results["indexes"].append(f"{fullname}.{name}")


def _iter_index_specs(model) -> Iterable[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    primary_elements = getattr(model, "__my_index_elements__", None)
    if primary_elements:
        specs.append(
            {
                "name": getattr(model, "__primary_index_name__", f"{model.__tablename__}_idx_primary"),
                "columns": primary_elements,
                "unique": True,
                "using": None,
            }
        )
    for extra in getattr(model, "__my_additional_indexes__", []) or []:
        specs.append(
            {
                "name": extra.get("name") or f"{model.__tablename__}_{'_'.join(extra.get('index_elements', []))}_idx",
                "columns": extra.get("index_elements", ()),
                "unique": extra.get("unique", False),
                "using": extra.get("using"),
                "where": extra.get("where"),
            }
        )
    return specs


def _build_index_sql(name: str, schema_prefix: str, table_name: str, spec: Dict[str, Any]) -> str:
    columns = spec.get("columns") or ()
    column_clause = ", ".join(columns)
    using_clause = f" USING {spec['using']}" if spec.get("using") else ""
    unique_clause = "UNIQUE " if spec.get("unique") else ""
    where_clause = f" WHERE {spec['where']}" if spec.get("where") else ""
    qualified_table = f'{schema_prefix}"{table_name}"'
    return f"CREATE {unique_clause}INDEX IF NOT EXISTS {name} ON {qualified_table}{using_clause} ({column_clause}){where_clause};"


def render_sync_summary(results: Dict[str, List[str]]) -> None:
    if results["tables"]:
        click.echo(f"Created tables: {', '.join(results['tables'])}")
    if results["columns"]:
        click.echo(f"Added columns: {', '.join(results['columns'])}")
    if results["indexes"]:
        click.echo(f"Added indexes: {', '.join(results['indexes'])}")
    if results["skipped_columns"]:
        click.echo(
            "Skipped non-nullable columns without defaults: "
            + ", ".join(results["skipped_columns"])
        )
