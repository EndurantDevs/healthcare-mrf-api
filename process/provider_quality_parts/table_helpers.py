# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database table helpers for provider-quality import staging."""

from __future__ import annotations

import hashlib

from db.connection import db


def _index_name_for_table(table_name: str, base_name: str) -> str:
    if len(base_name) <= 63:
        return base_name
    digest = hashlib.sha1(base_name.encode("utf-8")).hexdigest()[:10]
    prefix = table_name[: 63 - len(digest) - 1].rstrip("_")
    return f"{prefix}_{digest}"


async def _ensure_indexes(model_class: type, db_schema: str) -> None:
    if hasattr(model_class, "__my_index_elements__") and model_class.__my_index_elements__:
        cols = ", ".join(model_class.__my_index_elements__)
        name = _index_name_for_table(
            model_class.__tablename__,
            f"{model_class.__tablename__}_idx_primary",
        )
        await db.status(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            + f"{name} ON {db_schema}.{model_class.__tablename__} ({cols});"
        )
    if hasattr(model_class, "__my_additional_indexes__") and model_class.__my_additional_indexes__:
        for idx in model_class.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            base_name = idx.get("name") or f"{model_class.__tablename__}_{'_'.join(elements)}_idx"
            if getattr(model_class, "__main_table__", model_class.__tablename__) != model_class.__tablename__:
                name = f"{model_class.__tablename__}_{base_name}"
            else:
                name = base_name
            name = _index_name_for_table(model_class.__tablename__, name)
            using = idx.get("using")
            where = idx.get("where")
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{model_class.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await db.status(statement)


async def _build_staging_indexes(classes: dict[str, type], schema: str) -> None:
    for model in classes.values():
        await _ensure_indexes(model, schema)


async def _is_table_available(schema: str, table: str) -> bool:
    table_ref = f"{schema}.{table}"
    result = await db.scalar("SELECT to_regclass(:table_ref)", table_ref=table_ref)
    return result is not None


async def _table_columns(schema: str, table: str) -> set[str]:
    rows = await db.all(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = :table
        """,
        schema=schema,
        table=table,
    )
    return {str(getattr(row, "column_name", "") or "").strip() for row in rows if getattr(row, "column_name", None)}
