# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database table helpers for provider-quality import staging."""

from __future__ import annotations

from db.connection import db


async def _ensure_indexes(obj: type, db_schema: str) -> None:
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            + f"{obj.__tablename__}_idx_primary ON {db_schema}.{obj.__tablename__} ({cols});"
        )
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        for idx in obj.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            base_name = idx.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
            if getattr(obj, "__main_table__", obj.__tablename__) != obj.__tablename__:
                name = f"{obj.__tablename__}_{base_name}"
            else:
                name = base_name
            using = idx.get("using")
            where = idx.get("where")
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{obj.__tablename__}"
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


async def _table_exists(schema: str, table: str) -> bool:
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
