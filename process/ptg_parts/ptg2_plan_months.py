# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Parameter-bounded plan-month statements within the caller's transaction."""

from typing import Any

from db.connection import db
from db.models import PTG2PlanMonth


async def upsert_plan_month_batches(plan_month_entries: list[dict[str, Any]]) -> None:
    """Upsert bounded batches after the caller locks every snapshot fence."""

    table = PTG2PlanMonth.__table__
    # Leave margin below asyncpg's 32,767-bind limit, including omitted columns
    # that may acquire client-side defaults as the table evolves.
    batch_size = max(1, 30_000 // len(table.c))
    for start in range(0, len(plan_month_entries), batch_size):
        statement = db.insert(table).values(plan_month_entries[start : start + batch_size])
        update_values_by_column = {
            column.name: getattr(statement.excluded, column.name)
            for column in table.c
            if column.name not in PTG2PlanMonth.__my_index_elements__
        }
        statement = statement.on_conflict_do_update(
            index_elements=list(PTG2PlanMonth.__my_index_elements__),
            set_=update_values_by_column,
        )
        await statement.status()
