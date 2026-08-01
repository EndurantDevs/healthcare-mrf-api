# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Loader shared by capacity-v2 migration unit and PostgreSQL tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path


CAPACITY_V2_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260801130000_provider_directory_capacity_lease_v2.py"
)


def load_capacity_v2_migration():
    """Load the unique capacity-v2 descendant without Alembic side effects."""

    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_capacity_lease_v2_test",
        CAPACITY_V2_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def capacity_constraint_definition(database, schema: str) -> str:
    """Return the exact live capacity-ledger CHECK definition."""

    return str(
        await database.scalar(
            """
            SELECT pg_get_constraintdef(constraint_row.oid, true)
              FROM pg_constraint AS constraint_row
              JOIN pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = :schema
               AND relation.relname =
                   'provider_directory_profile_capacity_lease_consumption'
               AND constraint_row.conname =
                   'pd_profile_capacity_consumption_values_check';
            """,
            schema=schema,
        )
    )
