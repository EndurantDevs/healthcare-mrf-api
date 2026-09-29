# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared final-schema DDL for reference-family archive fixtures."""

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260929000000_cms_doctor_group_site.py"
_SPEC = importlib.util.spec_from_file_location("reference_family_generation_fixture_migration", _MIGRATION_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MIGRATION = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MIGRATION)


def generation_shape_check() -> str:
    previous, counts = _MIGRATION._shape_support()
    return previous._shape({**counts, "cms-doctors": 3})


async def install_source_generation_guards(connection, schema: str) -> None:
    """Apply current revision protection without changing historical fixture chains."""
    path = _MIGRATION_PATH.with_name("20260929040000_reference_source_generation_guard.py")
    spec = importlib.util.spec_from_file_location("reference_source_guard_fixture_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema

    def apply(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    await connection.run_sync(apply)


async def install_source_generation_guards_from_dsn(dsn: str, schema: str) -> None:
    """Migrate committed asyncpg fixtures through a real Alembic connection."""
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await install_source_generation_guards(connection, schema)
    finally:
        await engine.dispose()
