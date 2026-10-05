# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply the real candidate migration to focused PostgreSQL fixture relations."""

import importlib.util
from contextlib import nullcontext
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations


async def apply_candidate_migration(connection, schema_name, tables):
    """Convert only the real relations represented by a focused test fixture."""
    path = Path(__file__).parents[1] / "alembic/versions/20261005110000_ptg_set_validation.py"
    spec = importlib.util.spec_from_file_location("ptg_candidate_fixture_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration.TABLES = tuple(tables)
    migration._schema = lambda: schema_name

    def upgrade(sync_connection):
        context = MigrationContext.configure(sync_connection)
        migration.op = Operations(context)
        # These fixture relations already belong to one setup transaction;
        # the dedicated migration tests exercise committed phases and retries.
        context.autocommit_block = nullcontext
        migration._phase = lambda **_options: nullcontext()
        migration.upgrade()

    await connection.run_sync(upgrade)
