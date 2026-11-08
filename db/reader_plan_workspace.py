# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Empty fixed plan scratch storage on one genuine Reader connection."""

import asyncio
from contextlib import asynccontextmanager

from sqlalchemy import text


@asynccontextmanager
async def plan_reader_connection(reader):
    """Prepare no payload, then hold the same physical connection through RO reads."""
    from api.custom_import_plan_sql import prepare_plan_query_tables

    async with reader.engine.connect() as connection:
        try:
            identity = (await connection.execute(text("SELECT session_user,current_user"))).first()
            if tuple(identity) != (reader._reader_login[0], reader._reader_login[0]):
                raise RuntimeError("Reader workspace login identity differs")
            await connection.rollback()
            connection = await connection.execution_options(postgresql_readonly=False)
            async with connection.begin():
                await prepare_plan_query_tables(connection, preserve_rows=True)
            connection = await connection.execution_options(postgresql_readonly=True)
            yield connection
        finally:
            # Retire only this physical connection; its private TEMP state cannot be reused.
            invalidation = asyncio.create_task(connection.invalidate())
            try:
                await asyncio.shield(invalidation)
            except asyncio.CancelledError:
                await invalidation
                raise
