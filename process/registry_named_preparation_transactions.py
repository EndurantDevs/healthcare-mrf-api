# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded caller-owned metadata transactions and ordered scope locks."""

import asyncio
from functools import wraps

from process.registry_named_preparation_records import require

MUTATION_SECONDS = 3


def preparation_mutation(method):
    """Rollback each bounded metadata operation without ending the caller transaction."""

    @wraps(method)
    async def bounded(store, *args, **kwargs):
        """Finish or roll back this operation under the original caller deadline."""
        require(store.connection.is_in_transaction(), "transaction_required")
        async with asyncio.timeout(MUTATION_SECONDS), store.connection.transaction():
            await _limit_statements(store.connection)
            store.authority = {
                "owner_role": store.roles.owner_role,
                "publisher_role": store.roles.publisher_role,
                "runtime_roles": store.roles.runtime_roles,
            }
            return await method(store, *args, **kwargs)

    return bounded


async def _limit_statements(connection):
    await connection.execute(
        """SELECT pg_catalog.set_config(name,CASE
        WHEN name = 'jit' THEN value
        WHEN pg_catalog.current_setting(name)::interval = interval '0'
          OR pg_catalog.current_setting(name)::interval > value::interval
        THEN value ELSE pg_catalog.current_setting(name) END,true)
        FROM (VALUES ('jit','off'),('lock_timeout','250ms'),('statement_timeout','2500ms'))
          AS limits(name,value)"""
    )


async def lock_scopes(connection, scopes):
    """Acquire sorted transaction scopes without waiting for another owner."""
    for scope in sorted(set(scopes)):
        require(
            await connection.fetchval(
                "SELECT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended($1,0))",
                "snapshot-retention:" + scope,
            ),
            "busy",
        )
