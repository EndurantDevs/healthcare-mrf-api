# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrator grants for schema-qualified bootstrap snapshots."""

import asyncpg

from process.network_address_projection import _identifier
from process.network_bootstrap_sources import (
    NetworkBootstrapSourceError,
    _transaction,
    verify_network_bootstrap_sources,
)


class BootstrapReadAccessError(NetworkBootstrapSourceError):
    """A named snapshot or its requested native grant is unavailable."""


async def _operator(connection, schema_name, table_names):
    """Let PostgreSQL authorize the owner role needed for a native DCL operation."""
    await _transaction(connection)
    owner = await connection.fetchval(
        "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if owner is None:
        raise BootstrapReadAccessError("Named read-access snapshot is unavailable")
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner)}")
    tables = ",".join(f"{_identifier(schema_name)}.{_identifier(name)}" for name in table_names)
    await connection.execute(f"LOCK TABLE {tables} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    return original_role


async def _grant(connection, schema_name, table_names, reader, *, revoke=False):
    action, direction = ("REVOKE", "FROM") if revoke else ("GRANT", "TO")
    namespace, role = _identifier(schema_name), _identifier(reader)
    for name in table_names:
        await connection.execute(f"{action} SELECT ON {namespace}.{_identifier(name)} {direction} {role}")
    await connection.execute(f"{action} USAGE ON SCHEMA {namespace} {direction} {role}")


async def _change_read_access(connection, receipt, reader_role, *, revoke):
    _identifier(reader_role)
    await _transaction(connection)
    try:
        async with connection.transaction():
            await verify_network_bootstrap_sources(connection, receipt)
            schema_name = receipt.address_source.schema_name
            tables = ("entity_address_unified", "npi")
            original_role = await _operator(connection, schema_name, tables)
            await _grant(connection, schema_name, tables, reader_role, revoke=revoke)
            await connection.execute(f"SET LOCAL ROLE {original_role}")
            return {
                "schema_name": schema_name,
                "generation_id": receipt.generation_sha256,
                "reader_role": reader_role,
            }
    except asyncpg.PostgresError:
        raise BootstrapReadAccessError("Named bootstrap read-access change failed") from None


async def install_bootstrap_read_access(connection, receipt, *, reader_role):
    """Grant USAGE and SELECT without changing the stored data edition."""
    return await _change_read_access(connection, receipt, reader_role, revoke=False)


async def revoke_bootstrap_read_access(connection, receipt, *, reader_role):
    """Revoke the named reader's direct grants atomically."""
    return await _change_read_access(connection, receipt, reader_role, revoke=True)
