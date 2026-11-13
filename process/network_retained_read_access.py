# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrator grants for an existing named retained network generation."""

import asyncpg

from process.network_address_projection import _identifier
from process.network_bootstrap_read_access import BootstrapReadAccessError, _grant, _operator
from process.network_bootstrap_sources import _transaction


class RetainedReadAccessError(BootstrapReadAccessError):
    """A retained logical generation or its native grant is unavailable."""


async def _change_read_access(connection, address_source, reader_role, control_schema, *, revoke):
    from process.network_serving_read import resolve_retained_network_address_source

    _identifier(reader_role)
    await _transaction(connection)
    try:
        async with connection.transaction():
            manifest = await resolve_retained_network_address_source(
                connection, address_source, control_schema=control_schema
            )
            relations = tuple(dict.fromkeys((manifest.address_relation, *manifest.candidate_relation_pins)))
            by_schema = {}
            for relation in relations:
                by_schema.setdefault(relation.schema_name, []).append(relation.table_name)
            for schema_name, tables in by_schema.items():
                original_role = await _operator(connection, schema_name, tables)
                await _grant(connection, schema_name, tables, reader_role, revoke=revoke)
                await connection.execute(f"SET LOCAL ROLE {original_role}")
            return {
                "schema_name": manifest.schema_name,
                "manifest_sha256": manifest.manifest_sha256,
                "reader_role": reader_role,
            }
    except asyncpg.PostgresError:
        raise RetainedReadAccessError("Named retained read-access change failed") from None


async def install_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Grant USAGE and SELECT for the selected generation's actual query relations."""
    return await _change_read_access(connection, address_source, reader_role, control_schema, revoke=False)


async def revoke_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Revoke direct grants for the same pinned logical generation."""
    return await _change_read_access(connection, address_source, reader_role, control_schema, revoke=True)
