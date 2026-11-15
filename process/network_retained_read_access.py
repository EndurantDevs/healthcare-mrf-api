# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrator grants for an existing named retained network generation."""

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_bootstrap_read_access import BootstrapReadAccessError, _grant, _operator
from process.network_bootstrap_sources import _transaction

LEDGER = "network_retained_read_grant"
GUARD = "network_retained_read_grant_guard"


class RetainedReadAccessError(BootstrapReadAccessError):
    """A retained logical generation or its native grant is unavailable."""


async def _ledger_names(connection, ledger, guard, reader_role, manifest, relation_names, *, revoke):
    """Record this generation's grant and return the relation names other generations still need."""
    # A real row write rejects stale RR snapshots, including when the ledger was initially empty.
    # Keep guard rows after the last revoke so a recreated key cannot erase that conflict.
    await connection.execute(
        f"INSERT INTO {guard} (reader_role) VALUES ($1) ON CONFLICT (reader_role) "
        "DO UPDATE SET reader_role=EXCLUDED.reader_role",
        reader_role,
    )
    identity = (reader_role, manifest.schema_name, manifest.manifest_sha256)
    if not revoke:
        await connection.execute(
            f"INSERT INTO {ledger} (reader_role,schema_name,manifest_sha256,relation_names) "
            "VALUES ($1,$2,$3,$4::text[]) ON CONFLICT DO NOTHING",
            *identity,
            relation_names,
        )
        return set()
    await connection.execute(
        f"DELETE FROM {ledger} WHERE reader_role=$1 AND schema_name=$2 AND manifest_sha256=$3", *identity
    )
    kept = await connection.fetchval(
        f"SELECT coalesce(array_agg(DISTINCT name),'{{}}') FROM {ledger}, unnest(relation_names) AS name "
        "WHERE reader_role=$1",
        reader_role,
    )
    return set(kept)


async def _revoke_unneeded(connection, schema_name, tables, reader_role, kept):
    """Revoke only what no remaining generation of this reader still names."""
    namespace, role = _identifier(schema_name), _identifier(reader_role)
    for name in tables:
        if f"{schema_name}.{name}" not in kept:
            await connection.execute(f"REVOKE SELECT ON {namespace}.{_identifier(name)} FROM {role}")
    if not any(name.split(".", 1)[0] == schema_name for name in kept):
        await connection.execute(f"REVOKE USAGE ON SCHEMA {namespace} FROM {role}")


async def _change_read_access(connection, address_source, reader_role, control_schema, *, revoke):
    from process.network_serving_read import resolve_retained_network_address_source

    _identifier(reader_role)
    namespace = _identifier(control_schema or registry_schema())
    ledger, guard = namespace + "." + _identifier(LEDGER), namespace + "." + _identifier(GUARD)
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
            names = [f"{relation.schema_name}.{relation.table_name}" for relation in relations]
            kept = await _ledger_names(connection, ledger, guard, reader_role, manifest, names, revoke=revoke)
            for schema_name, tables in by_schema.items():
                original_role = await _operator(connection, schema_name, tables)
                if revoke:
                    await _revoke_unneeded(connection, schema_name, tables, reader_role, kept)
                else:
                    await _grant(connection, schema_name, tables, reader_role)
                await connection.execute(f"SET LOCAL ROLE {original_role}")
            return {
                "schema_name": manifest.schema_name,
                "manifest_sha256": manifest.manifest_sha256,
                "reader_role": reader_role,
            }
    except asyncpg.SerializationError:
        # The caller must retry its entire outer transaction with a fresh snapshot.
        raise
    except asyncpg.PostgresError:
        raise RetainedReadAccessError("Named retained read-access change failed") from None


async def install_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Grant USAGE and SELECT for the selected generation's actual query relations and record them."""
    return await _change_read_access(connection, address_source, reader_role, control_schema, revoke=False)


async def revoke_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Revoke this generation's grants except those another recorded generation of the reader still needs."""
    return await _change_read_access(connection, address_source, reader_role, control_schema, revoke=True)
