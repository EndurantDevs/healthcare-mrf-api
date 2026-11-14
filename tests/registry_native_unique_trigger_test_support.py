# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify native deferred uniqueness with data, not catalog identity attestation."""

import asyncpg
import pytest

from process.entity_address_snapshot_receipt import _schema_name


async def verify_native_unique_trigger_predicate(connection, schema_name):
    """A deferred duplicate must fail and leave the original candidate intact."""
    schema = _schema_name(schema_name)
    table = f'"{schema}".entity_address_unified'
    before = await connection.fetchval(f"SELECT count(*) FROM {table}")
    assert before > 0
    with pytest.raises(asyncpg.UniqueViolationError):
        async with connection.transaction():
            await connection.execute("SET CONSTRAINTS ALL DEFERRED")
            await connection.execute(
                f"INSERT INTO {table} (entity_type,entity_id,location_key,checksum,type,base_address_version) "
                f"SELECT entity_type,entity_id||'_duplicate',location_key||'_duplicate',checksum,type,base_address_version "
                f"FROM {table} LIMIT 1"
            )
            await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert await connection.fetchval(f"SELECT count(*) FROM {table}") == before
