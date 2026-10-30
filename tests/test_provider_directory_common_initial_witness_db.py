# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native read-session component proof; actual deployed Source grants remain a separate gate."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from process import provider_directory_common_initial_witness as witness
from tests.test_provider_directory_ordinary_row_retention_db import _native_owner


@pytest.mark.asyncio
@pytest.mark.parametrize("payload_type", ["json", "jsonb"])
async def test_native_snapshot_keeps_actual_actor_and_json_type(monkeypatch, payload_type):
    async with _native_owner(monkeypatch) as (_prepared, database, schema):
        await database.status(
            f'CREATE TABLE "{schema}".source_rows(npi bigint PRIMARY KEY,payload {payload_type} NOT NULL,CHECK(npi>0))'
        )
        await database.status(
            f'INSERT INTO "{schema}".source_rows VALUES(1,CAST(:payload AS {payload_type}))',
            payload='{ "a": [1, 2], "x": true }',
        )
        fhir = SimpleNamespace(db=database)
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with witness._read_transaction(fhir) as observed:
                actual_by_field = dict(await observed.driver.fetchrow(witness._ACTOR_SQL))
                assert observed.actor == actual_by_field
                native = (await witness._catalog(observed, schema, ("source_rows",), actual_by_field["owner"]))[
                    "source_rows"
                ]
                assert native["columns"][1]["type"] == payload_type
                assert await observed.driver.fetchval("SHOW transaction_read_only") == "on"
                assert await observed.driver.fetchval(f'SELECT count(*) FROM "{schema}".source_rows') == 1


@pytest.mark.asyncio
async def test_native_rc_requires_all_ten_real_freeze_locks(monkeypatch):
    async with _native_owner(monkeypatch) as (_prepared, database, schema):
        for name in witness.common.documents.FAMILIES.values():
            await database.status(f'CREATE TABLE "{schema}"."{name}"(identity bigint PRIMARY KEY)')
        protocol = AsyncMock()
        monkeypatch.setattr(witness.receipts, "lock_metadata", protocol)
        async with database.transaction():
            async with witness._read_transaction(SimpleNamespace(db=database)) as observed:
                assert observed.isolation == "read committed"
                with pytest.raises(ValueError, match="preparation_lock_required"):
                    await witness._lock_read_committed(observed, schema)
        protocol.assert_not_awaited()
