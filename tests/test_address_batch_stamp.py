# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared stamping uses native fields without changing SQL fallback or update scope."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ext import address_batch_stamp as stamp
from process.ext import address_canon

FIELDS = {"first_line": "line_one", "second_line": "line_two", "city": "city", "state": "state", "zip": "zip"}


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [None, '"pg_temp"."prepared"'])
@pytest.mark.parametrize("update_existing", [False, True])
async def test_native_shard_preserves_scope_and_nullable_result(monkeypatch, prepared, update_existing):
    @asynccontextmanager
    async def staging(database, schema, name, source_sql, **options):
        assert schema == "data" and name.startswith("address_stamp_")
        assert options["temporary"] is True
        assert "ctid" not in source_sql.split("FROM")[0]
        assert "hashtext(ctid::text)" in source_sql
        assert ("address_key IS NULL" in source_sql) is (not update_existing)
        yield prepared

    database = SimpleNamespace(status=AsyncMock(return_value=4))
    monkeypatch.setattr(stamp.address_batch_stage, "staged_address_fields", staging)
    updated_count = await stamp.stamp_address_key_shard(
        database,
        1,
        schema="data",
        staging_table="input_rows",
        field_map=FIELDS,
        shards=3,
        update_existing=update_existing,
    )
    if prepared is None:
        assert updated_count is None
        database.status.assert_not_awaited()
    else:
        assert updated_count == 4
        statement = database.status.await_args.args[0]
        assert 'LEFT JOIN "pg_temp"."prepared"' in statement
        assert '"prepared".native_fields IS NOT NULL' in statement
        assert 'ELSE "data".addr_key_v1(source.first_line' in statement
        assert "COALESCE(" not in statement
        assert "target.ctid=stamped.target_row" in statement
        assert ("target.address_key IS DISTINCT FROM" in statement) is update_existing
        assert database.status.await_args.kwargs == {"shard": 1, "shards": 3}


@pytest.mark.asyncio
async def test_native_zero_count_does_not_repeat_sql(monkeypatch):
    native = AsyncMock(return_value=0)
    monkeypatch.setattr(stamp, "stamp_address_key_shard", native)
    database = SimpleNamespace(status=AsyncMock(side_effect=AssertionError("unexpected SQL fallback")))
    monkeypatch.setattr(address_canon, "db", database)
    assert (
        await address_canon._stamp_address_key_shard(
            0, schema="data", staging_table="input_rows", field_map=FIELDS, shards=1, update_existing=True
        )
        == 0
    )
    native.assert_awaited_once()
