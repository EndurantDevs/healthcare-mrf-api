# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native binary COPY batches retain byte/row bounds, ordering and rollback."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from process.ptg_parts import ptg2_snapshot_candidates as candidates
from tests.ptg2_provider_tax_identity_postgres_support import async_database_url

COPY_CASES = (
    ("ptg2_provider_tax_identity", ("tin_key", "tin_id_128", "tin_hmac_sha256"), 2, 1000, [2, 2, 2]),
    ("ptg2_provider_tax_identity", ("tin_key", "tin_id_128", "tin_hmac_sha256"), 4096, 99, [1] * 6),
    (
        "ptg2_provider_group_tax_identity",
        ("provider_group_global_id_128", "tax_identity_state", "tin_key", "source_bitmap"),
        4096,
        140,
        [2, 2],
    ),
)


@pytest.mark.asyncio
async def test_snapshot_transfer_bounds_and_failure_rollback(monkeypatch):
    engine = create_async_engine(async_database_url())
    schema = "ptg_copy_bounds_" + uuid.uuid4().hex
    batch_counts = []
    native_window = candidates._copy_window

    async def begin(_session, _schema, table, _snapshot, _token):
        return table + "_candidate"

    async def finish(_session, _schema, _candidate, count):
        return count

    async def window(*args):
        boundary, count = await native_window(*args)
        if count:
            batch_counts.append(count)
        return boundary, count

    monkeypatch.setattr(candidates, "begin_snapshot_candidate", begin)
    monkeypatch.setattr(candidates, "finish_snapshot_candidate", finish)
    monkeypatch.setattr(candidates, "_copy_window", window)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            await _create_stages(connection, schema)
        for table, columns, row_limit, byte_limit, expected_batches in COPY_CASES:
            monkeypatch.setattr(candidates, "COPY_MAX_ROWS", row_limit)
            monkeypatch.setattr(candidates, "COPY_MAX_BYTES", byte_limit)
            batch_counts.clear()
            async with engine.begin() as connection:
                await connection.execute(text(f"TRUNCATE {schema}.{table}_candidate"))
                count = await _transfer(connection, schema, table, columns, sum(expected_batches))
                assert count == sum(expected_batches)
                assert batch_counts == expected_batches
                await _assert_equal_rows(connection, schema, table, columns)
        monkeypatch.setattr(candidates, "COPY_MAX_BYTES", 98)
        with pytest.raises(RuntimeError, match="record exceeds byte bound"):
            async with engine.begin() as connection:
                await connection.execute(text(f"TRUNCATE {schema}.ptg2_provider_tax_identity_candidate"))
                await _transfer(connection, schema, COPY_CASES[0][0], COPY_CASES[0][1], 6)
        async with engine.begin() as connection:
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity_candidate"))
                == 6
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


async def _assert_equal_rows(connection, schema, table, columns):
    fields = ",".join(columns)
    assert not await connection.scalar(
        text(f"""
        SELECT EXISTS((SELECT {fields} FROM {schema}.{table}_candidate
                       EXCEPT ALL SELECT {fields} FROM {schema}.{table}_stage)
            UNION ALL (SELECT {fields} FROM {schema}.{table}_stage
                       EXCEPT ALL SELECT {fields} FROM {schema}.{table}_candidate))
        """)
    )


async def _transfer(connection, schema, table, columns, count):
    return await candidates.copy_snapshot_candidate(
        connection,
        schema_name=schema,
        table=table,
        snapshot_key=11,
        build_token="synthetic",
        stage_table=table + "_stage",
        columns=columns,
        expected_count=count,
    )


async def _create_stages(connection, schema):
    await connection.execute(
        text(f"""CREATE TABLE {schema}.ptg2_provider_tax_identity_stage (
        tin_key integer PRIMARY KEY, tin_id_128 bytea NOT NULL, tin_hmac_sha256 bytea NOT NULL)""")
    )
    await connection.execute(
        text(f"""INSERT INTO {schema}.ptg2_provider_tax_identity_stage
        SELECT n,decode(repeat('aa',16),'hex'),decode(repeat('aa',32),'hex') FROM generate_series(0,5) n""")
    )
    await connection.execute(
        text(f"""CREATE TABLE {schema}.ptg2_provider_group_tax_identity_stage (
        provider_group_global_id_128 bytea PRIMARY KEY, tax_identity_state text NOT NULL,
        tin_key integer, source_bitmap bytea NOT NULL)""")
    )
    await connection.execute(
        text(f"""INSERT INTO {schema}.ptg2_provider_group_tax_identity_stage
        SELECT decode(lpad(n::text,32,'0'),'hex'),state,CASE WHEN n=1 THEN 0 END,decode('01','hex')
          FROM (VALUES(1,'matched_ein'),(2,'missing'),(3,'malformed'),(4,'unsupported_type')) states(n,state)""")
    )
    for table in ("ptg2_provider_tax_identity", "ptg2_provider_group_tax_identity"):
        await connection.execute(
            text(f"CREATE TABLE {schema}.{table}_candidate (snapshot_key bigint, LIKE {schema}.{table}_stage)")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["export_count", "export_bytes", "import_count", "total_count"])
async def test_snapshot_transfer_never_finishes_incomplete_copy(monkeypatch, failure):
    """Every transfer census and actual spool bound must hold before sealing."""

    async def export(*_args, output, **_kwargs):
        output.write(b"x" * (33 if failure == "export_bytes" else 21))
        return "COPY 1" if failure == "export_count" else "COPY 2"

    driver = SimpleNamespace(
        copy_from_query=AsyncMock(side_effect=export),
        copy_to_table=AsyncMock(return_value="COPY 1" if failure == "import_count" else "COPY 2"),
    )
    finish = AsyncMock()
    monkeypatch.setattr(candidates, "COPY_MAX_BYTES", 32)
    monkeypatch.setattr(candidates, "candidate_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(candidates, "begin_snapshot_candidate", AsyncMock(return_value="candidate"))
    monkeypatch.setattr(candidates, "finish_snapshot_candidate", finish)
    monkeypatch.setattr(candidates, "_copy_window", AsyncMock(side_effect=[(2, 2), (None, 0)]))

    with pytest.raises(RuntimeError, match="COPY.*census differs"):
        await _transfer(object(), "synthetic", COPY_CASES[0][0], COPY_CASES[0][1], 3 if failure == "total_count" else 2)

    finish.assert_not_awaited()
    assert driver.copy_from_query.await_count == 1
    assert driver.copy_to_table.await_count == int(failure in {"import_count", "total_count"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,records,batch_rows,error",
    [
        ("batch_size", [(1,)], 0, ValueError),
        ("field_type", [({},)], 1, TypeError),
        ("record_bytes", [(b"x" * 32,)], 1, RuntimeError),
        ("intermediate_count", [(1,), (2,)], 1, RuntimeError),
        ("final_count", [(1,)], 1, RuntimeError),
    ],
)
async def test_record_copy_rejects_invalid_stream_without_retry(monkeypatch, failure, records, batch_rows, error):
    """A failed native batch cannot be retried as a partial row insert."""
    copy = AsyncMock(return_value="COPY 0")
    driver = SimpleNamespace(copy_records_to_table=copy)
    get_driver = AsyncMock(return_value=driver)
    monkeypatch.setattr(candidates, "COPY_MAX_BYTES", 40)
    monkeypatch.setattr(candidates, "candidate_driver", get_driver)
    with pytest.raises(error):
        await candidates.copy_candidate_records(
            object(), "synthetic", "candidate", ("key",), iter(records), batch_rows=batch_rows
        )
    assert copy.await_count == int(failure in {"intermediate_count", "final_count"})
    assert get_driver.await_count == int(failure != "batch_size")
    if copy.await_count:
        assert copy.await_args.kwargs["records"] == [(1,)]


@pytest.mark.asyncio
async def test_metadata_publication_rejects_invalid_batch_before_authorization(monkeypatch):
    begin = AsyncMock()
    monkeypatch.setattr(candidates, "begin_snapshot_candidate", begin)
    with pytest.raises(ValueError, match="batch size must be positive"):
        await candidates.publish_snapshot_records(
            object(),
            schema_name="synthetic",
            table="ptg2_v4_heavy_owner",
            snapshot_key=1,
            build_token="owned",
            columns=("npi_key",),
            entries=[(1,)],
            batch_rows=0,
        )
    begin.assert_not_awaited()
