# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused staging and batching proofs for source-local tax observations."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_tax_identity_source_observations as observations
from process.ptg_parts import ptg2_tax_identity_source_preflight as source_preflight
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from process.ptg_parts import (
    ptg2_tax_identity_source_target_preflight as target_preflight,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_tax_identity_source_artifact import (
    _ERROR,
    _prepare,
    _record,
    _sidecar,
)


def _stage_handle() -> source_stage.StagedTaxIdentitySourceProjection:
    seal_token = "a" * 32
    table_name = f"ptg2_tax_source_stage_{seal_token[:20]}"
    return source_stage.StagedTaxIdentitySourceProjection(
        table_name=table_name,
        seal_table_name=f"{table_name}_seal",
        stage_oid=11,
        seal_oid=12,
        seal_token=seal_token,
    )


@pytest.mark.parametrize(
    "source_ordinal_map",
    [
        [{"shard_id": "shard-a", "ordinal": False}],
        [{"shard_id": "shard-a", "ordinal": 0.0}],
        [{"shard_id": "shard-a", "ordinal": 0, "extra": True}],
        [
            {"shard_id": "shard-a", "ordinal": 0},
            {"shard_id": "shard-a", "ordinal": 1},
        ],
        [
            {"shard_id": "shard-b", "ordinal": 0},
            {"shard_id": "shard-a", "ordinal": 1},
        ],
    ],
    ids=("bool", "float", "extra", "duplicate", "unsorted"),
)
def test_fresh_aggregate_source_map_is_schema_strict(source_ordinal_map):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        target_preflight.normalize_source_ordinal_entries(source_ordinal_map)


def test_stage_handle_requires_exact_dataclass_and_integer_oids():
    class StagedSubclass(source_stage.StagedTaxIdentitySourceProjection):
        pass

    class IntegerSubclass(int):
        pass

    handle = _stage_handle()
    subclass_handle = StagedSubclass(
        table_name=handle.table_name,
        seal_table_name=handle.seal_table_name,
        stage_oid=handle.stage_oid,
        seal_oid=handle.seal_oid,
        seal_token=handle.seal_token,
    )

    assert source_preflight._validated_stage_handle(handle) is handle
    for invalid_handle in (
        subclass_handle,
        replace(handle, stage_oid=IntegerSubclass(handle.stage_oid)),
        replace(handle, seal_oid=IntegerSubclass(handle.seal_oid)),
    ):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_preflight._validated_stage_handle(invalid_handle)


@pytest.mark.asyncio
async def test_stage_streams_authenticated_copy_once(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="9",
        sidecar_records=(_record(1, 2),),
    )
    prepared = _prepare(tmp_path, (sidecar,))
    consumed_chunks: list[bytes] = []

    async def copy_to_table(_table_name, *, source, **_kwargs):
        while copy_chunk := source.read(11):
            consumed_chunks.append(copy_chunk)

    copy_driver = SimpleNamespace(copy_to_table=AsyncMock(side_effect=copy_to_table))
    raw_connection = SimpleNamespace(driver_connection=copy_driver)
    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=raw_connection)
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))

    await source_stage._copy_prepared_projection(
        session,
        prepared,
        stage_table="source_stage",
    )

    assert sum(map(len, consumed_chunks)) == prepared.copy_byte_count
    assert copy_driver.copy_to_table.await_count == 1
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await source_stage._copy_prepared_projection(
            session,
            prepared,
            stage_table="source_stage",
        )
    assert copy_driver.copy_to_table.await_count == 1


@pytest.mark.asyncio
async def test_stage_rejects_copy_consumer_that_stops_before_eof(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="9",
        sidecar_records=(_record(1, 2),),
    )
    prepared = _prepare(tmp_path, (sidecar,))
    copy_driver = SimpleNamespace(copy_to_table=AsyncMock(return_value=None))
    raw_connection = SimpleNamespace(driver_connection=copy_driver)
    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=raw_connection)
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await source_stage._copy_prepared_projection(
            session,
            prepared,
            stage_table="source_stage",
        )

    assert copy_driver.copy_to_table.await_count == 1


@pytest.mark.asyncio
async def test_observation_publication_uses_multiple_bounded_batches(monkeypatch):
    boundary_reader = AsyncMock(
        side_effect=[
            (2, 0, 1),
            (2, 0, 3),
            (1, 0, 4),
            None,
        ]
    )
    batch_publisher = AsyncMock()
    heartbeat = Mock()
    monkeypatch.setattr(observations, "_observation_boundary", boundary_reader)
    monkeypatch.setattr(
        observations,
        "_publish_observation_batch",
        batch_publisher,
    )

    await observations._publish_observations(
        object(),
        schema='"mrf"',
        stage='"pg_temp"."source_stage"',
        snapshot_key=17,
        prepared=SimpleNamespace(provider_group_occurrence_count=5),
        heartbeat_callback=heartbeat,
    )

    assert boundary_reader.await_count == 4
    assert batch_publisher.await_count == 3
    assert [
        awaited_call.kwargs["expected_count"]
        for awaited_call in batch_publisher.await_args_list
    ] == [2, 2, 1]
    assert [
        awaited_call.kwargs["range_parameters_by_name"]
        for awaited_call in batch_publisher.await_args_list
    ] == [
        {
            "previous_source_key": -1,
            "previous_ordinal": -1,
            "last_source_key": 0,
            "last_ordinal": 1,
        },
        {
            "previous_source_key": 0,
            "previous_ordinal": 1,
            "last_source_key": 0,
            "last_ordinal": 3,
        },
        {
            "previous_source_key": 0,
            "previous_ordinal": 3,
            "last_source_key": 0,
            "last_ordinal": 4,
        },
    ]
    assert heartbeat.call_count == 3
