# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused staging and batching proofs for source-local tax observations."""

from __future__ import annotations

import hashlib
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_tax_identity_source_observations as observations
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_tax_identity_source_artifact import (
    _ERROR,
    _prepare,
    _record,
    _sidecar,
)


@pytest.mark.asyncio
async def test_stage_rejects_same_content_copy_replacement(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="9",
        sidecar_records=(_record(1, 2),),
    )
    prepared = _prepare(tmp_path, (sidecar,))
    copy_bytes = prepared.copy_path.read_bytes()
    replacement_path = tmp_path / "replacement.copy"
    replacement_path.write_bytes(copy_bytes)
    os.utime(
        replacement_path,
        ns=(prepared.copy_mtime_ns, prepared.copy_mtime_ns),
    )
    os.replace(replacement_path, prepared.copy_path)
    replacement_metadata = prepared.copy_path.stat()
    assert hashlib.sha256(copy_bytes).hexdigest() == prepared.copy_sha256
    assert replacement_metadata.st_size == prepared.copy_byte_count
    assert replacement_metadata.st_mtime_ns == prepared.copy_mtime_ns
    assert replacement_metadata.st_ino != prepared.copy_inode
    session = SimpleNamespace(connection=AsyncMock())

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await source_stage._copy_prepared_projection(
            session,
            prepared,
            stage_table="source_stage",
        )

    session.connection.assert_not_awaited()
    prepared.cleanup()


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
