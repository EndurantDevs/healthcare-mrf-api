# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)

from tests.ptg2_shared_snapshot_dictionary_test_support import (
    _DenseDictionaryRangeDriver,
    _SlowProgressAcquire,
    _SlowProgressDriver,
    _assert_dense_dictionary_range_statements,
    _assert_shared_publish_cadence,
    _dense_dictionary_progress_stage,
    _install_shared_publish_progress_capture,
    _publish_progress_to_live,
    _row_result,
)
from tests.ptg2_shared_snapshot_atomic_test_support import (
    _AtomicSourceResult,
    _AtomicSourceSession,
    _AtomicSourceTransactionFixture,
    _atomic_source_transaction_fixture,
    _failed_tax_stage_compilation,
    _install_atomic_source_transaction_mocks,
    _tax_stage_contract,
)

@pytest.mark.asyncio
async def test_v4_source_projection_shares_atomic_graph_transaction(
    monkeypatch,
    tmp_path,
) -> None:
    """Merged and source-local tax rows must roll back as one graph bundle."""

    atomic_fixture = _atomic_source_transaction_fixture()
    _install_atomic_source_transaction_mocks(monkeypatch, atomic_fixture)

    with pytest.raises(RuntimeError, match="post-source graph failure"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=44,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
            tax_identity_source_artifacts=({},),
        )

    assert atomic_fixture.session.candidate_read_calls == [{"snapshot": 44, "token": "exact-build"}]
    assert atomic_fixture.session.info == {}
    shared_snapshot_publish.attach_snapshot_candidates.assert_not_awaited()
    assert atomic_fixture.publication_events == [
        ("begin", atomic_fixture.session),
        ("commit", atomic_fixture.session),
        ("begin", atomic_fixture.session),
        ("source-stage", atomic_fixture.session),
        ("physical-layout-lock", atomic_fixture.session),
        ("merged-tax-groups", atomic_fixture.session),
        ("source-local-tax", atomic_fixture.session),
        ("rollback", atomic_fixture.session),
        ("cleanup", None),
    ]


@pytest.mark.asyncio
async def test_v4_tax_group_lookup_is_bounded_and_heartbeated(monkeypatch):
    """The candidate COPY lane keeps heartbeats without per-row database reads."""

    clock_seconds = [0.0]
    snapshots = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "tax relation proof",
        lambda _stage, counters: snapshots.append(dict(counters)),
        clock=lambda: clock_seconds[0],
    )
    progress.add("validated_dictionary_rows", 4)
    progress.flush()

    async def await_statement(statement_awaitable, **kwargs):
        result = await statement_awaitable
        clock_seconds[0] = 4.0
        kwargs["heartbeat_callback"]()
        return result, 5.0

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_row_result(())),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_await_v4_dictionary_statement",
        await_statement,
    )

    copy_mock = AsyncMock(return_value=4)
    monkeypatch.setattr(shared_snapshot_publish, "copy_snapshot_candidate", copy_mock)
    await shared_snapshot_publish._publish_v4_tax_group_ranges(
        session, schema='"mrf"', snapshot_key=7, stage_table="group_tax_stage",
        expected_count=4, build_token="owned", progress_callback=None,
        heartbeat_callback=progress.heartbeat,
    )
    copy_mock.assert_awaited_once()
    assert copy_mock.await_args.kwargs["expected_count"] == 4
    session.execute.assert_not_awaited()
    assert snapshots == [
        {"validated_dictionary_rows": 4},
        {"validated_dictionary_rows": 4},
    ]


def test_v4_tax_group_digest_preserves_canonical_encoding():
    """Canonical digest bytes remain unchanged after moving semantics to SQL."""
    digest = hashlib.sha256()
    shared_snapshot_publish._append_v4_tax_group_digest(digest,
        group_id=b"g"*16, tax_state="matched_ein", tin_key=0, source_bitmap=b"\x01")
    expected = b"g"*16 + b"\x01\x01" + struct.pack(">I",0) + struct.pack(">I",1) + b"\x01"
    assert digest.digest() == hashlib.sha256(expected).digest()


@pytest.mark.asyncio
async def test_v4_tax_stages_are_removed_after_transaction_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """A fenced publication failure must not strand token-bearing stages."""
    status_mock = _install_failing_tax_publication(monkeypatch)
    with pytest.raises(RuntimeError, match="fenced publication"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=41,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )
    cleanup_statement = str(status_mock.await_args.args[0])
    assert "DROP TABLE IF EXISTS" in cleanup_statement
    assert "ptg2_v4_tax_identity_stage_" in cleanup_statement
    assert "ptg2_v4_group_tax_identity_stage_" in cleanup_statement


def _install_failing_tax_publication(monkeypatch):
    """Install a fenced V4 tax publication and return cleanup observer."""

    session = _AtomicSourceSession()

    @asynccontextmanager
    async def transaction():
        yield session

    status_mock = AsyncMock()
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "transaction",
        transaction,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_v4_cas_block_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(side_effect=RuntimeError("fenced publication")),
    )
    return status_mock


@pytest.mark.asyncio
async def test_v4_partial_stage_creation_is_removed(
    monkeypatch,
    tmp_path,
) -> None:
    """A failed CREATE sequence removes every randomized stage name."""

    status_mock = AsyncMock(
        side_effect=(
            None,
            None,
            None,
            None,
            None,
            RuntimeError("stage create failed"),
            None,
        )
    )
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )

    with pytest.raises(RuntimeError, match="stage create failed"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=42,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    cleanup_statement = str(status_mock.await_args.args[0])
    assert "DROP TABLE IF EXISTS" in cleanup_statement
    assert "ptg2_v4_tax_identity_stage_" in cleanup_statement


@pytest.mark.asyncio
async def test_v4_first_stage_creation_failure_preserves_original_error(
    monkeypatch,
    tmp_path,
) -> None:
    """No invalid empty DROP may mask failure of the first stage CREATE."""

    status_mock = AsyncMock(side_effect=RuntimeError("first stage create failed"))
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )

    with pytest.raises(RuntimeError, match="first stage create failed"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=43,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    status_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_v4_dictionary_cleanup_preserves_primary_error_and_releases_copy(
    monkeypatch,
) -> None:
    """A failed stage drop must not mask publication failure or skip file cleanup."""

    drop_stages = AsyncMock(side_effect=RuntimeError("stage drop failed"))
    prepared = SimpleNamespace(cleanup=Mock())
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_drop_v4_dictionary_stages",
        drop_stages,
    )

    await shared_snapshot_publish._cleanup_v4_dictionary_attempt(
        schema='"mrf"',
        stages=("stage-a",),
        prepared_tax_identity_source=prepared,
        preserve_primary_error=True,
    )

    prepared.cleanup.assert_called_once_with()


@pytest.mark.asyncio
async def test_v4_dictionary_cleanup_reports_drop_error_after_success(
    monkeypatch,
) -> None:
    """A stage-drop failure remains visible when no publication error exists."""

    drop_stages = AsyncMock(side_effect=RuntimeError("stage drop failed"))
    prepared = SimpleNamespace(cleanup=Mock())
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_drop_v4_dictionary_stages",
        drop_stages,
    )

    with pytest.raises(RuntimeError, match="stage drop failed"):
        await shared_snapshot_publish._cleanup_v4_dictionary_attempt(
            schema='"mrf"',
            stages=("stage-a",),
            prepared_tax_identity_source=prepared,
            preserve_primary_error=False,
        )

    prepared.cleanup.assert_called_once_with()
