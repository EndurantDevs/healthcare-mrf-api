from __future__ import annotations

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_manifest_publish
from process.ptg_parts.domain import PTG2FileProcessResult
from process.ptg_parts.ptg2_shared_publish import (
    _SHARED_BLOCK_STAGE_COLUMNS,
    _upsert_shared_block_mappings,
    create_shared_block_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    shared_block_stage_name,
)
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_reuse import SharedPhysicalArtifactIdentity
from process.ptg_parts.ptg2_shared_finalize import PTG2_V3_SERVING_RUN_RECORD_BYTES

process_ptg = importlib.import_module("process.ptg")
from tests.ptg2_shared_publish_test_support import (
    _FirstBatchProgress,
    _OneRowResult,
    _RowsResult,
    _SlowSharedBlockSQLDriver,
    _SlowV4CASSQLDriver,
    _assert_shared_stage_sql,
    _assert_slow_shared_block_publication,
    _assert_slow_v4_cas_publication,
    _bounded_stage_session,
    _copy_connection,
    _dictionary_summary,
    _finalizer_contract,
    _provider_set_metadata_entries,
    _serving_run_entries,
    _session_transaction,
    _unannotated_file_result,
)


@pytest.mark.asyncio
async def test_shared_block_stage_returns_only_bounded_sql_aggregates(monkeypatch):
    """The publication result is built only from bounded SQL aggregates."""

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_OneRowResult((1,))),
        scalar=AsyncMock(),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_shared_layout_for_dense_write",
        AsyncMock(),
    )
    expected = ptg2_shared_publish.SharedBlockStagePublication(
        object_kinds=("a_kind", "z_kind"),
        mapping_count=3,
        unique_block_count=2,
        logical_byte_count=30,
        stored_byte_count=20,
    )
    publish_batched = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "prepare_shared_cas_block_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_shared_block_stage_batched",
        publish_batched,
    )
    delete_pins = AsyncMock(return_value=2)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "delete_shared_block_build_pins",
        delete_pins,
    )

    publication = await publish_shared_block_stage(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        build_token="build-42",
    )
    _assert_bounded_stage_publication(
        publication,
        session,
        publish_batched,
        delete_pins,
    )


@pytest.mark.asyncio
async def test_shared_block_stage_prepares_hash_access_before_scan_and_cleans_failure(
    monkeypatch,
):
    events = []

    async def record_status(statement):
        events.append(str(statement).strip())

    async def fail_scan(*_args, **_kwargs):
        events.append("scan")
        raise RuntimeError("scan failed")

    monkeypatch.setattr(ptg2_shared_publish.db, "status", record_status)
    monkeypatch.setattr(ptg2_shared_publish.db, "all", fail_scan)

    with pytest.raises(RuntimeError, match="scan failed"):
        await publish_shared_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_proof",
            snapshot_key=42,
            build_token="build-42",
        )

    index_name = ptg2_shared_publish._ptg2_snapshot_index_name(
        "ptg2_v3_block_stage_proof",
        "block_hash_idx",
    )
    assert events == [
        f'CREATE INDEX IF NOT EXISTS "{index_name}" '
        'ON "mrf"."ptg2_v3_block_stage_proof" (block_hash) '
        'WITH (fillfactor = 100);',
        'ANALYZE "mrf"."ptg2_v3_block_stage_proof" (block_hash);',
        "scan",
        'DROP TABLE IF EXISTS "mrf"."ptg2_v3_block_stage_proof";',
    ]


@pytest.mark.asyncio
async def test_shared_block_stage_releases_hash_index_after_cas_preparation(
    monkeypatch,
):
    status = AsyncMock()
    select_rows = AsyncMock(side_effect=([], []))
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)
    monkeypatch.setattr(ptg2_shared_publish.db, "all", select_rows)

    await ptg2_shared_publish.prepare_shared_cas_block_stage(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        build_token="build-42",
        expected_generation="shared_blocks_v3",
    )

    index_name = ptg2_shared_publish._ptg2_snapshot_index_name(
        "ptg2_v3_block_stage_proof",
        "block_hash_idx",
    )
    statements = [str(call.args[0]).strip() for call in status.await_args_list]
    assert statements[-1] == f'DROP INDEX IF EXISTS "mrf"."{index_name}";'
    assert select_rows.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("last_hash", (None, b"a" * 32))
async def test_v4_hash_cursor_uses_an_explicit_later_page_range(monkeypatch, last_hash):
    select_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(ptg2_shared_publish.db, "all", select_rows)

    await ptg2_shared_publish._v4_stage_hash_batch(
        schema='"mrf"',
        stage='"ptg2_v3_block_stage_proof"',
        last_hash=last_hash,
    )

    statement = str(select_rows.await_args.args[0])
    assert "NOT :has_last_hash" not in statement
    assert ("WHERE staged.block_hash > :last_hash" in statement) == (
        last_hash is not None
    )
    assert select_rows.await_args.kwargs == {
        "batch_rows": ptg2_shared_publish._SHARED_BLOCK_PUBLISH_BATCH_ROWS,
        **({"last_hash": last_hash} if last_hash is not None else {}),
    }


def _assert_bounded_stage_publication(
    publication,
    session,
    publish_batched,
    delete_pins,
) -> None:
    """Assert bounded aggregate projection and its exact SQL calls."""

    assert publication.object_kinds == ("a_kind", "z_kind")
    assert publication.mapping_count == 3
    assert publication.unique_block_count == 2
    assert publication.logical_byte_count == 30
    assert publication.stored_byte_count == 20
    session.scalar.assert_not_awaited()
    publish_batched.assert_awaited_once_with(
        session,
        schema='"mrf"',
        stage='"ptg2_v3_block_stage_proof"',
        snapshot_key=42,
        progress_callback=ptg2_shared_publish._discard_publish_work,
    )
    delete_pins.assert_awaited_once()


@pytest.mark.asyncio
async def test_slow_sql_stage_reports_exact_bounded_rows_before_completion(
    monkeypatch,
):
    """A blocked later batch cannot hide completed, measured SQL work."""

    sql_driver = _SlowSharedBlockSQLDriver()
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=sql_driver.execute_stage_statement),
        scalar=AsyncMock(side_effect=sql_driver.read_identity_count),
    )
    monkeypatch.setattr(
        ptg2_shared_publish.db,
        "transaction",
        lambda: _session_transaction(session),
    )
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_shared_layout_for_dense_write",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "prepare_shared_cas_block_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "delete_shared_block_build_pins",
        AsyncMock(return_value=4_101),
    )
    progress_capture = _FirstBatchProgress()
    publish_task = asyncio.create_task(
        publish_shared_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_slow_sql",
            snapshot_key=42,
            build_token="build-42",
            progress_callback=progress_capture,
        )
    )
    await asyncio.wait_for(
        progress_capture.first_batch_reported.wait(),
        timeout=1.0,
    )

    assert not publish_task.done()
    assert progress_capture.events == [
        ("sql_stage_rows", 4_096),
        ("publish_batches", 1),
    ]
    sql_driver.release_second_batch.set()
    publication = await publish_task
    _assert_slow_shared_block_publication(
        publication,
        progress_capture.events,
        session,
    )


def test_batch_validation_is_driven_by_bounded_point_probes():
    """Each batch stays bounded even when durable tables are very large."""

    validation_sql = ptg2_shared_publish._BATCH_AGGREGATE_SQL
    assert "FROM {batch} AS batch" in validation_sql
    assert "WHERE candidate.ctid = batch.row_ctid" in validation_sql
    assert "WHERE candidate.block_hash = staged.block_hash" in validation_sql
    assert "WHERE candidate.snapshot_key = :snapshot_key" in validation_sql
    assert "candidate.object_kind = staged.object_kind" in validation_sql
    assert "candidate.block_key = staged.block_key" in validation_sql
    assert "candidate.fragment_no = staged.fragment_no" in validation_sql
    assert validation_sql.count("JOIN LATERAL") == 3
    assert validation_sql.count("LIMIT 1") == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("with_progress", (False, True))
async def test_v4_cas_session_helper_is_read_only_for_both_paths(
    monkeypatch,
    with_progress,
):
    """Callback-free and reporting paths do not mutate shared lock state."""

    publication = object()
    publish_batched = AsyncMock(return_value=publication)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_v4_cas_stage_batched",
        publish_batched,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value="2750ms"),
        execute=AsyncMock(),
    )
    progress_callback = AsyncMock() if with_progress else None

    publication_result = (
        await ptg2_shared_publish._publish_v4_cas_block_stage_in_session(
            session,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_v4proof",
            progress_callback=progress_callback,
        )
    )

    assert publication_result is publication
    publish_batched.assert_awaited_once_with(
        session,
        schema='"mrf"',
        stage='"ptg2_v3_block_stage_v4proof"',
        progress_callback=(
            progress_callback
            if progress_callback is not None
            else ptg2_shared_publish._discard_publish_work
        ),
    )
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_v4_cas_publisher_fails_closed_without_db_work(
    monkeypatch,
):
    """Older imports receive a safe error instead of a partial CAS commit."""

    transaction_mock = Mock()
    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction_mock)

    with pytest.raises(RuntimeError, match="complete atomic V4 graph"):
        await ptg2_shared_publish.publish_v4_cas_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_retired",
            snapshot_key=17,
            build_token="retired-contract",
        )

    transaction_mock.assert_not_called()


@pytest.mark.asyncio
async def test_v4_cas_session_helper_preserves_publication_failure(monkeypatch):
    """A failed transaction body is rolled back without a masking restore."""

    publish_batched = AsyncMock(side_effect=RuntimeError("CAS failed"))
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_v4_cas_stage_batched",
        publish_batched,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value="4s"),
        execute=AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="CAS failed"):
        await ptg2_shared_publish._publish_v4_cas_block_stage_in_session(
            session,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_v4proof",
        )

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_slow_v4_cas_sql_reports_exact_batches_before_completion(
    monkeypatch,
):
    """V4 CAS publication exposes completed rows while a later batch waits."""

    sql_driver = _SlowV4CASSQLDriver()
    session = SimpleNamespace(
        scalar=AsyncMock(return_value="0"),
        execute=AsyncMock(side_effect=sql_driver.execute_stage_statement),
    )
    progress_capture = _FirstBatchProgress()
    publish_task = asyncio.create_task(
        ptg2_shared_publish._publish_v4_cas_block_stage_in_session(
            session,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_v4_slow_sql",
            progress_callback=progress_capture,
        )
    )
    await asyncio.wait_for(
        progress_capture.first_batch_reported.wait(),
        timeout=1.0,
    )

    assert not publish_task.done()
    assert progress_capture.events == [
        ("sql_stage_rows", 4_096),
        ("publish_batches", 1),
    ]
    sql_driver.release_second_batch.set()
    publication = await publish_task
    _assert_slow_v4_cas_publication(
        publication,
        progress_capture.events,
    )


@pytest.mark.asyncio
async def test_shared_block_stage_rejects_incompatible_version_in_combined_scan(
    monkeypatch,
):
    with pytest.raises(RuntimeError, match="incompatible format version"):
        ptg2_shared_publish._validate_shared_block_batch(
            (1, 1, 3, 3, ["serving"], True, False, True)
        )

@pytest.mark.asyncio
async def test_shared_block_mapping_upsert_counts_stage_when_not_supplied():
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=3),
        execute=AsyncMock(return_value=_OneRowResult((3,), rowcount=3)),
    )

    await _upsert_shared_block_mappings(
        session,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
    )

    session.scalar.assert_awaited_once()
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_shared_block_mapping_upsert_rejects_negative_expected_count():
    session = SimpleNamespace(scalar=AsyncMock(), execute=AsyncMock())

    with pytest.raises(RuntimeError, match="mapping count is invalid"):
        await _upsert_shared_block_mappings(
            session,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_proof",
            snapshot_key=42,
            expected_count=-1,
        )

    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("summary", "message"),
    (
        (
            ptg2_shared_publish._BatchedBlockStageSummary(
                mapping_count=1,
                unique_block_count=2,
                coordinate_count=1,
                logical_byte_count=3,
                stored_byte_count=3,
                object_kinds={"serving"},
            ),
            "invalid aggregates",
        ),
        (
            ptg2_shared_publish._BatchedBlockStageSummary(
                mapping_count=2,
                unique_block_count=2,
                coordinate_count=1,
                logical_byte_count=3,
                stored_byte_count=3,
                object_kinds={"serving"},
            ),
            "mapping conflicts",
        ),
    ),
    ids=("invalid-counts", "coordinate-mismatch"),
)
async def test_shared_block_stage_rejects_invalid_aggregate_proof(
    monkeypatch,
    summary,
    message,
):
    with pytest.raises(RuntimeError, match=message):
        ptg2_shared_publish._validated_batched_stage_publication(summary)
