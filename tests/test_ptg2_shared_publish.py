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
async def test_strict_v3_stage_creates_only_price_inputs(monkeypatch):
    status = AsyncMock()
    register_stages = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(ptg2_manifest_publish.db, "status", status)
    monkeypatch.setattr(
        ptg2_manifest_publish,
        "register_attempt_stage_tables",
        register_stages,
    )

    stage_table = await ptg2_manifest_publish._create_serving_stage_table(
        "strict-run",
        snapshot_id="strict-v3-snapshot",
        internal_run_id="strict-v3-run",
        storage_generation="shared_blocks_v3",
    )

    register_stages.assert_not_awaited()
    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert stage_table == "ptg2_manifest_stage_serving_strict_run"
    assert "ptg2_manifest_stage_price_atom_strict_run" in statements
    assert "ptg2_manifest_stage_price_set_atom_strict_run" in statements
    assert "ptg2_manifest_stage_price_set_summary_strict_run" in statements
    assert "minimum_negotiated_rate numeric NOT NULL" in statements
    for retired_kind in (
        "provider_group_member",
        "provider_npi_scope",
        "code_count",
        "provider_set_dictionary",
    ):
        assert retired_kind not in statements
    assert "CREATE UNLOGGED TABLE \"mrf\".\"ptg2_manifest_stage_serving_strict_run\"" not in statements


@pytest.mark.asyncio
async def test_v4_stage_registers_all_physical_tables(monkeypatch):
    status = AsyncMock()
    register_stages = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(ptg2_manifest_publish.db, "status", status)
    monkeypatch.setattr(
        ptg2_manifest_publish,
        "register_attempt_stage_tables",
        register_stages,
    )

    stage_table = await ptg2_manifest_publish._create_serving_stage_table(
        "v4-run",
        snapshot_id="v4-snapshot",
        internal_run_id="v4-run",
        storage_generation="shared_blocks_v4",
    )

    register_stages.assert_awaited_once_with(
        ptg2_manifest_publish.db,
        schema_name="mrf",
        snapshot_id="v4-snapshot",
        internal_run_id="v4-run",
        table_names=[
            stage_table,
            "ptg2_manifest_stage_price_atom_v4_run",
            "ptg2_manifest_stage_price_set_atom_v4_run",
            "ptg2_manifest_stage_price_set_summary_v4_run",
        ],
    )


@pytest.mark.asyncio
async def test_strict_v3_precopy_loads_only_price_inputs(tmp_path, monkeypatch):
    copy_files_by_kind = {}
    for kind in (
        "manifest_lean_serving",
        "price_atom",
        "price_set_atom",
        "price_set_summary",
        "provider_group_member",
        "provider_npi_scope",
        "code_count",
        "provider_set_dictionary",
    ):
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(b"copy")
        copy_files_by_kind[kind] = [{"path": str(path), "row_count": 3}]
    price_atom_copy = AsyncMock()
    price_set_atom_copy = AsyncMock()
    price_set_summary_copy = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(process_ptg, "_copy_price_atom_file", price_atom_copy)
    monkeypatch.setattr(process_ptg, "_copy_price_atom_member_file", price_set_atom_copy)
    monkeypatch.setattr(
        process_ptg,
        "_copy_price_set_summary_file",
        price_set_summary_copy,
    )

    metrics = await process_ptg._merge_ptg2_manifest_files(
        successful_files=[
            {"summary": {"manifest": {"copy_files": copy_files_by_kind}}}
        ],
        manifest_stage_table="ptg2_manifest_stage_serving_strict",
    )

    assert metrics["strict_v3_price_only"] is True
    assert set(metrics["kinds"]) == {
        "price_atom",
        "price_set_atom",
        "price_set_summary",
    }
    assert metrics["source_files_by_kind"]["price_set_summary"] == 1
    price_atom_copy.assert_awaited_once()
    price_set_atom_copy.assert_awaited_once()
    price_set_summary_copy.assert_awaited_once()
    assert not hasattr(process_ptg, "_copy_lean_manifest_serving_file")
    assert not hasattr(process_ptg, "_copy_provider_group_member_file")
    assert not (tmp_path / "price_atom.copy").exists()
    assert not (tmp_path / "price_set_atom.copy").exists()
    assert not (tmp_path / "price_set_summary.copy").exists()
    assert (tmp_path / "manifest_lean_serving.copy").exists()


@pytest.mark.asyncio
async def test_strict_v3_precopy_missing_kind_still_cleans_present_price_files(
    tmp_path,
    monkeypatch,
):
    price_atom_path = tmp_path / "price-atom.copy"
    price_set_atom_path = tmp_path / "price-set-atom.copy"
    price_atom_path.write_bytes(b"atom")
    price_set_atom_path.write_bytes(b"membership")
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")

    with pytest.raises(RuntimeError, match="price_set_summary"):
        await process_ptg._merge_ptg2_manifest_files(
            successful_files=[
                {
                    "summary": {
                        "manifest": {
                            "copy_files": {
                                "price_atom": [
                                    {"path": str(price_atom_path), "row_count": 1}
                                ],
                                "price_set_atom": [
                                    {"path": str(price_set_atom_path), "row_count": 1}
                                ],
                            }
                        }
                    }
                }
            ],
            manifest_stage_table="ptg2_manifest_stage_serving_strict",
        )

    assert not price_atom_path.exists()
    assert not price_set_atom_path.exists()


def test_strict_v3_pending_cleanup_registers_price_copy_artifacts(tmp_path):
    copy_entries_by_kind = {}
    for kind in (
        "serving_run",
        "serving_code_dictionary",
        "source_audit_witness",
        "provider_set_metadata",
        "price_atom",
        "price_set_atom",
        "price_set_summary",
    ):
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(b"scratch")
        copy_entries_by_kind[kind] = [{"path": str(path), "row_count": 1}]

    entries = process_ptg._pending_strict_v3_copy_entries(
        [{"summary": {"manifest": {"copy_files": copy_entries_by_kind}}}]
    )

    assert set(entries) == set(copy_entries_by_kind)
    process_ptg._cleanup_manifest_copy_entries(entries)
    assert not any(tmp_path.iterdir())


def test_shared_block_stage_name_is_bounded_and_identifier_safe():
    assert shared_block_stage_name("Run_ABC-123") == "ptg2_v3_block_stage_runabc123"
    generated = shared_block_stage_name("---")
    assert generated.startswith("ptg2_v3_block_stage_")
    assert len(generated) <= 41


def test_shared_block_binary_copy_contract_is_explicit_and_stable():
    assert _SHARED_BLOCK_STAGE_COLUMNS == (
        "block_hash",
        "format_version",
        "object_kind",
        "block_key",
        "fragment_no",
        "entry_count",
        "codec",
        "raw_byte_count",
        "stored_byte_count",
        "payload",
    )


@pytest.mark.asyncio
async def test_shared_block_stage_allows_metadata_only_reused_rows(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)

    await create_shared_block_stage(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
    )

    ddl = status.await_args_list[1].args[0]
    assert "payload bytea CHECK" in ddl
    assert "payload IS NULL OR octet_length(payload) = stored_byte_count" in ddl


@pytest.mark.asyncio
async def test_shared_block_mapping_upsert_combines_insert_and_conflict_check():
    session = SimpleNamespace(
        scalar=AsyncMock(),
        execute=AsyncMock(return_value=_OneRowResult((11,), rowcount=11)),
    )

    await _upsert_shared_block_mappings(
        session,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        expected_count=11,
    )

    statement = str(session.execute.await_args.args[0])
    assert "canonical_mapping" not in statement
    assert "applied_mapping" not in statement
    assert "ON CONFLICT (snapshot_key, object_kind, block_key, fragment_no)" in statement
    assert "DO NOTHING" in statement
    assert "DO UPDATE" not in statement
    assert 'FROM "mrf"."ptg2_v3_block_stage_proof"' in statement
    assert session.execute.await_args.args[1] == {"snapshot_key": 42}
    session.scalar.assert_not_awaited()
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_shared_block_mapping_upsert_rejects_conflicting_existing_mapping():
    session = SimpleNamespace(
        scalar=AsyncMock(),
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((10,), rowcount=10),
                _OneRowResult((10,)),
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="mapping conflicts"):
        await _upsert_shared_block_mappings(
            session,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_proof",
            snapshot_key=42,
            expected_count=11,
        )
    reconciliation_sql = str(session.execute.await_args_list[-1].args[0])
    assert "canonical_mapping AS MATERIALIZED" in reconciliation_sql
    assert "mapping.entry_count = canonical_mapping.entry_count" in reconciliation_sql
    assert "mapping.block_hash = canonical_mapping.block_hash" in reconciliation_sql
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_block_mapping_upsert_uses_read_only_identical_retry_path():
    session = SimpleNamespace(
        scalar=AsyncMock(),
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((0,), rowcount=0),
                _OneRowResult((11,)),
            ]
        ),
    )

    await _upsert_shared_block_mappings(
        session,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        expected_count=11,
    )

    insert_sql = str(session.execute.await_args_list[0].args[0])
    assert "ON CONFLICT" in insert_sql
    assert "DO NOTHING" in insert_sql
    reconciliation_sql = str(session.execute.await_args_list[1].args[0])
    assert "canonical_mapping AS MATERIALIZED" in reconciliation_sql
    assert "LEFT JOIN" in reconciliation_sql
    assert "DO UPDATE" not in reconciliation_sql
    session.scalar.assert_not_awaited()
