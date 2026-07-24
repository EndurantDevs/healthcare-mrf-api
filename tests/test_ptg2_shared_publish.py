from __future__ import annotations

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_manifest_publish
from process.ptg_parts.domain import PTG2FileProcessResult
from process.ptg_parts.ptg2_shared_publish import (
    _SHARED_BLOCK_STAGE_COLUMNS,
    _upsert_shared_block_mappings,
    create_shared_block_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    publish_v4_cas_block_stage,
    shared_block_stage_name,
)
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_reuse import SharedPhysicalArtifactIdentity
from process.ptg_parts.ptg2_shared_finalize import PTG2_V3_SERVING_RUN_RECORD_BYTES

process_ptg = importlib.import_module("process.ptg")


def _finalizer_contract():
    return {
        "format": "ptg2_v3_direct_finalizer_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "price_key_map": {
            "copy_format": "postgresql_binary_copy",
            "row_count": 1,
            "dense_price_ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            "keys_unique_dense_contiguous": True,
            "source_ids_exact_match": True,
        },
        "dense_keys": {
            "price": {
                "count": 1,
                "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            }
        },
        "blocks": {
            "serving": {
                "copy_bytes": 1,
                "copy_sha256": "a" * 64,
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                }
            },
            "price_dictionary": {
                "copy_bytes": 1,
                "copy_sha256": "b" * 64,
                "artifact_record_counts": {"by_code_price_dictionary": 1}
            },
        },
    }


def _provider_set_metadata_entries(tmp_path, *, row_count: int = 1):
    if row_count == 0:
        return ()
    path = tmp_path / "provider-set-metadata.copy"
    path.write_text(f"{'01' * 16}\t1\t{{}}\n", encoding="ascii")
    payload = path.read_bytes()
    return (
        {
            "path": str(path),
            "row_count": row_count,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "format": "ptg2_v3_provider_set_metadata_copy",
            "version": 1,
        },
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

    metrics = await process_ptg._merge_and_copy_ptg2_manifest_files(
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
        await process_ptg._merge_and_copy_ptg2_manifest_files(
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


def _serving_run_entries(tmp_path):
    entries = []
    for partition in range(2):
        path = tmp_path / f"run-{partition}"
        path.write_bytes(b"r" * PTG2_V3_SERVING_RUN_RECORD_BYTES)
        entries.append(
            {
                "path": str(path),
                "format": "ptg2_v3_serving_run",
                "version": 1,
                "partition": partition,
                "partition_count": 2,
                "row_count": 1,
                "bytes": PTG2_V3_SERVING_RUN_RECORD_BYTES,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return entries


def _unannotated_file_result(tmp_path):
    dictionary_path = tmp_path / "codes.ready"
    dictionary_path.write_bytes(b"c" * 64)
    dictionary_entries = [
        {
            "path": str(dictionary_path),
            "format": "ptg2_v3_serving_code_dictionary",
            "version": 4,
            "row_count": 1,
            "bytes": 64,
            "sha256": hashlib.sha256(dictionary_path.read_bytes()).hexdigest(),
        }
    ]
    return PTG2FileProcessResult(
        "in_network",
        "https://example.invalid/rates.json.gz",
        True,
        summary={
            "manifest": {
                "copy_files": {
                    "serving_run": _serving_run_entries(tmp_path),
                    "serving_code_dictionary": dictionary_entries,
                }
            },
            "scanner": {
                "summary": {
                    "serving_run_files": 2,
                    "serving_run_rows": 2,
                    "serving_run_bytes": 2 * PTG2_V3_SERVING_RUN_RECORD_BYTES,
                    "serving_code_dictionary_files": 1,
                    "serving_code_dictionary_rows": 1,
                    "serving_code_dictionary_bytes": 64,
                },
                "config": {"serving_run_partition_count": 2},
            },
        },
    )


def test_post_scan_annotation_adds_identity_to_every_serving_run_entry(tmp_path):
    """Bind all serving and dictionary shards to one physical identity."""

    file_result = _unannotated_file_result(tmp_path)
    identity = SharedPhysicalArtifactIdentity(
        "in_network",
        "logical_json_sha256_v1",
        "a" * 64,
    )

    annotated = process_ptg._annotate_v3_file_result_source_identity(
        file_result,
        identity,
        {
            "raw_container_sha256": "b" * 64,
            "logical_json_sha256": "a" * 64,
            "logical_hash_deferred": False,
        },
    )

    assert annotated is file_result
    manifest = annotated.summary["manifest"]
    assert manifest["physical_artifact_identity"] == identity.as_dict()
    annotated_entries = manifest["copy_files"]["serving_run"]
    assert all(
        {field_name: entry[field_name] for field_name in identity.as_dict()}
        == identity.as_dict()
        for entry in annotated_entries
    )
    assert all("source_run_contract_sha256" in entry for entry in annotated_entries)
    annotated_dictionary_entries = manifest["copy_files"][
        "serving_code_dictionary"
    ]
    assert len(annotated_dictionary_entries) == 1
    assert "code_dictionary_contract_sha256" in annotated_dictionary_entries[0]
    assert "code_dictionary_source_contract" in annotated_dictionary_entries[0]


@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_non_32_byte_expected_scope_before_database_work(
    monkeypatch,
):
    status = AsyncMock()
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)

    with pytest.raises(ValueError, match="exactly 32 bytes"):
        await publish_shared_finalizer_dictionaries(
            {},
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"short",
            provider_set_metadata_entries=(),
        )

    status.assert_not_awaited()


@pytest.mark.parametrize(
    ("support_digest", "message"),
    (
        ("not-hex", "support digest is invalid"),
        ((b"short").hex(), "support digest must contain 32 bytes"),
    ),
)
@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_invalid_support_digest_before_database_work(
    monkeypatch,
    support_digest,
    message,
):
    status = AsyncMock()
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)

    with pytest.raises(RuntimeError, match=message):
        await publish_shared_finalizer_dictionaries(
            {
                **_finalizer_contract(),
                "output_directory": "/unused",
                "dictionaries": {
                    "code": {"path": "codes.copy", "row_count": 1},
                    "provider_set": {"path": "providers.copy", "row_count": 1},
                    "support_digest": support_digest,
                },
                "preservation": {"encoded_records": 1},
            },
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"s" * 32,
            provider_set_metadata_entries=(),
        )

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalizer_code_stage_uses_fixed_coverage_scope_id(tmp_path, monkeypatch):
    (tmp_path / "codes.copy").write_bytes(b"codes")
    (tmp_path / "providers.copy").write_bytes(b"providers")
    status = AsyncMock()
    copy = AsyncMock(side_effect=RuntimeError("stop after contract inspection"))
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)
    monkeypatch.setattr(ptg2_shared_publish, "_copy_binary_file_to_stage", copy)

    with pytest.raises(RuntimeError, match="contract inspection"):
        await publish_shared_finalizer_dictionaries(
            {
                **_finalizer_contract(),
                "output_directory": str(tmp_path),
                "dictionaries": {
                    "code": {"path": "codes.copy", "row_count": 1},
                    "provider_set": {"path": "providers.copy", "row_count": 1},
                    "support_digest": (b"s" * 32).hex(),
                },
                "preservation": {"encoded_records": 1},
        },
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"s" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(tmp_path),
    )

    code_stage_sql = status.await_args_list[0].args[0]
    assert "coverage_scope_id bytea NOT NULL" in code_stage_sql
    assert "octet_length(coverage_scope_id) = 32" in code_stage_sql
    assert "plan_id" not in code_stage_sql
    assert copy.await_args.kwargs["columns"] == (
        "code_key",
        "code_global_id_128",
        "coverage_scope_id",
        "reported_code_system",
        "reported_code",
        "negotiation_arrangement",
        "billing_code_type_version",
        "source_name",
        "source_description",
        "rate_count",
    )


class _OneRowResult:
    def __init__(self, row, *, rowcount=None):
        self.row = row
        self.rowcount = rowcount

    def one(self):
        return self.row

    def scalar(self):
        return self.row[0]


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _FirstBatchProgress:
    def __init__(self):
        self.events = []
        self.first_batch_reported = asyncio.Event()

    def __call__(self, metric, amount):
        self.events.append((metric, amount))
        if metric == "sql_stage_rows":
            self.first_batch_reported.set()


class _SlowSharedBlockSQLDriver:
    batch_sizes = (32, 7)
    new_hash_counts = (32, 5)

    def __init__(self):
        self.fetch_index = 0
        self.scalar_index = 0
        self.release_second_batch = asyncio.Event()

    async def execute_stage_statement(self, statement, params=None):
        statement_text = str(statement)
        if statement_text.startswith("FETCH FORWARD"):
            return await self._fetch_stage_rows()
        if "LEFT JOIN \"mrf\".ptg2_v3_snapshot_block AS mapping" in statement_text:
            batch_size = self.batch_sizes[self.fetch_index - 1]
            return _OneRowResult(
                (
                    batch_size,
                    batch_size * 10,
                    batch_size * 7,
                    ["serving"],
                    False,
                    False,
                    False,
                )
            )
        return _OneRowResult((0,))

    async def _fetch_stage_rows(self):
        current_index = self.fetch_index
        self.fetch_index += 1
        if current_index == 1:
            await self.release_second_batch.wait()
        if current_index >= len(self.batch_sizes):
            return _RowsResult(())
        return _RowsResult(
            tuple(
                (f"({current_index},{row_offset + 1})",)
                for row_offset in range(self.batch_sizes[current_index])
            )
        )

    async def read_identity_count(self, statement):
        batch_index, identity_kind = divmod(self.scalar_index, 2)
        self.scalar_index += 1
        if identity_kind == 0:
            return self.new_hash_counts[batch_index]
        return self.batch_sizes[batch_index]


class _SlowV4CASSQLDriver:
    batch_sizes = (32, 4)
    unique_counts = (30, 3)

    def __init__(self):
        self.fetch_index = 0
        self.release_second_batch = asyncio.Event()

    async def execute_stage_statement(self, statement, params=None):
        statement_text = str(statement)
        if statement_text.startswith("FETCH FORWARD"):
            return await self._fetch_stage_rows()
        batch_index = max(self.fetch_index - 1, 0)
        if "SUM(staged.entry_count)" in statement_text:
            batch_size = self.batch_sizes[batch_index]
            return _OneRowResult(
                (
                    batch_size,
                    batch_size * 2,
                    batch_size * 10,
                    batch_size * 7,
                    ["v4_graph"],
                    False,
                    False,
                    False,
                )
            )
        if "JOIN \"mrf\".ptg2_v3_block AS stored USING" in statement_text:
            unique_count = self.unique_counts[batch_index]
            return _OneRowResult(
                (unique_count, unique_count * 10, unique_count * 7)
            )
        return _OneRowResult((0,))

    async def _fetch_stage_rows(self):
        batch_index = self.fetch_index
        self.fetch_index += 1
        if batch_index == 1:
            await self.release_second_batch.wait()
        if batch_index >= len(self.batch_sizes):
            return _RowsResult(())
        return _RowsResult(
            tuple(
                (f"({batch_index},{row_offset + 1})",)
                for row_offset in range(self.batch_sizes[batch_index])
            )
        )


@asynccontextmanager
async def _session_transaction(session):
    yield session


def _assert_slow_shared_block_publication(publication, progress_events, session):
    assert publication.mapping_count == 39
    assert publication.unique_block_count == 37
    assert publication.logical_byte_count == 390
    assert publication.stored_byte_count == 273
    assert progress_events == [
        ("sql_stage_rows", 32),
        ("publish_batches", 1),
        ("sql_stage_rows", 7),
        ("publish_batches", 1),
    ]
    statements = "\n".join(
        str(call.args[0]) for call in session.execute.await_args_list
    )
    assert "FETCH FORWARD 32" in statements
    assert "JOIN \"ptg2_publish_batch_" in statements


def _assert_slow_v4_cas_publication(publication, progress_events):
    assert publication.staged_row_count == 36
    assert publication.staged_entry_count == 72
    assert publication.unique_block_count == 33
    assert publication.logical_byte_count == 360
    assert publication.stored_byte_count == 252
    assert publication.unique_logical_byte_count == 330
    assert publication.unique_stored_byte_count == 231
    assert progress_events[-2:] == [
        ("sql_stage_rows", 4),
        ("publish_batches", 1),
    ]


def test_shared_publish_rejects_invalid_identifiers_and_summary_values():
    invalid_operations = (
        (lambda: ptg2_shared_publish._safe_identifier("mrf;drop"), ValueError, "unsafe"),
        (
            lambda: ptg2_shared_publish._validated_coverage_scope_id("not-bytes"),
            ValueError,
            "exactly 32 bytes",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_mapping(None, "blocks"),
            RuntimeError,
            "missing blocks",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer(True, "count"),
            RuntimeError,
            "invalid count",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer("bad", "count"),
            RuntimeError,
            "invalid count",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer(-1, "count"),
            RuntimeError,
            "negative count",
        ),
    )

    for operation, error_type, message in invalid_operations:
        with pytest.raises(error_type, match=message):
            operation()


def test_finalizer_output_file_rejects_unsafe_or_missing_paths(tmp_path):
    outside = tmp_path.parent / "outside.copy"
    outside.write_bytes(b"outside")
    operations = (
        (str(outside), "invalid code path"),
        ("../outside.copy", "escapes its output directory"),
        ("missing.copy", "output is missing or empty"),
    )

    for raw_path, message in operations:
        with pytest.raises(RuntimeError, match=message):
            ptg2_shared_publish._finalizer_output_file(
                tmp_path,
                raw_path,
                "code",
            )


@pytest.mark.asyncio
async def test_shared_block_existence_query_rejects_unrequested_hash(monkeypatch):
    monkeypatch.setattr(
        ptg2_shared_publish.db,
        "all",
        AsyncMock(return_value=[(b"z" * 32,)]),
    )

    with pytest.raises(RuntimeError, match="unexpected hash"):
        await ptg2_shared_publish._existing_shared_block_hashes(
            schema_name="mrf",
            requested_hashes=(b"a" * 32,),
        )


@pytest.mark.asyncio
async def test_shared_block_existence_query_uses_exact_lateral_batches(monkeypatch):
    batch_rows = ptg2_shared_publish._SHARED_BLOCK_EXISTENCE_BATCH_ROWS
    requested_hashes = {
        index.to_bytes(32, "big") for index in range(batch_rows + 1)
    }
    observed_batches = []
    observed_statements = []

    async def return_requested_hashes(statement, *, block_hashes):
        observed_statements.append(str(statement))
        observed_batches.append(tuple(block_hashes))
        return [(block_hash,) for block_hash in block_hashes]

    monkeypatch.setattr(ptg2_shared_publish.db, "all", return_requested_hashes)

    existing_hashes = await ptg2_shared_publish._existing_shared_block_hashes(
        schema_name="mrf",
        requested_hashes=requested_hashes,
    )

    assert existing_hashes == requested_hashes
    assert list(map(len, observed_batches)) == [batch_rows, 1]
    assert len(set().union(*map(set, observed_batches))) == sum(
        map(len, observed_batches)
    )
    assert all("CROSS JOIN LATERAL" in statement for statement in observed_statements)
    assert all(
        "candidate.block_hash = requested.block_hash" in statement
        for statement in observed_statements
    )
    assert all("LIMIT 1" in statement for statement in observed_statements)


def _copy_connection(copy_to_table=None):
    driver = object() if copy_to_table is None else SimpleNamespace(
        copy_to_table=copy_to_table
    )
    return SimpleNamespace(
        raw_connection=SimpleNamespace(driver_connection=driver)
    )


@pytest.mark.asyncio
async def test_stage_copy_helpers_require_driver_copy_support(tmp_path, monkeypatch):
    path = tmp_path / "stage.copy"
    path.write_bytes(b"stage")

    @asynccontextmanager
    async def acquire():
        yield _copy_connection()

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    with pytest.raises(NotImplementedError, match="binary COPY"):
        await ptg2_shared_publish._copy_binary_file_to_stage(
            path,
            schema_name="mrf",
            stage_table="binary_stage",
            columns=("value",),
        )
    with pytest.raises(NotImplementedError, match="text COPY"):
        await ptg2_shared_publish._copy_text_file_to_stage(
            path,
            schema_name="mrf",
            stage_table="text_stage",
            columns=("value",),
            expected_bytes=path.stat().st_size,
            expected_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("digest_matches", [True, False], ids=["stable", "changed"])
async def test_text_stage_copy_verifies_consumed_content(
    tmp_path,
    monkeypatch,
    digest_matches,
):
    path = tmp_path / "stage.copy"
    path.write_bytes(b"provider metadata")

    async def copy_to_table(_table, *, source, **_kwargs):
        chunk = source.read(3)
        while chunk:
            chunk = source.read(3)

    @asynccontextmanager
    async def acquire():
        yield _copy_connection(copy_to_table)

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)
    expected_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    if not digest_matches:
        expected_sha256 = "0" * 64

    invocation = ptg2_shared_publish._copy_text_file_to_stage(
        path,
        schema_name="mrf",
        stage_table="text_stage",
        columns=("value",),
        expected_bytes=path.stat().st_size,
        expected_sha256=expected_sha256,
    )
    if digest_matches:
        await invocation
    else:
        with pytest.raises(RuntimeError, match="changed during publication"):
            await invocation


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("non-mapping", "entry must be an object"),
        ("missing", "file is missing or repeated"),
        ("repeated", "file is missing or repeated"),
        ("boolean-rows", "row count is invalid"),
        ("invalid-rows", "row count is invalid"),
        ("empty", "must contain rows"),
        ("format", "format is incompatible"),
        ("digest", "digest contract is invalid"),
        ("required", "metadata is required"),
    ),
)
def test_provider_set_metadata_files_reject_invalid_contracts(
    tmp_path,
    case,
    message,
):
    valid_entry_by_field = dict(_provider_set_metadata_entries(tmp_path)[0])
    entries_by_case = {
        "non-mapping": (None,),
        "missing": (
            {
                **valid_entry_by_field,
                "path": str(tmp_path / "missing.copy"),
            },
        ),
        "repeated": (
            valid_entry_by_field,
            dict(valid_entry_by_field),
        ),
        "boolean-rows": ({**valid_entry_by_field, "row_count": True},),
        "invalid-rows": ({**valid_entry_by_field, "row_count": "bad"},),
        "empty": ({**valid_entry_by_field, "row_count": 0},),
        "format": ({**valid_entry_by_field, "version": 2},),
        "digest": ({**valid_entry_by_field, "sha256": "bad"},),
        "required": (),
    }
    entries = entries_by_case[case]

    with pytest.raises(RuntimeError, match=message):
        ptg2_shared_publish._provider_set_metadata_files(
            entries,
            required=case == "required",
        )


def _assert_shared_stage_sql(session):
    block_insert_sql = str(session.execute.await_args_list[0].args[0])
    assert "NOT EXISTS" in block_insert_sql
    assert "staged.format_version = :format_version" in block_insert_sql
    assert "staged.payload IS NOT NULL" in block_insert_sql
    assert "stored.block_hash = staged.block_hash" in block_insert_sql
    assert "ON CONFLICT (block_hash) DO NOTHING" in block_insert_sql
    aggregate_sql = str(session.execute.await_args_list[-1].args[0])
    assert "LEFT JOIN" in aggregate_sql
    assert "stored.block_hash IS NULL" in aggregate_sql
    assert "stored.payload" not in aggregate_sql
    assert "staged.payload" not in aggregate_sql
    assert "BOOL_OR" in aggregate_sql
    assert "staged.format_version <> :format_version" in aggregate_sql
    assert "stored.format_version <> staged.format_version" in aggregate_sql
    assert "stored.object_kind <> staged.object_kind" in aggregate_sql
    assert "stored.codec <> staged.codec" in aggregate_sql
    assert "stored.entry_count <> staged.entry_count" in aggregate_sql
    assert "stored.raw_byte_count <> staged.raw_byte_count" in aggregate_sql
    assert "stored.stored_byte_count <> staged.stored_byte_count" in aggregate_sql
    assert "COUNT(DISTINCT staged.block_hash)" in aggregate_sql
    assert "ARRAY_AGG(" in aggregate_sql
    assert "DISTINCT staged.object_kind" in aggregate_sql
    assert "ORDER BY staged.object_kind" in aggregate_sql
    assert "canonical_mapping" not in aggregate_sql
    assert 'FROM "mrf"."ptg2_v3_block_stage_proof"' in aggregate_sql
    format_by_field = {
        "format_version": ptg2_shared_publish.PTG2_V3_SHARED_FORMAT_VERSION
    }
    assert session.execute.await_args_list[0].args[1] == format_by_field
    assert session.execute.await_args_list[1].args[1] == format_by_field


@pytest.mark.asyncio
async def test_shared_block_stage_returns_only_bounded_sql_aggregates(monkeypatch):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                None,
                _OneRowResult(
                    (3, 2, 30, 20, ["a_kind", "z_kind"], False, False)
                ),
            ]
        ),
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
    mapping_upsert = AsyncMock()
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_upsert_shared_block_mappings",
        mapping_upsert,
    )

    publication = await publish_shared_block_stage(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        build_token="build-42",
    )

    assert publication.object_kinds == ("a_kind", "z_kind")
    assert publication.mapping_count == 3
    assert publication.unique_block_count == 2
    assert publication.logical_byte_count == 30
    assert publication.stored_byte_count == 20
    _assert_shared_stage_sql(session)
    session.scalar.assert_not_awaited()
    mapping_upsert.assert_awaited_once_with(
        session,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_proof",
        snapshot_key=42,
        expected_count=3,
    )


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
        ("sql_stage_rows", 32),
        ("publish_batches", 1),
    ]
    sql_driver.release_second_batch.set()
    publication = await publish_task
    _assert_slow_shared_block_publication(
        publication,
        progress_capture.events,
        session,
    )


@pytest.mark.asyncio
async def test_v4_cas_stage_publishes_exact_totals_without_snapshot_mappings(
    monkeypatch,
):
    """Prove V4 publishes deduplicated CAS totals without legacy mappings."""

    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                None,
                None,
                _OneRowResult(
                    (
                        3,
                        18,
                        2,
                        30,
                        20,
                        20,
                        14,
                        ["a_kind", "z_kind"],
                        False,
                        False,
                        False,
                    )
                ),
                None,
            ]
        )
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    drop_stage = AsyncMock()
    monkeypatch.setattr(ptg2_shared_publish.db, "status", drop_stage)
    layout_lock = AsyncMock()
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        layout_lock,
    )

    publication = await publish_v4_cas_block_stage(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_v4proof",
        snapshot_key=42,
        build_token="build-v4",
    )

    assert publication.object_kinds == ("a_kind", "z_kind")
    assert publication.staged_row_count == 3
    assert publication.staged_entry_count == 18
    assert publication.unique_block_count == 2
    assert publication.logical_byte_count == 30
    assert publication.stored_byte_count == 20
    assert publication.unique_logical_byte_count == 20
    assert publication.unique_stored_byte_count == 14
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "FOR KEY SHARE OF stored" in statements[0]
    assert "INSERT INTO \"mrf\".ptg2_v3_block" in statements[1]
    assert "WITH canonical AS MATERIALIZED" in statements[2]
    assert "DELETE FROM \"mrf\".ptg2_v3_gc_candidate" in statements[3]
    assert not any("ptg2_v3_snapshot_block" in statement for statement in statements)
    layout_lock.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_key=42,
        build_token="build-v4",
    )
    drop_stage.assert_awaited_once_with(
        'DROP TABLE IF EXISTS "mrf"."ptg2_v3_block_stage_v4proof";'
    )


@pytest.mark.asyncio
async def test_slow_v4_cas_sql_reports_exact_batches_before_completion(
    monkeypatch,
):
    """V4 CAS publication exposes completed rows while a later batch waits."""

    sql_driver = _SlowV4CASSQLDriver()
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=sql_driver.execute_stage_statement)
    )
    monkeypatch.setattr(
        ptg2_shared_publish.db,
        "transaction",
        lambda: _session_transaction(session),
    )
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    progress_capture = _FirstBatchProgress()
    publish_task = asyncio.create_task(
        publish_v4_cas_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_v4_slow_sql",
            snapshot_key=42,
            build_token="build-v4",
            progress_callback=progress_capture,
        )
    )
    await asyncio.wait_for(
        progress_capture.first_batch_reported.wait(),
        timeout=1.0,
    )

    assert not publish_task.done()
    assert progress_capture.events == [
        ("sql_stage_rows", 32),
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
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                None,
                _OneRowResult((1, 1, 3, 3, ["serving"], True, True)),
            ]
        ),
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
    mapping_upsert = AsyncMock()
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_upsert_shared_block_mappings",
        mapping_upsert,
    )

    with pytest.raises(RuntimeError, match="incompatible format version"):
        await publish_shared_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_proof",
            snapshot_key=42,
            build_token="build-42",
        )

    mapping_upsert.assert_not_awaited()


def _dictionary_summary(tmp_path, *, row_count: int) -> dict[str, object]:
    (tmp_path / "codes.copy").write_bytes(b"codes")
    (tmp_path / "providers.copy").write_bytes(b"providers")
    return {
        **_finalizer_contract(),
        "output_directory": str(tmp_path),
        "dictionaries": {
            "code": {"path": "codes.copy", "row_count": row_count},
            "provider_set": {"path": "providers.copy", "row_count": row_count},
            "support_digest": (b"s" * 32).hex(),
        },
        "preservation": {"encoded_records": row_count},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed_code",
    [
        (2, 2, 2, 1),
        (2, 2, 1, 0),
    ],
    ids=["mixed-scopes", "wrong-scope"],
)
async def test_finalizer_dictionary_rejects_scope_mismatch_before_code_insert(
    tmp_path,
    monkeypatch,
    observed_code,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[_OneRowResult((7,)), _OneRowResult(observed_code)]
        ),
        scalar=AsyncMock(side_effect=[2, 2, False]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="coverage scope does not match"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=2),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(
                tmp_path,
                row_count=2,
            ),
        )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 2
    assert "FOR KEY SHARE" in statements[0]
    assert "build_token = :build_token" in statements[0]
    assert session.execute.await_args_list[0].args[1]["build_token"] == "attempt-7"
    assert "COUNT(DISTINCT coverage_scope_id)" in statements[1]
    assert not any("INSERT INTO \"mrf\".ptg2_v3_code" in sql for sql in statements)
    assert session.execute.await_args.args[1] == {
        "expected_coverage_scope_id": b"e" * 32
    }


@pytest.mark.asyncio
async def test_finalizer_dictionary_preserves_empty_scope_semantics(tmp_path, monkeypatch):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((0, 0, 0, 0)),
                _OneRowResult((0,)),
                _OneRowResult((0,)),
            ]
        ),
        scalar=AsyncMock(return_value=0),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    publication = await publish_shared_finalizer_dictionaries(
        _dictionary_summary(tmp_path, row_count=0),
        schema_name="mrf",
        snapshot_key=7,
        build_token="attempt-7",
        expected_coverage_scope_id=b"e" * 32,
        provider_set_metadata_entries=(),
    )

    assert publication.code_count == 0
    assert publication.serving_rate_count == 0


@pytest.mark.asyncio
async def test_finalizer_provider_metadata_join_decodes_the_smaller_stage(
    tmp_path,
    monkeypatch,
):
    status = AsyncMock()
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((1, 1, 1, 1)),
                None,
                None,
                None,
            ]
        ),
        scalar=AsyncMock(side_effect=[1, 1, False, False]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    publication = await publish_shared_finalizer_dictionaries(
        _dictionary_summary(tmp_path, row_count=1),
        schema_name="mrf",
        snapshot_key=7,
        build_token="attempt-7",
        expected_coverage_scope_id=b"e" * 32,
        provider_set_metadata_entries=_provider_set_metadata_entries(tmp_path),
    )

    executed_statements = [str(call.args[0]) for call in session.execute.await_args_list]
    provider_metadata_sql = "\n".join(executed_statements[2:4])
    assert "decode(metadata.provider_set_global_id_128, 'hex')" in provider_metadata_sql
    assert "encode(provider_stage.provider_set_global_id_128, 'hex')" not in provider_metadata_sql
    stage_index_sql = "\n".join(str(call.args[0]) for call in status.await_args_list)
    assert "CREATE UNIQUE INDEX" in stage_index_sql
    assert "((decode(provider_set_global_id_128, 'hex')))" in stage_index_sql
    assert stage_index_sql.count("ANALYZE") == 2
    assert publication.provider_set_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed_code", "observed_scalars", "message"),
    (
        ((1, 2, 1, 1), (2, 2, False), "code dictionary row count changed"),
        ((2, 1, 1, 2), (2, 2, False), "rate counts do not preserve"),
        ((2, 2, 1, 2), (1, 2, False), "provider dictionary row count changed"),
        ((2, 2, 1, 2), (2, 1, False), "metadata row count changed"),
        ((2, 2, 1, 2), (2, 2, True), "conflicting network names"),
    ),
    ids=(
        "code-count",
        "rate-count",
        "provider-count",
        "metadata-count",
        "metadata-conflict",
    ),
)
async def test_finalizer_dictionary_rejects_post_copy_count_mismatches(
    tmp_path,
    monkeypatch,
    observed_code,
    observed_scalars,
    message,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[_OneRowResult((7,)), _OneRowResult(observed_code)]
        ),
        scalar=AsyncMock(side_effect=observed_scalars),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match=message):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=2),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(
                tmp_path,
                row_count=2,
            ),
        )


@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_scope_rows_for_empty_dictionary(
    tmp_path,
    monkeypatch,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((0, 0, 1, 0)),
            ]
        ),
        scalar=AsyncMock(return_value=0),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="empty code dictionary has scope rows"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=0),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=(),
        )


@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_unmatched_provider_metadata(
    tmp_path,
    monkeypatch,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((1, 1, 1, 1)),
                None,
            ]
        ),
        scalar=AsyncMock(side_effect=[1, 1, False, True]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="does not exactly cover"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=1),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(tmp_path),
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
    ("aggregate_row", "message"),
    (
        ((1, 1, 3, 3, ["serving"], False, True), "conflicts with stored"),
        ((1, 2, 3, 3, ["serving"], False, False), "invalid aggregates"),
        ((2, 2, 3, 3, ["z_kind", "a_kind"], False, False), "invalid object kinds"),
    ),
    ids=("stored-mismatch", "invalid-counts", "invalid-kinds"),
)
async def test_shared_block_stage_rejects_invalid_aggregate_proof(
    monkeypatch,
    aggregate_row,
    message,
):
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[None, _OneRowResult(aggregate_row)]),
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
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_upsert_shared_block_mappings",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match=message):
        await publish_shared_block_stage(
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_proof",
            snapshot_key=42,
            build_token="build-42",
        )
