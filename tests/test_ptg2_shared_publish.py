from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_manifest_publish
from process.ptg_parts.domain import PTG2FileProcessResult
from process.ptg_parts.ptg2_shared_publish import (
    _SHARED_BLOCK_STAGE_COLUMNS,
    publish_shared_finalizer_dictionaries,
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
            "dense_price_ordering": (
                "minimum_negotiated_rate_then_global_id_128_v1"
            ),
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
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                }
            },
            "price_dictionary": {
                "artifact_record_counts": {"by_code_price_dictionary": 1}
            },
        },
    }


def _provider_set_metadata_entries(tmp_path, *, row_count: int = 1):
    if row_count == 0:
        return ()
    path = tmp_path / "provider-set-metadata.copy"
    path.write_text(f"{'01' * 16}\t{{}}\n", encoding="ascii")
    return ({"path": str(path), "row_count": row_count},)


@pytest.mark.asyncio
async def test_strict_v3_stage_creates_only_price_inputs(monkeypatch):
    status = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(ptg2_manifest_publish.db, "status", status)

    stage_table = await ptg2_manifest_publish._create_ptg2_manifest_serving_stage_table(
        "strict-run"
    )

    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert stage_table == "ptg2_manifest_stage_serving_strict_run"
    assert "ptg2_manifest_stage_price_atom_strict_run" in statements
    assert "ptg2_manifest_stage_price_set_atom_strict_run" in statements
    for retired_kind in (
        "provider_group_member",
        "provider_npi_scope",
        "code_count",
        "provider_set_dictionary",
    ):
        assert retired_kind not in statements
    assert "CREATE UNLOGGED TABLE \"mrf\".\"ptg2_manifest_stage_serving_strict_run\"" not in statements


@pytest.mark.asyncio
async def test_strict_v3_precopy_loads_only_price_inputs(tmp_path, monkeypatch):
    copy_files = {}
    for kind in (
        "manifest_lean_serving",
        "price_atom",
        "price_set_atom",
        "provider_group_member",
        "provider_npi_scope",
        "code_count",
        "provider_set_dictionary",
    ):
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(b"copy")
        copy_files[kind] = [{"path": str(path), "row_count": 3}]
    price_atom_copy = AsyncMock()
    price_set_atom_copy = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_price_atom_file", price_atom_copy)
    monkeypatch.setattr(process_ptg, "_copy_price_atom_member_file", price_set_atom_copy)

    metrics = await process_ptg._merge_and_copy_ptg2_manifest_files(
        successful_files=[{"summary": {"manifest": {"copy_files": copy_files}}}],
        manifest_stage_table="ptg2_manifest_stage_serving_strict",
    )

    assert metrics["strict_v3_price_only"] is True
    assert set(metrics["kinds"]) == {"price_atom", "price_set_atom"}
    price_atom_copy.assert_awaited_once()
    price_set_atom_copy.assert_awaited_once()
    assert not hasattr(process_ptg, "_copy_lean_manifest_serving_file")
    assert not hasattr(process_ptg, "_copy_ptg2_manifest_provider_group_member_file")
    assert not (tmp_path / "price_atom.copy").exists()
    assert not (tmp_path / "price_set_atom.copy").exists()
    assert (tmp_path / "manifest_lean_serving.copy").exists()


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
    def __init__(self, row):
        self.row = row

    def one(self):
        return self.row

    def scalar(self):
        return self.row[0]


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
