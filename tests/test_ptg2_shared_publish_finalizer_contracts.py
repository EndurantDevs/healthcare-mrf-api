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
