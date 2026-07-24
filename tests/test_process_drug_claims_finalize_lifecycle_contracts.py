# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location(
    "drug_claims_finalize_lifecycle_contracts",
    MODULE_PATH,
)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_finalize_lifecycle_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


def _finalize_request(redis=None, manifest=None) -> object:
    return drug_claims.DrugClaimsFinalizeRequest(
        test_mode=True,
        import_id="import-one",
        run_id="run-one",
        stage_suffix="stage-one",
        schema="mrf",
        redis=redis,
        manifest=manifest or {},
        expected_chunks=2,
    )


@pytest.mark.asyncio
async def test_process_chunk_rejects_invalid_contracts(tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="missing required fields"):
        await drug_claims.drug_claims_process_chunk({}, {})
    with pytest.raises(RuntimeError, match="does not exist"):
        await drug_claims.drug_claims_process_chunk(
            {},
            {
                "dataset_key": "provider_drug",
                "chunk_id": "one",
                "chunk_path": str(tmp_path / "missing.csv"),
            },
        )

    chunk_path = tmp_path / "unknown.csv"
    chunk_path.write_text("header\n", encoding="utf-8")
    monkeypatch.setattr(drug_claims, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        drug_claims,
        "_staging_classes",
        lambda suffix, schema: {},
    )
    with pytest.raises(RuntimeError, match="Unsupported dataset_key"):
        await drug_claims.drug_claims_process_chunk(
            {},
            {
                "dataset_key": "unknown",
                "chunk_id": "one",
                "chunk_path": str(chunk_path),
            },
        )


def test_finalize_request_uses_manifest_fallbacks(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    drug_claims._write_manifest(
        manifest_path,
        {
            "run_id": "manifest-run",
            "stage_suffix": "manifest-stage",
            "total_chunks": 4,
        },
    )
    monkeypatch.setattr(
        drug_claims,
        "get_import_schema",
        lambda *args: "synthetic",
    )
    request = drug_claims._drug_claims_finalize_request(
        {"redis": "redis"},
        {"import_id": "import-one", "manifest_path": str(manifest_path)},
        True,
    )
    assert request.run_id == "manifest-run"
    assert request.stage_suffix == "manifest-stage"
    assert request.schema == "synthetic"
    assert request.expected_chunks == 4


def test_finalize_manifest_rejects_empty_and_partial_source_handoffs(
    monkeypatch,
):
    monkeypatch.setattr(
        drug_claims,
        "DATASETS",
        (
            drug_claims.DatasetConfig("provider_drug", "provider", 1),
            drug_claims.DatasetConfig("drug_spending", "spending", 1),
        ),
    )
    with pytest.raises(RuntimeError, match="nonempty chunk manifest"):
        drug_claims._validate_drug_claims_finalize_manifest(
            _finalize_request()
        )

    partial_request = _finalize_request(
        manifest={
            "import_id": "import-one",
            "run_id": "run-one",
            "stage_suffix": "stage-one",
            "total_chunks": 1,
            "sources": {
                "provider_drug": [
                    {"url": "https://files.invalid/provider.csv"}
                ],
                "drug_spending": [
                    {"url": "https://files.invalid/spending.csv"}
                ],
            },
            "chunks": [
                {
                    "dataset_key": "provider_drug",
                    "source_index": 0,
                }
            ],
        }
    )
    with pytest.raises(RuntimeError, match="drug_spending:0"):
        drug_claims._validate_drug_claims_finalize_manifest(
            partial_request
        )


@pytest.mark.asyncio
async def test_finalize_permission_retry_and_idempotency(monkeypatch):
    already_redis = SimpleNamespace(get=AsyncMock(return_value=b"1"))
    assert await drug_claims._await_drug_claims_finalize_permission(
        _finalize_request(already_redis)
    ) == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run-one",
        "import_id": "import-one",
    }
    with pytest.raises(RuntimeError, match="redis context"):
        await drug_claims._await_drug_claims_finalize_permission(
            _finalize_request(None)
        )

    waiting_redis = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr(
        drug_claims,
        "_get_run_progress",
        AsyncMock(return_value=(2, 1)),
    )
    with pytest.raises(drug_claims.Retry):
        await drug_claims._await_drug_claims_finalize_permission(
            _finalize_request(waiting_redis)
        )

    monkeypatch.setattr(
        drug_claims,
        "_get_run_progress",
        AsyncMock(return_value=(2, 2)),
    )
    monkeypatch.setattr(
        drug_claims,
        "_has_claimed_finalize_lock",
        AsyncMock(return_value=False),
    )
    with pytest.raises(drug_claims.Retry):
        await drug_claims._await_drug_claims_finalize_permission(
            _finalize_request(waiting_redis)
        )


@pytest.mark.asyncio
async def test_finalize_permission_marks_finalizing(monkeypatch):
    redis = SimpleNamespace(get=AsyncMock(return_value=None))
    mark_control = AsyncMock()
    monkeypatch.setattr(
        drug_claims,
        "_get_run_progress",
        AsyncMock(return_value=(2, 2)),
    )
    monkeypatch.setattr(
        drug_claims,
        "_has_claimed_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(drug_claims, "mark_control_run", mark_control)

    assert await drug_claims._await_drug_claims_finalize_permission(
        _finalize_request(redis)
    ) is None
    assert mark_control.await_args.kwargs["progress"]["pct"] == 99


@pytest.mark.asyncio
async def test_finalize_permission_releases_lock_when_status_update_fails(
    monkeypatch,
):
    request = _finalize_request(
        SimpleNamespace(get=AsyncMock(return_value=None))
    )
    release_lock = AsyncMock()
    monkeypatch.setattr(
        drug_claims,
        "_get_run_progress",
        AsyncMock(return_value=(2, 2)),
    )
    monkeypatch.setattr(
        drug_claims,
        "_has_claimed_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        drug_claims,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("status unavailable")),
    )
    monkeypatch.setattr(
        drug_claims,
        "_release_finalize_lock_safely",
        release_lock,
    )

    with pytest.raises(RuntimeError, match="status unavailable"):
        await drug_claims._await_drug_claims_finalize_permission(request)

    release_lock.assert_awaited_once_with(request)


@pytest.mark.asyncio
async def test_finalize_success_cleans_workspace(tmp_path, monkeypatch):
    work_dir_root = tmp_path / "drug-claims"
    run_directory = work_dir_root / "import-one" / "run-one"
    run_directory.mkdir(parents=True)
    request = _finalize_request(
        SimpleNamespace(set=AsyncMock(), expire=AsyncMock()),
        {"work_dir": str(run_directory)},
    )
    monkeypatch.setattr(drug_claims, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        drug_claims,
        "_drug_claims_finalize_request",
        lambda *args: request,
    )
    monkeypatch.setattr(
        drug_claims,
        "_validate_drug_claims_finalize_manifest",
        lambda _request: None,
    )
    monkeypatch.setattr(
        drug_claims,
        "_await_drug_claims_finalize_permission",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        drug_claims,
        "_materialize_and_publish_drug_claims",
        AsyncMock(),
    )
    monkeypatch.setattr(drug_claims, "mark_control_run", AsyncMock())
    monkeypatch.setattr(
        drug_claims,
        "DRUG_CLAIMS_WORKDIR",
        str(work_dir_root),
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_KEEP_WORKDIR", False)
    release_lock = AsyncMock()
    monkeypatch.setattr(
        drug_claims,
        "_release_finalize_lock_safely",
        release_lock,
    )

    finalize_result = await drug_claims.drug_claims_finalize(
        {},
        {"test_mode": True},
    )
    assert finalize_result == {
        "ok": True,
        "import_id": "import-one",
        "run_id": "run-one",
        "stage_suffix": "stage-one",
        "schema": "mrf",
    }
    assert not run_directory.exists()
    request.redis.set.assert_awaited_once()
    release_lock.assert_awaited_once_with(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "finalize_failure",
    [
        RuntimeError("publish failed"),
        asyncio.CancelledError("publish cancelled"),
    ],
)
async def test_finalize_releases_lock_after_failure_or_cancellation(
    monkeypatch,
    finalize_failure,
):
    request = _finalize_request()
    release_lock = AsyncMock()
    monkeypatch.setattr(drug_claims, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        drug_claims,
        "_drug_claims_finalize_request",
        lambda *_args: request,
    )
    monkeypatch.setattr(
        drug_claims,
        "_validate_drug_claims_finalize_manifest",
        lambda _request: None,
    )
    monkeypatch.setattr(
        drug_claims,
        "_await_drug_claims_finalize_permission",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        drug_claims,
        "_materialize_and_publish_drug_claims",
        AsyncMock(side_effect=finalize_failure),
    )
    monkeypatch.setattr(
        drug_claims,
        "_release_finalize_lock_safely",
        release_lock,
    )

    with pytest.raises(type(finalize_failure)):
        await drug_claims.drug_claims_finalize(
            {},
            {"test_mode": True},
        )

    release_lock.assert_awaited_once_with(request)


@pytest.mark.asyncio
async def test_finalize_returns_existing_result_without_publish(monkeypatch):
    redis = SimpleNamespace(get=AsyncMock(return_value=b"1"))
    publish = AsyncMock()
    ensure_database = AsyncMock()
    monkeypatch.setattr(drug_claims, "ensure_database", ensure_database)
    monkeypatch.setattr(
        drug_claims,
        "_materialize_and_publish_drug_claims",
        publish,
    )

    assert await drug_claims.drug_claims_finalize(
        {"redis": redis},
        {"import_id": "import-one", "run_id": "run-one"},
    ) == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run-one",
        "import_id": "import_one",
    }
    ensure_database.assert_not_awaited()
    publish.assert_not_awaited()
