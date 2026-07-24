# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_lifecycle_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_lifecycle_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


def _start_request(redis) -> object:
    return drug_claims.DrugClaimsStartRequest(
        test_mode=True,
        import_id="import-one",
        run_id="run-one",
        stage_suffix="stage-one",
        redis=redis,
    )


def _workspace(tmp_path: Path) -> object:
    return drug_claims.DrugClaimsWorkspace(
        work_dir=tmp_path,
        downloads_dir=tmp_path / "downloads",
        chunks_root=tmp_path / "chunks",
    )


def test_start_request_rejects_missing_worker_redis():
    with pytest.raises(RuntimeError, match="redis context is unavailable"):
        drug_claims._drug_claims_start_request({}, {"run_id": "run-one"})

    redis = object()
    request = drug_claims._drug_claims_start_request(
        {"redis": redis},
        {"run_id": "run-one", "import_id": "import-one", "test_mode": True},
    )
    assert request.redis is redis
    assert request.run_id == "run-one"
    assert request.test_mode is True


@pytest.mark.asyncio
async def test_prepare_sources_builds_workspace_and_state(tmp_path, monkeypatch):
    redis = object()
    request = _start_request(redis)
    ensure_database = AsyncMock()
    prepare_tables = AsyncMock(return_value=({}, "synthetic"))
    fetch_catalog = AsyncMock(return_value={"dataset": []})
    init_state = AsyncMock()
    resolved_sources_by_dataset = {"provider_drug": [], "drug_spending": []}
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_WORKDIR", str(tmp_path))
    monkeypatch.setattr(drug_claims, "ensure_database", ensure_database)
    monkeypatch.setattr(drug_claims, "_prepare_tables", prepare_tables)
    monkeypatch.setattr(drug_claims, "_fetch_catalog", fetch_catalog)
    monkeypatch.setattr(
        drug_claims,
        "_resolve_sources",
        lambda catalog, test_mode: resolved_sources_by_dataset,
    )
    monkeypatch.setattr(drug_claims, "_init_run_state", init_state)
    monkeypatch.setattr(drug_claims, "_step_start", lambda label: 0.0)
    monkeypatch.setattr(drug_claims, "_step_end", lambda label, started_at: None)

    schema, sources_by_dataset, workspace = await drug_claims._prepare_drug_claims_sources(
        request
    )
    assert schema == "synthetic"
    assert sources_by_dataset == resolved_sources_by_dataset
    assert workspace.chunks_root.is_dir()
    ensure_database.assert_awaited_once_with(True)
    init_state.assert_awaited_once_with(redis, "run-one", 0)


@pytest.mark.asyncio
async def test_download_split_adds_year_and_source_identity(tmp_path, monkeypatch):
    request = _start_request(object())
    workspace = _workspace(tmp_path)
    download_file = AsyncMock(return_value=str(tmp_path / "download.csv"))
    split_source = AsyncMock(
        return_value=[
            {
                "dataset_key": "provider_drug",
                "chunk_index": 0,
                "chunk_path": str(tmp_path / "chunk.csv"),
            }
        ]
    )
    monkeypatch.setattr(drug_claims, "_download_source_file", download_file)
    monkeypatch.setattr(drug_claims, "_split_source_into_chunks", split_source)
    monkeypatch.setattr(drug_claims, "_step_start", lambda label: 0.0)
    monkeypatch.setattr(drug_claims, "_step_end", lambda label, started_at: None)

    dataset_key, chunk_manifests = await drug_claims._download_split_drug_claims_source(
        request,
        workspace,
        "provider_drug",
        {"url": "https://files.invalid/provider.csv", "reporting_year": "bad"},
        3,
        asyncio.Semaphore(1),
    )
    assert dataset_key == "provider_drug"
    assert chunk_manifests[0]["reporting_year"] == 2013
    assert chunk_manifests[0]["source_index"] == 3
    download_file.assert_awaited_once_with(
        "provider_drug",
        {"url": "https://files.invalid/provider.csv", "reporting_year": "bad"},
        str(workspace.downloads_dir),
        True,
        reporting_year=2013,
    )


@pytest.mark.asyncio
async def test_enqueue_chunk_normalizes_exact_job_contract(tmp_path):
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    request = _start_request(redis)
    chunk_manifest_by_field = {
        "dataset_key": "drug_spending",
        "chunk_index": -2,
        "chunk_path": str(tmp_path / "chunk.csv"),
        "reporting_year": 2010,
        "source_index": -1,
    }
    await drug_claims._enqueue_drug_claims_chunk(
        request,
        "mrf",
        chunk_manifest_by_field,
    )

    enqueue_call = redis.enqueue_job.await_args
    assert enqueue_call.args == (
        "drug_claims_process_chunk",
        {
            "import_id": "import-one",
            "run_id": "run-one",
            "stage_suffix": "stage-one",
            "schema": "mrf",
            "test_mode": True,
            "dataset_key": "drug_spending",
            "chunk_id": "drug_spending:2013:0:0",
            "chunk_path": str(tmp_path / "chunk.csv"),
            "reporting_year": 2013,
        },
    )
    assert enqueue_call.kwargs["_job_id"].endswith("drug_spending_2013_0_0")


@pytest.mark.asyncio
async def test_collect_chunks_updates_totals_only_for_nonempty_sources(tmp_path, monkeypatch):
    redis = object()
    request = _start_request(redis)
    workspace = _workspace(tmp_path)
    dataset_configs = (
        drug_claims.DatasetConfig("provider_drug", "unused", 1),
        drug_claims.DatasetConfig("drug_spending", "unused", 1),
    )
    provider_chunk_by_field = {
        "dataset_key": "provider_drug",
        "chunk_index": 0,
        "chunk_path": "provider.csv",
    }
    download_split = AsyncMock(
        side_effect=[
            ("provider_drug", [provider_chunk_by_field]),
            ("drug_spending", []),
        ]
    )
    enqueue_chunk = AsyncMock()
    increment_total = AsyncMock()
    monkeypatch.setattr(drug_claims, "DATASETS", dataset_configs)
    monkeypatch.setattr(drug_claims, "_download_split_drug_claims_source", download_split)
    monkeypatch.setattr(drug_claims, "_enqueue_drug_claims_chunk", enqueue_chunk)
    monkeypatch.setattr(drug_claims, "_increment_total_chunks", increment_total)

    chunk_manifests = await drug_claims._collect_and_enqueue_source_chunks(
        request,
        "mrf",
        {
            "provider_drug": [{"url": "provider"}],
            "drug_spending": [{"url": "spending"}],
        },
        workspace,
    )
    assert chunk_manifests == [provider_chunk_by_field]
    enqueue_chunk.assert_awaited_once_with(request, "mrf", provider_chunk_by_field)
    increment_total.assert_awaited_once_with(redis, "run-one", 1)


@pytest.mark.asyncio
async def test_collect_chunks_cancels_sibling_after_failure(tmp_path, monkeypatch):
    request = _start_request(object())
    workspace = _workspace(tmp_path)
    cancellation_seen = asyncio.Event()
    dataset_configs = (
        drug_claims.DatasetConfig("provider_drug", "unused", 1),
        drug_claims.DatasetConfig("drug_spending", "unused", 1),
    )

    async def download_split(request_arg, workspace_arg, dataset_key, *args):
        if dataset_key == "provider_drug":
            await asyncio.sleep(0)
            raise RuntimeError("split failed")
        try:
            await asyncio.Event().wait()
        finally:
            cancellation_seen.set()

    monkeypatch.setattr(drug_claims, "DATASETS", dataset_configs)
    monkeypatch.setattr(drug_claims, "_download_split_drug_claims_source", download_split)
    with pytest.raises(RuntimeError, match="split failed"):
        await drug_claims._collect_and_enqueue_source_chunks(
            request,
            "mrf",
            {
                "provider_drug": [{"url": "provider"}],
                "drug_spending": [{"url": "spending"}],
            },
            workspace,
        )
    await asyncio.sleep(0)
    assert cancellation_seen.is_set()


@pytest.mark.asyncio
async def test_start_writes_manifest_before_finalize(tmp_path, monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    request = _start_request(redis)
    workspace = _workspace(tmp_path)
    sources_by_dataset = {
        "provider_drug": [{"url": "https://files.invalid/provider.csv"}],
        "drug_spending": [{"url": "https://files.invalid/spending.csv"}],
    }
    chunk_manifests = [
        {
            "dataset_key": "provider_drug",
            "chunk_index": 0,
            "chunk_path": str(tmp_path / "chunk.csv"),
            "source_index": 0,
        },
        {
            "dataset_key": "drug_spending",
            "chunk_index": 0,
            "chunk_path": str(tmp_path / "spending-chunk.csv"),
            "source_index": 0,
        },
    ]
    enqueue_finalize = AsyncMock()
    monkeypatch.setattr(drug_claims, "_drug_claims_start_request", lambda ctx, task: request)
    monkeypatch.setattr(
        drug_claims,
        "_prepare_drug_claims_sources",
        AsyncMock(return_value=("mrf", sources_by_dataset, workspace)),
    )
    monkeypatch.setattr(
        drug_claims,
        "_collect_and_enqueue_source_chunks",
        AsyncMock(return_value=chunk_manifests),
    )
    monkeypatch.setattr(drug_claims, "_enqueue_drug_claims_finalize", enqueue_finalize)
    monkeypatch.setattr(drug_claims, "_mark_drug_claims_chunks_queued", AsyncMock())
    monkeypatch.setattr(drug_claims, "mark_control_run", AsyncMock())
    monkeypatch.setattr(drug_claims, "_step_start", lambda label: 0.0)
    monkeypatch.setattr(drug_claims, "_step_end", lambda label, started_at: None)

    start_result = await drug_claims.drug_claims_start({"redis": redis}, {})
    manifest_fields = drug_claims._read_manifest(start_result["manifest_path"])
    assert start_result["total_chunks"] == 2
    assert manifest_fields["sources"] == sources_by_dataset
    assert manifest_fields["chunks"] == chunk_manifests
    assert manifest_fields["work_dir"] == str(tmp_path)
    enqueue_finalize.assert_awaited_once_with(
        request,
        "mrf",
        Path(start_result["manifest_path"]),
    )


def _drug_source_gap_contract(tmp_path):
    sources_by_dataset = {
        "provider_drug": [{"url": "https://files.invalid/provider.csv"}],
        "drug_spending": [{"url": "https://files.invalid/spending.csv"}],
    }
    provider_chunks = [
        {
            "dataset_key": "provider_drug",
            "chunk_index": 0,
            "chunk_path": str(tmp_path / "provider-chunk.csv"),
            "source_index": 0,
        }
    ]
    return sources_by_dataset, provider_chunks


def _configure_drug_source_gap(
    monkeypatch,
    request,
    workspace,
    sources_by_dataset,
    provider_chunks,
    collaborators,
):
    persist_manifest, enqueue_finalize, mark_control = collaborators
    monkeypatch.setattr(
        drug_claims,
        "_drug_claims_start_request",
        lambda _ctx, _task: request,
    )
    monkeypatch.setattr(
        drug_claims,
        "_prepare_drug_claims_sources",
        AsyncMock(return_value=("mrf", sources_by_dataset, workspace)),
    )
    monkeypatch.setattr(
        drug_claims,
        "_collect_and_enqueue_source_chunks",
        AsyncMock(return_value=provider_chunks),
    )
    monkeypatch.setattr(
        drug_claims,
        "_persist_drug_claims_manifest",
        persist_manifest,
    )
    monkeypatch.setattr(
        drug_claims,
        "_enqueue_drug_claims_finalize",
        enqueue_finalize,
    )
    monkeypatch.setattr(drug_claims, "mark_control_run", mark_control)
    monkeypatch.setattr(drug_claims, "_step_start", lambda _label: 0.0)
    monkeypatch.setattr(
        drug_claims,
        "_step_end",
        lambda _label, _started_at: None,
    )


@pytest.mark.asyncio
async def test_start_refuses_finalize_when_a_required_source_has_no_rows(
    tmp_path,
    monkeypatch,
):
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    collaborators = (AsyncMock(), AsyncMock(), AsyncMock())
    contract_fields = _drug_source_gap_contract(tmp_path)
    _configure_drug_source_gap(
        monkeypatch,
        _start_request(redis),
        _workspace(tmp_path),
        *contract_fields,
        collaborators,
    )
    persist_manifest, enqueue_finalize, mark_control = collaborators

    with pytest.raises(RuntimeError, match="drug_spending:0"):
        await drug_claims.drug_claims_start({"redis": redis}, {})

    persist_manifest.assert_not_awaited()
    enqueue_finalize.assert_not_awaited()
    assert mark_control.await_args.kwargs["status"] == "failed"
    assert mark_control.await_args.kwargs["metrics"] == {
        "missing_sources": ["drug_spending:0"]
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_key", ["provider_drug", "drug_spending"])
async def test_process_chunk_routes_and_reports_progress(
    tmp_path,
    monkeypatch,
    dataset_key,
):
    chunk_path = tmp_path / f"{dataset_key}.csv"
    chunk_path.write_text("header\n", encoding="utf-8")
    redis = object()
    provider_loader = AsyncMock()
    spending_loader = AsyncMock()
    monkeypatch.setattr(drug_claims, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        drug_claims,
        "_staging_classes",
        lambda suffix, schema: {
            "PricingProviderPrescription": "provider-stage",
            "PricingPrescription": "prescription-stage",
        },
    )
    monkeypatch.setattr(drug_claims, "_load_provider_drug_rows", provider_loader)
    monkeypatch.setattr(drug_claims, "_load_drug_spending_rows", spending_loader)
    monkeypatch.setattr(drug_claims, "_mark_chunk_done_with_retry", AsyncMock())
    monkeypatch.setattr(drug_claims, "_get_run_progress", AsyncMock(return_value=(2, 1)))
    live_progress_calls = []
    monkeypatch.setattr(
        drug_claims,
        "enqueue_live_progress",
        lambda **fields: live_progress_calls.append(fields),
    )

    chunk_result = await drug_claims.drug_claims_process_chunk(
        {"redis": redis},
        {
            "dataset_key": dataset_key,
            "chunk_id": "chunk-one",
            "chunk_path": str(chunk_path),
            "run_id": "run-one",
            "reporting_year": 2023,
        },
    )
    assert chunk_result == {
        "ok": True,
        "chunk_id": "chunk-one",
        "dataset_key": dataset_key,
    }
    assert len(live_progress_calls) == 1
    assert live_progress_calls[0]["message"] == "processed 1/2 chunks"
    if dataset_key == "provider_drug":
        provider_loader.assert_awaited_once()
        spending_loader.assert_not_awaited()
    else:
        spending_loader.assert_awaited_once()
        provider_loader.assert_not_awaited()
