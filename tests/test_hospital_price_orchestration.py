# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused pipeline and lifecycle proof for hospital-price orchestration."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.hospital_price_orchestration_support import (
    ROOT,
    ArtifactStore,
    Attempt,
    DownloadedSource,
    configure_incomplete_import,
    orchestrator_module,
)


_ArtifactStore = ArtifactStore
_Attempt = Attempt
_DownloadedSource = DownloadedSource
_orchestrator_module = orchestrator_module


def test_locator_groups_duplicate_urls_once():
    orchestrator = _orchestrator_module()
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "c", "name": "C", "cms_hpt_url": "https://c/locator"},
    )

    groups = orchestrator._locator_groups(hospitals)

    assert [group[0] for group in groups] == ["https://a/locator", "https://c/locator"]
    assert [hospital["hospital_id"] for hospital in groups[0][1]] == ["a", "b"]


@pytest.mark.asyncio
async def test_source_attempts_group_equivalent_canonical_urls(monkeypatch):
    orchestrator = _orchestrator_module()
    attempts = (
        _Attempt(
            "one", "a", "A",
            "https://example.test/prices.json?X-Amz-Signature=first", 0,
        ),
        _Attempt(
            "two", "b", "B",
            "https://example.test/prices.json?X-Amz-Signature=second", 0,
        ),
    )
    candidates = tuple(
        SimpleNamespace(
            hospital_id=attempt.hospital_id,
            initial_error_code=None,
            initial_error_detail=None,
        )
        for attempt in attempts
    )

    monkeypatch.setattr(orchestrator, "candidates_from_locators", lambda _rows: candidates)

    async def start(*_args: Any) -> tuple[tuple[Any, ...], int]:
        return attempts, 0

    monkeypatch.setattr(orchestrator, "_start_attempts", start)
    grouped, active, failed = await orchestrator._resolve_attempts(
        (), [], "hospital-prices:test", 300
    )

    assert list(grouped) == [attempts[0].source_url]
    assert grouped[attempts[0].source_url] == list(attempts)
    assert (active, failed) == (0, 0)

    stable_url = "https://example.test/stable.json"
    ordered = orchestrator._ordered_source_jobs(
        {stable_url: [attempts[0]], attempts[1].source_url: [attempts[1]]}
    )
    assert [url for url, _grouped_attempts in ordered] == [
        attempts[1].source_url,
        stable_url,
    ]


@pytest.mark.asyncio
async def test_locator_download_uses_deleted_private_store(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    roots: list[Path] = []
    lease_roots: list[Path] = []

    @contextlib.contextmanager
    def lease_context(*, owner: str, store: Any):
        assert owner == "hospital-prices:test"
        lease_roots.append(Path(store.root))
        yield object()

    async def fetch(_item: Any, source_store: Any) -> str:
        root = Path(source_store.root)
        roots.append(root)
        (root / "raw").mkdir()
        (root / "raw" / "cms-hpt.txt").write_text("https://example.test/mrf.json")
        return "parsed locator"

    monkeypatch.setattr(orchestrator, "fetch_locator", fetch)
    monkeypatch.setattr(orchestrator, "artifact_lease_context", lease_context)

    assert await orchestrator._fetch_transient_locator(
        ("https://example.test/cms-hpt.txt", ()),
        _ArtifactStore(tmp_path),
        "hospital-prices:test",
    ) == "parsed locator"
    assert len(roots) == 1
    assert lease_roots == roots
    assert not roots[0].exists()


@pytest.mark.asyncio
async def test_expired_source_refreshes_matching_locator_once(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    expired_url = "https://files.example/prices.json?sig=expired"
    fresh_url = "https://files.example/prices.json?sig=fresh"
    locator_url = "https://hospital.example/cms-hpt.txt"
    attempt = _Attempt(
        "one", "a", "Hospital A", expired_url, 0,
        locator_name="Hospital A", locator_url=locator_url,
        source_http_status=403,
    )
    locator_result = SimpleNamespace(
        url=locator_url,
        locator_id="locator-a",
        observation_id="observation-a",
        hospitals=({
            "hospital_id": "a", "name": "Hospital A",
            "cms_hpt_url": locator_url,
        },),
        records=(SimpleNamespace(mrf_url=fresh_url),),
        error_code=None,
        error_detail=None,
    )
    fetch = AsyncMock(return_value=locator_result)
    rebind = AsyncMock()
    monkeypatch.setattr(orchestrator, "fetch_locator", fetch)
    monkeypatch.setattr(orchestrator, "rebind_attempt_sources", rebind)
    monkeypatch.setattr(
        orchestrator,
        "candidates_from_locators",
        lambda results: (
            SimpleNamespace(
                hospital_id="a", source_url=results[0].records[0].mrf_url,
                locator_id=results[0].locator_id,
                observation_id=results[0].observation_id,
                locator_name="Hospital A",
                locator_url=results[0].url,
                initial_error_code=None,
            ),
        ),
    )

    refreshed = await orchestrator._refreshed_source_job(
        (expired_url, (attempt,)), _ArtifactStore(tmp_path)
    )

    assert refreshed == (fresh_url, (attempt,))
    fetch.assert_awaited_once()
    rebind.assert_awaited_once()
    assert rebind.await_args.args[0][0][0] is attempt
    assert attempt.source_url == fresh_url

    locator_result.records = (
        SimpleNamespace(mrf_url="https://other.example/prices.json"),
    )
    assert await orchestrator._refreshed_source_job(
        (expired_url, (attempt,)), _ArtifactStore(tmp_path)
    ) is None
    assert rebind.await_count == 1


@pytest.mark.asyncio
async def test_source_refresh_requires_every_attempt_binding(tmp_path, monkeypatch):
    """Do not share refreshed credentials with an unresolved hospital."""

    orchestrator = _orchestrator_module()
    expired_url = "https://files.example/prices.json?sig=expired"
    locator_url = "https://hospital.example/cms-hpt.txt"
    attempts = tuple(
        _Attempt(
            f"attempt-{hospital_id}", hospital_id, f"Hospital {hospital_id.upper()}",
            expired_url, 0, locator_name=f"Hospital {hospital_id.upper()}",
            locator_url=locator_url, source_http_status=403,
        )
        for hospital_id in ("a", "b")
    )
    locator_result = SimpleNamespace(
        url=locator_url, locator_id="locator", observation_id="fresh-observation",
        hospitals=(), records=(SimpleNamespace(
            mrf_url="https://files.example/prices.json?sig=fresh"
        ),), error_code=None, error_detail=None,
    )
    monkeypatch.setattr(
        orchestrator, "fetch_locator", AsyncMock(return_value=locator_result)
    )
    monkeypatch.setattr(
        orchestrator, "candidates_from_locators", lambda _results: (
            SimpleNamespace(hospital_id="a", initial_error_code=None),
            SimpleNamespace(hospital_id="b", initial_error_code="locator_unmatched"),
        )
    )
    rebind = AsyncMock()
    monkeypatch.setattr(orchestrator, "rebind_attempt_sources", rebind)

    refreshed = await orchestrator._refreshed_source_job(
        (expired_url, attempts), _ArtifactStore(tmp_path)
    )

    assert refreshed is None
    rebind.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_worker_queues_only_refreshed_source(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    expired_url = "https://files.example/prices.json?sig=expired"
    fresh_url = "https://files.example/prices.json?sig=fresh"
    attempt = _Attempt("one", "a", "Hospital A", expired_url, 0)
    raw = SimpleNamespace(raw_sha256="a" * 64)
    calls: list[str] = []

    async def download(source_job, _store, _max_bytes, **kwargs):
        url, attempts = source_job
        calls.append(url)
        if url == expired_url:
            return _DownloadedSource(
                url, None, attempts, "permission", "expired",
                auth_refresh_required=True,
            )
        assert kwargs == {"exact_url_only": True}
        return _DownloadedSource(url, raw, attempts)

    monkeypatch.setattr(orchestrator, "download_source", download)
    monkeypatch.setattr(
        orchestrator, "_refreshed_source_job",
        AsyncMock(return_value=(fresh_url, (attempt,))),
    )
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    source_jobs, downloads = asyncio.Queue(), asyncio.Queue()
    source_jobs.put_nowait((expired_url, (attempt,)))
    source_jobs.put_nowait(None)

    worker = asyncio.create_task(orchestrator._download_worker(
        source_jobs, downloads, _ArtifactStore(tmp_path),
        "hospital-prices:test", 1024, 1,
    ))
    downloaded, acknowledgement = await asyncio.wait_for(
        downloads.get(), timeout=1
    )
    assert downloaded.raw is raw
    assert calls == [expired_url, fresh_url]
    assert downloads.empty()
    acknowledgement.set_result(None)
    await asyncio.wait_for(worker, timeout=1)


@pytest.mark.asyncio
async def test_parser_failure_cleans_private_directory(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    output_paths: list[Path] = []

    async def has_version(*_args: Any) -> bool:
        return False

    async def failed_parser(_source: Path, output: Path, *_args: Any) -> None:
        output_paths.append(output)
        (output / "partial.copy").write_bytes(b"partial")
        raise ValueError("invalid MRF")

    monkeypatch.setattr(orchestrator, "has_existing_version", has_version)
    monkeypatch.setattr(orchestrator, "run_native_parser", failed_parser)
    source_path = tmp_path / "hospital-mrf-source-test" / "raw" / "source.json"
    source_path.parent.mkdir(parents=True)
    source_path.write_text("{}")
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(source_path), byte_count=2
    )
    with pytest.raises(ValueError, match="invalid MRF"):
        await orchestrator._ensure_content(
            {}, {}, _ArtifactStore(tmp_path), raw, 2048, 1024
        )

    assert len(output_paths) == 1
    assert not output_paths[0].exists()


@pytest.mark.asyncio
async def test_existing_content_skips_native_parser(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    monkeypatch.setattr(
        orchestrator, "has_existing_version", AsyncMock(return_value=True)
    )
    parser = AsyncMock(side_effect=AssertionError("parser must not run"))
    monkeypatch.setattr(orchestrator, "run_native_parser", parser)
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
    )

    assert await orchestrator._ensure_content(
        {}, {}, _ArtifactStore(tmp_path), raw, 2048, 1024
    ) == orchestrator.hospital_price_version_id(raw.raw_sha256)
    parser.assert_not_awaited()


def test_control_plane_wiring_names_one_dedicated_queue():
    process_source = (ROOT / "process/__init__.py").read_text()
    worker_source = (ROOT / "api/control_workers.py").read_text()
    imports_source = (ROOT / "api/control_imports.py").read_text()

    assert "class HospitalPrices:" in process_source
    assert 'queue_name = "arq:HospitalPrices"' in process_source
    assert 'process_group.add_command(hospital_prices, name="hospital-prices")' in process_source
    assert 'WorkerSpec("arq:HospitalPrices", "process.HospitalPrices"' in worker_source
    assert '"hospital-prices": {' in imports_source
    assert '"queue": "arq:HospitalPrices"' in imports_source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolved_failures", "pipeline_metrics", "expected_metrics"),
    (
        (1, {"processed": 1, "published": 1, "superseded": 0, "unchanged": 0,
             "failed": 0, "contents": 1}, {"published": 1, "failed": 1}),
        (1, {"processed": 1, "published": 0, "superseded": 0, "unchanged": 0,
             "failed": 1, "contents": 0}, {"published": 0, "failed": 2}),
        (0, {"processed": 2, "published": 1, "superseded": 1, "unchanged": 0,
             "failed": 0, "contents": 1}, {"published": 1, "superseded": 1}),
    ),
)
async def test_bulk_import_rejects_every_incomplete_selected_cohort(
    tmp_path, monkeypatch, resolved_failures, pipeline_metrics, expected_metrics
):
    orchestrator = _orchestrator_module()
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://b/locator"},
    )

    configure_incomplete_import(
        orchestrator, monkeypatch, resolved_failures, pipeline_metrics
    )
    collect = AsyncMock(return_value=0)
    monkeypatch.setattr(
        orchestrator, "garbage_collect_superseded_versions", collect
    )
    context_by_field: dict[str, Any] = {}
    with pytest.raises(RuntimeError, match="did not complete every selected hospital"):
        await orchestrator._run_import(
            context_by_field, {}, hospitals, _ArtifactStore(tmp_path), [],
            "hospital-prices:test", 300,
        )
    actual_metrics = context_by_field["context"]["hospital_price_metrics"]
    assert {name: actual_metrics[name] for name in expected_metrics} == expected_metrics
    collect.assert_awaited_once_with()
