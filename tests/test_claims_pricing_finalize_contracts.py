# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from tests.claims_pricing_contract_fakes import RecordingRedis


claims_pricing = importlib.import_module("process.claims_pricing")


def _finalize_spec() -> object:
    return claims_pricing._ClaimsFinalizeSpec(
        "import_a",
        "run-a",
        False,
        "mrf",
        "stage-a",
        0,
    )


def test_cleanup_requires_explicit_existing_work_dir(monkeypatch, tmp_path):
    remove_tree = Mock()
    monkeypatch.setattr(claims_pricing.shutil, "rmtree", remove_tree)
    monkeypatch.setattr(claims_pricing, "CLAIMS_KEEP_WORKDIR", False)
    monkeypatch.setattr(
        claims_pricing,
        "CLAIMS_WORKDIR",
        str(tmp_path / "claims"),
    )
    claims_pricing._cleanup_claims_work_dir({}, _finalize_spec())
    claims_pricing._cleanup_claims_work_dir(
        {"work_dir": str(tmp_path / "missing")},
        _finalize_spec(),
    )
    remove_tree.assert_not_called()


def test_cleanup_honors_retention_flag(monkeypatch, tmp_path):
    remove_tree = Mock()
    monkeypatch.setattr(claims_pricing.shutil, "rmtree", remove_tree)
    monkeypatch.setattr(claims_pricing, "CLAIMS_KEEP_WORKDIR", True)
    claims_pricing._cleanup_claims_work_dir(
        {"work_dir": str(tmp_path)},
        _finalize_spec(),
    )
    remove_tree.assert_not_called()


def test_cleanup_removes_only_manifest_work_dir(monkeypatch, tmp_path):
    work_dir_root = tmp_path / "claims"
    run_work_dir = work_dir_root / "import_a" / "run-a"
    run_work_dir.mkdir(parents=True)
    remove_tree = Mock()
    monkeypatch.setattr(claims_pricing.shutil, "rmtree", remove_tree)
    monkeypatch.setattr(claims_pricing, "CLAIMS_KEEP_WORKDIR", False)
    monkeypatch.setattr(
        claims_pricing,
        "CLAIMS_WORKDIR",
        str(work_dir_root),
    )
    claims_pricing._cleanup_claims_work_dir(
        {"work_dir": str(run_work_dir)},
        _finalize_spec(),
    )
    remove_tree.assert_called_once_with(
        run_work_dir.resolve(),
        ignore_errors=True,
    )


def test_cleanup_rejects_outside_and_symlinked_work_dirs(
    monkeypatch,
    tmp_path,
):
    work_dir_root = tmp_path / "claims"
    expected_work_dir = work_dir_root / "import_a" / "run-a"
    outside_work_dir = tmp_path / "outside"
    outside_work_dir.mkdir()
    expected_work_dir.parent.mkdir(parents=True)
    expected_work_dir.symlink_to(outside_work_dir, target_is_directory=True)
    remove_tree = Mock()
    monkeypatch.setattr(claims_pricing.shutil, "rmtree", remove_tree)
    monkeypatch.setattr(claims_pricing, "CLAIMS_KEEP_WORKDIR", False)
    monkeypatch.setattr(
        claims_pricing,
        "CLAIMS_WORKDIR",
        str(work_dir_root),
    )

    claims_pricing._cleanup_claims_work_dir(
        {"work_dir": str(outside_work_dir)},
        _finalize_spec(),
    )
    claims_pricing._cleanup_claims_work_dir(
        {"work_dir": str(expected_work_dir)},
        _finalize_spec(),
    )

    remove_tree.assert_not_called()
    assert outside_work_dir.is_dir()


def test_finalize_manifest_rejects_empty_and_partial_source_handoffs(
    monkeypatch,
):
    finalize_spec = _finalize_spec()
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (
            claims_pricing.DatasetConfig("provider", "provider", 1),
            claims_pricing.DatasetConfig("geo_service", "geo", 1),
        ),
    )

    with pytest.raises(RuntimeError, match="nonempty chunk manifest"):
        claims_pricing._validate_claims_finalize_manifest(
            {},
            finalize_spec,
        )

    partial_manifest_by_field = {
        "import_id": "import_a",
        "run_id": "run-a",
        "stage_suffix": "stage-a",
        "total_chunks": 1,
        "sources": {
            "provider": [{"url": "https://example.test/provider.csv"}],
            "geo_service": [{"url": "https://example.test/geo.csv"}],
        },
        "chunks": [
            {
                "dataset_key": "provider",
                "source_index": 0,
            }
        ],
    }
    with pytest.raises(RuntimeError, match="geo_service:0"):
        claims_pricing._validate_claims_finalize_manifest(
            partial_manifest_by_field,
            finalize_spec,
        )


@pytest.mark.asyncio
async def test_record_finalized_skips_missing_runtime_identity():
    redis = RecordingRedis()
    blank_run = claims_pricing._ClaimsFinalizeSpec("i", "", False, "mrf", "stage", 0)
    await claims_pricing._record_claims_finalized(None, blank_run)
    await claims_pricing._record_claims_finalized(redis, blank_run)
    assert redis.values_by_key == {}


@pytest.mark.asyncio
async def test_record_finalized_sets_value_and_ttl():
    redis = RecordingRedis()
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "run-a", False, "mrf", "stage", 0)
    await claims_pricing._record_claims_finalized(redis, finalize_spec)
    finalized_key = "claims_pricing:run-a:finalized"
    assert redis.values_by_key[finalized_key] == "1"
    assert [key for key, _seconds in redis.expired_keys].count(finalized_key) == 2


@pytest.mark.asyncio
async def test_finalize_returns_idempotent_response(monkeypatch):
    redis = RecordingRedis()
    redis.values_by_key["claims_pricing:run-a:finalized"] = "1"
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    response_by_field = await claims_pricing.claims_pricing_finalize(
        {"redis": redis},
        {"import_id": "import-a", "run_id": "run-a", "stage_suffix": "stage-a"},
    )
    assert response_by_field == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run-a",
        "import_id": "import_a",
    }


@pytest.mark.asyncio
async def test_finalize_publishes_diagnostics_and_cleans_manifest(
    monkeypatch,
    tmp_path,
):
    diagnostics_by_field = {
        "profile_scope_rows": [{"geography_scope": "national", "rows": 2}],
        "peer_scope_rows": [{"geography_scope": "national", "rows": 1}],
        "key_coverage": [
            {"geography_scope": "national", "coverage_pct": 50.0}
        ],
    }
    work_dir_root, run_work_dir, manifest_path = (
        _complete_claims_manifest_paths(tmp_path)
    )
    _configure_successful_claims_finalize(
        monkeypatch,
        work_dir_root,
        diagnostics_by_field,
    )
    redis = RecordingRedis()

    response_by_field = await claims_pricing.claims_pricing_finalize(
        {"redis": redis},
        {
            "import_id": "import-a",
            "manifest_path": str(manifest_path),
            "schema": "mrf",
        },
    )

    assert response_by_field["cost_level_diagnostics"] == (
        diagnostics_by_field
    )
    assert response_by_field["run_id"] == "run-a"
    assert not run_work_dir.exists()


def _complete_claims_manifest_paths(
    tmp_path,
) -> tuple[Path, Path, Path]:
    work_dir_root = tmp_path / "claims"
    run_work_dir = work_dir_root / "import_a" / "run-a"
    run_work_dir.mkdir(parents=True)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "import_id": "import_a",
                "run_id": "run-a",
                "stage_suffix": "stage-a",
                "sources": {
                    "provider": [
                        {"url": "https://example.test/provider.csv"}
                    ]
                },
                "chunks": [
                    {
                        "dataset_key": "provider",
                        "source_index": 0,
                    }
                ],
                "total_chunks": 1,
                "work_dir": str(run_work_dir),
            }
        )
    )
    return work_dir_root, run_work_dir, manifest_path


def _configure_successful_claims_finalize(
    monkeypatch,
    work_dir_root: Path,
    diagnostics_by_field: dict,
) -> None:
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "provider", 1),),
    )
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "_wait_for_claims_finalize_turn",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(claims_pricing, "_staging_classes", lambda *_args: {"stage": object()})
    monkeypatch.setattr(
        claims_pricing,
        "_materialize_and_publish_claims",
        AsyncMock(return_value=diagnostics_by_field),
    )
    monkeypatch.setattr(claims_pricing, "mark_control_run", AsyncMock())
    monkeypatch.setattr(claims_pricing, "CLAIMS_WORKDIR", str(work_dir_root))
    monkeypatch.setattr(claims_pricing, "CLAIMS_KEEP_WORKDIR", False)


@pytest.mark.asyncio
async def test_finalize_with_redis_records_terminal_state(monkeypatch):
    redis = RecordingRedis()
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "r", False, "mrf", "s", 0)
    monkeypatch.setattr(
        claims_pricing,
        "_claims_finalize_spec",
        lambda *_args: finalize_spec,
    )
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "_validate_claims_finalize_manifest",
        Mock(),
    )
    monkeypatch.setattr(claims_pricing, "_wait_for_claims_finalize_turn", AsyncMock(return_value=None))
    monkeypatch.setattr(claims_pricing, "_staging_classes", lambda *_args: {})
    monkeypatch.setattr(claims_pricing, "_materialize_and_publish_claims", AsyncMock(return_value={}))
    monkeypatch.setattr(claims_pricing, "_mark_claims_succeeded", AsyncMock())
    await claims_pricing.claims_pricing_finalize({"redis": redis}, {})
    assert redis.values_by_key["claims_pricing:r:finalized"] == "1"
    assert "imports:finalize_mutex" not in redis.values_by_key


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
    redis = RecordingRedis()
    redis.eval = AsyncMock(side_effect=RuntimeError("global release failed"))
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "r",
        False,
        "mrf",
        "s",
        0,
    )
    release_lock = AsyncMock()
    monkeypatch.setattr(
        claims_pricing,
        "_claims_finalize_spec",
        lambda *_args: finalize_spec,
    )
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "_validate_claims_finalize_manifest",
        Mock(),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_wait_for_claims_finalize_turn",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_staging_classes",
        lambda *_args: {},
    )
    monkeypatch.setattr(
        claims_pricing,
        "_materialize_and_publish_claims",
        AsyncMock(side_effect=finalize_failure),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_release_claims_finalize_lock_safely",
        release_lock,
    )

    with pytest.raises(
        type(finalize_failure),
        match=re.escape(str(finalize_failure)),
    ):
        await claims_pricing.claims_pricing_finalize(
            {"redis": redis},
            {},
        )

    release_lock.assert_awaited_once_with(redis, finalize_spec)


@pytest.mark.asyncio
async def test_legacy_split_helper_attaches_reporting_year(monkeypatch, tmp_path):
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (
            claims_pricing.DatasetConfig("provider", "provider", 1),
            claims_pricing.DatasetConfig("geo_service", "geo", 1),
        ),
    )
    split_source = AsyncMock(
        side_effect=[
            [{"dataset_key": "provider", "chunk_path": "provider.csv"}],
            [{"dataset_key": "geo_service", "chunk_path": "geo.csv"}],
        ]
    )
    monkeypatch.setattr(claims_pricing, "_split_source_into_chunks", split_source)
    chunk_entries = await claims_pricing._split_sources_to_chunks(
        {"provider": "provider.csv", "geo_service": "geo.csv"},
        tmp_path,
        {
            "provider": [{"reporting_year": "2023"}],
            "geo_service": [],
        },
        test_mode=False,
    )
    assert [chunk_entry["reporting_year"] for chunk_entry in chunk_entries] == [2023, 2013]


@pytest.mark.asyncio
async def test_run_timed_step_reports_completion(monkeypatch):
    completed_steps = []

    async def operation():
        completed_steps.append("operation")

    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 5.0)
    step_end = Mock()
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    await claims_pricing._run_timed_step("synthetic", operation())
    assert completed_steps == ["operation"]
    step_end.assert_called_once_with("synthetic", 5.0)


@pytest.mark.asyncio
async def test_run_timed_step_reports_failure_without_completion(monkeypatch):
    step_end = Mock()
    step_failed = Mock()
    failure = RuntimeError("synthetic failure")
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 5.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    monkeypatch.setattr(claims_pricing, "_step_failed", step_failed)

    async def failed_operation():
        raise failure

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await claims_pricing._run_timed_step(
            "synthetic",
            failed_operation(),
        )

    step_end.assert_not_called()
    step_failed.assert_called_once_with("synthetic", 5.0, failure)


@pytest.mark.asyncio
async def test_finish_main_includes_explicit_manifest(monkeypatch):
    redis = RecordingRedis()
    monkeypatch.setattr(claims_pricing, "_create_claims_pool", AsyncMock(return_value=redis))
    response_by_field = await claims_pricing.finish_main(
        "import-a",
        "run-a",
        test_mode=True,
        manifest_path="/tmp/manifest.json",
    )
    assert response_by_field["import_id"] == "import_a"
    assert redis.jobs[0]["task"]["manifest_path"] == "/tmp/manifest.json"


@pytest.mark.asyncio
async def test_finish_main_derives_canonical_manifest(monkeypatch, tmp_path):
    redis = RecordingRedis()
    monkeypatch.setattr(
        claims_pricing,
        "_create_claims_pool",
        AsyncMock(return_value=redis),
    )
    monkeypatch.setattr(
        claims_pricing,
        "CLAIMS_WORKDIR",
        str(tmp_path),
    )

    await claims_pricing.finish_main(
        "import-a",
        "run-a",
        test_mode=True,
    )

    assert redis.jobs[0]["task"]["manifest_path"] == str(
        tmp_path / "import_a" / "run-a" / "manifest.json"
    )


def test_staging_classes_use_schema_override(monkeypatch):
    calls = []

    def make_stage(base_cls, suffix, schema_override):
        calls.append((base_cls.__name__, suffix, schema_override))
        return object()

    monkeypatch.setattr(claims_pricing, "make_class", make_stage)
    classes_by_name = claims_pricing._staging_classes("stage-a", "mrf")
    assert len(classes_by_name) == 8
    assert all(suffix == "stage-a" and schema == "mrf" for _name, suffix, schema in calls)
