# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-flow contracts for provider-quality ingestion and finalization."""

from __future__ import annotations

import importlib
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

provider_quality = importlib.import_module("process.provider_quality")


class _FinalizeRedis:
    def __init__(self, finalized: bool = False) -> None:
        self.value_by_key: dict[str, str] = {}
        self.expirations: list[tuple[str, int]] = []
        if finalized:
            self.value_by_key["finalized"] = "1"

    async def get(self, key: str):
        if "finalized" in key and "finalized" in self.value_by_key:
            return self.value_by_key["finalized"]
        return self.value_by_key.get(key)

    async def set(self, key: str, value: str, *, ex: int):
        self.value_by_key[key] = value
        self.expirations.append((key, ex))

    async def expire(self, key: str, ttl: int):
        self.expirations.append((key, ttl))


def _patch_finalize_common(monkeypatch) -> dict[str, AsyncMock]:
    calls_by_name = {
        "ensure": AsyncMock(),
        "materialize": AsyncMock(),
        "materialize_sharded": AsyncMock(),
        "indexes": AsyncMock(),
        "publish": AsyncMock(),
        "metadata": AsyncMock(),
        "control": AsyncMock(),
        "failed": AsyncMock(),
        "release": AsyncMock(),
    }
    monkeypatch.setattr(provider_quality, "ensure_database", calls_by_name["ensure"])
    monkeypatch.setattr(provider_quality, "_staging_classes", lambda *_args: {})
    monkeypatch.setattr(provider_quality, "_materialize_quality_rows", calls_by_name["materialize"])
    monkeypatch.setattr(
        provider_quality,
        "_materialize_quality_rows_sharded",
        calls_by_name["materialize_sharded"],
    )
    monkeypatch.setattr(provider_quality, "_build_staging_indexes", calls_by_name["indexes"])
    monkeypatch.setattr(provider_quality, "_publish_by_table_rename", calls_by_name["publish"])
    monkeypatch.setattr(provider_quality, "_insert_run_metadata", calls_by_name["metadata"])
    monkeypatch.setattr(provider_quality, "mark_control_run", calls_by_name["control"])
    monkeypatch.setattr(
        provider_quality,
        "_mark_provider_quality_finalize_failed",
        calls_by_name["failed"],
    )
    monkeypatch.setattr(
        provider_quality,
        "_release_global_finalize_lock",
        calls_by_name["release"],
    )
    monkeypatch.setattr(provider_quality, "_step_start", lambda _label: 0.0)
    monkeypatch.setattr(provider_quality, "_step_end", lambda *_args: None)
    return calls_by_name


@pytest.mark.asyncio
async def test_process_chunk_rejects_missing_identity_and_file(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="missing required fields"):
        await provider_quality.provider_quality_process_chunk({}, {})
    with pytest.raises(RuntimeError, match="does not exist"):
        await provider_quality.provider_quality_process_chunk(
            {},
            {
                "dataset_key": "qpp_provider",
                "chunk_id": "chunk",
                "chunk_path": str(tmp_path / "missing.csv"),
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dataset_key", "loader_name", "model_name"),
    (
        ("qpp_provider", "_load_qpp_rows", "PricingQppProvider"),
        ("svi_zcta", "_load_svi_rows", "PricingSviZcta"),
    ),
)
async def test_process_chunk_routes_supported_dataset(
    monkeypatch,
    tmp_path,
    dataset_key,
    loader_name,
    model_name,
) -> None:
    chunk_path = tmp_path / f"{dataset_key}.csv"
    chunk_path.write_text("header\n", encoding="utf-8")
    loader = AsyncMock()
    monkeypatch.setattr(provider_quality, "ensure_database", AsyncMock())
    monkeypatch.setattr(provider_quality, loader_name, loader)
    monkeypatch.setattr(
        provider_quality,
        "_staging_classes",
        lambda *_args: {model_name: object},
    )

    chunk_result = await provider_quality.provider_quality_process_chunk(
        {},
        {
            "dataset_key": dataset_key,
            "chunk_id": "chunk",
            "chunk_path": str(chunk_path),
            "reporting_year": "bad",
            "test_mode": True,
        },
    )

    assert chunk_result["dataset_key"] == dataset_key
    loader.assert_awaited_once_with(
        str(chunk_path),
        object,
        provider_quality.PROVIDER_QUALITY_MIN_YEAR,
        test_mode=True,
    )


@pytest.mark.asyncio
async def test_process_chunk_reports_progress_and_rejects_unknown_dataset(
    monkeypatch,
    tmp_path,
) -> None:
    chunk_path = tmp_path / "chunk.csv"
    chunk_path.write_text("header\n", encoding="utf-8")
    redis = _FinalizeRedis()
    progress_events: list[dict[str, object]] = []
    monkeypatch.setattr(provider_quality, "ensure_database", AsyncMock())
    monkeypatch.setattr(provider_quality, "_staging_classes", lambda *_args: {})
    monkeypatch.setattr(provider_quality, "_mark_chunk_done_with_retry", AsyncMock())
    monkeypatch.setattr(
        provider_quality,
        "_get_run_progress",
        AsyncMock(return_value=(4, 2)),
    )
    monkeypatch.setattr(
        provider_quality,
        "enqueue_live_progress",
        lambda **event: progress_events.append(event),
    )

    with pytest.raises(RuntimeError, match="Unsupported dataset_key"):
        await provider_quality.provider_quality_process_chunk(
            {"redis": redis},
            {
                "dataset_key": "unknown",
                "chunk_id": "chunk",
                "chunk_path": str(chunk_path),
                "run_id": "run",
            },
        )

    monkeypatch.setattr(provider_quality, "_load_qpp_rows", AsyncMock())
    monkeypatch.setattr(
        provider_quality,
        "_staging_classes",
        lambda *_args: {"PricingQppProvider": object},
    )
    await provider_quality.provider_quality_process_chunk(
        {"redis": redis},
        {
            "dataset_key": "qpp_provider",
            "chunk_id": "chunk",
            "chunk_path": str(chunk_path),
            "run_id": "run",
        },
    )
    assert progress_events[-1]["done"] == 2


@pytest.mark.asyncio
async def test_finalize_returns_existing_terminal_state(monkeypatch) -> None:
    redis = _FinalizeRedis(finalized=True)
    _patch_finalize_common(monkeypatch)

    finalize_result = await provider_quality.provider_quality_finalize(
        {"redis": redis},
        {"run_id": "run", "import_id": "import"},
    )

    assert finalize_result == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run",
        "import_id": "import",
    }


@pytest.mark.asyncio
async def test_finalize_waits_for_chunks_and_each_lock(monkeypatch) -> None:
    redis = _FinalizeRedis()
    _patch_finalize_common(monkeypatch)
    monkeypatch.setattr(
        provider_quality,
        "_get_run_progress",
        AsyncMock(return_value=(3, 2)),
    )
    with pytest.raises(provider_quality.Retry):
        await provider_quality.provider_quality_finalize(
            {"redis": redis},
            {"run_id": "run", "total_chunks": 3},
        )

    monkeypatch.setattr(
        provider_quality,
        "_get_run_progress",
        AsyncMock(return_value=(3, 3)),
    )
    monkeypatch.setattr(
        provider_quality,
        "_has_finalize_lock",
        AsyncMock(return_value=False),
    )
    with pytest.raises(provider_quality.Retry):
        await provider_quality.provider_quality_finalize(
            {"redis": redis},
            {"run_id": "run", "total_chunks": 3},
        )

    monkeypatch.setattr(
        provider_quality,
        "_has_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        provider_quality,
        "_has_global_finalize_lock",
        AsyncMock(return_value=False),
    )
    with pytest.raises(provider_quality.Retry):
        await provider_quality.provider_quality_finalize(
            {"redis": redis},
            {"run_id": "run", "total_chunks": 3},
        )


@pytest.mark.asyncio
async def test_finalize_publishes_sharded_degraded_run_and_cleans_workdir(
    monkeypatch,
    tmp_path,
) -> None:
    redis = _FinalizeRedis()
    calls_by_name = _patch_finalize_common(monkeypatch)
    work_dir = tmp_path / "run"
    work_dir.mkdir()
    manifest_by_field = {
        "run_id": "manifest-run",
        "stage_suffix": "manifest-stage",
        "total_chunks": 2,
        "degraded_sources": [{"dataset_key": "qpp_provider"}],
        "work_dir": str(work_dir),
    }
    monkeypatch.setattr(provider_quality, "_read_manifest", lambda _path: manifest_by_field)
    monkeypatch.setattr(
        provider_quality,
        "_get_run_progress",
        AsyncMock(return_value=(2, 2)),
    )
    monkeypatch.setattr(provider_quality, "_has_finalize_lock", AsyncMock(return_value=True))
    monkeypatch.setattr(
        provider_quality,
        "_has_global_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(provider_quality, "_cohort_models_present", lambda _classes: True)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", True)
    monkeypatch.setattr(
        provider_quality,
        "PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED",
        True,
    )
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_DEFER_STAGE_INDEXES", True)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_KEEP_WORKDIR", False)

    finalize_result = await provider_quality.provider_quality_finalize(
        {"redis": redis},
        {"import_id": "import", "manifest_path": "manifest.json"},
    )

    assert finalize_result["run_id"] == "manifest-run"
    assert finalize_result["stage_suffix"] == "manifest-stage"
    assert not work_dir.exists()
    calls_by_name["materialize_sharded"].assert_awaited_once()
    calls_by_name["indexes"].assert_awaited_once()
    assert calls_by_name["metadata"].await_args.kwargs["status"] == "degraded_test"
    calls_by_name["release"].assert_awaited_once_with(redis, "manifest-run")


@pytest.mark.asyncio
@pytest.mark.parametrize("redis", (None, _FinalizeRedis()))
async def test_finalize_requires_identity_for_sharded_materialization(
    monkeypatch,
    redis,
) -> None:
    calls_by_name = _patch_finalize_common(monkeypatch)
    monkeypatch.setattr(provider_quality, "_cohort_models_present", lambda _classes: True)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", True)
    monkeypatch.setattr(
        provider_quality,
        "PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED",
        True,
    )

    with pytest.raises(RuntimeError, match="requires redis and run_id"):
        await provider_quality.provider_quality_finalize({"redis": redis}, {})

    calls_by_name["failed"].assert_awaited_once()


@pytest.mark.asyncio
async def test_finalize_retry_releases_global_lock_without_failure_mark(
    monkeypatch,
) -> None:
    redis = _FinalizeRedis()
    calls_by_name = _patch_finalize_common(monkeypatch)
    monkeypatch.setattr(
        provider_quality,
        "_get_run_progress",
        AsyncMock(return_value=(0, 0)),
    )
    monkeypatch.setattr(provider_quality, "_has_finalize_lock", AsyncMock(return_value=True))
    monkeypatch.setattr(
        provider_quality,
        "_has_global_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        provider_quality,
        "_materialize_quality_rows",
        AsyncMock(side_effect=provider_quality.Retry(defer=1)),
    )
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", False)

    with pytest.raises(provider_quality.Retry):
        await provider_quality.provider_quality_finalize(
            {"redis": redis},
            {"run_id": "run"},
        )

    calls_by_name["failed"].assert_not_awaited()
    calls_by_name["release"].assert_awaited_once_with(redis, "run")


@pytest.mark.asyncio
async def test_finalize_unsharded_publication_preserves_empty_manifest(monkeypatch) -> None:
    calls_by_name = _patch_finalize_common(monkeypatch)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", False)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_DEFER_STAGE_INDEXES", False)

    finalize_result = await provider_quality.provider_quality_finalize(
        {},
        {"run_id": "run", "stage_suffix": "stage", "schema": "mrf"},
    )

    assert finalize_result["ok"] is True
    calls_by_name["materialize"].assert_awaited_once_with({}, "mrf", "run")
    assert calls_by_name["metadata"].await_args.kwargs["status"] == "published"
    calls_by_name["indexes"].assert_not_awaited()


def test_source_policy_and_rx_table_name_boundaries(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_quality,
        "PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY",
        True,
    )
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR", True)
    assert not provider_quality._should_fail_on_source_error(True)
    assert provider_quality._should_fail_on_source_error(False)
    assert (
        provider_quality._provider_quality_rx_agg_table(
            {
                "PricingQppProvider": type(
                    "Qpp",
                    (),
                    {"__tablename__": "pricing_qpp_provider_stage"},
                )
            }
        )
        == "pricing_provider_quality_rx_agg_stage"
    )
    assert provider_quality._provider_quality_rx_agg_table({}) == (
        "pricing_provider_quality_rx_agg"
    )


@pytest.mark.asyncio
async def test_rx_aggregate_skips_empty_unavailable_and_existing_cases(
    monkeypatch,
) -> None:
    table_available = AsyncMock(side_effect=(False, True))
    monkeypatch.setattr(provider_quality, "_is_table_available", table_available)
    monkeypatch.setattr(provider_quality.db, "status", AsyncMock())
    monkeypatch.setattr(provider_quality.db, "scalar", AsyncMock(return_value=2))

    await provider_quality._ensure_provider_quality_rx_agg_table({}, "mrf", ())
    await provider_quality._ensure_provider_quality_rx_agg_table({}, "mrf", (2024,))
    await provider_quality._ensure_provider_quality_rx_agg_table({}, "mrf", (2024,))

    provider_quality.db.scalar.assert_awaited_once()
    assert any(
        "ANALYZE" in call.args[0]
        for call in provider_quality.db.status.await_args_list
    )


@pytest.mark.asyncio
async def test_split_source_handles_empty_and_bounded_test_rows(
    monkeypatch,
    tmp_path,
) -> None:
    empty_path = tmp_path / "empty.csv"
    empty_path.write_text("", encoding="utf-8")
    assert await provider_quality._split_source_into_chunks(
        "unknown",
        str(empty_path),
        tmp_path / "empty-chunks",
        False,
    ) == []

    source_path = tmp_path / "source.csv"
    source_path.write_text(
        "npi,score\n1,1\n2,2\n3,3\n4,4\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(provider_quality, "_is_row_allowed_for_test", lambda row: row != 2)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_CHUNK_TARGET_BYTES", 1)
    dataset = provider_quality.DATASET_BY_KEY["qpp_provider"]
    monkeypatch.setitem(
        provider_quality.DATASET_BY_KEY,
        "qpp_provider",
        replace(dataset, row_limit_test=2),
    )

    chunks = await provider_quality._split_source_into_chunks(
        "qpp_provider",
        str(source_path),
        tmp_path / "chunks",
        True,
    )

    assert len(chunks) == 2
    assert all(Path(chunk["chunk_path"]).exists() for chunk in chunks)
