# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-policy boundaries for provider-quality ingestion."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

provider_quality = importlib.import_module("process.provider_quality")


def test_source_templates_and_test_window_choose_explicit_years(monkeypatch) -> None:
    monkeypatch.setattr(provider_quality, "QPP_CSV_URL_TEMPLATE", "https://qpp/static.csv")
    monkeypatch.setattr(provider_quality, "SVI_CSV_URL_TEMPLATE", "https://svi/static.csv")
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_YEAR_WINDOW", (2023, 2024))

    assert provider_quality._qpp_url_for_year(2024) == "https://qpp/static.csv"
    assert provider_quality._svi_url_for_year(2024) == "https://svi/static.csv"
    source_by_dataset = provider_quality._resolve_sources(test_mode=True)
    assert [row["reporting_year"] for row in source_by_dataset["qpp_provider"]] == [
        2024
    ]
    assert [row["reporting_year"] for row in source_by_dataset["svi_zcta"]] == [
        2024
    ]


@pytest.mark.asyncio
async def test_finalize_failure_without_run_identity_is_a_noop(monkeypatch) -> None:
    control_update = AsyncMock()
    monkeypatch.setattr(provider_quality, "mark_control_run", control_update)
    await provider_quality._mark_provider_quality_finalize_failed(
        "",
        RuntimeError("failure"),
    )
    control_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_source_url_is_strict_or_explicitly_degraded(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(provider_quality, "_should_fail_on_source_error", lambda _mode: True)
    with pytest.raises(RuntimeError, match="source URL is missing"):
        await provider_quality._download_source_file(
            "qpp_provider",
            {"reporting_year": 2024},
            str(tmp_path),
            False,
        )

    monkeypatch.setattr(provider_quality, "_should_fail_on_source_error", lambda _mode: False)
    source_path, degraded = await provider_quality._download_source_file(
        "qpp_provider",
        {"reporting_year": 2024},
        str(tmp_path),
        True,
    )
    assert degraded is True
    assert "npi" in Path(source_path).read_text(encoding="utf-8").splitlines()[0]


@pytest.mark.asyncio
async def test_download_uses_test_and_full_fetch_paths(monkeypatch, tmp_path) -> None:
    head_fetch = AsyncMock()
    full_fetch = AsyncMock()
    monkeypatch.setattr(provider_quality, "_download_csv_head", head_fetch)
    monkeypatch.setattr(provider_quality, "download_it_and_save", full_fetch)

    await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.test/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        True,
    )
    await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.test/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        False,
    )

    head_fetch.assert_awaited_once()
    full_fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_error",
    (
        RuntimeError("temporary"),
        pytest.param(None, id="retry-control-flow"),
    ),
)
async def test_download_retries_each_supported_failure_class(
    monkeypatch,
    tmp_path,
    first_error,
) -> None:
    attempts: list[int] = []
    sleeps: list[float] = []

    async def flaky_fetch(*_args):
        attempts.append(1)
        if len(attempts) == 1:
            if first_error is None:
                raise provider_quality.Retry(defer=0)
            raise first_error

    async def no_wait(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr(provider_quality, "DOWNLOAD_RETRIES", 2)
    monkeypatch.setattr(provider_quality, "_download_csv_head", flaky_fetch)
    monkeypatch.setattr(provider_quality.asyncio, "sleep", no_wait)

    source_path, degraded = await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.test/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        True,
    )
    assert source_path.endswith("qpp_provider_2024.csv")
    assert degraded is False
    assert len(attempts) == 2
    assert sleeps == [5]


@pytest.mark.asyncio
async def test_terminal_retry_can_fall_back_only_under_degraded_policy(
    monkeypatch,
    tmp_path,
) -> None:
    async def fail_fetch(*_args):
        raise provider_quality.Retry(defer=0)

    monkeypatch.setattr(provider_quality, "DOWNLOAD_RETRIES", 1)
    monkeypatch.setattr(provider_quality, "_download_csv_head", fail_fetch)
    monkeypatch.setattr(provider_quality, "_should_fail_on_source_error", lambda _mode: False)

    source_path, degraded = await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.test/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        True,
    )
    assert degraded is True
    assert Path(source_path).read_text(encoding="utf-8").splitlines()


@pytest.mark.asyncio
async def test_zero_download_retry_budget_uses_degraded_policy(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(provider_quality, "DOWNLOAD_RETRIES", 0)
    monkeypatch.setattr(provider_quality, "_should_fail_on_source_error", lambda _mode: False)
    source_path, degraded = await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.test/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        True,
    )
    assert degraded is True
    assert open(source_path, encoding="utf-8").readline()


@pytest.mark.asyncio
async def test_prepare_tables_obeys_deferred_index_policy(monkeypatch) -> None:
    class _Model:
        __table__ = object()

    model_classes = [
        type(f"Model{index}", (_Model,), {})
        for index in range(6)
    ]
    stage_class_by_model = {
        model_class: type(
            f"Stage{index}",
            (),
            {"__tablename__": f"stage_{index}", "__table__": object()},
        )
        for index, model_class in enumerate(model_classes)
    }
    monkeypatch.setattr(provider_quality, "PricingQppProvider", model_classes[0])
    monkeypatch.setattr(provider_quality, "PricingSviZcta", model_classes[1])
    monkeypatch.setattr(
        provider_quality,
        "PricingProviderQualityMeasure",
        model_classes[2],
    )
    monkeypatch.setattr(
        provider_quality,
        "PricingProviderQualityDomain",
        model_classes[3],
    )
    monkeypatch.setattr(
        provider_quality,
        "PricingProviderQualityScore",
        model_classes[4],
    )
    monkeypatch.setattr(provider_quality, "PricingQualityRun", model_classes[0])
    monkeypatch.setattr(provider_quality, "_cohort_model_classes", lambda: (model_classes[5],))
    monkeypatch.setattr(provider_quality, "get_import_schema", lambda *_args: "mrf")
    monkeypatch.setattr(
        provider_quality,
        "make_class",
        lambda model, *_args, **_kwargs: stage_class_by_model[model],
    )
    monkeypatch.setattr(provider_quality.db, "status", AsyncMock())
    monkeypatch.setattr(provider_quality.db, "create_table", AsyncMock())
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(provider_quality, "_ensure_indexes", ensure_indexes)

    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_DEFER_STAGE_INDEXES", False)
    classes, schema = await provider_quality._prepare_tables("stage", True)
    assert schema == "mrf"
    assert set(classes) == {model.__name__ for model in model_classes}
    assert ensure_indexes.await_count == len(model_classes)

    ensure_indexes.reset_mock()
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_DEFER_STAGE_INDEXES", True)
    await provider_quality._prepare_tables("stage", True)
    ensure_indexes.assert_not_awaited()


def _legacy_classes() -> dict[str, type]:
    return {
        name: type(name, (), {"__tablename__": name.lower()})
        for name in (
            "PricingQppProvider",
            "PricingSviZcta",
            "PricingProviderQualityMeasure",
            "PricingProviderQualityDomain",
            "PricingProviderQualityScore",
        )
    }


@pytest.mark.asyncio
async def test_materialization_selects_cohort_and_legacy_fallbacks(monkeypatch) -> None:
    cohort_call = AsyncMock()
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", True)
    monkeypatch.setattr(provider_quality, "_cohort_models_present", lambda _classes: True)
    monkeypatch.setattr(provider_quality, "_materialize_quality_rows_cohort", cohort_call)
    await provider_quality._materialize_quality_rows({}, "mrf", "run")
    cohort_call.assert_awaited_once()

    table_checks = iter((True, True))
    monkeypatch.setattr(provider_quality, "_cohort_models_present", lambda _classes: False)
    monkeypatch.setattr(
        provider_quality,
        "_is_table_available",
        AsyncMock(side_effect=lambda *_args: next(table_checks)),
    )
    monkeypatch.setattr(provider_quality.db, "status", AsyncMock())
    await provider_quality._materialize_quality_rows(
        _legacy_classes(),
        "mrf",
        "run",
    )
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_COHORT_ENABLED", False)
    monkeypatch.setattr(
        provider_quality,
        "_is_table_available",
        AsyncMock(return_value=False),
    )
    await provider_quality._materialize_quality_rows(
        _legacy_classes(),
        "mrf",
        "run",
    )
    assert provider_quality.db.status.await_count >= 4


def test_extract_qpp_score_accepts_percent_and_numeric_values() -> None:
    assert provider_quality._extract_qpp_score({"score": " 83% "}, "score") == 83.0
    assert provider_quality._extract_qpp_score({"score": 72}, "score") == 72.0


@pytest.mark.asyncio
async def test_header_only_source_produces_no_chunks(tmp_path) -> None:
    source_path = tmp_path / "header.csv"
    source_path.write_text("npi,score\n", encoding="utf-8")
    assert await provider_quality._split_source_into_chunks(
        "qpp_provider",
        str(source_path),
        tmp_path / "chunks",
        False,
    ) == []


@pytest.mark.asyncio
async def test_finish_main_carries_explicit_manifest_path(monkeypatch) -> None:
    enqueue_job = AsyncMock()
    redis = SimpleNamespace(enqueue_job=enqueue_job)
    monkeypatch.setattr(
        provider_quality,
        "create_pool",
        AsyncMock(return_value=redis),
    )
    result = await provider_quality.finish_main(
        "import",
        "run",
        manifest_path="/tmp/manifest.json",
    )
    assert result["queued"] is True
    assert enqueue_job.await_args.args[1]["manifest_path"] == "/tmp/manifest.json"
