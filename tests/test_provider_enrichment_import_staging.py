"""Batching and source-staging coverage for Provider Enrichment."""

import importlib
from unittest.mock import AsyncMock, Mock, call

import pytest

pytest.importorskip("pytz")

enrichment = importlib.import_module("process.provider_enrichment")


def _provider_import_state(**overrides):
    import_field_map = {
        "ctx": {"import_date": "20260801"},
        "context": {"run": 0},
        "audit": {
            "dataset_stats": {},
            "rows_accepted": 0,
            "rows_dropped_missing_npi": 0,
        },
        "run_id": "run-provider-enrichment",
        "test_mode": False,
        "db_schema": "mrf_test",
        "sources": [],
        "batch_size": 2,
        "max_pending_save_tasks": 2,
    }
    import_field_map.update(overrides)
    return enrichment._ProviderEnrichmentImport(**import_field_map)


def _install_row_pipeline(monkeypatch, saved_task_maps):
    def build_row(source_row, _specification_map, _source_map, _columns):
        if source_row["kind"] == "missing":
            return None, "missing_npi"
        if source_row["kind"] == "other":
            return None, "ignored"
        return {"npi": int(source_row["kind"])}, None

    async def save_rows(_worker_context_map, task_map):
        saved_task_maps.append(task_map)

    monkeypatch.setattr(enrichment, "_model_columns", Mock(return_value={"npi"}))
    monkeypatch.setattr(enrichment, "_build_row_payload", build_row)
    monkeypatch.setattr(enrichment, "save_provider_enrichment_data", save_rows)


@pytest.mark.asyncio
async def test_stage_rows_batches_drops_and_flushes_pending_tasks(monkeypatch, tmp_path):
    source_path = tmp_path / "source.csv"
    source_path.write_text("kind\nmissing\nother\n1\n2\n3\n4\n", encoding="utf-8")
    saved_task_maps = []
    _install_row_pipeline(monkeypatch, saved_task_maps)

    stats = await enrichment._stage_provider_enrichment_rows(
        _provider_import_state(batch_size=2, max_pending_save_tasks=2),
        {},
        {"model": enrichment.ProviderEnrollmentHospital, "task_key": "hospital_rows"},
        str(source_path),
        "utf-8",
    )

    assert stats == enrichment._ProviderEnrichmentSourceStats(
        processed_rows=6,
        rows_accepted=4,
        rows_dropped_missing_npi=1,
    )
    assert saved_task_maps == [
        {"hospital_rows": [{"npi": 1}, {"npi": 2}]},
        {"hospital_rows": [{"npi": 3}, {"npi": 4}]},
    ]


@pytest.mark.asyncio
async def test_stage_rows_test_limit_flushes_partial_batch(monkeypatch, tmp_path):
    source_path = tmp_path / "source.csv"
    source_path.write_text("kind\n1\n2\n", encoding="utf-8")
    saved_task_maps = []
    _install_row_pipeline(monkeypatch, saved_task_maps)
    monkeypatch.setattr(enrichment, "TEST_PROVIDER_ENRICHMENT_ROWS", 1)

    stats = await enrichment._stage_provider_enrichment_rows(
        _provider_import_state(test_mode=True, batch_size=10),
        {},
        {"model": enrichment.ProviderEnrollmentHospital, "task_key": "hospital_rows"},
        str(source_path),
        "utf-8",
    )

    assert stats.processed_rows == 1
    assert stats.rows_accepted == 1
    assert saved_task_maps == [{"hospital_rows": [{"npi": 1}]}]


def test_record_source_stats_accumulates_dataset_totals():
    import_state = _provider_import_state()
    source_map = {
        "spec_key": "hospital",
        "dataset_title": "Hospital",
        "distribution_title": "Current",
        "download_url": "https://example.test/hospital.csv",
        "reporting_year": 2026,
    }
    stats = enrichment._ProviderEnrichmentSourceStats(
        processed_rows=3,
        rows_accepted=2,
        rows_dropped_missing_npi=1,
    )

    enrichment._record_provider_enrichment_source_stats(import_state, source_map, stats)

    assert import_state.audit["dataset_stats"]["hospital"][0]["rows_processed"] == 3
    assert import_state.audit["rows_accepted"] == 2
    assert import_state.audit["rows_dropped_missing_npi"] == 1


@pytest.mark.asyncio
async def test_stage_source_runs_download_parse_stats_and_progress(monkeypatch, tmp_path):
    source_map = {
        "spec_key": "hospital",
        "distribution_title": "Current hospital",
        "download_url": "https://example.test/hospital.csv",
    }
    import_state = _provider_import_state(sources=[source_map])
    download_source = AsyncMock()
    select_encoding = Mock(return_value="utf-8")
    source_stats = enrichment._ProviderEnrichmentSourceStats(processed_rows=1, rows_accepted=1)
    stage_rows = AsyncMock(return_value=source_stats)
    record_stats = Mock()
    report_progress = Mock()
    monkeypatch.setattr(enrichment, "_download_source", download_source)
    monkeypatch.setattr(enrichment, "_provider_enrichment_csv_encoding", select_encoding)
    monkeypatch.setattr(enrichment, "_stage_provider_enrichment_rows", stage_rows)
    monkeypatch.setattr(enrichment, "_record_provider_enrichment_source_stats", record_stats)
    monkeypatch.setattr(enrichment, "_report_provider_enrichment_source_progress", report_progress)

    await enrichment._stage_provider_enrichment_source(import_state, source_map, 0, str(tmp_path))

    local_path = str(tmp_path / "provider_enrichment_hospital_0.csv")
    assert report_progress.call_args_list == [
        call(import_state, source_map, 0, loaded=False),
        call(import_state, source_map, 0, loaded=True),
    ]
    download_source.assert_awaited_once_with(source_map["download_url"], local_path)
    stage_rows.assert_awaited_once_with(
        import_state,
        source_map,
        enrichment.SPEC_BY_KEY["hospital"],
        local_path,
        "utf-8",
    )
    record_stats.assert_called_once_with(import_state, source_map, source_stats)
