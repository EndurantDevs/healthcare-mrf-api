"""Runtime orchestration coverage for Provider Enrichment discovery."""

import importlib
from pathlib import Path
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


@pytest.mark.asyncio
async def test_process_data_stages_each_source_and_increments_run(monkeypatch):
    source_maps = [{"spec_key": "hospital"}, {"spec_key": "hospice"}]
    import_state = _provider_import_state(context={"run": 2}, sources=source_maps)
    prepare_import = AsyncMock(return_value=import_state)
    stage_calls = []

    async def stage_source(import_state_arg, source_map, source_index, tmpdirname):
        assert Path(tmpdirname).is_dir()
        stage_calls.append((import_state_arg, source_map, source_index, tmpdirname))

    monkeypatch.setattr(enrichment, "_prepare_provider_enrichment_import", prepare_import)
    monkeypatch.setattr(enrichment, "_stage_provider_enrichment_source", stage_source)
    worker_context_map = {"import_date": "20260801"}

    await enrichment.process_data(worker_context_map, None)

    prepare_import.assert_awaited_once_with(worker_context_map, {})
    assert [(stage_call[1], stage_call[2]) for stage_call in stage_calls] == [
        (source_maps[0], 0),
        (source_maps[1], 1),
    ]
    assert stage_calls[0][3] == stage_calls[1][3]
    assert import_state.context["run"] == 3


@pytest.mark.asyncio
async def test_audit_nppes_headers_covers_enabled_and_disabled(monkeypatch):
    gap_check = AsyncMock()
    monkeypatch.setattr(enrichment, "_run_nppes_gap_check", gap_check)
    monkeypatch.setattr(enrichment, "ENABLE_NPPES_GAP_CHECK", True)
    enabled_context_map = {}

    await enrichment._audit_nppes_headers(enabled_context_map)

    gap_check.assert_awaited_once_with(enabled_context_map)
    monkeypatch.setattr(enrichment, "ENABLE_NPPES_GAP_CHECK", False)
    disabled_context_map = {}
    await enrichment._audit_nppes_headers(disabled_context_map)

    assert disabled_context_map["context"]["audit"]["nppes_gap_report"] == {
        "checked": False,
        "skipped": True,
        "reason": "disabled_by_config",
        "source_zip": None,
        "unmapped_fields": [],
        "unmapped_field_count": 0,
        "medical_school_headers": [],
        "error": None,
    }


def test_discovery_progress_is_silent_without_run_and_emits_with_run(monkeypatch):
    enqueue_progress = Mock()
    monkeypatch.setattr(enrichment, "enqueue_live_progress", enqueue_progress)
    source_maps = [{"spec_key": "hospital"}]

    enrichment._report_provider_enrichment_discovery(
        _provider_import_state(run_id="", sources=source_maps),
        2,
    )
    enqueue_progress.assert_not_called()

    enrichment._report_provider_enrichment_discovery(
        _provider_import_state(run_id="run-live", sources=source_maps),
        2,
    )
    enqueue_progress.assert_called_once_with(
        run_id="run-live",
        importer="provider-enrichment",
        status="running",
        phase="provider-enrichment sources discovered",
        unit="sources",
        done=0,
        total=1,
        message="1 sources discovered",
    )


@pytest.mark.asyncio
async def test_prepare_provider_enrichment_import_binds_runtime_contract(monkeypatch):
    source_maps = [{"spec_key": "hospital"}]
    ensure_database = AsyncMock()
    prepare_staging = AsyncMock()
    audit_headers = AsyncMock()
    discover_sources = AsyncMock(return_value=(source_maps, ["unmapped"]))
    report_discovery = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "provider_test")
    monkeypatch.setenv("HLTHPRT_PROVIDER_ENRICHMENT_BATCH_SIZE", "13")
    monkeypatch.setenv("HLTHPRT_PROVIDER_ENRICHMENT_MAX_PENDING_SAVE_TASKS", "3")
    monkeypatch.setattr(enrichment, "ensure_database", ensure_database)
    monkeypatch.setattr(enrichment, "_prepare_staging_tables", prepare_staging)
    monkeypatch.setattr(enrichment, "_audit_nppes_headers", audit_headers)
    monkeypatch.setattr(enrichment, "_discover_sources", discover_sources)
    monkeypatch.setattr(enrichment, "_report_provider_enrichment_discovery", report_discovery)
    worker_context_map = {"import_date": "20260801", "control_run_id": " run-top "}

    import_state = await enrichment._prepare_provider_enrichment_import(
        worker_context_map,
        {"test_mode": True},
    )

    assert import_state.ctx is worker_context_map
    assert import_state.run_id == "run-top"
    assert import_state.test_mode is True
    assert import_state.db_schema == "provider_test"
    assert import_state.sources is source_maps
    assert import_state.batch_size == 13
    assert import_state.max_pending_save_tasks == 3
    assert import_state.audit["unmapped_datasets"] == ["unmapped"]
    ensure_database.assert_awaited_once_with(True)
    prepare_staging.assert_awaited_once_with("20260801", "provider_test")
    audit_headers.assert_awaited_once_with(worker_context_map)
    discover_sources.assert_awaited_once_with(test_mode=True)
    report_discovery.assert_called_once_with(import_state, 1)


@pytest.mark.asyncio
async def test_prepare_provider_enrichment_import_rejects_empty_discovery(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(enrichment, "ensure_database", AsyncMock())
    monkeypatch.setattr(enrichment, "_prepare_staging_tables", AsyncMock())
    monkeypatch.setattr(enrichment, "_audit_nppes_headers", AsyncMock())
    monkeypatch.setattr(enrichment, "_discover_sources", AsyncMock(return_value=([], [])))

    with pytest.raises(RuntimeError, match="No registered provider-enrichment sources"):
        await enrichment._prepare_provider_enrichment_import(
            {"import_date": "20260801", "context": {"test_mode": False}},
            {},
        )


def test_source_progress_covers_silent_loading_and_loaded_states(monkeypatch):
    enqueue_progress = Mock()
    monkeypatch.setattr(enrichment, "enqueue_live_progress", enqueue_progress)
    source_map = {"spec_key": "hospital", "distribution_title": "Hospital current"}

    enrichment._report_provider_enrichment_source_progress(
        _provider_import_state(run_id="", sources=[source_map]),
        source_map,
        0,
        loaded=False,
    )
    enqueue_progress.assert_not_called()
    import_state = _provider_import_state(run_id="run-live", sources=[source_map])
    enrichment._report_provider_enrichment_source_progress(import_state, source_map, 0, loaded=False)
    enrichment._report_provider_enrichment_source_progress(import_state, source_map, 0, loaded=True)

    assert [progress_call.kwargs["phase"] for progress_call in enqueue_progress.call_args_list] == [
        "provider-enrichment loading hospital",
        "provider-enrichment loaded hospital",
    ]
    assert [progress_call.kwargs["done"] for progress_call in enqueue_progress.call_args_list] == [0, 1]


@pytest.mark.parametrize(
    ("encoding", "warning_count"),
    ((enrichment.CSV_PRIMARY_ENCODING, 0), ("cp1252", 1)),
)
def test_provider_enrichment_csv_encoding_validates_and_warns(
    monkeypatch,
    capsys,
    encoding,
    warning_count,
):
    select_encoding = Mock(return_value=encoding)
    read_header = Mock(return_value=["NPI"])
    validate_headers = Mock()
    monkeypatch.setattr(enrichment, "_select_csv_encoding", select_encoding)
    monkeypatch.setattr(enrichment, "_read_csv_header", read_header)
    monkeypatch.setattr(enrichment, "_validate_headers", validate_headers)
    source_specification_map = {"fields": []}
    source_map = {"distribution_title": "Current", "dataset_title": "Dataset"}

    selected_encoding = enrichment._provider_enrichment_csv_encoding(
        "/tmp/source.csv",
        source_specification_map,
        source_map,
    )

    assert selected_encoding == encoding
    validate_headers.assert_called_once_with(["NPI"], source_specification_map, "Current")
    assert capsys.readouterr().out.count("fallback CSV encoding") == warning_count
