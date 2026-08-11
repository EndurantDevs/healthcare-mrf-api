"""Lifecycle contracts for unified-address stage materialization."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


entity_address = importlib.import_module("process.entity_address_unified")


class _StageClass:
    __tablename__ = "entity_address_unified_20260724"
    __table__ = object()


def _configure_database_harness(monkeypatch, sql_events):
    """Route database effects into a deterministic SQL event log."""
    async def record_status(statement, **_params):
        sql_events.append(str(statement))

    async def record_create_table(_table, **_kwargs):
        sql_events.append("CREATE_STAGE")

    async def scalar(statement, **_params):
        statement_text = str(statement)
        sql_events.append(statement_text)
        if (
            "base_address_version IS DISTINCT FROM" in statement_text
            or (
                "address_alias_v1" in statement_text
                and "WHERE revoked_at IS NULL" in statement_text
            )
        ):
            return 0
        return 1

    async def run_sql_phase(statement, **options):
        sql_events.append(str(statement))
        if options.get("unit") == "rows":
            return 2
        return 1

    async def is_table_present(_schema, table_name):
        return not table_name.endswith("_missing")

    monkeypatch.setattr(entity_address, "ensure_database", AsyncMock())
    monkeypatch.setattr(entity_address, "make_class", lambda _model, _date: _StageClass)
    monkeypatch.setattr(entity_address, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(entity_address, "_has_table", is_table_present)
    monkeypatch.setattr(entity_address, "_has_table_column", AsyncMock(return_value=True))
    monkeypatch.setattr(entity_address, "_is_address_canon_available", AsyncMock(return_value=True))
    monkeypatch.setattr(entity_address, "_address_alias_generation", AsyncMock(return_value=0))
    monkeypatch.setattr(entity_address, "_validate_raw_alias_integrity", AsyncMock())
    monkeypatch.setattr(entity_address.db, "status", record_status)
    monkeypatch.setattr(entity_address.db, "create_table", record_create_table)
    monkeypatch.setattr(entity_address.db, "scalar", scalar)
    monkeypatch.setattr(entity_address, "_run_sql_phase", run_sql_phase)


def _configure_source_harness(monkeypatch, environment_flag_by_name, runtime_option_by_name):
    """Configure source selection and materialization feature decisions."""
    monkeypatch.setattr(
        entity_address,
        "_source_selects",
        lambda *_args, **_kwargs: ["SELECT 'source-one'", "SELECT 'source-two'"],
    )
    monkeypatch.setattr(
        entity_address,
        "_current_provider_directory_source_selects",
        lambda _schema, _available, source_selects, **_kwargs: source_selects,
    )
    monkeypatch.setattr(
        entity_address,
        "_is_task_or_env_enabled",
        lambda *_args, **_kwargs: runtime_option_by_name["serving_only"],
    )
    monkeypatch.setattr(
        entity_address,
        "_is_env_enabled",
        lambda name, default=False: environment_flag_by_name.get(name, default),
    )
    monkeypatch.setattr(entity_address, "_env_int", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(entity_address, "_npi_table_ranges", AsyncMock(return_value=[]))
    monkeypatch.setattr(entity_address, "_should_require_inline_evidence", lambda: False)
    monkeypatch.setattr(entity_address, "_should_aggregate_source_record_ids", lambda: True)
    monkeypatch.setattr(entity_address, "_should_keep_raw_stage", lambda: False)
    monkeypatch.setattr(entity_address, "_should_compute_final_summary_counts", lambda: True)
    monkeypatch.setattr(entity_address, "_prepare_inference_stage_indexes", AsyncMock())
    monkeypatch.setattr(entity_address, "_compact_hot_row_source_record_ids", AsyncMock(return_value=3))
    monkeypatch.setattr(entity_address, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(entity_address, "_ensure_stage_primary_key", AsyncMock())
    monkeypatch.setattr(entity_address, "_ensure_entity_address_unified_live_columns", AsyncMock())
    monkeypatch.setattr(entity_address, "_preflight_provider_directory_partial_scope_index", AsyncMock())


def _configure_support_harness(monkeypatch, progress_events):
    """Provide stable summary, support-stage, and progress evidence."""
    monkeypatch.setattr(
        entity_address,
        "_stage_summary_counts",
        AsyncMock(
            return_value={
                "staged_rows": 12,
                "npi_rows": 10,
                "inferred_rows": 2,
                "multi_source_rows": 4,
            }
        ),
    )
    monkeypatch.setattr(
        entity_address,
        "_promote_approved_facility_anchor_npi_candidates",
        AsyncMock(return_value=2),
    )
    monkeypatch.setattr(entity_address, "_create_support_stage_indexes", AsyncMock())
    monkeypatch.setattr(
        entity_address,
        "_prepare_support_stage_tables",
        AsyncMock(return_value={"evidence": SimpleNamespace(__tablename__="evidence_stage")}),
    )
    monkeypatch.setattr(
        entity_address,
        "_populate_support_stage_tables",
        AsyncMock(return_value={"evidence": 5, "bridge": 3}),
    )
    monkeypatch.setattr(
        entity_address,
        "enqueue_live_progress",
        lambda **payload: progress_events.append(payload),
    )
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "true")


def _configure_materialization_harness(monkeypatch):
    """Assemble a stage-only harness with observable SQL and progress."""
    sql_events = []
    progress_events = []
    environment_flag_by_name = {
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_INLINE_SOURCE_EVIDENCE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPI_OTHER_IDENTIFIER_INFERENCE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE": True,
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE": True,
    }
    runtime_option_by_name = {"serving_only": True}
    _configure_database_harness(monkeypatch, sql_events)
    _configure_source_harness(monkeypatch, environment_flag_by_name, runtime_option_by_name)
    _configure_support_harness(monkeypatch, progress_events)
    return sql_events, progress_events, environment_flag_by_name, runtime_option_by_name


@pytest.mark.asyncio
async def test_materialization_lifecycle_keeps_publish_separate_and_records_reviewable_stage_evidence(monkeypatch):
    sql_events, progress_events, environment_flag_by_name, runtime_option_by_name = _configure_materialization_harness(
        monkeypatch
    )
    import_context_by_key = {
        "import_date": "20260724",
        "context": {"control_run_id": "run-address-contract"},
    }

    await entity_address.process_data(
        import_context_by_key,
        {"publish": False, "limit_per_source": 0},
    )

    stage_context = import_context_by_key["context"]
    assert stage_context["publish_requested"] is False
    assert stage_context["stage_prepared"] is True
    assert stage_context["support_stage_skipped"] is True
    assert stage_context["source_evidence_inlined"] is True
    assert stage_context["source_select_count"] == 2
    assert stage_context["staged_rows"] == 12
    assert stage_context["npi_rows"] == 10
    assert stage_context["inferred_rows"] == 2
    assert stage_context["multi_source_rows"] == 4
    assert stage_context["hot_row_source_record_ids_compacted_rows"] == 3
    assert stage_context["run"] == 1
    assert any("CREATE_STAGE" == event for event in sql_events)
    assert any("DROP TABLE IF EXISTS mrf.entity_address_unified_20260724_raw" in event for event in sql_events)
    assert any(event["phase"] == "entity-address-unified staged" for event in progress_events)
    assert not any("ALTER TABLE" in event and "RENAME" in event for event in sql_events)

    environment_flag_by_name["HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD"] = False
    environment_flag_by_name["HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS"] = False
    runtime_option_by_name["serving_only"] = False
    import_context_by_key["context"]["stage_indexes_prepared"] = False
    import_context_by_key["context"]["support_stage_prepared"] = False
    import_context_by_key["context"]["support_stage_populated"] = False
    import_context_by_key["context"]["support_stage_indexes_prepared"] = False

    await entity_address.process_data(import_context_by_key, {"publish": False, "source_limit": 3})

    stage_context = import_context_by_key["context"]
    assert stage_context["run"] == 2
    assert stage_context["limit_per_source"] == 3
    assert stage_context["support_stage_skipped"] is False
    assert stage_context["support_stage_populated"] is True
    assert stage_context["support_counts"] == {"evidence": 5, "bridge": 3}
    assert stage_context["support_stage_indexes_prepared"] is True
    assert any("evidence" in event.lower() for event in sql_events)


@pytest.mark.asyncio
async def test_partial_refresh_rejects_reusing_a_full_refresh_stage(monkeypatch):
    monkeypatch.setattr(entity_address, "ensure_database", AsyncMock())
    monkeypatch.setattr(entity_address, "make_class", lambda _model, _date: _StageClass)
    monkeypatch.setattr(
        entity_address,
        "_is_env_enabled",
        lambda name, _default=False: name == "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE",
    )

    import_context_by_key = {"import_date": "20260724", "context": {}}
    task_by_field = {
        "refresh_mode": entity_address.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL,
        "provider_directory_partial_scope": "all",
    }

    with pytest.raises(RuntimeError, match="cannot be combined with.*REUSE_STAGE"):
        await entity_address.process_data(import_context_by_key, task_by_field)

    assert import_context_by_key["context"]["partial_provider_directory_refresh"] is True
    assert import_context_by_key["context"]["serving_only_refresh"] is True


@pytest.mark.asyncio
async def test_partial_refresh_builds_an_atomic_replacement_stage_without_patching_live_rows(monkeypatch):
    sql_events, _progress_events, _environment_flags, _runtime_options = _configure_materialization_harness(
        monkeypatch
    )
    monkeypatch.setattr(
        entity_address,
        "_provider_directory_partial_replacement_source_selects",
        lambda *_args, **_kwargs: ["SELECT 'partial-source'"],
    )
    monkeypatch.setattr(
        entity_address,
        "_prepare_partial_affected_groups_sql",
        lambda *_args, **_kwargs: "CREATE TABLE affected_groups AS SELECT 1",
    )
    monkeypatch.setattr(
        entity_address,
        "_index_partial_affected_groups_sql",
        lambda *_args, **_kwargs: "CREATE INDEX affected_groups_idx ON affected_groups ((1))",
    )
    import_context_by_key = {
        "import_date": "20260724",
        "context": {"control_run_id": "run-partial-contract"},
    }
    task_by_field = {
        "refresh_mode": entity_address.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL,
        "provider_directory_partial_scope": "all",
        "provider_directory_source_ids": ["source-a", "source-b"],
        "provider_directory_source_batch_size": 1,
        "publish": False,
    }

    await entity_address.process_data(import_context_by_key, task_by_field)

    stage_context = import_context_by_key["context"]
    assert stage_context["partial_provider_directory_refresh"] is True
    assert stage_context["partial_provider_directory_source_ids"] == ["source-a", "source-b"]
    assert stage_context["partial_provider_directory_source_batches"] == 2
    assert stage_context["partial_provider_directory_replacement_publish"] is True
    assert stage_context["partial_provider_directory_main_patch_publish"] is False
    assert stage_context["partial_main_patch_publish"] is False
    assert stage_context["partial_provider_directory_affected_groups"] == 1
    assert stage_context["partial_provider_directory_unaffected_live_rows_copied"] == 2
    assert stage_context["partial_provider_directory_affected_stage_rows_copied"] == 2
    assert stage_context["partial_provider_directory_replacement_rows"] == 4
    assert stage_context["partial_provider_directory_replacement_stage_indexes_invalidated"] is True
    assert any("affected" in statement.lower() for statement in sql_events)
    assert any("replacement" in statement.lower() for statement in sql_events)
    assert not any(
        "UPDATE mrf.entity_address_unified" in statement
        and "entity_address_unified_20260724" not in statement
        for statement in sql_events
    )


@pytest.mark.asyncio
async def test_partial_refresh_checks_exact_dataset_before_materialization(monkeypatch):
    _configure_materialization_harness(monkeypatch)
    monkeypatch.setattr(
        entity_address,
        "_provider_directory_partial_replacement_source_selects",
        lambda *_args, **_kwargs: ["SELECT 'synthetic-partial-source'"],
    )
    monkeypatch.setattr(
        entity_address,
        "_prepare_partial_affected_groups_sql",
        lambda *_args, **_kwargs: "CREATE TABLE affected_groups AS SELECT 1",
    )
    monkeypatch.setattr(
        entity_address,
        "_index_partial_affected_groups_sql",
        lambda *_args, **_kwargs: (
            "CREATE INDEX affected_groups_idx ON affected_groups ((1))"
        ),
    )
    dataset_assertion = AsyncMock()
    monkeypatch.setattr(
        entity_address,
        "_assert_current_provider_directory_dataset",
        dataset_assertion,
    )

    await entity_address.process_data(
        {
            "import_date": "20260724",
            "context": {"control_run_id": "synthetic-control-run"},
        },
        {
            "refresh_mode": (
                entity_address.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
            ),
            "provider_directory_partial_scope": "latest-run",
            "provider_directory_source_ids": ["synthetic-source"],
            "provider_directory_run_id": "synthetic-root-run",
            "provider_directory_dataset_id": "synthetic-dataset",
            "publish": False,
        },
    )

    dataset_assertion.assert_awaited_once_with(
        "mrf",
        source_id="synthetic-source",
        expected_dataset_id="synthetic-dataset",
        expected_root_run_id="synthetic-root-run",
    )
