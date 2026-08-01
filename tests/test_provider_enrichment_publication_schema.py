"""Schema and summary coverage for Provider Enrichment publication."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

pytest.importorskip("pytz")

enrichment = importlib.import_module("process.provider_enrichment")


def test_provider_enrichment_table_and_union_names_are_deterministic(monkeypatch):
    class IncludedModel:
        __main_table__ = "included"

    class ExcludedModel:
        __main_table__ = "excluded"

    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (IncludedModel, ExcludedModel))
    monkeypatch.setattr(
        enrichment,
        "make_class",
        lambda model, suffix: SimpleNamespace(__tablename__=f"{model.__main_table__}_{suffix}"),
    )
    table_name_by_main = enrichment._provider_enrichment_table_names("20260801")
    assert table_name_by_main == {
        "included": "included_20260801",
        "excluded": "excluded_20260801",
    }
    monkeypatch.setattr(
        enrichment,
        "ENROLLMENT_DATASET_SPECS",
        (
            {"key": "hospital", "model": IncludedModel},
            {"key": "ffs_address", "model": ExcludedModel},
        ),
    )

    union_sql = enrichment._provider_enrollment_union_sql(table_name_by_main, "provider_test")

    assert "provider_test.included_20260801" in union_sql
    assert "'hospital'::varchar AS dataset_key" in union_sql
    assert "excluded_20260801" not in union_sql


@pytest.mark.asyncio
async def test_pricing_cte_covers_available_and_absent_tables(monkeypatch):
    monkeypatch.setattr(
        enrichment,
        "_is_table_available",
        AsyncMock(side_effect=(True, False)),
    )

    available_sql = await enrichment._provider_enrichment_pricing_cte("provider_test")
    absent_sql = await enrichment._provider_enrichment_pricing_cte("provider_test")

    assert "FROM provider_test.pricing_provider" in available_sql
    assert "GROUP BY npi" in available_sql
    assert "WHERE FALSE" in absent_sql


@pytest.mark.asyncio
async def test_summary_bindings_include_tables_pricing_and_nppes(monkeypatch):
    models = (
        enrichment.ProviderEnrichmentSummary,
        enrichment.ProviderEnrollmentFFS,
        enrichment.ProviderEnrollmentFFSAdditionalNPI,
        enrichment.ProviderEnrollmentFFSAddress,
        enrichment.ProviderEnrollmentFFSSecondarySpecialty,
        enrichment.ProviderEnrollmentFFSReassignment,
    )
    table_name_by_main = {
        model.__main_table__: f"{model.__main_table__}_stage"
        for model in models
    }
    monkeypatch.setattr(
        enrichment,
        "_provider_enrichment_table_names",
        Mock(return_value=table_name_by_main),
    )
    monkeypatch.setattr(
        enrichment,
        "_provider_enrollment_union_sql",
        Mock(return_value="SELECT enrollment"),
    )
    monkeypatch.setattr(
        enrichment,
        "_provider_enrichment_pricing_cte",
        AsyncMock(return_value="SELECT pricing"),
    )

    binding_map = await enrichment._provider_enrichment_summary_bindings(
        "20260801",
        "provider_test",
        {"unmapped_field_count": "2", "medical_school_headers": ["School", 3]},
    )

    summary_name = enrichment.ProviderEnrichmentSummary.__main_table__
    assert binding_map["summary_stage"] == table_name_by_main[summary_name]
    assert binding_map["enrollment_union_sql"] == "SELECT enrollment"
    assert binding_map["pricing_cte"] == "SELECT pricing"
    assert binding_map["nppes_unmapped_count_sql"] == "2"
    assert binding_map["nppes_medical_school_fields_sql"] == "ARRAY['School', '3']::varchar[]"


@pytest.mark.asyncio
async def test_materialize_summary_truncates_then_inserts(monkeypatch):
    summary_bindings = AsyncMock(
        return_value={
            "db_schema": "provider_test",
            "summary_stage": "summary_stage",
            "pricing_cte": "SELECT pricing",
        }
    )
    db_status = AsyncMock()
    monkeypatch.setattr(enrichment, "_provider_enrichment_summary_bindings", summary_bindings)
    monkeypatch.setattr(
        enrichment,
        "_PROVIDER_ENRICHMENT_SUMMARY_SQL",
        "INSERT {db_schema}.{summary_stage} {pricing_cte}",
    )
    monkeypatch.setattr(enrichment.db, "status", db_status)

    await enrichment._materialize_summary("20260801", "provider_test", {"checked": True})

    assert db_status.await_args_list == [
        call("TRUNCATE TABLE provider_test.summary_stage;"),
        call("INSERT provider_test.summary_stage SELECT pricing"),
    ]


@pytest.mark.asyncio
async def test_staging_tables_validate_every_model(monkeypatch):
    class FirstModel:
        __main_table__ = "first"

    class SecondModel:
        __main_table__ = "second"

    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (FirstModel, SecondModel))
    monkeypatch.setattr(
        enrichment,
        "make_class",
        lambda model, suffix: SimpleNamespace(__tablename__=f"{model.__main_table__}_{suffix}"),
    )
    table_available = AsyncMock(return_value=True)
    monkeypatch.setattr(enrichment, "_is_table_available", table_available)

    staging_table_map = await enrichment._provider_enrichment_staging_tables(
        "20260801",
        "provider_test",
    )

    assert set(staging_table_map) == {"first", "second"}
    assert table_available.await_args_list == [
        call("provider_test", "first_20260801"),
        call("provider_test", "second_20260801"),
    ]


@pytest.mark.asyncio
async def test_staging_tables_reject_missing_table(monkeypatch):
    class MissingModel:
        __main_table__ = "missing"

    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (MissingModel,))
    monkeypatch.setattr(
        enrichment,
        "make_class",
        lambda _model, _suffix: SimpleNamespace(__tablename__="missing_stage"),
    )
    monkeypatch.setattr(enrichment, "_is_table_available", AsyncMock(return_value=False))

    with pytest.raises(RuntimeError, match="missing_stage is missing"):
        await enrichment._provider_enrichment_staging_tables("20260801", "provider_test")


@pytest.mark.asyncio
async def test_required_stage_counts_accept_nonempty_and_reject_empty(monkeypatch):
    class FirstModel:
        __main_table__ = "first"

    class SecondModel:
        __main_table__ = "second"

    monkeypatch.setattr(
        enrichment,
        "_PROVIDER_ENRICHMENT_REQUIRED_STAGES",
        (FirstModel, SecondModel),
    )
    staging_table_map = {
        "first": SimpleNamespace(__tablename__="first_stage"),
        "second": SimpleNamespace(__tablename__="second_stage"),
    }
    monkeypatch.setattr(enrichment.db, "scalar", AsyncMock(side_effect=(3, 2)))

    count_by_table = await enrichment._required_provider_enrichment_stage_counts(
        staging_table_map,
        "provider_test",
    )

    assert count_by_table == {"first": 3, "second": 2}
    monkeypatch.setattr(enrichment, "_PROVIDER_ENRICHMENT_REQUIRED_STAGES", (FirstModel,))
    monkeypatch.setattr(enrichment.db, "scalar", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="first_stage is empty"):
        await enrichment._required_provider_enrichment_stage_counts(
            staging_table_map,
            "provider_test",
        )


@pytest.mark.asyncio
async def test_materialize_provider_summary_returns_row_count(monkeypatch):
    materialize_summary = AsyncMock()
    scalar = AsyncMock(return_value=7)
    monkeypatch.setattr(enrichment, "_materialize_summary", materialize_summary)
    monkeypatch.setattr(enrichment.db, "scalar", scalar)
    summary_table = enrichment.ProviderEnrichmentSummary.__main_table__
    staging_table_map = {summary_table: SimpleNamespace(__tablename__="summary_stage")}
    context_map = {"audit": {"nppes_gap_report": {"unmapped_field_count": 2}}}

    summary_row_count = await enrichment._materialize_provider_enrichment_summary(
        {"import_date": "20260801"},
        context_map,
        staging_table_map,
        "provider_test",
    )

    assert summary_row_count == 7
    materialize_summary.assert_awaited_once_with(
        "20260801",
        "provider_test",
        {"unmapped_field_count": 2},
    )
    scalar.assert_awaited_once_with("SELECT COUNT(*) FROM provider_test.summary_stage;")
