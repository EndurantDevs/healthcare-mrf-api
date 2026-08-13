# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import pytest

from tests.test_process_provider_quality_unit import (
    provider_quality,
    provider_quality_cohort_context,
)

@pytest.mark.asyncio
async def test_load_qpp_rows_parses_real_cms_field_names(monkeypatch, tmp_path):
    csv_path = tmp_path / "qpp_real_headers.csv"
    csv_path.write_text(
        (
            "Clinician_NPI,Performance_Year,Quality_category_score,"
            "Cost_category_score,final_MIPS_score\n"
            "1234567890,2024,91.5,82.0,88.25\n"
        ),
        encoding="utf-8",
    )

    captured_rows: list[dict[str, object]] = []

    async def _capture_push(rows, _cls, **_kwargs):
        captured_rows.extend(rows)

    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", _capture_push)

    fake_cls = type("QppStage", (), {"__tablename__": "pricing_qpp_provider_stage"})
    await provider_quality._load_qpp_rows(str(csv_path), fake_cls, 2024, test_mode=False)

    assert len(captured_rows) == 1
    row = captured_rows[0]
    assert row["npi"] == 1234567890
    assert row["year"] == 2024
    assert row["quality_score"] == 91.5
    assert row["cost_score"] == 82.0
    assert row["final_score"] == 88.25


@pytest.mark.asyncio
async def test_materialize_query_contains_state_benchmark_and_extra_measures(monkeypatch):
    statements: list[str] = []

    async def _fake_status(statement: str, *args, **kwargs):
        statements.append(statement)

    async def is_table_existing(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality, "_is_table_available", is_table_existing)
    monkeypatch.setattr(provider_quality.db, "status", _fake_status)

    staging_classes_by_name = {
        "PricingQppProvider": type("QppStage", (), {"__tablename__": "pricing_qpp_provider_stage"}),
        "PricingSviZcta": type("SviStage", (), {"__tablename__": "pricing_svi_zcta_stage"}),
        "PricingProviderQualityMeasure": type("MeasureStage", (), {"__tablename__": "pricing_provider_quality_measure_stage"}),
        "PricingProviderQualityDomain": type("DomainStage", (), {"__tablename__": "pricing_provider_quality_domain_stage"}),
        "PricingProviderQualityScore": type("ScoreStage", (), {"__tablename__": "pricing_provider_quality_score_stage"}),
    }

    await provider_quality._materialize_quality_rows(staging_classes_by_name, "mrf", "run_test")

    materialize_sql = "\n".join(statements)
    assert "peers_state AS (" in materialize_sql
    assert "peers_zip_exact AS (" in materialize_sql
    assert "zip_choice AS (" in materialize_sql
    assert "benchmark_modes AS (" in materialize_sql
    assert "CROSS JOIN benchmark_modes bm" in materialize_sql
    assert "zip_ring:" in materialize_sql
    assert "|| ':' || 'r' ||" in materialize_sql
    assert "|| ':r' ||" not in materialize_sql
    assert "::numeric, 2)::float8 AS score_0_100" in materialize_sql
    assert "ROUND(LEAST(" not in materialize_sql
    assert "CAST(:run_id AS varchar) AS run_id" in materialize_sql
    assert ":run_id::varchar AS run_id" not in materialize_sql
    assert "appropriateness_drug_proxy" in materialize_sql
    assert "cost_qpp_component" in materialize_sql


@pytest.mark.asyncio
async def test_materialize_cohort_query_skips_unusable_lsh_and_l0(monkeypatch):
    statements: list[str] = []

    async def _fake_status(statement: str, *args, **kwargs):
        statements.append(statement)

    async def is_table_existing(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality.db, "status", _fake_status)
    monkeypatch.setattr(provider_quality, "_is_table_available", is_table_existing)

    classes = provider_quality._staging_classes("stage_test", "mrf")
    await provider_quality._materialize_quality_rows_cohort(classes, "mrf", "run_test")

    materialize_sql = "\n".join(statements)
    assert "taxonomy_choice AS (" in materialize_sql
    assert "provider_enrichment_choice AS (" in materialize_sql
    assert "doctor_address_choice AS (" in materialize_sql
    assert "unified_address_choice AS (" in materialize_sql
    assert "npi_address_choice AS (" in materialize_sql
    assert "FROM mrf.doctor_clinician_address" not in materialize_sql
    assert "signatures AS (" not in materialize_sql
    assert "bands AS (" not in materialize_sql
    assert "cohort_expanded AS (" in materialize_sql
    assert "benchmark_modes AS (" in materialize_sql
    assert "VALUES ('L1'), ('L2'), ('L3')" in materialize_sql
    assert "VALUES ('L0'), ('L1'), ('L2'), ('L3')" not in materialize_sql
    assert "<> 'L0'" in materialize_sql
    assert "bm.benchmark_mode" in materialize_sql
    assert "AND LOWER(COALESCE(t.benchmark_mode, 'national')) = bm.benchmark_mode" in materialize_sql
    assert "COALESCE(c.procedure_bucket, 'bucket:none')::varchar AS procedure_bucket" in materialize_sql
    assert "COALESCE(c.specialty, 'unknown')::varchar AS specialty" in materialize_sql
    assert "COALESCE(c.taxonomy, 'unknown')::varchar AS taxonomy" in materialize_sql
    assert "ON cm.npi = b.npi" in materialize_sql
    assert "ON cm.npi = c.npi" in materialize_sql
    assert "WHEN bm.benchmark_mode = 'zip'" in materialize_sql
    assert "({peer_scope_expr} = 'zip'" not in materialize_sql
    assert "CASE WHEN c.threshold_met THEN 0 ELSE 1 END" in materialize_sql


@pytest.mark.asyncio
async def test_shard_queries_delete_partition_before_insert(monkeypatch):
    async def is_table_existing(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality, "_is_table_available", is_table_existing)
    classes = provider_quality._staging_classes("stage_test", "mrf")
    ctx = await provider_quality._build_cohort_materialization_context(classes, "mrf")

    lsh_sql = provider_quality._cohort_sql_phase_2_lsh_shard(ctx)
    measure_sql = provider_quality._cohort_sql_phase_5_measure_shard(ctx)
    domain_sql = provider_quality._cohort_sql_phase_6_domain_shard(ctx)
    score_sql = provider_quality._cohort_sql_phase_7_score_shard(ctx)

    assert "WITH deleted AS (" in lsh_sql
    assert f"DELETE FROM mrf.{ctx['lsh_table']} d" in lsh_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in lsh_sql

    assert "WITH deleted AS (" in measure_sql
    assert f"DELETE FROM mrf.{ctx['measure_table']} d" in measure_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in measure_sql

    assert "WITH deleted AS (" in domain_sql
    assert f"DELETE FROM mrf.{ctx['domain_table']} d" in domain_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in domain_sql

    assert "WITH deleted AS (" in score_sql
    assert f"DELETE FROM mrf.{ctx['score_table']} d" in score_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in score_sql


@pytest.mark.parametrize(
    ("model_name", "columns", "error_pattern"),
    [
        ("PricingProviderQualityFeature", ("npi",), "feature model"),
        (
            "PricingProviderQualityProcedureLSH",
            ("npi", "year", "band_no"),
            "LSH model",
        ),
        (
            "PricingProviderQualityPeerTarget",
            (
                "year",
                "geography_scope",
                "geography_value",
                "cohort_level",
                "peer_n",
                "target_appropriateness",
                "target_cost",
                "target_effectiveness",
                "target_qpp_cost",
            ),
            "peer-target model",
        ),
    ],
)
@pytest.mark.asyncio
async def test_cohort_context_rejects_incomplete_required_models(
    model_name,
    columns,
    error_pattern,
):
    """Materialization fails before SQL when a required model column is absent."""

    async def is_table_present(_schema: str, _table: str) -> bool:
        return False

    classes = provider_quality._staging_classes("stage_test", "mrf")
    table_name = classes[model_name].__tablename__
    classes[model_name] = type(
        f"Incomplete{model_name}",
        (),
        {
            "__tablename__": table_name,
            "__table__": SimpleNamespace(
                columns=tuple(SimpleNamespace(name=column) for column in columns)
            ),
        },
    )

    with pytest.raises(RuntimeError, match=error_pattern):
        await provider_quality_cohort_context._build_cohort_materialization_context(
            classes,
            "mrf",
            table_exists=is_table_present,
        )


@pytest.mark.asyncio
async def test_cohort_context_projects_only_available_measure_metadata():
    """Optional score metadata follows the exact columns retained by the measure model."""

    async def is_table_present(_schema: str, _table: str) -> bool:
        return False

    classes = provider_quality._staging_classes("stage_test", "mrf")
    measure_name = "PricingProviderQualityMeasure"
    table_name = classes[measure_name].__tablename__
    columns = (
        "cohort_geography_scope",
        "cohort_geography_value",
        "cohort_classification",
    )
    classes[measure_name] = type(
        "SelectedMeasureMetadata",
        (),
        {
            "__tablename__": table_name,
            "__table__": SimpleNamespace(
                columns=tuple(SimpleNamespace(name=column) for column in columns)
            ),
        },
    )

    context = await provider_quality_cohort_context._build_cohort_materialization_context(
        classes,
        "mrf",
        table_exists=is_table_present,
    )

    assert context["measure_meta_sources"] == {
        "selected_geography_scope": "cohort_geography_scope",
        "selected_geography_value": "cohort_geography_value",
        "selected_classification": "cohort_classification",
    }


@pytest.mark.asyncio
async def test_measure_shard_limits_rx_cte_to_provider_base():
    async def is_table_existing(_schema: str, table: str) -> bool:
        return table == "pricing_provider_prescription"

    classes = provider_quality._staging_classes("stage_test", "mrf")
    ctx = await provider_quality_cohort_context._build_cohort_materialization_context(
        classes,
        "mrf",
        table_exists=is_table_existing,
    )

    measure_sql = provider_quality._cohort_sql_phase_5_measure_shard(ctx)

    assert "FROM provider_base b" in measure_sql
    assert "JOIN mrf.pricing_provider_prescription r" in measure_sql
    assert "ON r.npi = b.npi" in measure_sql
    assert "AND r.year = b.year" in measure_sql


@pytest.mark.asyncio
async def test_measure_shard_prefers_rx_aggregate_table():
    async def is_table_existing(_schema: str, table: str) -> bool:
        return table == "pricing_provider_quality_rx_agg_stage_test"

    classes = provider_quality._staging_classes("stage_test", "mrf")
    ctx = await provider_quality_cohort_context._build_cohort_materialization_context(
        classes,
        "mrf",
        table_exists=is_table_existing,
    )

    measure_sql = provider_quality._cohort_sql_phase_5_measure_shard(ctx)

    assert ctx["rx_agg_table"] == "pricing_provider_quality_rx_agg_stage_test"
    assert "JOIN mrf.pricing_provider_quality_rx_agg_stage_test r" in measure_sql
    assert "SUM(COALESCE(r.total_claims" not in measure_sql
