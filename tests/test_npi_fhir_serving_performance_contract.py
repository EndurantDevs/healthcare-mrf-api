# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module
from db.models import ProviderDirectoryDatasetResource


@pytest.mark.asyncio
async def test_npi_detail_preaggregates_children_after_direct_npi_lookup(
    monkeypatch,
):
    """Keep serving-type filters outside the direct-NPI lookup barrier."""
    count_result = types.SimpleNamespace(scalar=lambda: 1)
    detail_result = types.SimpleNamespace(all=lambda: [])
    execute_mock = AsyncMock(side_effect=(count_result, detail_result))
    session = types.SimpleNamespace(execute=execute_mock)
    filter_capability_map = {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
    }
    address_columns = {
        column.key
        for column in npi_module.EntityAddressUnified.__table__.columns
    }
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        AsyncMock(return_value=filter_capability_map),
    )
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value=address_columns),
    )

    details = await npi_module._build_npi_details(
        1234567890,
        address_limit=25,
        include_address_total=True,
        session=session,
    )

    assert details == {}
    captured_statements = [call.args[0] for call in execute_mock.await_args_list]
    assert len(captured_statements) == 2
    count_sql, detail_sql = map(str, captured_statements)
    assert count_sql.index("entity_address_unified.npi =") < count_sql.index(
        "count_npi_address_rows.type IN"
    )
    assert detail_sql.index("entity_address_unified.npi =") < detail_sql.index(
        "npi_address_rows.type IN"
    )
    for aggregate_name in (
        "taxonomy_aggregate",
        "taxonomy_group_aggregate",
        "address_aggregate",
    ):
        assert aggregate_name in detail_sql
    assert "GROUP BY mrf.npi.npi" not in detail_sql


def test_relation_evidence_deduplicates_before_payload_projection():
    """Keep relation-backed evidence narrow until selected plans are returned."""
    role_sql = npi_module._provider_directory_role_evidence_sql(
        "mrf",
        has_catalog=True,
        has_dataset_network_plan=True,
    )
    affiliation_sql = npi_module._provider_directory_affiliation_evidence_sql(
        "mrf",
        has_catalog=True,
        has_dataset_network_plan=True,
    )

    for evidence_sql in (role_sql, affiliation_sql):
        assert "evidence_count AS MATERIALIZED" in evidence_sql
        assert "SELECT COUNT(*)::bigint AS evidence_row_total" in evidence_sql
        assert "CROSS JOIN evidence_count" in evidence_sql
        assert "COUNT(*) OVER ()" not in evidence_sql
        assert "plan.payload_json::jsonb - ARRAY[" in evidence_sql
        assert "LEFT JOIN current_resources AS plan" in evidence_sql
    assert "dataset_network_plan_candidates AS MATERIALIZED" in role_sql
    assert "dataset_network_plan_resource_keys AS MATERIALIZED" in role_sql
    assert "SELECT DISTINCT candidate.dataset_id, candidate.resource_id" in role_sql
    assert "dataset_network_plan_resources AS MATERIALIZED" in role_sql
    assert "dataset_affiliation_plan_candidates AS MATERIALIZED" in affiliation_sql
    assert "dataset_affiliation_plan_resource_keys AS MATERIALIZED" in affiliation_sql
    assert "dataset_affiliation_plan_resources AS MATERIALIZED" in affiliation_sql
    assert "insurance_plan.dataset_id = candidate.dataset_id" in role_sql
    assert "insurance_plan.dataset_id = candidate.dataset_id" in affiliation_sql
    assert npi_module.MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE == 100


def test_immutable_plan_lookup_index_matches_model_and_migration():
    index_spec = next(
        item
        for item in ProviderDirectoryDatasetResource.__my_additional_indexes__
        if item["name"]
        == "provider_directory_dataset_resource_plan_lookup_idx"
    )
    assert index_spec["index_elements"] == ("dataset_id", "resource_id")
    assert index_spec["where"] == "resource_type = 'InsurancePlan'"

    migration_text = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260713234000_provider_directory_plan_lookup_index.py"
    ).read_text()
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in migration_text
    assert "20260713233000_provider_directory_resource_identifiers" in migration_text
