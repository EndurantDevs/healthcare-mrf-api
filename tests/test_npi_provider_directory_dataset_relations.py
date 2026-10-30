# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api.endpoint import npi as npi_module
from db.provider_directory_readiness import READINESS_INDEXES


def _role_sql(*, use_relations: bool) -> str:
    return npi_module._provider_directory_role_evidence_sql(
        "mrf",
        has_catalog=True,
        has_affiliations=True,
        has_dataset_network_plan=use_relations,
        has_dataset_affiliation_organization=use_relations,
    )


def test_current_relation_proof_is_dataset_bound():
    sql = _role_sql(use_relations=True)

    assert "dataset.dataset_network_plan_metadata ->> 'complete'" in sql
    assert "dataset.dataset_network_plan_metadata ->> 'version'" in sql
    assert "dataset.dataset_network_plan_metadata ->> 'dataset_id'" in sql
    assert "dataset.dataset_affiliation_organization_metadata ->> 'complete'" in sql
    assert "dataset.dataset_affiliation_organization_metadata ->> 'version'" in sql
    assert "dataset.dataset_affiliation_organization_metadata ->> 'dataset_id'" in sql
    assert "resource.dataset_id AS dataset_id" in sql


def test_current_dataset_readiness_is_scoped_to_requested_source_endpoints():
    for sql in (
        _role_sql(use_relations=True),
        npi_module._provider_directory_affiliation_evidence_sql("mrf", has_catalog=True),
    ):
        counts, metadata = sql.split("), current_dataset_metadata AS MATERIALIZED (", 1)
        metadata, datasets = metadata.split("current_datasets AS MATERIALIZED (", 1)
        datasets, _resources = datasets.split("), current_resources AS NOT MATERIALIZED (", 1)
        assert "GROUP BY dataset.endpoint_id" in counts
        assert "HAVING COUNT(*) = 1" in counts
        assert ":source_ids" not in counts
        assert "dataset.endpoint_id IN (" in metadata
        assert "SELECT requested_source.endpoint_id" in metadata
        assert "FROM mrf.provider_directory_source AS requested_source" in metadata
        assert "WHERE requested_source.source_id = ANY(:source_ids)" in metadata
        assert "JOIN mrf.provider_directory_source" not in metadata
        assert "dataset.status = 'published'" in metadata
        assert "dataset.superseded_at IS NULL" in metadata
        assert metadata.count("publication_metadata_json::jsonb") == 2
        assert "-> 'dataset_network_plan' AS dataset_network_plan_metadata" in metadata
        assert "-> 'dataset_affiliation_organization' AS dataset_affiliation_organization_metadata" in metadata
        assert "publication_metadata_json" not in datasets
        assert "FROM current_dataset_metadata AS dataset" in datasets
        assert datasets.count(") = 'true'") == 2
        assert datasets.count(") = '1'") == 2
        assert datasets.count(") = dataset.dataset_id") == 2
        assert "::boolean" not in datasets
        assert "AS dataset_network_plan_complete" in datasets
        assert "AS dataset_affiliation_organization_complete" in datasets


def test_role_sql_uses_dataset_relations():
    sql = _role_sql(use_relations=True)

    assert "provider_directory_dataset_network_plan AS network_plan" in sql
    assert "network_plan.dataset_id = role_network.dataset_id" in sql
    assert "network_plan.network_resource_id = role_network.resource_id" in sql
    assert "dataset_network_plan_candidates AS MATERIALIZED" in sql
    assert "network_plan.insurance_plan_resource_id AS resource_id" in sql
    assert "JOIN mrf.provider_directory_dataset_resource AS insurance_plan" in sql
    assert "insurance_plan.dataset_id = candidate.dataset_id" in sql
    assert "insurance_plan.resource_id = candidate.resource_id" in sql
    assert "direct_plan.resource_id =" in sql
    assert "network_plan.insurance_plan_resource_id" in sql
    assert "provider_directory_dataset_affiliation_organization" in sql
    assert "affiliation_locator.dataset_id = role_organization.dataset_id" in sql


def test_role_sql_falls_back_per_incomplete_dataset():
    sql = _role_sql(use_relations=True)

    assert "NOT role_network.dataset_network_plan_complete" in sql
    assert "legacy_role_insurance_plans_sources AS MATERIALIZED" in sql
    assert "WHERE NOT dataset_network_plan_complete" in sql
    assert "JOIN legacy_role_insurance_plans AS insurance_plan" in sql
    assert "NOT direct_plan.dataset_network_plan_complete" in sql
    assert "NOT role_organization.dataset_affiliation_organization_complete" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "provider_directory_organization_affiliation AS affiliation_locator" in sql


def test_role_sql_without_relations_is_legacy_only():
    sql = _role_sql(use_relations=False)

    assert "provider_directory_dataset_network_plan AS network_plan" not in sql
    assert "provider_directory_dataset_affiliation_organization" not in sql
    assert "provider_directory_organization_affiliation AS affiliation_locator" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql


@pytest.mark.parametrize("affiliation", [False, True])
@pytest.mark.parametrize("available_indexes", [0, 1, 2])
async def test_retained_readiness_uses_exact_header_despite_current_indexes(affiliation, available_indexes):
    statements = []

    class EvidenceResult:
        def __init__(self, rows=()):
            self.rows = rows

        def all(self):
            return self.rows

    class EvidenceSession:
        async def execute(self, statement, params=None):
            statements.append(str(statement))
            if params and "table_names" in params:
                rows = [{"table_name": name, "is_available": True} for name in params["table_names"]]
                rows.extend(
                    {"table_name": name, "is_available": position < available_indexes}
                    for position, name in enumerate(params["readiness_index_names"])
                )
                assert params["readiness_index_names"] == [name for _key, name in READINESS_INDEXES]
                return EvidenceResult(rows)
            return EvidenceResult()

    fetch = (
        npi_module._fetch_provider_directory_affiliation_evidence_map
        if affiliation
        else npi_module._fetch_provider_directory_role_evidence_map
    )
    assert (
        await fetch(
            [("source", "resource")], session=EvidenceSession(), source_datasets=(("source", "dataset-source"),)
        )
        == {}
    )
    sql = statements[-1]
    metadata = sql.split("current_dataset_metadata AS MATERIALIZED (", 1)[1].split(
        "current_datasets AS MATERIALIZED (", 1
    )[0]
    assert metadata.count("-> 'dataset_network_plan' AS dataset_network_plan_metadata") == 1
    assert metadata.count("-> 'dataset_affiliation_organization' AS dataset_affiliation_organization_metadata") == 1
    assert "AS ready WHERE ready.dataset_id = dataset.dataset_id" not in sql
    assert "requested_dataset_scope AS MATERIALIZED" in sql
    assert "dataset.dataset_network_plan_metadata ->> 'complete'" in sql
    assert "dataset.dataset_affiliation_organization_metadata ->> 'complete'" in sql
