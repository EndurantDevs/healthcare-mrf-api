# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api.endpoint import npi as npi_module


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

    assert "-> 'dataset_network_plan' ->> 'complete'" in sql
    assert "-> 'dataset_network_plan' ->> 'version'" in sql
    assert "-> 'dataset_network_plan' ->> 'dataset_id'" in sql
    assert "-> 'dataset_affiliation_organization' ->> 'complete'" in sql
    assert "-> 'dataset_affiliation_organization' ->> 'version'" in sql
    assert "-> 'dataset_affiliation_organization' ->> 'dataset_id'" in sql
    assert "resource.dataset_id AS dataset_id" in sql


def test_role_sql_uses_dataset_relations():
    sql = _role_sql(use_relations=True)

    assert "provider_directory_dataset_network_plan AS network_plan" in sql
    assert "network_plan.dataset_id = role_network.dataset_id" in sql
    assert "network_plan.network_resource_id = role_network.resource_id" in sql
    assert "JOIN current_insurance_plans AS insurance_plan" in sql
    assert "insurance_plan.resource_id = network_plan.insurance_plan_resource_id" in sql
    assert "network_plan.insurance_plan_resource_id = direct_plan.resource_id" in sql
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
