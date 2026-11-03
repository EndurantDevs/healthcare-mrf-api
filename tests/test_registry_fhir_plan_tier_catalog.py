# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed catalog evidence keeps explicit references and rejects mismatched plans."""

import copy
from dataclasses import asdict

import pytest

from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_reference_rollup import staged_plan_tier_references
from process.registry_fhir_plan_tier_catalog import _validated_pair
from tests.test_registry_fhir_catalog_reference_rollup_postgres import (
    copy_resources,
    resource_row,
)
from tests.test_registry_fhir_catalog_reference_rollup_postgres import (
    reference_database as reference_database,
)
from tests.test_registry_fhir_plan_tier_bindings import tier_document
from tests.test_registry_fhir_plan_tier_pipeline import plan_tier_rows


def _rows():
    return [
        {field: row[field] for field in ("resource_type", "resource_id", "payload_hash", "payload_json")}
        for row in plan_tier_rows(1, "dataset")
        if row["resource_type"] in {"InsurancePlan", "PractitionerRole"}
    ]


def test_plan_declared_tiers_keep_organization_edges():
    rows = _rows()
    plan = next(row for row in rows if row["resource_type"] == "InsurancePlan")
    plan["payload_json"]["network_refs"] = ["Organization/network"]
    plan["payload_json"]["plan_json"]["network_tiers"] = ["preferred", "standard"]
    old = extract_fhir_catalog_batch([plan], profile="published_normalized")
    typed = extract_fhir_catalog_batch([plan], profile="published_normalized", plan_tiers=True)
    assert typed.edges[-1] == old.edges[0]
    declarations = [edge for edge in typed.edges if getattr(edge, "plan_tier", None) is not None]
    assert {edge.plan_tier["network_key_id"] for edge in declarations} == {
        tier_document(tier=tier)["network_key_id"] for tier in ("preferred", "standard")
    }
    assert typed.edges[0].plan_tier is None


@pytest.mark.parametrize("damage", ("absent", "different-plan", "different-scope", "forged-tier"))
def test_projection_requires_exact_observed_plan(damage):
    edges = extract_fhir_catalog_batch(_rows(), profile="published_normalized", plan_tiers=True).edges
    pair_by_kind = {"plan": asdict(edges[0]), "edge": asdict(edges[1])}
    if damage == "absent":
        pair_by_kind["plan"] = None
    elif damage == "different-plan":
        pair_by_kind["plan"]["owner_resource_id"] = "another-plan"
    elif damage == "different-scope":
        pair_by_kind["plan"]["plan_scope"] = tier_document(year=2027)["plan_scope"]
    else:
        pair_by_kind["edge"]["plan_tier"]["network_tier"] = "different"
    with pytest.raises(ValueError):
        _validated_pair(pair_by_kind)


@pytest.mark.asyncio
async def test_role_rollup_keeps_exact_tiers_and_direct_reference_counts(reference_database):
    fixture = reference_database
    role = next(row for row in _rows() if row["resource_type"] == "PractitionerRole")
    payload = role["payload_json"] | {"network_refs": ["Organization/network"]}
    await copy_resources(
        fixture, [resource_row("PractitionerRole", f"role-{index:04}", payload) for index in range(20)]
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with staged_plan_tier_references(
            fixture.connection, schema_name=fixture.schema, dataset_id="dataset", page_rows=7
        ) as stage:
            observations = [observation async for page in stage.iter_batches() for observation in page]
            assert stage.accounting.source_rows == 20 and stage.accounting.reference_rows == 40
            assert len(observations) == 2 and all(observation.reference_rows == 20 for observation in observations)
            assert {
                getattr(observation.edge, "plan_tier", None)["network_key_id"]
                for observation in observations
                if getattr(observation.edge, "plan_tier", None)
            } == {tier_document()["network_key_id"]}
            assert any(observation.edge.network_resource_id == "network" for observation in observations)


@pytest.mark.asyncio
async def test_rollup_refuses_a_forged_tier_without_hiding_it(reference_database):
    fixture = reference_database
    role = next(row for row in _rows() if row["resource_type"] == "PractitionerRole")
    payload = copy.deepcopy(role["payload_json"])
    payload["network_tier"] = "different"
    await copy_resources(fixture, [resource_row("PractitionerRole", "forged", payload)])
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ValueError):
            async with staged_plan_tier_references(
                fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
            ) as stage:
                [observation async for page in stage.iter_batches() for observation in page]
