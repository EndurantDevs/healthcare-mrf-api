# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan-tier graph versioning preserves the old graph and exact semantic identities."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.network_fhir_plan_tier_graph import semantic_network_references
from process.network_fhir_source_witness_contract import source_origin
from process.network_fhir_typed_contract import (
    CONTRACT,
    PLAN_TIER_CONTRACT,
    PLAN_TIER_NETWORK_POLICY,
    graph_document,
    validate_graph_document,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_registry_fhir_plan_tier_bindings import tier_document
from tests.test_registry_typed_source_recipe import _recipe


def test_plan_tier_semantics_have_a_distinct_closed_graph_version():
    original = _recipe().source_pin.capture["graph"]
    before = deepcopy(original)
    origin = replace(
        source_origin(original["origin"]),
        source_id=UHC_PROVIDER_FILE_SOURCE_ID,
        source_aliases=(UHC_PROVIDER_FILE_SOURCE_ID,),
    )
    upgraded = graph_document(origin, original["membership_as_of"], original["counts"], plan_tiers=True)
    assert upgraded["contract"] == PLAN_TIER_CONTRACT and upgraded["derivation_version"] == 4
    assert upgraded["network_policy"] == PLAN_TIER_NETWORK_POLICY
    assert "database_identity" not in upgraded
    assert upgraded["office_policy"] == original["office_policy"]
    assert validate_graph_document(upgraded) == upgraded
    assert validate_graph_document(original) == before and original["contract"] == CONTRACT
    with pytest.raises(ValueError):
        validate_graph_document({**upgraded, "network_policy": "unknown"})
    with pytest.raises(ValueError):
        validate_graph_document({**original, "network_policy": PLAN_TIER_NETWORK_POLICY})
    with pytest.raises(ValueError):
        graph_document(
            asdict(source_origin(original["origin"])),
            original["membership_as_of"],
            original["counts"],
            plan_tiers=True,
        )


def test_raw_refs_preserve_multiplicity_and_add_only_exact_semantic_key():
    role_payload_by_field = {"network_refs": ["Organization/direct", "Organization/direct"], "network_key_id": "a" * 64}
    before = deepcopy(role_payload_by_field)
    assert semantic_network_references(role_payload_by_field, ["Organization/direct", "Organization/plan"]) == [
        "Organization/direct",
        "Organization/direct",
        "Organization/plan",
        "UHCPlanTier/" + "a" * 64,
    ]
    assert role_payload_by_field == before
    assert semantic_network_references({"network_refs": [], "network_key_id": None}, []) == []


@pytest.mark.parametrize("key", [1, "a" * 63, "A" * 64, "invalid"])
def test_raw_semantic_refs_reject_malformed_typed_key(key):
    with pytest.raises(ValueError):
        semantic_network_references({"network_refs": [], "network_key_id": key}, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("use_serialized_payload", [False, True])
async def test_repeated_roles_validate_and_read_plan_metadata_once_per_distinct_tier(
    monkeypatch, use_serialized_payload
):
    from process import network_fhir_plan_tier_graph as graph

    tier = tier_document()
    role_payload_by_field = {
        "network_refs": [],
        "insurance_plan_refs": ["InsurancePlan/" + tier["plan_resource_id"]],
        **{name: tier[name] for name in ("plan_scope", "network_tier", "network_key_id")},
    }
    connection = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[
                {
                    "resource_id": tier["plan_resource_id"],
                    "declared_id": tier["plan_resource_id"],
                    "payload_sha256": "a" * 64,
                    "contract": "healthporta.uhc.provider-directory-canonical.v2",
                    "plan_scope": tier["plan_scope"],
                    "network_refs": [],
                }
            ]
        )
    )
    validator = Mock(wraps=graph.role_plan_tier)
    monkeypatch.setattr(graph, "role_plan_tier", validator)
    projection = graph._PlanProjection(connection, "synthetic_plans")
    for index in range(10):
        source_row_by_field = {
            "resource_type": "PractitionerRole",
            "resource_id": "role-" + str(index),
            "payload_json": json.dumps(role_payload_by_field) if use_serialized_payload else role_payload_by_field,
        }
        wire_row_by_field = {**source_row_by_field, "payload_json": json.dumps(role_payload_by_field)}
        prepared_rows = await projection.prepare((source_row_by_field,), (wire_row_by_field,))
        assert prepared_rows[0]["plan_network_refs"] == []
    assert connection.fetch.await_count == validator.call_count == 1
