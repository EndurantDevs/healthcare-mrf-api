# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained product selectors share complete expansion and bounded preview semantics."""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_membership_source as membership
from process import registry_imported_selection_preview as preview
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_retained_capture_contract import PRODUCER
from tests.test_network_fhir_retained_capture import pin, retained


def _recipe():
    capture = retained()
    source = pin(capture)
    return SimpleNamespace(
        source_pin=source,
        binding_coordinates=RegistryNetworkSourceCoordinates(
            "fhir", source.source_id, source.schema_name, source.dataset_id, PRODUCER, capture.capture_id
        ),
    )


def test_product_preview_reuses_complete_mapping_guard_without_membership_paging():
    recipe = _recipe()
    sql = preview._impact_sql(recipe, '"registry_example"', '"selected_example"', "registry_example")
    shared = membership.effective_fhir_network_bindings_sql(
        recipe.source_pin.read_schema_name,
        dataset_parameter="$8",
        source_parameter="$9",
        release_parameter="$10",
        capture_parameter="$13",
    )
    assert shared in sql
    assert "(SELECT invalid FROM fhir_product_selection_guard) AS product_selection_invalid" in sql
    assert "LIMIT" not in sql and "resource_documents AS" not in sql
    assert (
        "count(DISTINCT (resource_type,resource_id,payload_hash,network_id,provider_system,provider_id,site_id))" in sql
    )
    assert set(map(int, re.findall(r"\$(\d+)", sql))) == set(range(1, 14))


def test_product_extraction_bounds_resolved_fanout_and_preserves_missing_witness_refusal():
    sql = membership._extraction_sql(_recipe().source_pin, '"registry_example"', True, True)
    assert "approved.network_resource_id=expanded.network_resource_id" in sql
    assert "approved.record_key IS NOT NULL AND NOT (network.resource_id IS NOT NULL" in sql
    assert "expanded.network_resource_id IS NOT NULL AND approved.record_key IS NULL AS omitted" in sql
    assert "LIMIT 5001 /* reviewed mappings */" in sql
    assert "SELECT DISTINCT jsonb_build_object('network_id',network_id" in sql
    assert "(SELECT count(*) FROM resolved)<=5000" in sql
    assert "+octet_length(source_outcomes)+(SELECT octet_length(facts) FROM fact_transfer)<=8388608" in sql


@pytest.mark.parametrize("parameter", ["", "$0", "$1 OR true", 1])
def test_mapping_helper_refuses_nonparameter_sql_fragments(parameter):
    with pytest.raises(membership.FHIRMembershipSourceError, match="SQL parameters"):
        membership.effective_fhir_network_bindings_sql("synthetic_source", dataset_parameter=parameter)


def test_incomplete_selected_plan_refuses_even_an_empty_provider_page():
    with pytest.raises(membership.FHIRMembershipSourceError, match="product plan is unavailable"):
        membership._fhir_batch(_recipe().source_pin, {"product_selection_invalid": True}, None, None, True)


@pytest.mark.asyncio
async def test_retained_preview_binds_capture_without_changing_expanded_source_fact_kind(monkeypatch):
    recipe = _recipe()
    monkeypatch.setattr(preview, "_require_recipe_custody", AsyncMock())
    require_source = AsyncMock()
    monkeypatch.setattr(preview, "require_fhir_source", require_source)
    parameters = await preview._require_recipe_source(object(), recipe)
    assert parameters[-1] == recipe.source_pin.retained_capture.capture_sha256
    assert len(parameters) == 6
    assert require_source.await_args.args[2]["source_key_kind"] == "organization_resource_id"
