# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Serialized input proofs must retain their exact database, scope and native roots."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_native_inputs as inputs
from tests.test_cms_protected_fhir_inputs import _physical, _policy
from tests.test_cms_protected_fhir_inputs import catalog as catalog
from tests.test_provider_directory_cms_address import address_build_case as address_build_case


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["database", "schema", "parent_oid", "missing_parent", "dual_tiger"])
async def test_fhir_descriptor_cannot_borrow_foreign_database_or_replaced_parent(catalog, changed):
    proof = await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    fence_by_field = dict(
        version=5,
        database=dict(database_oid=proof["database_oid"]),
        fhir_protected=proof,
        relations=_physical(proof),
    )
    inputs._require_fhir_fence(fence_by_field, "mrf")
    before = deepcopy(proof)
    previous_events = list(catalog.events)
    schema = "mrf"
    match changed:
        case "database":
            fence_by_field["database"]["database_oid"] += 1
        case "schema":
            schema = "foreign_schema"
        case "parent_oid":
            fence_by_field["relations"]['"mrf"."' + proof["parent_oids"][0][0] + '"']["relation_oid"] += 1
        case "missing_parent":
            del fence_by_field["relations"]['"mrf"."' + proof["parent_oids"][0][0] + '"']
        case "dual_tiger":
            fence_by_field.update(tiger_protected={}, tiger_captured={})
    with pytest.raises(RuntimeError, match="cms_address_native_fhir_proof_changed"):
        inputs._require_fhir_fence(fence_by_field, schema)
    assert proof == before
    assert catalog.events == previous_events


@pytest.mark.parametrize(
    "claim,version,error",
    [
        ("nucc_protected", 1, "nucc_proof_changed"),
        ("tiger_protected", 1, "tiger_proof_changed"),
        ("tiger_captured", 1, "tiger_proof_changed"),
        ("tiger_protected", 3, "tiger_proof_changed"),
        ("tiger_captured", 4, "tiger_proof_changed"),
    ],
)
def test_input_fence_version_cannot_smuggle_or_omit_a_protected_family(address_build_case, claim, version, error):
    fence_by_field = deepcopy(address_build_case[4])
    inputs.require_supported_native_address_inputs(fence_by_field)
    fence_by_field["version"] = version
    if version == 1:
        fence_by_field[claim] = object()
    with pytest.raises(RuntimeError, match="cms_address_native_" + error):
        inputs.require_supported_native_address_inputs(fence_by_field)


def test_native_input_fence_requires_the_complete_office_read_roster(address_build_case):
    fence_by_field = deepcopy(address_build_case[4])
    inputs.require_supported_native_address_inputs(fence_by_field)
    fence_by_field["cms_office_read_relations"].pop(next(iter(fence_by_field["cms_office_read_relations"])))
    with pytest.raises(RuntimeError, match="cms_address_native_office_read_fence_invalid"):
        inputs.require_supported_native_address_inputs(fence_by_field)
