# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""CMS projection policy is bound to the recipe and both transform paths."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.provider_directory_fhir import parse_fhir_resource
from process.provider_directory_projection_contract import (
    CMS_NPD_NPI_IDENTITY_POLICY,
    projection_recipe_identity,
    validated_physical_projection_recipe_identity,
)
from process.provider_directory_projection_copy_summary import (
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_native import (
    _launch_native_copy_process,
    _NativeExecutionSettings,
    validate_native_projection_recipe,
)
from process.provider_directory_projection_transform import projection_resource_row
from process.provider_directory_projection_types import ProviderDirectoryProjectionError
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.test_provider_directory_projection_contract_boundaries import (
    _recipe_arguments,
)
from tests.test_provider_directory_projection_workset_contract import _manifest


def _policy_claim():
    claim = synthetic_projection_context("ndjson").claim
    arguments = _recipe_arguments(_manifest())
    arguments.update(
        decoder_contract_id=NATIVE_DECODER_CONTRACT_ID,
        transform_contract_id=NATIVE_TRANSFORM_CONTRACT_ID,
        source_ids=("cms-npd",),
        transform_context={
            **arguments["transform_context"],
            "npi_identity_policy": CMS_NPD_NPI_IDENTITY_POLICY,
        },
    )
    recipe = projection_recipe_identity(**arguments).physical
    return replace(claim, recipe_lease=replace(claim.recipe_lease, recipe=recipe))


def _row(claim, identifier=None, *, resource_type="Organization"):
    resource_by_field = {"resourceType": resource_type, "id": "1003000126", "active": True}
    if identifier is not None:
        resource_by_field["identifier"] = identifier
    return projection_resource_row(
        resource_by_field,
        claim=claim,
        input_ordinal=0,
        payload_hash=hashlib.sha256(repr(resource_by_field).encode()).hexdigest(),
    )


def test_cms_recipe_requires_versioned_policy_and_has_distinct_physical_identity():
    arguments = _recipe_arguments(_manifest())
    legacy = projection_recipe_identity(**arguments)
    arguments["source_ids"] = ("cms-npd",)
    with pytest.raises(ProviderDirectoryProjectionError, match="cms_npd_npi_policy_required"):
        projection_recipe_identity(**arguments)

    arguments["transform_context"] = {
        **arguments["transform_context"],
        "npi_identity_policy": CMS_NPD_NPI_IDENTITY_POLICY,
    }
    cms = projection_recipe_identity(**arguments)
    assert cms.recipe_id != legacy.recipe_id
    assert validated_physical_projection_recipe_identity(cms.physical) == cms.physical

    arguments["source_ids"] = ("cms-npd", "source-a")
    with pytest.raises(ProviderDirectoryProjectionError, match="cms_npd_npi_policy_required"):
        projection_recipe_identity(**arguments)

    arguments["source_ids"] = ("source-a",)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="npi_identity_policy_source_invalid",
    ):
        projection_recipe_identity(**arguments)


def test_cms_projection_accepts_only_checksum_valid_explicit_npi():
    legacy_claim = synthetic_projection_context("ndjson").claim
    cms_claim = _policy_claim()
    assert _row(legacy_claim)["summary_npi"] == 1003000126
    assert _row(cms_claim)["summary_npi"] is None
    assert _row(cms_claim, [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}])["summary_npi"] is None
    assert (
        _row(cms_claim, [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}])["summary_npi"]
        == 1234567893
    )
    assert (
        _row(cms_claim, [{"type": {"text": "National Provider Identifier"}, "value": "1234567893"}])["summary_npi"]
        == 1234567893
    )


def test_legacy_numeric_suffix_keeps_canonical_and_physical_npi():
    identifier_by_field = {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893\u2163"}
    _, canonical_row = parse_fhir_resource(
        "source-a",
        {"resourceType": "Organization", "id": "1003000126", "identifier": [identifier_by_field]},
    )
    physical_row = _row(synthetic_projection_context("ndjson").claim, [identifier_by_field])
    assert canonical_row["npi"] == physical_row["summary_npi"] == 1234567893


def test_cms_affiliation_preserves_direct_plan_references_without_inference():
    affiliation_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": "affiliation-example",
        "organization": {"reference": "Organization/insurer-example"},
        "participatingOrganization": {"reference": "Organization/group-example"},
        "insurancePlan": [
            {"reference": "InsurancePlan/plan-example"},
            {"reference": "InsurancePlan/unresolved"},
        ],
    }
    _, explicit_row = parse_fhir_resource("cms-npd", affiliation_by_field)
    _, sparse_row = parse_fhir_resource(
        "cms-npd", {key: value for key, value in affiliation_by_field.items() if key != "insurancePlan"}
    )
    assert explicit_row["insurance_plan_refs"] == ["InsurancePlan/plan-example", "InsurancePlan/unresolved"]
    assert sparse_row["insurance_plan_refs"] == []
    assert "relationship_type" not in explicit_row and "ownership_status" not in explicit_row


@pytest.mark.parametrize(
    "resource_type",
    ("HealthcareService", "Organization", "Practitioner", "PractitionerRole"),
)
@pytest.mark.parametrize(
    ("identifier_value", "expected_npi"),
    (
        ("123-456-7893", 1234567893),
        ("1234567893\u0661", None),
        ("1234567893\u00b2", None),
        ("1234567893\u2163", None),
    ),
)
def test_cms_projection_rejects_unicode_numeric_suffix(resource_type, identifier_value, expected_npi):
    identifiers = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": identifier_value}]
    projection_row = _row(_policy_claim(), identifiers, resource_type=resource_type)
    assert projection_row["summary_npi"] == expected_npi


def test_native_recipe_rejects_policy_added_without_rehashing():
    claim = synthetic_projection_context("ndjson").claim
    forged_recipe = replace(
        claim.recipe_lease.recipe,
        transform_context={
            **claim.recipe_lease.recipe.transform_context,
            "npi_identity_policy": CMS_NPD_NPI_IDENTITY_POLICY,
        },
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="physical_recipe_mismatch"):
        validate_native_projection_recipe(forged_recipe)


@pytest.mark.asyncio
async def test_native_command_receives_recipe_bound_policy(monkeypatch):
    commands = []

    async def launch(*args, **_kwargs):
        commands.append(args)
        return SimpleNamespace(stdin=object(), stdout=object(), stderr=object())

    monkeypatch.setattr("asyncio.create_subprocess_exec", launch)
    settings = _NativeExecutionSettings(1, "/synthetic-native-runner")
    for claim in (synthetic_projection_context("ndjson").claim, _policy_claim()):
        await _launch_native_copy_process(claim, "ndjson", settings)
    assert len(commands[0]) == 6
    assert commands[0][-1] == "ndjson"
    assert commands[1][-2:] == ("ndjson", CMS_NPD_NPI_IDENTITY_POLICY)
