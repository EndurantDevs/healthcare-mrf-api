# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic permanent omissions have no remappable edge or CMS transform identity."""

from dataclasses import asdict, replace

import pytest

from process import network_fhir_membership_source as fhir
from process import registry_source_contribution_store as facts
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_retained_capture_contract import GENERIC_UNVERIFIED_NETWORK
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from tests.test_network_fhir_retained_capture import pin, retained


def _legacy_pin(model=fhir.PinnedFHIRMembershipSource):
    return model(
        "source_example",
        "cms-npd",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "release-one",
        101,
        "medical",
        "2026-01-01",
    )


def _recipe(source):
    capture = source.retained_capture
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source.source_id,
        source.schema_name,
        source.dataset_id,
        "registry-retained-normalized-fhir-v1" if capture else "producer-one",
        capture.capture_id if capture else "edition-one",
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


def _sentinel(source):
    return dict(
        resource_type="PractitionerRole",
        resource_id="role-one",
        payload_sha256="a" * 64,
        omission_reason=GENERIC_UNVERIFIED_NETWORK,
        source_key=None,
        source_scope_json=None,
        provider_system=None,
        provider_id=None,
        location_id=None,
        evidence_basis=None,
    )


def test_generic_flat_codec_physical_accessors_and_transform_are_distinct():
    source = pin(retained())
    recipe = _recipe(source)
    assert fhir.fhir_physical_source(source) is source
    assert source.read_schema_name == source.retained_capture.schema_name
    assert (
        source.read_resource_table_oid
        == dict(source.retained_capture.relation_oids)["provider_directory_dataset_resource"]
    )
    assert "retained_successor" not in source.coordinates
    wire = canonical_registry_source_recipes((recipe,))
    assert decode_registry_source_recipes(wire) == (recipe,)
    identity, capture = facts.recipe_contribution_identity(recipe)
    assert capture == facts._digest([identity, facts.GENERIC_TRANSFORM_VERSION])
    assert capture != facts._digest([identity, facts.TRANSFORM_VERSION])


def test_generic_successor_and_mixed_or_nested_custody_do_not_gain_authority():
    source = pin(retained())
    with pytest.raises(fhir.FHIRMembershipSourceError):
        fhir.with_fhir_successor(source, pin(retained()))
    with pytest.raises(fhir.FHIRMembershipSourceError):
        replace(source, retained_successor=source)
    with pytest.raises(fhir.FHIRMembershipSourceError):
        replace(_legacy_pin(), retained_capture=source.retained_capture)


@pytest.mark.parametrize(
    "field", ["source_key", "source_scope_json", "provider_system", "provider_id", "location_id", "evidence_basis"]
)
def test_permanent_generic_sentinel_requires_every_edge_field_null(field):
    source = pin(retained())
    sentinel = _sentinel(source)
    facts._validate_fhir_fact(sentinel, source)
    with pytest.raises(ValueError):
        facts._validate_fhir_fact({**sentinel, field: "forbidden-edge"}, source)


def test_permanent_generic_omissions_never_map_or_enter_cms():
    source = pin(retained())
    with pytest.raises(ValueError):
        facts._validate_fhir_fact(_sentinel(source), _legacy_pin())
    assert "ON fact_json->>'omission_reason' IS NULL AND" in facts._FACT_MAP_SQL
    assert "fact_json->>'omission_reason'='generic_unverified_unapproved_network'" in facts._FACT_MAP_SQL


def test_generic_sentinel_receipt_binds_its_own_transform_and_complete_resource_count():
    recipe = _recipe(pin(retained()))
    identity, capture = facts.recipe_contribution_identity(recipe)
    descriptor_by_field = dict(
        recipe_sha256=identity,
        capture_sha256=capture,
        mapping_sha256="c" * 64,
        fact_sha256="d" * 64,
        source_generation=facts._digest([capture, "c" * 64]),
        transform_version=facts.GENERIC_TRANSFORM_VERSION,
        fact_rows=1,
        membership_rows=0,
        omitted_rows=1,
        raw_source_rows=1,
        accepted_raw_source_rows=0,
        omitted_raw_source_rows=1,
        provider_only_rows=0,
    )
    receipt_by_field = dict(
        component=facts.SUMMARY_KEY, revision=1, table_name=facts.CONTRIBUTION_TABLE, sources=[descriptor_by_field]
    )
    assert facts.validate_registry_source_contribution_receipt(receipt_by_field) == receipt_by_field
    with pytest.raises(facts.RegistrySourceContributionError):
        facts.validate_registry_source_contribution_receipt(
            {**receipt_by_field, "sources": [{**descriptor_by_field, "transform_version": facts.TRANSFORM_VERSION}]}
        )
    with pytest.raises(facts.RegistrySourceContributionError):
        facts.validate_registry_source_contribution_receipt(
            {**receipt_by_field, "sources": [{**descriptor_by_field, "raw_source_rows": 2}]}
        )


def test_witnessed_generic_facts_remain_remappable_and_keep_physical_evidence():
    source_pin = pin(retained())
    fact_by_field = dict(
        resource_type="PractitionerRole",
        resource_id="role-one",
        payload_sha256="a" * 64,
        omission_reason=None,
        source_key="network-one",
        source_scope_json={
            "organization_id": "network-one",
            "capture_sha256": source_pin.retained_capture.capture_sha256,
        },
        provider_system="npi",
        provider_id="1000000004",
        location_id=None,
        evidence_basis=[
            source_pin.dataset_id,
            source_pin.source_id,
            source_pin.release_id,
            "PractitionerRole",
            "role-one",
            "a" * 64,
            None,
            "1000000004",
            None,
        ],
    )
    facts._validate_fhir_fact(fact_by_field, source_pin)
    with pytest.raises(ValueError):
        facts._validate_fhir_fact(
            {**fact_by_field, "source_scope_json": {**fact_by_field["source_scope_json"], "capture_sha256": "0" * 64}},
            source_pin,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("generic", [False, True])
async def test_provider_scope_selection_keeps_cms_alias_and_original_generic_capture(monkeypatch, generic):
    """Check SQL selection after isolated authority doubles; native tests prove admission."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from api import network_provider_only_read as readers
    from process.network_approved_membership_source import ApprovedMembershipSource

    source_pin = pin(retained()) if generic else _legacy_pin()
    recipe, approved = _recipe(source_pin), ApprovedMembershipSource(1, "a" * 64)
    current = SimpleNamespace(
        candidate_id=str(uuid4()),
        approved_custom_revision=1,
        source_generations={facts.SUMMARY_KEY: "b" * 64, "custom_membership": approved.generation_id},
    )
    monkeypatch.setattr(readers, "verify_registry_source_contributions", lambda _: {"sources": []})
    monkeypatch.setattr(readers, "verify_registry_source_recipes", lambda _: (recipe,))
    from process.network_compact_layout import CompactAddressRelation

    relation = CompactAddressRelation("control_example", "registry_approved_record", 101)
    approval_pin = AsyncMock(return_value=(approved, relation))
    mapping_pin = AsyncMock()
    monkeypatch.setattr(readers, "pin_api_retained_approved_membership_source", approval_pin)
    monkeypatch.setattr(readers, "require_retained_approved_network_source_bindings", mapping_pin)
    monkeypatch.setattr(readers, "_fact_relations", AsyncMock(return_value="closed_fact_relation"))
    connection = SimpleNamespace(get_server_pid=lambda: 99)
    _, selected, arguments = await readers._source_bindings(connection, current, "control_example", candidate={})
    approval_pin.assert_awaited_once_with(connection, current, control_schema="control_example")
    assert mapping_pin.await_args.kwargs["read_identity"] == (relation, 99)
    expected_key = "capture_sha256" if generic else "alias_scope"
    expected_value = source_pin.retained_capture.capture_sha256 if generic else source_pin.alias_scope
    assert arguments[-1] == expected_value
    assert f"->>'{expected_key}'=$9::text" in selected[0]
    assert ("->>'organization_id'=record_json->>'source_key'" in selected[0]) is generic
