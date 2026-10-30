# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Flat capture selection and old wire compatibility; no native admission claim."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_fhir_membership_source as fhir
from process import network_fhir_source_epoch as epoch
from process import network_registry_cms_prepared_cleanup as cleanup
from process import network_registry_cms_prepared_pair as prepared
from process.registry_source_recipe_store import (
    RegistrySourceRecipeError,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from tests.test_cms_registry_source_pair_slot import _prepared
from tests.test_registry_source_recipe_store_postgres import _cms_epoch_recipe


def _successor(original, dataset="accepted-next"):
    flat = replace(original, retained_epoch=None, dataset_id=dataset, dataset_sha256="1" * 64, release_id="2" * 64)
    epoch_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    receipt = replace(
        original.retained_epoch,
        origin_coordinates=tuple(flat.coordinates.values()),
        epoch_id=epoch_id,
        schema_name="registry_cms_epoch_" + epoch_id.hex,
        content_sha256="3" * 64,
    )
    return replace(flat, retained_epoch=receipt)


def test_successor_keeps_original_scope_and_replaces_without_a_chain():
    original = _cms_epoch_recipe().source_pin
    physical = _successor(original)
    wrapped = fhir.with_fhir_successor(original, physical)
    assert wrapped.dataset_id == original.dataset_id
    assert wrapped.read_schema_name == physical.read_schema_name
    assert wrapped.read_resource_table_oid == physical.read_resource_table_oid
    assert fhir.fhir_physical_source(wrapped) == physical
    assert fhir.with_fhir_successor(wrapped, physical) is wrapped
    next_physical = _successor(original, "accepted-third")
    advanced = fhir.with_fhir_successor(wrapped, next_physical)
    assert replace(advanced, retained_successor=None) == original
    assert advanced.retained_successor == next_physical
    assert fhir.with_fhir_successor(advanced, original) == original
    with pytest.raises(fhir.FHIRMembershipSourceError):
        fhir.with_fhir_successor(original, wrapped)


@pytest.mark.parametrize("field,value", [("source_id", "other"), ("endpoint_id", "other"), ("alias_scope", "other")])
def test_successor_cannot_change_enrollment_identity(field, value):
    original = _cms_epoch_recipe().source_pin
    changed = replace(_successor(original), retained_epoch=None, **{field: value})
    receipt = replace(original.retained_epoch, origin_coordinates=tuple(changed.coordinates.values()))
    with pytest.raises(fhir.FHIRMembershipSourceError):
        fhir.with_fhir_successor(original, replace(changed, retained_epoch=receipt))


def test_recipe_roundtrip_preserves_old_bytes_and_binds_both_captures():
    recipe = _cms_epoch_recipe()
    old_document = json.loads(canonical_registry_source_recipes((recipe,)))
    assert "retained_successor" not in old_document[0]["source_pin"]
    wrapped = replace(recipe, source_pin=fhir.with_fhir_successor(recipe.source_pin, _successor(recipe.source_pin)))
    wire = canonical_registry_source_recipes((wrapped,))
    assert decode_registry_source_recipes(wire) == (wrapped,)
    assert wrapped.binding_coordinates == recipe.binding_coordinates
    assert wrapped.source_pin.generation_id != recipe.source_pin.generation_id
    document = json.loads(wire)
    document[0]["source_pin"]["retained_successor"]["retained_successor"] = None
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(json.dumps(document))


def test_producer_request_wire_stays_flat_and_rejects_successor():
    pair = _prepared()
    document = pair.as_dict()
    assert "retained_successor" not in document["request"]["source_pin"]
    assert cleanup._request(json.loads(json.dumps(document["request"]))) == pair.request
    assert set(document["request"]["source_pin"]) == set(asdict(pair.request.source_pin)) - {
        "retained_successor",
        "retained_capture",
    }
    wrapped = fhir.with_fhir_successor(pair.recipe.source_pin, _successor(pair.recipe.source_pin))
    with pytest.raises(ValueError, match="registry_cms_retention_request_invalid"):
        replace(pair.request, source_pin=wrapped)
    with pytest.raises(epoch.FHIRSourceEpochError):
        epoch._origin(wrapped)
    document["request"]["source_pin"]["retained_successor"] = None
    with pytest.raises(ValueError):
        cleanup._request(document["request"])


@pytest.mark.asyncio
async def test_successor_declaration_requires_exact_closed_native_continuity():
    recipe = _cms_epoch_recipe()
    physical = _successor(recipe.source_pin)
    # The descriptor normally comes from the native approved pin; this test checks only its closed comparison.
    original_by_field = {
        **asdict(recipe.binding_coordinates),
        "alias_scope": physical.alias_scope,
        "source_key_kind": "organization_resource_id",
    }
    declaration_by_field = {**original_by_field, "dataset_id": physical.dataset_id, "edition_id": "accepted-next"}
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"declaration": json.dumps(declaration_by_field), "as_of": physical.as_of})
    )
    assert await fhir._successor_descriptor(connection, physical, original_by_field) == declaration_by_field
    for key, value in (("producer_id", "other"), ("source_key_kind", "other"), ("extra", "unbound")):
        connection.fetchrow.return_value = {
            "declaration": json.dumps({**declaration_by_field, key: value}),
            "as_of": physical.as_of,
        }
        with pytest.raises(fhir.FHIRMembershipSourceError):
            await fhir._successor_descriptor(connection, physical, original_by_field)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", None),
        ("endpoint_id", " spaced "),
        ("dataset_id", ""),
        ("release_id", "line\nbreak"),
        ("alias_scope", "a" * 513),
        ("retained_epoch", {}),
        ("retained_capture", {}),
    ],
)
def test_source_pin_rejects_unbound_or_malformed_coordinates_before_use(field, value):
    with pytest.raises(fhir.FHIRMembershipSourceError):
        replace(_cms_epoch_recipe().source_pin, **{field: value})


@pytest.mark.parametrize("untrusted", [None, {}, SimpleNamespace(retained_successor=None)])
def test_physical_source_selection_requires_the_exact_pin_type(untrusted):
    with pytest.raises(fhir.FHIRMembershipSourceError, match="Pinned FHIR source is invalid"):
        fhir.fhir_physical_source(untrusted)


def test_cms_successor_cannot_drop_the_retained_epoch_proof():
    original = _cms_epoch_recipe().source_pin
    without_epoch = replace(_successor(original), retained_epoch=None)
    with pytest.raises(fhir.FHIRMembershipSourceError, match="successor coordinates"):
        fhir.with_fhir_successor(original, without_epoch)


@pytest.mark.parametrize("field", ["retained_epoch", "retained_capture"])
def test_recipe_wire_rejects_malformed_retention_evidence_without_substitution(field):
    document = json.loads(canonical_registry_source_recipes((_cms_epoch_recipe(),)))
    document[0]["source_pin"][field] = {"untrusted": True}
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(json.dumps(document))
