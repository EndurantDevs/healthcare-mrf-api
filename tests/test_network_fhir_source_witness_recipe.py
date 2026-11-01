# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure Source recipe compatibility and refusal tests; synthetic receipts confer no native custody."""

import hashlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_fhir_membership_source as fhir
from process import network_fhir_retained_capture_contract as reader
from process import network_fhir_source_witness_contract as witness
from process import network_provider_only_projection as provider_only
from process import registry_source_contribution_store as facts
from process import registry_source_fact_offices as fact_offices
from process import registry_source_inventory_store as inventory_store
from process import registry_source_recipe_composition as composition
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_source_inventory_contract import RegistrySourceInventoryError, make_source_inventory_entry
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    RegistrySourceRecipeError,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from tests.test_network_fhir_source_witness_contract import captured, reader_capture

_LEGACY_COORDINATES = {
    "schema_name": "source_example",
    "source_id": "source-example",
    "endpoint_id": "endpoint-example",
    "dataset_id": "dataset-one",
    "dataset_sha256": "a" * 64,
    "release_id": "release-one",
    "resource_table_oid": 123,
    "alias_scope": "medical",
    "as_of": "2026-10-01",
}


def _coordinates(pin, producer, edition):
    return RegistryNetworkSourceCoordinates("fhir", pin.source_id, pin.schema_name, pin.dataset_id, producer, edition)


def _source_recipe():
    """Keep the expected Core anchor explicit and separate from the synthetic capture."""
    anchor = witness.SourceWitnessCaptureAnchor(
        "00000000-0000-0000-0000-000000000001",
        "registry-source-witness:00000000-0000-0000-0000-000000000001",
        "2026-01-02T00:00:00.000000+00:00",
    )
    capture = captured()
    origin = capture.origin
    pin = fhir.PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        capture.release_id,
        origin.resource_parent_oid,
        "medical",
        origin.semantic_projection_as_of,
        source_witness_capture=capture,
        source_witness_anchor=anchor,
    )
    return RegistrySourceMembershipRecipe(pin, _coordinates(pin, witness.PRODUCER, capture.capture_id))


def _reader_recipe():
    capture = reader_capture()
    origin = capture.origin
    pin = fhir.PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        capture.release_id,
        origin.resource_parent_oid,
        "medical",
        "2026-01-01",
        retained_capture=capture,
    )
    return RegistrySourceMembershipRecipe(pin, _coordinates(pin, reader.PRODUCER, capture.capture_id))


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def test_old_pin_bytes_and_generation_remain_exact():
    pin = fhir.PinnedFHIRMembershipSource(**_LEGACY_COORDINATES)
    coordinates = _coordinates(pin, "producer-example", "edition-one")
    recipe = RegistrySourceMembershipRecipe(pin, coordinates)
    expected_wire = _canonical(
        [
            {
                "binding_coordinates": asdict(coordinates),
                "selection_policy": "approved-bindings-only",
                "source_kind": "fhir",
                "source_pin": _LEGACY_COORDINATES,
            }
        ]
    )
    assert pin.coordinates == _LEGACY_COORDINATES
    assert pin.generation_id == hashlib.sha256(_canonical(_LEGACY_COORDINATES).encode()).hexdigest()
    assert canonical_registry_source_recipes((recipe,)) == expected_wire
    assert decode_registry_source_recipes(expected_wire) == (recipe,)
    assert "source_witness" not in expected_wire


def test_reader_recipe_bytes_and_generation_keep_the_old_capture():
    recipe = _reader_recipe()
    pin = recipe.source_pin
    expected_pin_by_field = {
        **{name: getattr(pin, name) for name in _LEGACY_COORDINATES},
        "retained_capture": reader_capture().as_dict(),
    }
    expected_wire = _canonical(
        [
            {
                "binding_coordinates": asdict(recipe.binding_coordinates),
                "selection_policy": "approved-bindings-only",
                "source_kind": "fhir",
                "source_pin": expected_pin_by_field,
            }
        ]
    )
    assert pin.coordinates == expected_pin_by_field
    assert pin.generation_id == hashlib.sha256(_canonical(expected_pin_by_field).encode()).hexdigest()
    assert canonical_registry_source_recipes((recipe,)) == expected_wire
    assert decode_registry_source_recipes(expected_wire) == (recipe,)
    assert "source_witness" not in expected_wire
    assert type(pin.retained_capture) is reader.RetainedFHIRCapture


def test_source_pair_has_closed_roundtrip_and_original_coordinates():
    recipe = _source_recipe()
    pin, capture = recipe.source_pin, recipe.source_pin.source_witness_capture
    wire = canonical_registry_source_recipes((recipe,))
    decoded = decode_registry_source_recipes(wire)
    assert decoded == (recipe,) and canonical_registry_source_recipes(decoded) == wire
    document = json.loads(wire)[0]["source_pin"]
    assert set(document) == set(_LEGACY_COORDINATES) | {"source_witness_capture", "source_witness_anchor"}
    assert document["source_witness_capture"] == capture.as_dict()
    assert document["source_witness_anchor"] == asdict(pin.source_witness_anchor)
    assert type(decoded[0].source_pin.source_witness_capture) is witness.SourceWitnessCapture
    assert type(decoded[0].source_pin.source_witness_anchor) is witness.SourceWitnessCaptureAnchor
    assert pin.schema_name == capture.origin.schema_name
    assert pin.read_schema_name == capture.schema_name and pin.read_schema_name != pin.schema_name
    assert pin.resource_table_oid == capture.origin.resource_parent_oid
    assert pin.read_resource_table_oid == dict(capture.relation_oids)["provider_directory_dataset_resource"]
    assert pin.release_id == capture.release_id and pin.as_of == capture.origin.semantic_projection_as_of
    assert fhir.fhir_normalized_capture(pin) is capture and pin.retained_capture is None


@pytest.mark.parametrize(
    "changes",
    [
        {"source_witness_capture": None},
        {"source_witness_anchor": None},
        {"source_witness_capture": captured().as_dict()},
        {"source_witness_capture": reader_capture()},
        {"source_witness_anchor": {"capture_id": "untrusted"}},
        {"retained_capture": reader_capture()},
        {"retained_epoch": object()},
        {"custody_owner_role": "other_owner"},
    ],
)
def test_source_pair_requires_genuine_types_without_mixed_custody(changes):
    with pytest.raises(ValueError):
        replace(_source_recipe().source_pin, **changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_name", "other_schema"),
        ("source_id", "other-source"),
        ("endpoint_id", "other-endpoint"),
        ("dataset_id", "other-dataset"),
        ("dataset_sha256", "f" * 64),
        ("resource_table_oid", 999),
        ("release_id", "another-release"),
        ("as_of", "2026-01-03"),
    ],
)
def test_source_pin_cannot_cross_any_original_coordinate(field, value):
    with pytest.raises(ValueError):
        replace(_source_recipe().source_pin, **{field: value})


def test_source_successors_are_explicitly_deferred():
    source = _source_recipe().source_pin
    with pytest.raises(ValueError):
        replace(source, retained_successor=source)
    with pytest.raises(ValueError):
        fhir.with_fhir_successor(source, _reader_recipe().source_pin)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_capture",
        "missing_anchor",
        "null_capture",
        "null_anchor",
        "extra_pin",
        "extra_capture",
        "extra_anchor",
        "changed_anchor_time",
        "changed_source",
        "changed_digest",
        "changed_release",
        "target_oid",
        "reader_origin",
        "mixed_reader",
        "mixed_epoch",
        "mixed_custody",
        "successor",
        "old_producer",
        "wrong_edition",
    ],
)
def test_closed_source_recipe_rejects_malformed_or_mixed_wire(damage):
    document = json.loads(canonical_registry_source_recipes((_source_recipe(),)))
    pin = document[0]["source_pin"]
    if damage.startswith("missing_"):
        pin.pop("source_witness_" + damage.removeprefix("missing_"))
    elif damage.startswith("null_"):
        pin["source_witness_" + damage.removeprefix("null_")] = None
    else:
        mutations_by_kind = {
            "extra_pin": (pin, "extra", True),
            "extra_capture": (pin["source_witness_capture"], "extra", True),
            "extra_anchor": (pin["source_witness_anchor"], "extra", True),
            "changed_anchor_time": (pin["source_witness_anchor"], "observed_at", "2026-01-03T00:00:00.000000+00:00"),
            "changed_source": (pin, "source_id", "other-source"),
            "changed_digest": (pin, "dataset_sha256", "f" * 64),
            "changed_release": (pin, "release_id", "another-release"),
            "target_oid": (pin, "resource_table_oid", 30),
            "reader_origin": (pin["source_witness_capture"]["origin"], "reader_role", "untrusted_reader"),
            "mixed_reader": (pin, "retained_capture", reader_capture().as_dict()),
            "mixed_epoch": (pin, "retained_epoch", {}),
            "mixed_custody": (pin, "custody_owner_role", "other_owner"),
            "successor": (pin, "retained_successor", {}),
            "old_producer": (document[0]["binding_coordinates"], "producer_id", reader.PRODUCER),
            "wrong_edition": (document[0]["binding_coordinates"], "edition_id", "other-edition"),
        }
        changed_by_field, name, value = mutations_by_kind[damage]
        changed_by_field[name] = value
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(json.dumps(document))


def test_rehashed_capture_cannot_replace_the_root_anchor():
    recipe = _source_recipe()
    capture = recipe.source_pin.source_witness_capture
    identity = "00000000-0000-0000-0000-000000000002"
    changed = replace(capture, capture_id=identity, schema_name=witness.SCHEMA_PREFIX + UUID(identity).hex)
    changed = replace(changed, capture_sha256=witness.capture_digest(changed))
    assert witness.validate_capture(changed) == changed
    with pytest.raises(ValueError):
        replace(recipe.source_pin, source_witness_capture=changed, release_id=changed.release_id)


def test_source_mapping_requires_approval_and_exact_producer():
    recipe = _source_recipe()
    source = recipe.source_pin
    approved = ApprovedMembershipSource(1, "a" * 64)
    descriptor = fhir._reviewed_descriptor(source, approved, recipe.binding_coordinates, True)
    assert (
        descriptor["producer_id"] == witness.PRODUCER
        and descriptor["edition_id"] == source.source_witness_capture.capture_id
    )
    for approval, coordinates in ((None, None), (approved, None), (None, recipe.binding_coordinates)):
        with pytest.raises(ValueError):
            fhir._reviewed_descriptor(source, approval, coordinates, True)
    with pytest.raises(ValueError):
        fhir._reviewed_descriptor(source, None, None, False)
    for field, value in (("producer_id", reader.PRODUCER), ("edition_id", "other-edition"), ("source_id", "same-name")):
        coordinates = replace(recipe.binding_coordinates, **{field: value})
        with pytest.raises(ValueError):
            fhir._reviewed_descriptor(source, approved, coordinates, True)


def test_normalized_fact_scope_keeps_source_capture_provenance():
    pin = _source_recipe().source_pin
    capture = pin.source_witness_capture
    assert facts.fhir_fact_transform(pin) == facts.GENERIC_TRANSFORM_VERSION
    assert facts.fhir_fact_transform(_reader_recipe().source_pin) == facts.GENERIC_TRANSFORM_VERSION
    assert facts.fhir_fact_transform(fhir.PinnedFHIRMembershipSource(**_LEGACY_COORDINATES)) == facts.TRANSFORM_VERSION
    fact_by_field = {
        "source_key": "network-one",
        "source_scope_json": {
            "organization_id": "network-one",
            "capture_sha256": capture.capture_sha256,
        },
    }
    facts._validate_fhir_scope(fact_by_field, pin)
    for changed_scope in (
        fact_by_field["source_scope_json"] | {"capture_sha256": "0" * 64},
        fact_by_field["source_scope_json"] | {"legacy_uuid": str(UUID(int=1))},
        fact_by_field["source_scope_json"] | {"organization_id": "same-name"},
    ):
        with pytest.raises(ValueError):
            facts._validate_fhir_scope(fact_by_field | {"source_scope_json": changed_scope}, pin)


def test_source_sql_uses_normalized_hashes_and_no_reader_header():
    pin = _source_recipe().source_pin
    sql = fhir._source_witness_sql_v2(pin)
    assert '"' + pin.read_schema_name + '".provider_directory_dataset_resource' in sql
    assert "provider_directory_endpoint_dataset" not in sql
    assert "provider_directory_insurance_network_source_binding" not in sql and "'legacy_uuid'" not in sql
    assert "'capture_sha256'" in sql and "network.payload_hash" in sql and "page.payload_hash" in sql
    binding = fhir.fhir_source_witness_binding_v2(pin)
    assert set(binding.values()) == {pin.generation_id}


@pytest.mark.parametrize("kind", ["source", "reader", "legacy"])
def test_inventory_indexes_exact_capture_or_legacy_scope(kind):
    legacy = fhir.PinnedFHIRMembershipSource(**_LEGACY_COORDINATES)
    recipes_by_kind = {
        "source": _source_recipe(),
        "reader": _reader_recipe(),
        "legacy": RegistrySourceMembershipRecipe(legacy, _coordinates(legacy, "producer-example", "edition-one")),
    }
    recipe = recipes_by_kind[kind]
    entry = make_source_inventory_entry(1, recipe)
    record = inventory_store.source_inventory_record(entry)
    record_by_field = dict(zip(inventory_store._COLUMNS, record, strict=True))
    assert (
        record_by_field["recipe_sha256"]
        == hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    )
    assert (
        tuple(record_by_field[name] for name in inventory_store._SCOPE_FIELDS)
        == recipe.binding_coordinates.sql_parameters
    )
    if kind == "legacy":
        assert (record_by_field["fhir_scope_key"], record_by_field["fhir_scope_value"]) == ("alias_scope", "medical")
    else:
        capture = recipe.source_pin.source_witness_capture if kind == "source" else recipe.source_pin.retained_capture
        assert (record_by_field["fhir_scope_key"], record_by_field["fhir_scope_value"]) == (
            "capture_sha256",
            capture.capture_sha256,
        )
    assert inventory_store.source_inventory_entry(record_by_field) == entry


def test_source_inventory_cannot_substitute_alias_scope():
    recipe = _source_recipe()
    entry = make_source_inventory_entry(1, recipe)
    record_by_field = dict(zip(inventory_store._COLUMNS, inventory_store.source_inventory_record(entry), strict=True))
    record_by_field.update(fhir_scope_key="alias_scope", fhir_scope_value=recipe.source_pin.alias_scope)
    with pytest.raises(RegistrySourceInventoryError):
        inventory_store.source_inventory_entry(record_by_field)


@pytest.mark.parametrize("version", [None, 2])
def test_provider_only_scope_uses_the_source_capture_digest(version):
    recipe = _source_recipe()
    approved = ApprovedMembershipSource(3, "a" * 64)
    sql = provider_only._source_query(
        recipe, '"candidate_example".network_source_contribution', "registry_example", descriptor_version=version
    )
    arguments = provider_only._source_arguments(recipe, approved, descriptor_version=version)
    assert "'capture_sha256'" in sql and "'alias_scope'" not in sql
    assert "record_json->'source_scope_json'->>'organization_id'=record_json->>'source_key'" in sql
    assert arguments[0] == approved.approved_revision
    assert arguments[1:7] == recipe.binding_coordinates.sql_parameters
    assert arguments[-1] == recipe.source_pin.source_witness_capture.capture_sha256
    assert arguments[-1] != recipe.source_pin.alias_scope
    identity = facts.verified_office_fact_identity if version == 2 else facts.recipe_contribution_identity
    assert arguments[7] == identity(recipe)[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [None, SimpleNamespace(verify=AsyncMock())])
async def test_direct_source_read_cannot_auto_pin_or_accept_a_duck_capability(read):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock())
    with pytest.raises(ValueError):
        await fhir._require_flat_pinned_source(connection, _source_recipe().source_pin, None, read)
    connection.fetchval.assert_not_awaited()
    if read is not None:
        read.verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [None, SimpleNamespace(verify=AsyncMock())])
async def test_fact_office_page_requires_a_genuine_source_read(read):
    connection = SimpleNamespace(fetchval=AsyncMock(), fetchrow=AsyncMock(), fetch=AsyncMock(), execute=AsyncMock())
    with pytest.raises(ValueError, match="registry_fact_office_read_unavailable"):
        await fact_offices._verify_source_page(connection, _source_recipe(), SimpleNamespace(retained_read=read))
    for method in (connection.fetchval, connection.fetchrow, connection.fetch, connection.execute):
        method.assert_not_awaited()
    if read is not None:
        read.verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True])
async def test_final_composition_refuses_source_receipt_even_with_cached_reads(cached):
    recipe = _source_recipe()
    connection = SimpleNamespace(fetchval=AsyncMock(), fetchrow=AsyncMock(), execute=AsyncMock())
    retained_reads = {recipe.source_pin.source_witness_capture: object()} if cached else {}
    with pytest.raises(composition.RegistrySourceCompositionError):
        await composition.require_registry_source_recipe_custody(connection, recipe, retained_fhir_reads=retained_reads)
    connection.fetchval.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()
