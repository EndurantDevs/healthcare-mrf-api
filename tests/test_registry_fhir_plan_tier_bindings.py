# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact typed source bindings preserve native encoding and office witness guards."""

import copy
import hashlib
import json
import struct
from dataclasses import replace
from uuid import UUID

import pytest

from process import network_approved_source_bindings as approved
from process import registry_source_contribution_store as facts
from process.network_fhir_source_witness_contract import capture_digest
from process.registry_fhir_plan_tier import KIND, source_key, validate_binding_scope
from process.registry_network_catalog_read import catalog_binding_document
from process.uhc_provider_file_identity import UHCSourceFileDescriptor, logical_scope_for_file
from process.uhc_provider_file_semantic_identity import network_key_for_plan, plan_key_for_scope
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_network_fhir_source_witness_recipe import _reader_recipe, _source_recipe
from tests.test_registry_source_contributions import _descriptor, _fact, _recipe
from tests.test_verified_office_source_witnesses_v2 import _edge, _header, _page


def tier_document(*, tier="preferred", plan_id="12345AZ001", year=2026, family="ifp", product="AZIEX"):
    scope = logical_scope_for_file(
        UHCSourceFileDescriptor(family, "provider_membership", "JSON_Providers_" + product + ".json")
    )
    plan = plan_key_for_scope(scope, plan_id_type="hios", plan_id=plan_id, plan_year=year)
    network = network_key_for_plan(plan, network_tier=tier)
    return dict(
        kind=KIND,
        network_tier=tier,
        network_key_id=network.network_key_id,
        plan_resource_id="uhcplan-" + plan.plan_key_id[:48],
        plan_scope={
            **{
                field: getattr(scope, field)
                for field in ("logical_scope_id", "family", "market", "product", "jurisdiction")
            },
            **{field: getattr(plan, field) for field in ("plan_id_type", "plan_id", "plan_year", "plan_key_id")},
        },
    )


def binding(tier=None):
    tier = tier_document() if tier is None else tier
    return dict(
        binding_id=str(UUID(int=1)),
        source_system="fhir",
        source_id=UHC_PROVIDER_FILE_SOURCE_ID,
        dataset_schema="synthetic_source",
        dataset_id="edition-one",
        producer_id="registry-source-fhir-witness-v1",
        edition_id=str(UUID(int=2)),
        source_key=source_key(tier),
        source_scope_json=tier | {"capture_sha256": "a" * 64},
        network_id=42,
        evidence_id="synthetic-evidence",
        evidence_sha256="b" * 64,
        operation="bind",
        expected_revision=0,
        expected_network_id=None,
    )


def witness_pin():
    pin = _source_recipe().source_pin
    capture = pin.source_witness_capture
    origin = replace(
        capture.origin, source_id=UHC_PROVIDER_FILE_SOURCE_ID, source_aliases=(UHC_PROVIDER_FILE_SOURCE_ID,)
    )
    capture = replace(capture, origin=origin)
    capture = replace(capture, capture_sha256=capture_digest(capture))
    return replace(pin, source_id=UHC_PROVIDER_FILE_SOURCE_ID, source_witness_capture=capture)


def typed_fact(pin, *, v2=False):
    fact = _edge(pin) if v2 else _fact(pin)
    tier = tier_document()
    fact.update(
        source_key=source_key(tier),
        source_scope_json=tier | {"capture_sha256": pin.source_witness_capture.capture_sha256},
    )
    return fact


def _copy_fields(encoded):
    offset, values = 21, []
    for _ in range(struct.unpack("!h", encoded[19:21])[0]):
        size = struct.unpack("!i", encoded[offset : offset + 4])[0]
        offset += 4
        values.append(None if size < 0 else encoded[offset : offset + size])
        offset += max(size, 0)
    assert encoded[offset:] == b"\xff\xff"
    return values


@pytest.mark.parametrize(
    "tier,plan_id,family,product",
    [
        ("preferred", "12345AZ001", "ifp", "AZIEX"),
        ("réseau 😃", "plan-é", "ifp", "AZIEX"),
        ("tier", "x" * 256, "ifp", "AZIEX"),
        ("standard", "plan-one", "cs", "AZMA"),
        ("standard", "plan-two", "ifp", "UHCEX_HIX"),
    ],
)
def test_native_copy_and_python_scope_have_identical_keys_and_payload(tier, plan_id, family, product):
    import ptg2_address_canon as native

    row = binding(tier_document(tier=tier, plan_id=plan_id, family=family, product=product))
    assert validate_binding_scope(row["source_scope_json"], row["producer_id"]) == tier_document(
        tier=tier, plan_id=plan_id, family=family, product=product
    )
    encoded, count = native.encode_network_source_binding_batch(json.dumps([row], ensure_ascii=False).encode())
    assert count == 1
    fields = _copy_fields(encoded)
    assert json.loads(fields[8][1:]) == row["source_scope_json"]
    identity_by_field = {
        field: row[field]
        for field in (
            "source_system",
            "source_id",
            "dataset_schema",
            "dataset_id",
            "producer_id",
            "edition_id",
            "source_key",
            "source_scope_json",
        )
    }
    expected = hashlib.sha256(
        b"registry_network_source_binding:v1:"
        + json.dumps(identity_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert fields[9].decode() == expected


@pytest.mark.parametrize(
    "damage", ["tier", "plan", "market", "year", "capture", "source", "producer", "key", "extra", "missing", "rebind"]
)
def test_native_refuses_late_invalid_typed_binding_without_partial_output(damage):
    import ptg2_address_canon as native

    first, row = binding(), binding(tier_document(tier="standard"))
    row.update(binding_id=str(UUID(int=3)), network_id=43)
    scope = row["source_scope_json"]
    if damage in {"plan", "market", "year"}:
        scope["plan_scope"][{"plan": "plan_id", "market": "market", "year": "plan_year"}[damage]] = {
            "plan": "other",
            "market": "commercial",
            "year": 2027,
        }[damage]
    elif damage in {"tier", "capture", "extra"}:
        scope[{"tier": "network_tier", "capture": "capture_sha256", "extra": "unknown"}[damage]] = "wrong"
    elif damage == "missing":
        del scope["plan_scope"]["jurisdiction"]
    elif damage == "rebind":
        row.update(operation="rebind", expected_revision=1, expected_network_id=44)
    else:
        row[{"source": "source_id", "producer": "producer_id", "key": "source_key"}[damage]] = "other"
    with pytest.raises(ValueError):
        native.encode_network_source_binding_batch(json.dumps([first, row]).encode())


def test_native_rejects_duplicate_nested_fields_and_nonprintable_semantic_identity():
    import ptg2_address_canon as native

    encoded = json.dumps([binding()]).replace(
        '"network_tier": "preferred"', '"network_tier": "preferred", "network_tier": "preferred"'
    )
    with pytest.raises(ValueError):
        native.encode_network_source_binding_batch(encoded.encode())
    row = binding()
    scope = row["source_scope_json"]
    scope["network_tier"] = "pre\u200bferred"
    identity_by_field = {
        field: scope["plan_scope"][field]
        for field in ("logical_scope_id", "family", "market", "plan_id_type", "plan_id", "plan_year")
    }
    scope["network_key_id"] = hashlib.sha256(
        json.dumps(
            identity_by_field | {"contract": "healthporta-uhc-network-key-v1", "network_tier": scope["network_tier"]},
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    row["source_key"] = "UHCPlanTier/" + scope["network_key_id"]
    with pytest.raises(ValueError):
        validate_binding_scope(scope, row["producer_id"])
    with pytest.raises(ValueError, match="printable"):
        native.encode_network_source_binding_batch(json.dumps([row]).encode())


def test_catalog_scope_and_approved_rows_bind_exact_source_and_kind():
    binding_row = binding()
    approved._require_plan_tier_bindings(json.dumps([binding_row]), 1)
    for encoded, count in ((None, 1), (json.dumps([binding_row]), True), ("[]", 1)):
        with pytest.raises(ValueError):
            approved._require_plan_tier_bindings(encoded, count)
    binding_by_field = {
        key: binding_row[key]
        for key in (
            "binding_id",
            "source_system",
            "source_id",
            "dataset_schema",
            "dataset_id",
            "producer_id",
            "edition_id",
            "source_key",
            "evidence_id",
            "evidence_sha256",
        )
    }
    binding_by_field.update(
        namespace="source_binding",
        revision=1,
        producer_id="registry-fhir-catalog-v1",
        source_scope=tier_document() | {"source_generation": "c" * 64},
    )
    catalog_binding_document(binding_by_field)
    for field, changed_value in (
        ("source_id", "other"),
        ("source_key", "network-one"),
        ("producer_id", "registry-retained-normalized-fhir-v1"),
    ):
        with pytest.raises(ValueError):
            catalog_binding_document(binding_by_field | {field: changed_value})
        with pytest.raises(ValueError):
            approved._require_plan_tier_bindings(json.dumps([binding_row | {field: changed_value}]), 1)


@pytest.mark.parametrize("v2", [False, True])
def test_typed_fact_requires_genuine_source_capture_and_preserves_office_evidence(v2):
    pin = witness_pin()
    fact = typed_fact(pin, v2=v2)
    validate = facts._validate_witness_edge_v2 if v2 else facts._validate_fhir_fact
    validate(fact, pin)
    for changed in ("capture", "source", "location", "provider", "key"):
        damaged = copy.deepcopy(fact)
        if changed == "capture":
            damaged["source_scope_json"]["capture_sha256"] = "f" * 64
        else:
            damaged[
                {
                    "source": "source_scope_json",
                    "location": "location_id",
                    "provider": "provider_id",
                    "key": "source_key",
                }[changed]
            ] = {"source": {}, "location": str(UUID(int=99)), "provider": "1000000483", "key": "network-one"}[changed]
        with pytest.raises(ValueError):
            validate(damaged, pin)
    with pytest.raises(ValueError):
        validate(fact, _reader_recipe().source_pin)
    if v2:
        assert facts.decode_source_witness_page_v2(_page(pin, [_header(pin), fact]))


def test_targeted_transform_preserves_old_receipts():
    assert facts.fhir_fact_transform(witness_pin()) == facts.SOURCE_PLAN_TIER_TRANSFORM_VERSION
    assert facts.fhir_fact_transform(_source_recipe().source_pin) == facts.GENERIC_TRANSFORM_VERSION
    assert facts.fhir_fact_transform(_reader_recipe().source_pin) == facts.GENERIC_TRANSFORM_VERSION
    assert facts.fhir_fact_transform(_recipe().source_pin) == facts.TRANSFORM_VERSION
    descriptor = _descriptor(_recipe())
    for transform in (
        facts.TRANSFORM_VERSION,
        facts.GENERIC_TRANSFORM_VERSION,
        facts.SOURCE_PLAN_TIER_TRANSFORM_VERSION,
    ):
        capture = facts._digest([descriptor["recipe_sha256"], transform])
        fields = descriptor | dict(
            transform_version=transform,
            capture_sha256=capture,
            source_generation=facts._digest([capture, descriptor["mapping_sha256"]]),
        )
        facts.validate_source_contribution_descriptor(fields)


def test_witness_cache_identity_and_mapping_counts_use_the_targeted_transform():
    from process.network_fhir_membership_source import (
        FHIR_PLAN_TIER_WITNESS_TRANSFORM,
        FHIR_SOURCE_WITNESS_TRANSFORM_V2,
    )
    from tests.test_registry_source_mapping_v3 import _mapping

    recipe = _source_recipe()
    recipe = replace(
        recipe,
        source_pin=witness_pin(),
        binding_coordinates=replace(recipe.binding_coordinates, source_id=UHC_PROVIDER_FILE_SOURCE_ID),
    )
    identity, capture = facts.verified_office_fact_identity(recipe)
    assert capture == facts._digest([identity, FHIR_PLAN_TIER_WITNESS_TRANSFORM])
    assert capture != facts._digest([identity, FHIR_SOURCE_WITNESS_TRANSFORM_V2])
    mapping = _mapping(recipe, transform_version=FHIR_PLAN_TIER_WITNESS_TRANSFORM)
    facts.validate_source_mapping_descriptor_v3(mapping)
    old_capture = facts._digest([identity, FHIR_SOURCE_WITNESS_TRANSFORM_V2])
    old = mapping | dict(
        transform_version=FHIR_SOURCE_WITNESS_TRANSFORM_V2,
        capture_sha256=old_capture,
        source_generation=facts.source_mapping_generation_v3(
            old_capture, mapping["mapping_sha256"], mapping["covered_network_ids"]
        ),
    )
    facts.validate_source_mapping_descriptor_v3(old)


@pytest.mark.parametrize("typed", [False, True])
async def test_office_derivation_preserves_individual_witness_transform(typed):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import registry_verified_witness_offices as mapping
    from process.network_fhir_membership_source import fhir_source_witness_transform
    from tests.test_verified_inventory_v2 import context, prepared, rows

    pin = witness_pin() if typed else _source_recipe().source_pin
    recipe = _source_recipe()
    recipe = replace(
        recipe,
        source_pin=pin,
        binding_coordinates=replace(recipe.binding_coordinates, source_id=pin.source_id),
    )
    _, descriptor, _ = rows()
    identity, capture = facts.verified_office_fact_identity(recipe)
    descriptor.update(
        recipe_sha256=identity,
        capture_sha256=capture,
        transform_version=fhir_source_witness_transform(pin),
        source_generation=facts._digest([capture, descriptor["mapping_sha256"]]),
    )
    witness_source = mapping.WitnessOfficeSource(recipe, descriptor, "synthetic_members")
    document = await mapping._derivation(
        SimpleNamespace(fetchval=AsyncMock(return_value=1)),
        witness_source,
        (),
        SimpleNamespace(page_proofs_by_key={}),
        "synthetic_members",
        context(),
    )
    assert document["fact_artifact"]["transform_version"] == descriptor["transform_version"]
    assert document["contribution_sha256"] == facts._digest(descriptor)
    root, _ = prepared([(recipe, descriptor, document)])
    assert root.derivation_count == 1


@pytest.mark.parametrize("transform", ["unknown", facts.SOURCE_PLAN_TIER_TRANSFORM_VERSION, None, []])
def test_office_derivation_refuses_nonwitness_transforms(transform):
    from process.registry_verified_office_derivation import validate_verified_office_derivation_v2
    from tests.test_verified_inventory_v2 import rows

    _, _, document = rows()
    document["fact_artifact"]["transform_version"] = transform
    with pytest.raises(ValueError, match="registry_verified_office_derivation_invalid"):
        validate_verified_office_derivation_v2(document)
