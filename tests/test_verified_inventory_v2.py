# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure witness-v2 metadata checks; these fixtures grant no native custody or authority."""

import copy
import hashlib
from dataclasses import replace
from uuid import UUID

import pytest

from process import registry_source_inventory_contract as contract
from process import registry_source_inventory_preparation as preparation
from process import registry_source_inventory_summaries as summaries
from process import registry_verified_office_derivation as codec
from process.network_fhir_membership_source import fhir_source_witness_binding_v2, with_fhir_successor
from process.registry_source_contribution_store import _WITNESS_COUNTERS_V2, FACT_ANCHOR_FIELDS, _canonical
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_generic_fhir_facts import _legacy_pin, _recipe
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_inventory_preparation import _aca_recipe, _derivation, _descriptor
from tests.test_verified_office_source_witnesses_v2 import _descriptor as witness_descriptor


def context():
    return {
        "policy_version": codec.VERIFIED_OFFICE_POLICY_V2,
        "approved_revision": 7,
        "approved_generation_sha256": "a" * 64,
        "address_source": {
            "schema_name": "synthetic_addresses",
            "table_name": "entity_address_unified",
            "generation_id": "b" * 64,
        },
        "address_identity": {"schema_oid": 11, "table_oid": 12, "owner_oid": 13, "identity_sha256": "c" * 64},
        "origin_manifest_sha256": None,
        "canonicalizer_version": {
            "identity_version": 2,
            "identity_prefix": "v2",
            "ruleset_version": 1,
            "pub28_sha256": "d" * 64,
        },
    }


def derivation(recipe, descriptor):
    return {
        **context(),
        "derivation_version": 2,
        "recipe_sha256": descriptor["recipe_sha256"],
        "source_binding": fhir_source_witness_binding_v2(recipe.source_pin),
        "contribution_sha256": hashlib.sha256(_canonical(descriptor).encode()).hexdigest(),
        "fact_artifact": {
            field: descriptor[field]
            for field in (*FACT_ANCHOR_FIELDS, "capture_sha256", "transform_version", "fact_sha256", "fact_rows")
        },
        "mapping_sha256": descriptor["mapping_sha256"],
        "input_membership_rows": descriptor["membership_rows"],
        "accepted_membership_rows": descriptor["membership_rows"],
        "omitted_membership_rows": 0,
        "office_rows": descriptor["membership_rows"],
        "page_count": 1,
        "correspondence_sha256": "e" * 64,
    }


def source(index=0):
    return replace(pin(retained()), as_of=f"2026-10-{index + 1:02d}")


def rows(source_pin=None, anchored=False):
    source_pin = source_pin or source()
    recipe = _recipe(source_pin)
    descriptor = witness_descriptor(source_pin)
    if anchored:
        descriptor.update(fact_candidate_id=str(UUID(int=9)), fact_table_oid=71, fact_seal_sha256="f" * 64)
    return recipe, descriptor, derivation(recipe, descriptor)


def prepared(values):
    recipes, descriptors, derivations = zip(*values, strict=True)
    reference, entries = preparation.prepare_source_inventory(recipes, descriptors, derivations)
    return reference.root, entries


@pytest.mark.parametrize("anchored", [False, True])
@pytest.mark.parametrize("kind", ["legacy", "retained", "successor"])
def test_prepare_explicit_v2_and_exact_bindings(kind, anchored):
    source_pin = _legacy_pin() if kind == "legacy" else source()
    if kind == "successor":
        from tests.test_registry_generic_accepted_capture import semantic_capture

        source_pin = with_fhir_successor(
            pin(semantic_capture()), replace(pin(semantic_capture("dataset-next", "2026-02-01")), as_of="2026-02-01")
        )
        assert len(set(fhir_source_witness_binding_v2(source_pin).values())) == 3
    recipe, descriptor, document = rows(source_pin, anchored)
    root, entries = prepared([(recipe, descriptor, document)])
    assert root.entry_count == root.contribution_count == root.derivation_count == 1
    assert entries[0].contribution_json == contract._canonical(descriptor)
    assert entries[0].office_derivation_json == contract._canonical(document)
    parsed = codec.validate_verified_office_derivation_v2(document)
    parsed["address_source"]["schema_name"] = "changed"
    assert document["address_source"]["schema_name"] == "synthetic_addresses"
    assert summaries.contribution_summary_v4(root, iter([descriptor]))["unresolved_raw_source_rows"] == 1
    summary = summaries.office_summary_v5(root, context(), iter([document]))
    assert summary["policy_context_sha256"] == codec.verified_office_context_sha256(context())
    assert summary["verified_derivation_count"] == 1


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda document: document.update(authority=True), id="unknown"),
        pytest.param(lambda document: document.pop("contribution_sha256"), id="missing"),
        pytest.param(lambda document: document.update(derivation_version=True), id="version_bool"),
        pytest.param(lambda document: document.update(derivation_version=3), id="version_unknown"),
        pytest.param(lambda document: document["source_binding"].update(logical_generation_id="bad"), id="source_hash"),
        pytest.param(lambda document: document["source_binding"].update(extra="a" * 64), id="source_unknown"),
        pytest.param(lambda document: document["fact_artifact"].update(fact_table_oid=71), id="anchor"),
        pytest.param(
            lambda document: document["fact_artifact"].update(transform_version="fhir-source-facts-v1"), id="transform"
        ),
        pytest.param(lambda document: document["address_identity"].update(table_oid=True), id="oid_bool"),
        pytest.param(lambda document: document["address_identity"].update(owner_oid=0), id="oid_zero"),
        pytest.param(lambda document: document["canonicalizer_version"].update(identity_prefix="v1"), id="prefix"),
        pytest.param(
            lambda document: document["canonicalizer_version"].update(ruleset_version=True), id="ruleset_bool"
        ),
        pytest.param(lambda document: document.update(omitted_membership_rows=1), id="sum"),
        pytest.param(lambda document: document.update(office_rows=2), id="office"),
        pytest.param(lambda document: document.update(input_membership_rows=True), id="counter_bool"),
        pytest.param(lambda document: document.update(page_count=2**63 - 1), id="overflow"),
        pytest.param(lambda document: document["address_source"].update(generation_id="z" * (128 * 1024)), id="bound"),
    ],
)
def test_derivation_refuses_open_or_incoherent_metadata(mutate):
    _, _, document = rows()
    mutate(document)
    with pytest.raises(ValueError, match="registry_verified_office_derivation_invalid"):
        codec.validate_verified_office_derivation_v2(document)


@pytest.mark.parametrize(
    "field",
    [
        "logical_generation_id",
        "original_generation_id",
        "physical_generation_id",
        "contribution_sha256",
        "mapping_sha256",
        "fact_sha256",
        "fact_rows",
        "anchor",
        "input_membership_rows",
    ],
)
def test_preparation_rejects_valid_but_substituted_derivation(field):
    recipe, descriptor, document = rows()
    if field.endswith("_generation_id"):
        document["source_binding"][field] = "f" * 64
    elif field in {"fact_sha256", "fact_rows"}:
        document["fact_artifact"][field] = "f" * 64 if field == "fact_sha256" else 4
    elif field == "anchor":
        document["fact_artifact"].update(
            fact_candidate_id=str(UUID(int=8)), fact_table_oid=72, fact_seal_sha256="f" * 64
        )
    elif field == "input_membership_rows":
        document.update(input_membership_rows=0, accepted_membership_rows=0, office_rows=0)
    else:
        document[field] = "f" * 64
    codec.validate_verified_office_derivation_v2(document)
    builder = preparation.SourceInventoryPreparation([recipe])
    builder.add_contribution(descriptor)
    with pytest.raises(contract.RegistrySourceInventoryError):
        builder.add_derivation(document)
    with pytest.raises(contract.RegistrySourceInventoryError):
        builder.finish()


@pytest.mark.parametrize(
    "damage",
    [
        "legacy_derivation",
        "legacy_descriptor",
        "missing_derivation",
        "explicit_unknown",
        "implicit_v2",
        "missing_unresolved",
    ],
)
def test_explicit_dispatch_never_relabels_legacy_or_fills_unresolved_zeros(damage):
    recipe, descriptor, document = rows()
    builder = preparation.SourceInventoryPreparation([recipe])
    if damage == "legacy_descriptor":
        builder.add_contribution(_descriptor(recipe))
        with pytest.raises(contract.RegistrySourceInventoryError):
            builder.add_derivation(document)
        return
    if damage in {"explicit_unknown", "implicit_v2", "missing_unresolved"}:
        if damage == "explicit_unknown":
            descriptor["descriptor_version"] = 3
        elif damage == "implicit_v2":
            descriptor.pop("descriptor_version")
        else:
            descriptor.pop("unresolved_rows")
        with pytest.raises(contract.RegistrySourceInventoryError):
            builder.add_contribution(descriptor)
        return
    builder.add_contribution(descriptor)
    if damage == "legacy_derivation":
        with pytest.raises(contract.RegistrySourceInventoryError):
            builder.add_derivation(_derivation(recipe, descriptor))
    else:
        with pytest.raises(contract.RegistrySourceInventoryError):
            builder.finish()


def test_complete_mixed_inventory_and_both_aggregate_hashes():
    values = [rows(source(1)), rows(source(2), True)]
    raw_recipes = [ptg_recipe(), _aca_recipe()]
    raw_derivations = [_derivation(recipe) for recipe in raw_recipes]
    recipes, descriptors, docs = map(list, zip(*values, strict=True))
    reference, entries = preparation.prepare_source_inventory(
        recipes + raw_recipes, descriptors, docs + raw_derivations
    )
    root = reference.root
    ordered_descriptors = sorted(descriptors, key=lambda row: row["recipe_sha256"])
    ordered_docs = sorted(docs + raw_derivations, key=lambda row: row["recipe_sha256"])
    contribution = summaries.contribution_summary_v4(root, iter(ordered_descriptors))
    office = summaries.office_summary_v5(root, context(), iter(ordered_docs))
    assert contribution["sources_sha256"] == hashlib.sha256(_canonical(ordered_descriptors).encode()).hexdigest()
    assert office["correspondence_sha256"] == hashlib.sha256(_canonical(ordered_docs).encode()).hexdigest()
    assert office["verified_derivation_count"] == root.contribution_count == 2
    assert office["derivation_count"] == root.entry_count == 4
    assert all(contribution[field] == sum(row[field] for row in descriptors) for field in _WITNESS_COUNTERS_V2)
    assert contract.source_inventory_root(entries) == root
    with pytest.raises(contract.RegistrySourceInventoryError):
        summaries.contribution_summary_v3(root, ordered_descriptors)
    with pytest.raises(contract.RegistrySourceInventoryError):
        summaries.office_summary_v4(
            root,
            type("Approved", (), {"approved_revision": 7, "generation_id": "a" * 64})(),
            type("Address", (), {"generation_id": "b" * 64})(),
            ordered_docs,
            None,
        )


@pytest.mark.parametrize(
    "field",
    [
        "approved_revision",
        "approved_generation_sha256",
        "address_source",
        "address_identity",
        "origin_manifest_sha256",
        "canonicalizer_version",
    ],
)
def test_office_fold_refuses_another_valid_context_and_poison_is_permanent(field):
    recipe, descriptor, document = rows()
    root, _ = prepared([(recipe, descriptor, document)])
    if field == "approved_revision":
        document[field] = 8
    elif field == "address_source":
        document[field]["schema_name"] = "another_address"
    elif field == "address_identity":
        document[field]["table_oid"] = 99
    elif field == "canonicalizer_version":
        document[field]["pub28_sha256"] = "f" * 64
    else:
        document[field] = "f" * 64
    accumulator = summaries.WitnessOfficeSummaryAccumulator(root, context())
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.accept(document)
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.finish()


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("damage", ["duplicate", "reverse", "incomplete", "legacy"])
def test_new_folds_keep_order_completeness_and_poisoning(kind, damage):
    values = [rows(source(1)), rows(source(2))]
    root, _ = prepared(values)
    docs = sorted([value[1 if kind == "contribution" else 2] for value in values], key=lambda row: row["recipe_sha256"])
    accumulator = (
        summaries.WitnessContributionSummaryAccumulator(root)
        if kind == "contribution"
        else summaries.WitnessOfficeSummaryAccumulator(root, context())
    )
    if damage == "reverse":
        accumulator.accept(docs[1])
        with pytest.raises(contract.RegistrySourceInventoryError):
            accumulator.accept(docs[0])
    elif damage == "duplicate":
        accumulator.accept(docs[0])
        with pytest.raises(contract.RegistrySourceInventoryError):
            accumulator.accept(docs[0])
    elif damage == "incomplete":
        accumulator.accept(docs[0])
        with pytest.raises(contract.RegistrySourceInventoryError):
            accumulator.finish()
    else:
        old_recipe = values[0][0]
        old_descriptor = _descriptor(old_recipe, anchored=True)
        old = old_descriptor if kind == "contribution" else _derivation(old_recipe, old_descriptor, facts=True)
        with pytest.raises(contract.RegistrySourceInventoryError):
            accumulator.accept(old)
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.finish()


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_new_finish_is_defensive_idempotent_and_closes(kind):
    value = rows()
    root, _ = prepared([value])
    accumulator = (
        summaries.WitnessContributionSummaryAccumulator(root)
        if kind == "contribution"
        else summaries.WitnessOfficeSummaryAccumulator(root, context())
    )
    document = value[1 if kind == "contribution" else 2]
    accumulator.accept(document)
    first = accumulator.finish()
    second = accumulator.finish()
    first["revision"] = 999
    assert accumulator.finish() == second
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.accept(document)


def test_invalid_expected_context_and_verified_count_refuse():
    recipe, descriptor, document = rows()
    root, _ = prepared([(recipe, descriptor, document)])
    invalid = context()
    invalid["canonicalizer_version"]["identity_prefix"] = "v1"
    with pytest.raises(contract.RegistrySourceInventoryError):
        summaries.WitnessOfficeSummaryAccumulator(root, invalid)
    raw = _derivation(recipe, descriptor)
    with pytest.raises(contract.RegistrySourceInventoryError):
        summaries.office_summary_v5(root, context(), [raw])
    for broken_root in (replace(root, contribution_count=0), replace(root, derivation_count=0)):
        with pytest.raises(contract.RegistrySourceInventoryError):
            summaries.office_summary_v5(broken_root, context(), [document])
    summary = summaries.office_summary_v5(root, context(), [document])
    summary["verified_derivation_count"] = True
    with pytest.raises(contract.RegistrySourceInventoryError):
        summaries.validate_office_summary_v5(summary, root)


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_aggregate_counter_overflow_poisoning(kind):
    values = [rows(source(1)), rows(source(2))]
    root, _ = prepared(values)
    accumulator = (
        summaries.WitnessContributionSummaryAccumulator(root)
        if kind == "contribution"
        else summaries.WitnessOfficeSummaryAccumulator(root, context())
    )
    docs = []
    for _, descriptor, document in values:
        if kind == "contribution":
            descriptor.update(fact_rows=2**63 - 1)
            docs.append(descriptor)
        else:
            document["page_count"] = 2**63 - 2
            docs.append(document)
    docs.sort(key=lambda row: row["recipe_sha256"])
    accumulator.accept(docs[0])
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.accept(docs[1])
    with pytest.raises(contract.RegistrySourceInventoryError):
        accumulator.finish()


def test_zero_membership_witness_preserves_unresolved_classification_and_omissions():
    recipe, descriptor, _ = rows()
    descriptor.update(membership_rows=0, unresolved_rows=2, provider_only_rows=1)
    document = derivation(recipe, descriptor)
    root, _ = prepared([(recipe, descriptor, document)])
    contribution = summaries.contribution_summary_v4(root, [descriptor])
    office = summaries.office_summary_v5(root, context(), [document])
    assert contribution["unresolved_rows"] == 2
    assert contribution["unresolved_raw_source_rows"] == 1
    assert office["office_rows"] == 0 and office["verified_derivation_count"] == 1
    descriptor.update(membership_rows=1, unresolved_rows=1)
    document = derivation(recipe, descriptor)
    document.update(accepted_membership_rows=0, omitted_membership_rows=1, office_rows=0)
    root, _ = prepared([(recipe, descriptor, document)])
    assert summaries.office_summary_v5(root, context(), [document])["office_rows"] == 0


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda document, kind: document.update(authority=True), id="unknown"),
        pytest.param(lambda document, kind: document.update(revision=True), id="version_bool"),
        pytest.param(lambda document, kind: document.update(revision=document["revision"] - 1), id="version_old"),
        pytest.param(
            lambda document, kind: document.update(
                {"source_count" if kind == "contribution" else "verified_derivation_count": True}
            ),
            id="count_bool",
        ),
        pytest.param(
            lambda document, kind: document.pop(
                "unresolved_rows" if kind == "contribution" else "verified_derivation_count"
            ),
            id="count_missing",
        ),
        pytest.param(
            lambda document, kind: document.update(
                {"descriptor_version": True} if kind == "contribution" else {"policy_context_sha256": "bad"}
            ),
            id="context_hash",
        ),
    ],
)
def test_new_summaries_are_closed_and_explicit(kind, mutate):
    value = rows()
    root, _ = prepared([value])
    if kind == "contribution":
        document = summaries.contribution_summary_v4(root, [value[1]])
        validate = summaries.validate_contribution_summary_v4
    else:
        document = summaries.office_summary_v5(root, context(), [value[2]])
        validate = summaries.validate_office_summary_v5
    mutate(document, kind)
    with pytest.raises(contract.RegistrySourceInventoryError):
        validate(document, root)
