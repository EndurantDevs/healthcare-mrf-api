# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Streaming aggregate receipts preserve singleton codecs and legacy array hashes."""

import asyncio
import copy
import hashlib
import json
import weakref
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_source_contribution_store as legacy_contributions
from process import registry_source_inventory_contract as inventory
from process import registry_source_inventory_summaries as summaries
from process import registry_source_office_summary_v3 as legacy_offices
from process.registry_source_recipe_store import RegistrySourceRecipeError, canonical_registry_source_recipes
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_contributions import _descriptor
from tests.test_registry_source_office_summary_v3 import _derivation
from tests.test_registry_source_recipe_store_postgres import _example

_APPROVED = SimpleNamespace(approved_revision=7, generation_id="b" * 64)
_ADDRESS = SimpleNamespace(generation_id="c" * 64)


class OneShot:
    def __init__(self, values):
        self.values = values
        self.was_consumed = False

    def __iter__(self):
        assert not self.was_consumed, "Aggregate builder consumed the stream twice"
        self.was_consumed = True
        yield from self.values

    def __len__(self):
        pytest.fail("Aggregate builder must not collect or size the stream")


def _source_inventory(count):
    rows = []
    for index in range(count):
        recipe = _example(index)
        descriptor_by_field = legacy_contributions.locally_owned_source_descriptor(_descriptor(recipe))
        if index % 2:
            descriptor_by_field.update(
                fact_candidate_id=str(UUID(int=index + 1)), fact_table_oid=index + 1, fact_seal_sha256="d" * 64
            )
        derivation = _derivation(descriptor_by_field["recipe_sha256"], mode="facts" if index % 2 else "raw")
        rows.append((recipe, descriptor_by_field, derivation))
    rows.sort(key=lambda row: canonical_registry_source_recipes((row[0],)))
    entries = (
        inventory.make_source_inventory_entry(ordinal, recipe, contribution=descriptor, office_derivation=derivation)
        for ordinal, (recipe, descriptor, derivation) in enumerate(rows, 1)
    )
    root = inventory.source_inventory_root(entries)
    descriptors = sorted((row[1] for row in rows), key=lambda descriptor: descriptor["recipe_sha256"])
    derivations = sorted((row[2] for row in rows), key=lambda derivation: derivation["recipe_sha256"])
    return root, descriptors, derivations


@pytest.fixture(scope="module")
def sample_inventory():
    return _source_inventory(2)


def _office(root, derivations):
    return summaries.office_summary_v4(root, _APPROVED, _ADDRESS, derivations, None)


def _accumulator(kind, root):
    return (
        summaries.ContributionSummaryAccumulator(root)
        if kind == "contribution"
        else summaries.OfficeSummaryAccumulator(root, _APPROVED, _ADDRESS, None)
    )


@pytest.mark.parametrize("count", [101, 1000])
def test_large_streams_preserve_legacy_array_hashes_and_constant_metadata(count):
    root, descriptors, derivations = _source_inventory(count)
    contribution = summaries.contribution_summary_v3(root, OneShot(descriptors))
    office = _office(root, OneShot(derivations))
    assert contribution["source_count"] == office["derivation_count"] == office["recipe_count"] == count
    assert (
        contribution["sources_sha256"]
        == hashlib.sha256(legacy_contributions._canonical(descriptors).encode()).hexdigest()
    )
    assert office["correspondence_sha256"] == hashlib.sha256(legacy_offices.canonical(derivations).encode()).hexdigest()
    assert contribution["inventory_entries_sha256"] == office["inventory_entries_sha256"] == root.entries_sha256
    assert all(
        contribution[counter] == sum(descriptor[counter] for descriptor in descriptors)
        for counter in legacy_contributions._COUNTERS
    )
    assert office["office_rows"] == sum(derivation["office_rows"] for derivation in derivations)
    assert office["page_count"] == sum(derivation["page_count"] for derivation in derivations)
    assert set(contribution) == summaries._CONTRIBUTION_FIELDS and set(office) == summaries._OFFICE_FIELDS
    assert "sources" not in contribution and "derivations" not in office
    assert len(legacy_contributions._canonical(contribution).encode()) < 1024
    assert len(legacy_offices.canonical(office).encode()) < 1024
    with pytest.raises(legacy_contributions.RegistrySourceContributionError):
        legacy_contributions.validate_registry_source_contribution_receipt(
            {
                "component": legacy_contributions.SUMMARY_KEY,
                "revision": 2,
                "table_name": legacy_contributions.CONTRIBUTION_TABLE,
                "sources": descriptors,
            }
        )
    with pytest.raises(RegistrySourceRecipeError):
        canonical_registry_source_recipes(tuple(_example(index) for index in range(count)))


def test_legacy_accepted_array_hashes_match_new_summary(sample_inventory):
    root, descriptors, derivations = sample_inventory
    legacy = legacy_contributions.validate_registry_source_contribution_receipt(
        {
            "component": legacy_contributions.SUMMARY_KEY,
            "revision": 2,
            "table_name": legacy_contributions.CONTRIBUTION_TABLE,
            "sources": descriptors,
        }
    )
    old_office = legacy_offices.office_summary_v3(root.recipes_sha256, _APPROVED, _ADDRESS, derivations, None)
    assert (
        summaries.contribution_summary_v3(root, iter(descriptors))["sources_sha256"]
        == hashlib.sha256(legacy_contributions._canonical(legacy["sources"]).encode()).hexdigest()
    )
    assert _office(root, iter(derivations))["correspondence_sha256"] == old_office["correspondence_sha256"]


def test_v4_supports_all_raw_inventory_and_preserves_v3():
    recipe = ptg_recipe()
    identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    derivation = _derivation(identity, mode="raw")
    root = inventory.source_inventory_root(
        (inventory.make_source_inventory_entry(1, recipe, office_derivation=derivation),)
    )
    document = _office(root, iter((derivation,)))
    assert document["revision"] == 4 and document["recipe_count"] == 1 and root.contribution_count == 0
    assert summaries.validate_office_summary_v4(document, root) == document
    with pytest.raises(ValueError, match="v3_invalid"):
        legacy_offices.office_summary_v3(root.recipes_sha256, _APPROVED, _ADDRESS, (derivation,), None)


def test_zero_contribution_summary_is_explicit_and_office_summary_requires_sources():
    root = inventory.source_inventory_root(())
    document = summaries.contribution_summary_v3(root, iter(()))
    assert document["source_count"] == 0 and document["sources_sha256"] == hashlib.sha256(b"[]").hexdigest()
    assert all(document[counter] == 0 for counter in legacy_contributions._COUNTERS)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        _office(root, iter(()))


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("damage", ["reversed", "duplicate", "missing", "extra"])
def test_stream_order_uniqueness_and_complete_count(kind, damage, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = copy.deepcopy(descriptors if kind == "contribution" else derivations)
    if damage == "reversed":
        documents.reverse()
    elif damage == "duplicate":
        documents[1] = documents[0]
    elif damage == "missing":
        documents.pop()
    else:
        documents.append(documents[-1])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        (summaries.contribution_summary_v3 if kind == "contribution" else _office)(root, OneShot(documents))


@pytest.mark.parametrize("damage", ["revision_one", "partial_anchor", "bad_anchor", "bool_count", "extra", "hash"])
def test_contribution_singleton_revision_two_is_required(damage, sample_inventory):
    root, descriptors, _ = sample_inventory
    documents = copy.deepcopy(descriptors)
    descriptor = documents[0]
    mutations_by_case = {
        "partial_anchor": {"fact_candidate_id": None, "fact_table_oid": 42, "fact_seal_sha256": None},
        "bad_anchor": {"fact_candidate_id": "invalid", "fact_table_oid": 42, "fact_seal_sha256": "d" * 64},
        "bool_count": {"fact_rows": True},
        "extra": {"authority": True},
        "hash": {"source_generation": "f" * 64},
    }
    if damage == "revision_one":
        for field in legacy_contributions.FACT_ANCHOR_FIELDS:
            descriptor.pop(field)
    else:
        descriptor.update(mutations_by_case[damage])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        summaries.contribution_summary_v3(root, iter(documents))


@pytest.mark.parametrize("damage", ["extra", "mode", "raw_anchor", "hash", "bool_count"])
def test_office_singletons_retain_closed_derivation_contract(damage, sample_inventory):
    root, _, derivations = sample_inventory
    documents = copy.deepcopy(derivations)
    changes_by_case = {
        "extra": {"authority": True},
        "mode": {"mode": "unchecked"},
        "raw_anchor": {"mode": "raw", "fact_artifact": {"unchecked": True}},
        "hash": {"physical_source_sha256": "invalid"},
        "bool_count": {"office_rows": True},
    }
    documents[0].update(changes_by_case[damage])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        _office(root, iter(documents))


@pytest.mark.parametrize("field,value", [("revision", True), ("entry_count", 1001), ("contribution_count", True)])
def test_builder_revalidates_frozen_root_fields(field, value, sample_inventory):
    root, descriptors, derivations = sample_inventory
    forged_root = copy.copy(root)
    object.__setattr__(forged_root, field, value)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        summaries.contribution_summary_v3(forged_root, iter(descriptors))
    with pytest.raises(inventory.RegistrySourceInventoryError):
        _office(forged_root, iter(derivations))


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("field", ["inventory_entries_sha256", "source_count", "root_flags"])
def test_metadata_refuses_root_digest_count_or_flags(kind, field, sample_inventory):
    root, descriptors, derivations = sample_inventory
    builder = summaries.contribution_summary_v3 if kind == "contribution" else _office
    validator = (
        summaries.validate_contribution_summary_v3 if kind == "contribution" else summaries.validate_office_summary_v4
    )
    document = builder(root, iter(descriptors if kind == "contribution" else derivations))
    if field == "inventory_entries_sha256":
        root = replace(root, entries_sha256="f" * 64)
    elif field == "source_count":
        document["source_count" if kind == "contribution" else "derivation_count"] += 1
    else:
        root = replace(root, **{"contribution_count" if kind == "contribution" else "derivation_count": 0})
    with pytest.raises(inventory.RegistrySourceInventoryError):
        validator(document, root)


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("damage", ["bool", "negative", "overflow", "aggregate", "extra", "table_or_recipe"])
def test_closed_metadata_rejects_malformed_scalars(kind, damage, sample_inventory):
    root, descriptors, derivations = sample_inventory
    validator = (
        summaries.validate_contribution_summary_v3 if kind == "contribution" else summaries.validate_office_summary_v4
    )
    document = (
        summaries.contribution_summary_v3(root, iter(descriptors))
        if kind == "contribution"
        else _office(root, iter(derivations))
    )
    counter = "fact_rows" if kind == "contribution" else "office_rows"
    if damage in {"bool", "negative", "overflow"}:
        document[counter] = {"bool": True, "negative": -1, "overflow": 2**63}[damage]
    elif damage == "aggregate":
        document["raw_source_rows" if kind == "contribution" else "recipe_count"] += 1
    elif damage == "extra":
        document["authority"] = True
    else:
        document["table_name" if kind == "contribution" else "recipe_sha256"] = "different"
    with pytest.raises(inventory.RegistrySourceInventoryError):
        validator(document, root)


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_stream_totals_reject_signed_bigint_overflow(kind, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = copy.deepcopy(descriptors if kind == "contribution" else derivations)
    for document in documents:
        if kind == "contribution":
            document.update(
                fact_rows=2**62,
                raw_source_rows=2**62,
                accepted_raw_source_rows=2**62,
                omitted_raw_source_rows=0,
                membership_rows=0,
                omitted_rows=0,
                provider_only_rows=0,
            )
        else:
            document.update(office_rows=2**62, page_count=0)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        (summaries.contribution_summary_v3 if kind == "contribution" else _office)(root, OneShot(documents))


def test_office_cross_counter_overflow_is_rejected(sample_inventory):
    root, _, derivations = sample_inventory
    documents = copy.deepcopy(derivations)
    documents[0].update(office_rows=2**62, page_count=0)
    documents[1].update(office_rows=0, page_count=2**62)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        _office(root, iter(documents))


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_json_bounds_duplicate_keys_constants_and_canonical_size(kind, sample_inventory):
    root, descriptors, derivations = sample_inventory
    validator = (
        summaries.validate_contribution_summary_v3 if kind == "contribution" else summaries.validate_office_summary_v4
    )
    document = (
        summaries.contribution_summary_v3(root, iter(descriptors))
        if kind == "contribution"
        else _office(root, iter(derivations))
    )
    encoded = legacy_contributions._canonical(document)
    assert validator(encoded + " " * (4096 - len(encoded.encode())), root) == document
    invalid = (
        encoded + " " * (4097 - len(encoded.encode())),
        encoded[:-1] + ',"revision":3}',
        encoded[:-1] + ',"untrusted":NaN}',
        {**document, "untrusted": "é" * 4096},
    )
    for value in invalid:
        with pytest.raises(inventory.RegistrySourceInventoryError):
            validator(value, root)


def test_stream_fold_uses_legacy_ascii_encoding_and_never_retains_payloads():
    class TrackedDocument(dict):
        __slots__ = ("__weakref__",)

    references = []

    def validate(document):
        assert all(reference() is None for reference in references[:-1])
        checked = TrackedDocument(document)
        references.append(weakref.ref(checked))
        return checked

    documents = ({"recipe_sha256": f"{index:064x}", "caption": "é😀", "office_rows": 1} for index in range(1000))
    fold = summaries._SummaryFold(1000, validate, ("office_rows",))
    for document in OneShot(documents):
        fold.accept(document)
        assert all(reference() is None for reference in references)
    receipt = fold.finish(lambda count, digest, totals: {"count": count, "digest": digest, "totals": totals})
    expected_documents = [
        {"recipe_sha256": f"{index:064x}", "caption": "é😀", "office_rows": 1} for index in range(1000)
    ]
    assert (receipt["count"], receipt["totals"]) == (1000, {"office_rows": 1000})
    assert (
        receipt["digest"]
        == hashlib.sha256(json.dumps(expected_documents, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    assert (
        receipt["digest"]
        != hashlib.sha256(
            json.dumps(expected_documents, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
    )
    assert all(reference() is None for reference in references)
    overfull = summaries._SummaryFold(1000, lambda value: value, ())
    with pytest.raises(inventory.RegistrySourceInventoryError):
        for index in range(1001):
            overfull.accept({"recipe_sha256": f"{index:064x}"})


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("count", [101, 1000])
async def test_async_pages_match_sync_and_legacy_hashes(kind, count):
    root, descriptors, derivations = _source_inventory(count)
    documents = descriptors if kind == "contribution" else derivations
    accumulator = _accumulator(kind, root)
    for offset in range(0, count, 17):
        await asyncio.sleep(0)
        for document in documents[offset : offset + 17]:
            accumulator.accept(document)
    actual = accumulator.finish()
    expected = (
        summaries.contribution_summary_v3(root, OneShot(descriptors))
        if kind == "contribution"
        else _office(root, OneShot(derivations))
    )
    assert actual == expected and accumulator.finish() == expected
    digest_field = "sources_sha256" if kind == "contribution" else "correspondence_sha256"
    assert actual[digest_field] == hashlib.sha256(legacy_contributions._canonical(documents).encode()).hexdigest()
    assert len(legacy_contributions._canonical(actual).encode()) < 1024


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_finish_is_defensive_idempotent_and_closes_accepts(kind, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = descriptors if kind == "contribution" else derivations
    accumulator = _accumulator(kind, root)
    for document in documents:
        accumulator.accept(document)
    expected = accumulator.finish()
    changed = accumulator.finish()
    changed["revision"] = "damaged"
    assert accumulator.finish() == expected
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(documents[-1])
    assert accumulator.finish() == expected


@pytest.mark.parametrize("kind", ["contribution", "office"])
@pytest.mark.parametrize("damage", ["malformed", "duplicate", "incomplete"])
def test_invalid_accept_or_incomplete_finish_permanently_poison(kind, damage, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = copy.deepcopy(descriptors if kind == "contribution" else derivations)
    accumulator = _accumulator(kind, root)
    accumulator.accept(documents[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        if damage == "incomplete":
            accumulator.finish()
        elif damage == "duplicate":
            accumulator.accept(documents[0])
        else:
            documents[1]["fact_rows" if kind == "contribution" else "office_rows"] = True
            accumulator.accept(documents[1])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(documents[1])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_late_counter_overflow_poison_is_permanent(kind, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = copy.deepcopy(descriptors if kind == "contribution" else derivations)
    for document in documents:
        if kind == "contribution":
            document.update(
                fact_rows=2**62,
                raw_source_rows=2**62,
                accepted_raw_source_rows=2**62,
                omitted_raw_source_rows=0,
                membership_rows=0,
                omitted_rows=0,
                provider_only_rows=0,
            )
        else:
            document.update(office_rows=2**62, page_count=0)
    accumulator = _accumulator(kind, root)
    accumulator.accept(documents[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(documents[1])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()


def test_invalid_office_finish_poison_is_permanent(sample_inventory):
    root, _, derivations = sample_inventory
    documents = copy.deepcopy(derivations)
    documents[0].update(office_rows=2**62, page_count=0)
    documents[1].update(office_rows=0, page_count=2**62)
    accumulator = _accumulator("office", root)
    for document in documents:
        accumulator.accept(document)
    for _ in range(2):
        with pytest.raises(inventory.RegistrySourceInventoryError):
            accumulator.finish()


@pytest.mark.parametrize("kind", ["contribution", "office"])
def test_accept_snapshots_payload_without_retaining_it(kind, sample_inventory):
    root, descriptors, derivations = sample_inventory
    documents = copy.deepcopy(descriptors if kind == "contribution" else derivations)
    expected = (
        summaries.contribution_summary_v3(root, iter(descriptors))
        if kind == "contribution"
        else _office(root, iter(derivations))
    )
    accumulator = _accumulator(kind, root)
    for document in documents:
        accumulator.accept(document)
        document.clear()
    assert accumulator.finish() == expected
