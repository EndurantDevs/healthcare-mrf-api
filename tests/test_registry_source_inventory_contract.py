# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source inventory commitments preserve existing codec identities across bounded pages."""

import asyncio
import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from itertools import chain
from uuid import UUID

import pytest

from process import registry_source_inventory_contract as inventory
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    RegistrySourceRecipeError,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_contributions import _descriptor
from tests.test_registry_source_office_summary_v3 import _derivation
from tests.test_registry_source_recipe_store_postgres import (
    _cms_custody_recipe,
    _cms_epoch_recipe,
    _example,
)


def _entries(recipes):
    ordered = sorted(recipes, key=lambda recipe: canonical_registry_source_recipes((recipe,)))
    return tuple(inventory.make_source_inventory_entry(index, recipe) for index, recipe in enumerate(ordered, 1))


def _pages(entries, row_limit, byte_limit):
    after, pages = 0, []
    while True:
        page = inventory.source_inventory_page(
            (entry for entry in entries if entry.ordinal > after),
            after_ordinal=after,
            row_limit=row_limit,
            byte_limit=byte_limit,
        )
        pages.append(page)
        if page.next_after_ordinal is None:
            return pages
        after = page.next_after_ordinal


@pytest.mark.parametrize(
    "recipes",
    [
        (),
        (_example(),),
        (_example(2), _cms_custody_recipe(), _cms_epoch_recipe(), _ptg_recipe()),
    ],
)
def test_legacy_recipe_bytes_digest_and_constant_root_are_unchanged(recipes):
    entries = _entries(recipes)
    root = inventory.source_inventory_root(iter(entries))
    legacy = canonical_registry_source_recipes(recipes)
    assert b"[" + b",".join(entry.recipe_json.encode() for entry in entries) + b"]" == legacy.encode()
    assert root.recipes_sha256 == registry_source_recipes_sha256(legacy)
    assert root.recipe_bytes == len(legacy.encode())
    assert root.entry_count == len(recipes)
    encoded_root = inventory.encode_source_inventory_root(root)
    assert len(encoded_root) < 512 and b"table_oid" not in encoded_root
    with pytest.raises(FrozenInstanceError):
        root.entry_count = 5


def test_101_sources_and_mixed_page_cuts_have_identical_complete_roots():
    recipes = tuple(_example(index) for index in range(101))
    entries = _entries(reversed(recipes))
    root = inventory.source_inventory_root(iter(entries))
    expected_array = b"[" + b",".join(entry.recipe_json.encode() for entry in entries) + b"]"
    assert root.entry_count == 101 and root.recipes_sha256 == hashlib.sha256(expected_array).hexdigest()
    with pytest.raises(RegistrySourceRecipeError):
        canonical_registry_source_recipes(recipes)
    maximum_row = max(len(inventory.encode_source_inventory_entry(entry)) for entry in entries)
    for row_limit, byte_limit in ((1, maximum_row + 2), (17, 8192), (1000, 1048576)):
        pages = _pages(entries, row_limit, byte_limit)
        assert all(len(page.entries) <= row_limit and len(page.payload) <= byte_limit for page in pages)
        assert all(json.loads(page.payload) for page in pages)
        assert inventory.verify_source_inventory_root(chain.from_iterable(page.entries for page in pages), root) == root


@pytest.mark.asyncio
async def test_async_pages_feed_the_accumulator_without_collecting_the_inventory():
    entries = _entries(_example(index) for index in range(101))
    expected = inventory.source_inventory_root(entries)
    accumulator = inventory.SourceInventoryAccumulator()
    after, page_count = 0, 0
    while True:
        await asyncio.sleep(0)
        page = inventory.source_inventory_page(
            (entry for entry in entries if entry.ordinal > after),
            after_ordinal=after,
            row_limit=17,
            byte_limit=8192,
        )
        for entry in page.entries:
            accumulator.accept(entry)
        page_count += 1
        if page.next_after_ordinal is None:
            break
        after = page.next_after_ordinal
    assert page_count > 1 and accumulator.finish() == expected
    assert accumulator.finish() == expected
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(entries[-1])


def test_source_identity_duplicates_across_pages_poison_the_complete_fold():
    original = _example()
    recipes = (
        replace(original, binding_coordinates=replace(original.binding_coordinates, edition_id="a")),
        RegistrySourceMembershipRecipe(
            replace(original.source_pin, dataset_sha256="b" * 64),
            replace(original.binding_coordinates, edition_id="m"),
        ),
        replace(original, binding_coordinates=replace(original.binding_coordinates, edition_id="z")),
    )
    pages = _pages(_entries(recipes), row_limit=1, byte_limit=1048576)
    assert len(pages) == 3
    accumulator = inventory.SourceInventoryAccumulator()
    for page in pages[:2]:
        accumulator.accept(page.entries[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(pages[2].entries[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()


@pytest.mark.parametrize("anchored", [False, True])
def test_revision_two_contributions_and_fact_office_derivations_roundtrip(anchored):
    recipe = _example()
    descriptor_by_field = {
        **_descriptor(recipe),
        "fact_candidate_id": str(UUID(int=7)) if anchored else None,
        "fact_table_oid": 71 if anchored else None,
        "fact_seal_sha256": "d" * 64 if anchored else None,
    }
    derivation = _derivation(descriptor_by_field["recipe_sha256"], mode="facts")
    derivation["fact_artifact"].update(
        fact_candidate_id=str(UUID(int=7)),
        fact_table_oid=71,
        fact_seal_sha256="d" * 64,
        capture_sha256=descriptor_by_field["capture_sha256"],
        transform_version=descriptor_by_field["transform_version"],
        fact_sha256=descriptor_by_field["fact_sha256"],
        fact_rows=descriptor_by_field["fact_rows"],
    )
    entry = inventory.make_source_inventory_entry(
        1, recipe, contribution=descriptor_by_field, office_derivation=derivation
    )
    assert json.loads(entry.contribution_json) == descriptor_by_field
    assert json.loads(entry.office_derivation_json) == derivation
    root = inventory.source_inventory_root((entry,))
    assert root.contribution_count == root.derivation_count == 1
    partial_by_field = {**descriptor_by_field, "fact_table_oid": None if anchored else 71}
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.make_source_inventory_entry(1, recipe, contribution=partial_by_field)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.make_source_inventory_entry(1, recipe, office_derivation={**derivation, "fact_artifact": None})


def test_optional_metadata_is_closed_bound_to_the_singleton_recipe_and_committed():
    recipe = _example()
    descriptor = _descriptor(recipe)
    derivation = _derivation(descriptor["recipe_sha256"], mode="raw")
    entry = inventory.make_source_inventory_entry(1, recipe, contribution=descriptor, office_derivation=derivation)
    root = inventory.source_inventory_root((entry,))
    assert root.contribution_count == root.derivation_count == 1
    assert json.loads(entry.recipe_json) == json.loads(canonical_registry_source_recipes((recipe,)))[0]
    changed = inventory.make_source_inventory_entry(1, recipe, contribution=descriptor)
    assert inventory.source_inventory_root((changed,)).recipes_sha256 == root.recipes_sha256
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.verify_source_inventory_root((changed,), root)
    for field in ("recipe_sha256", "untrusted"):
        wrong_by_field = {**descriptor, field: "f" * 64}
        with pytest.raises(inventory.RegistrySourceInventoryError):
            inventory.make_source_inventory_entry(1, recipe, contribution=wrong_by_field)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.make_source_inventory_entry(1, recipe, office_derivation={**derivation, "recipe_sha256": "0" * 64})
    with pytest.raises(inventory.RegistrySourceInventoryError):
        replace(entry, contribution_json=entry.contribution_json[:-1] + ',"fact_rows":1}')


def test_unicode_page_bytes_include_exact_utf8_and_both_list_delimiters():
    original = _example()
    recipe = RegistrySourceMembershipRecipe(
        replace(original.source_pin, source_id="source-é😀"),
        replace(original.binding_coordinates, source_id="source-é😀"),
    )
    entry = inventory.make_source_inventory_entry(1, recipe)
    encoded = inventory.encode_source_inventory_entry(entry)
    assert len(encoded) > len(encoded.decode())
    exact = inventory.source_inventory_page((entry,), after_ordinal=0, row_limit=1, byte_limit=len(encoded) + 2)
    assert exact.payload == b"[" + encoded + b"]" and exact.next_after_ordinal is None
    with pytest.raises(inventory.RegistrySourceInventoryError, match="page_too_small"):
        inventory.source_inventory_page((entry,), after_ordinal=0, row_limit=1, byte_limit=len(encoded) + 1)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        replace(
            entry,
            recipe_json=json.dumps(json.loads(entry.recipe_json), ensure_ascii=True),
        )


@pytest.mark.parametrize("damage", ["missing", "ordinal", "order", "duplicate", "substitute"])
def test_root_verification_rejects_missing_reordered_duplicate_or_substituted_rows(
    damage,
):
    entries = _entries((_example(0), _example(1), _example(2)))
    root = inventory.source_inventory_root(entries)
    changed_entries = list(entries)
    if damage == "missing":
        changed_entries.pop()
    elif damage == "ordinal":
        changed_entries[1] = replace(changed_entries[1], ordinal=3)
    elif damage == "order":
        changed_entries = [replace(entry, ordinal=index) for index, entry in enumerate(reversed(changed_entries), 1)]
    elif damage == "duplicate":
        changed_entries[1] = replace(changed_entries[0], ordinal=2)
    else:
        changed_entries[-1] = inventory.make_source_inventory_entry(3, _example(99))
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.verify_source_inventory_root(iter(changed_entries), root)


def test_uniqueness_uses_existing_pin_type_and_generation_not_source_id():
    entries = _entries((_example(0), _example(1)))
    assert inventory.source_inventory_root(entries).entry_count == 2
    original = _example()
    alternate = replace(
        original,
        binding_coordinates=replace(original.binding_coordinates, edition_id="edition-two"),
    )
    with pytest.raises(RegistrySourceRecipeError):
        canonical_registry_source_recipes((original, alternate))
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.source_inventory_root(_entries((original, alternate)))


def test_count_1000_is_supported_and_1001_is_an_explicit_unsupported_bound():
    root = inventory.source_inventory_root(iter(_entries(_example(index) for index in range(1000))))
    assert root.entry_count == inventory.MAX_INVENTORY_ENTRIES == 1000
    with pytest.raises(inventory.RegistrySourceInventoryError, match="unsupported_bound"):
        inventory.make_source_inventory_entry(1001, _example(1001))


def test_total_byte_budget_stops_the_stream_at_the_first_overflow(monkeypatch):
    entries = _entries((_example(0), _example(1), _example(2)))
    first_two = inventory.source_inventory_root(entries[:2])
    assert inventory.MAX_INVENTORY_BYTES == 64 * 1024 * 1024
    monkeypatch.setattr(inventory, "MAX_INVENTORY_BYTES", first_two.encoded_bytes)
    assert inventory.source_inventory_root(iter(entries[:2])) == first_two
    with pytest.raises(inventory.RegistrySourceInventoryError, match="unsupported_bound"):
        inventory.source_inventory_root(iter(entries))


def test_page_row_limit_requires_one_lookahead_and_rejects_a_keyset_gap():
    entries = _entries((_example(0), _example(1), _example(2)))
    consumed_ordinals = []

    def rows():
        for entry in entries:
            consumed_ordinals.append(entry.ordinal)
            yield entry
        raise AssertionError("page assembler consumed beyond its single lookahead")

    page = inventory.source_inventory_page(rows(), after_ordinal=0, row_limit=1, byte_limit=1048576)
    assert consumed_ordinals == [1, 2] and page.next_after_ordinal == 1
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.source_inventory_page(entries[2:], after_ordinal=1, row_limit=1, byte_limit=1048576)


@pytest.mark.parametrize(
    "field,value",
    [
        ("after_ordinal", True),
        ("after_ordinal", -1),
        ("row_limit", 0),
        ("byte_limit", 1),
        ("byte_limit", 1048577),
    ],
)
def test_page_rejects_invalid_or_unbounded_request_shapes(field, value):
    arguments_by_name = {
        "after_ordinal": 0,
        "row_limit": 1,
        "byte_limit": 1048576,
        field: value,
    }
    with pytest.raises(inventory.RegistrySourceInventoryError):
        inventory.source_inventory_page((), **arguments_by_name)


def test_inventory_entry_cannot_attach_a_valid_contribution_from_another_recipe():
    selected_recipe, other_recipe = _example(1), _example(2)
    other_contribution = _descriptor(other_recipe)
    assert inventory.make_source_inventory_entry(1, other_recipe, contribution=other_contribution).contribution_json
    with pytest.raises(inventory.RegistrySourceInventoryError, match="inventory_invalid"):
        inventory.make_source_inventory_entry(1, selected_recipe, contribution=other_contribution)


def test_stored_contribution_requires_canonical_serialization_not_equivalent_json_whitespace():
    recipe = _example()
    selected = inventory.make_source_inventory_entry(1, recipe, contribution=_descriptor(recipe))
    expanded_json = json.dumps(json.loads(selected.contribution_json), indent=2)
    assert json.loads(expanded_json) == json.loads(selected.contribution_json)
    with pytest.raises(inventory.RegistrySourceInventoryError, match="inventory_invalid"):
        replace(selected, contribution_json=expanded_json)


@pytest.mark.parametrize("ordinal", [0, -1, True])
def test_inventory_entry_rejects_invalid_initial_ordinal_without_poisoning_another_entry(ordinal):
    (selected,) = _entries((_example(),))
    with pytest.raises(inventory.RegistrySourceInventoryError, match="inventory_invalid"):
        inventory.SourceInventoryEntry(ordinal, selected.recipe_json)
    assert inventory.source_inventory_root((selected,)).entry_count == 1
