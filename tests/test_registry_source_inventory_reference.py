# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inventory metadata cannot select another SQL relation or relax the closed root."""

import hashlib
import json
from dataclasses import replace

import pytest

from process.registry_source_inventory_contract import RegistrySourceInventoryError, source_inventory_root
from process.registry_source_inventory_reference import (
    INVENTORY_DIGEST_KEY,
    SourceInventoryReference,
    decode_source_inventory_reference,
    encode_source_inventory_reference,
    verify_source_recipe_metadata,
)
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
)
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_recipe_store_postgres import _example


def test_reference_roundtrip_retains_the_exact_commitment_without_payload():
    reference = SourceInventoryReference(source_inventory_root(()))
    encoded = encode_source_inventory_reference(reference)
    assert decode_source_inventory_reference(encoded) == reference
    assert decode_source_inventory_reference(reference.as_dict()) == reference
    assert "recipe_json" not in encoded and "schema_name" not in encoded


@pytest.mark.parametrize(
    "changes",
    [
        {"component": "other"},
        {"revision": True},
        {"revision": 2},
        {"table_name": "other_schema.network_source_inventory"},
        {"extra": True},
        {"root": []},
        {"root": {"entry_count": 1}},
    ],
)
def test_reference_cannot_change_storage_or_omit_its_complete_root(changes):
    document = SourceInventoryReference(source_inventory_root(())).as_dict()
    with pytest.raises(RegistrySourceInventoryError):
        decode_source_inventory_reference({**document, **changes})


def test_duplicate_fields_and_oversized_documents_are_rejected():
    encoded = encode_source_inventory_reference(SourceInventoryReference(source_inventory_root(())))
    duplicate = encoded[:-1] + ',"revision":1}'
    for value in (duplicate, json.dumps({"padding": "x" * 4096})):
        with pytest.raises(RegistrySourceInventoryError):
            decode_source_inventory_reference(value)


def test_reference_size_does_not_grow_with_source_count():
    empty = source_inventory_root(())
    roots = [replace(empty, entry_count=count, encoded_bytes=100000, recipe_bytes=80000) for count in (101, 1000)]
    encoded_references = [encode_source_inventory_reference(SourceInventoryReference(root)) for root in roots]
    assert abs(len(encoded_references[0]) - len(encoded_references[1])) <= 1
    assert all(len(document.encode()) < 1024 for document in encoded_references)


def _candidate(reference):
    return {
        "source_recipes_json": encode_source_inventory_reference(reference),
        "source_generations": (
            {
                INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
                **({RECIPE_DIGEST_KEY: reference.root.recipes_sha256} if reference.root.entry_count else {}),
            }
        ),
    }


@pytest.mark.parametrize("count", [0, 1, 100])
def test_legacy_metadata_preserves_the_existing_closed_codec_and_digest(count):
    canonical = canonical_registry_source_recipes(tuple(_example(index) for index in range(count)))
    digest = hashlib.sha256(canonical.encode()).hexdigest() if count else None
    metadata = verify_source_recipe_metadata(
        {"source_recipes_json": canonical, "source_generations": {RECIPE_DIGEST_KEY: digest} if count else {}}
    )
    assert (metadata.recipe_count, metadata.recipes_sha256, metadata.inventory) == (count, digest, None)


def test_reference_metadata_preserves_101_count_and_legacy_refusal():
    from process.registry_source_recipe_store import RegistrySourceRecipeError, verify_registry_source_recipes

    reference = SourceInventoryReference(source_inventory_root(_entries(_example(index) for index in range(101))))
    candidate = _candidate(reference)
    metadata = verify_source_recipe_metadata(candidate)
    assert (metadata.recipe_count, metadata.recipes_sha256, metadata.inventory) == (
        101,
        reference.root.recipes_sha256,
        reference,
    )
    with pytest.raises(RegistrySourceRecipeError):
        verify_registry_source_recipes(candidate)


@pytest.mark.parametrize("generations", [{}, {RECIPE_DIGEST_KEY: "0" * 64}, [], None])
def test_reference_metadata_rejects_missing_or_substituted_generation_commitments(generations):
    reference = SourceInventoryReference(source_inventory_root(_entries((_example(),))))
    with pytest.raises(RegistrySourceInventoryError):
        verify_source_recipe_metadata({**_candidate(reference), "source_generations": generations})


@pytest.mark.parametrize("count", [0, 1])
@pytest.mark.parametrize("digest", [None, "0" * 64])
def test_inventory_payload_commitment_is_required_even_for_empty_references(count, digest):
    reference = SourceInventoryReference(source_inventory_root(_entries(_example(index) for index in range(count))))
    candidate = _candidate(reference)
    if digest is None:
        candidate["source_generations"].pop(INVENTORY_DIGEST_KEY)
    else:
        candidate["source_generations"][INVENTORY_DIGEST_KEY] = digest
    with pytest.raises(RegistrySourceInventoryError):
        verify_source_recipe_metadata(candidate)


def test_empty_reference_requires_the_exact_empty_root_and_no_generation_marker():
    reference = SourceInventoryReference(source_inventory_root(()))
    metadata = verify_source_recipe_metadata(_candidate(reference))
    assert metadata.recipe_count == 0 and metadata.recipes_sha256 is None
    for candidate in (
        {**_candidate(reference), "source_generations": {RECIPE_DIGEST_KEY: reference.root.recipes_sha256}},
        _candidate(SourceInventoryReference(replace(reference.root, entries_sha256="0" * 64))),
    ):
        with pytest.raises(RegistrySourceInventoryError):
            verify_source_recipe_metadata(candidate)


def test_candidate_registration_and_exact_replay_bind_the_entire_inventory_root():
    from process.network_membership_candidate_lifecycle import (
        MembershipCandidateError,
        _candidate_source_metadata,
        _verify_candidate_metadata,
    )

    reference = SourceInventoryReference(source_inventory_root(_entries((_example(),))))
    generations, recipes = _candidate_source_metadata({}, (), reference)
    expected_by_field = {
        "source_generations": generations,
        "source_recipes_json": recipes,
        "approved_custom_revision": 0,
        "expected_head": 0,
        "expected_rows": 0,
        "validation_json": None,
    }
    _verify_candidate_metadata(expected_by_field, expected_by_field)
    substituted = SourceInventoryReference(replace(reference.root, entries_sha256="0" * 64))
    with pytest.raises(MembershipCandidateError, match="immutable"):
        _verify_candidate_metadata(
            {**expected_by_field, "source_recipes_json": encode_source_inventory_reference(substituted)},
            expected_by_field,
        )
    with pytest.raises(MembershipCandidateError, match="invalid"):
        _candidate_source_metadata({}, (_example(),), reference)


@pytest.mark.parametrize("count", [0, 101])
def test_metadata_only_reference_candidates_lack_publication_readiness(count):
    from process.network_membership_publication import NetworkPublicationError, _publication_readiness

    reference = SourceInventoryReference(source_inventory_root(_entries(_example(index) for index in range(count))))
    with pytest.raises(NetworkPublicationError, match="pinned validation report"):
        _publication_readiness(_candidate(reference))
