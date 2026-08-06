# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior and canonicalization tests for dormant publication intents."""

from __future__ import annotations

from public_evidence import staged_bundle_publication_contract as contract
from public_evidence import staged_bundle_publication_primitives as primitives
from tests.public_evidence_staged_bundle_publication_support import (
    BUILD_RUN_REF,
    CURRENT_GENERATION_REF,
    PREVIOUS_GENERATION_REF,
    SCHEMA,
    bundle_input,
    fingerprints,
    relation_input,
    two_relation_inputs,
)

INITIAL_GOLDEN = {
    "source_vector_sha256": (
        "166acbe8ab9d7e45309fd7bde77b7a711d7319fcbe592ac17c5db21f639564f9"
    ),
    "source_fence_sha256": (
        "fd513185d306b3dc085c6a8cca49ea2c71d75de3252d8e0195fa280755163c12"
    ),
    "pointer_fence_sha256": (
        "0122993641ede9c8ebbb57e587f05f5a24f670df04fa4a7f1d76f11b2ec6dc39"
    ),
    "oid_fence_sha256": (
        "fcfc8edeeaa97fbee88d87f82f5a307d5d565a5ab828c6daaa1535a1fe795ef0"
    ),
    "catalog_fence_sha256": (
        "08ebe0cbd018bd481f734cc143f2da179b90c97f497235809469abe0581135e2"
    ),
    "contract_sha256": (
        "4ebb2a3b1585a3b83f05d2fde592f5f6594b8a3d0c8f3383ce59dedbd7ddebb9"
    ),
}
REPLACEMENT_GOLDEN = {
    "pointer_fence_sha256": (
        "3d3ab5fa4eece68c5de960f19eaa51d175e8d9f5509ee0d808e37be643786020"
    ),
    "oid_fence_sha256": (
        "a87adbd45b3c0ba4693a750c1ae7326acecccaa001c862dc57879585e32c0cd8"
    ),
    "catalog_fence_sha256": (
        "93f108c42cdbc7a0c29c03ba4100fa6b83063f1cb3365427cf7721a1af6e59dc"
    ),
    "contract_sha256": (
        "9060aa5d38f95094eaa6e0ea068b5e8ec5f33e79f6dcc1ecb578dea379e195df"
    ),
}


def test_initial_intent_has_no_live_parity_or_execution_authority() -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())

    assert intent.mode == "initial"
    assert intent.expected_predecessor_generation_ref is None
    assert intent.expected_current_generation_ref is None
    assert intent.expected_previous_generation_ref is None
    assert all(
        relation.stage_catalog_state == "verified_equal"
        and relation.live_catalog_state == "not_applicable_no_live"
        and relation.live_fingerprints is None
        and relation.observed_live_oid is None
        for relation in intent.relations
    )
    assert intent.lock_order == tuple(
        sorted(relation.stage_relation for relation in intent.relations)
    )
    assert intent.lifecycle_state == "validated_intent_only"
    assert intent.serving_authority == "none"
    assert intent.current_pointer_authority == "none"
    assert intent.executor_authority == "none"
    assert intent.publication_authorized is False
    assert intent.publication_enabled is False
    assert intent.database_io_enabled is False
    assert intent.cleanup_authorized is False
    assert intent.reverse_swap_authorized is False
    assert intent.executable_rename_choreography_defined is False
    assert intent.index_rename_choreography_defined is False
    assert intent.retained_old_required is True
    assert intent.automatic_old_deletion_enabled is False
    assert intent.automatic_gc_enabled is False
    assert {
        field_name: getattr(intent, field_name) for field_name in INITIAL_GOLDEN
    } == INITIAL_GOLDEN


def test_replacement_intent_binds_live_parity_and_generation_pointers() -> None:
    intent = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )

    assert intent.mode == "replacement"
    assert intent.expected_predecessor_generation_ref == CURRENT_GENERATION_REF
    assert intent.expected_current_generation_ref == CURRENT_GENERATION_REF
    assert intent.expected_previous_generation_ref == PREVIOUS_GENERATION_REF
    assert all(
        relation.stage_catalog_state == "verified_equal"
        and relation.live_catalog_state == "verified_equal"
        and relation.expected_fingerprints == relation.stage_fingerprints
        and relation.stage_fingerprints == relation.live_fingerprints
        and relation.observed_live_oid is not None
        for relation in intent.relations
    )
    assert intent.lock_order == tuple(
        sorted(
            name
            for relation in intent.relations
            for name in (relation.live_relation, relation.stage_relation)
        )
    )
    assert intent.source_vector_sha256 == INITIAL_GOLDEN["source_vector_sha256"]
    assert intent.source_fence_sha256 == INITIAL_GOLDEN["source_fence_sha256"]
    assert {
        field_name: getattr(intent, field_name) for field_name in REPLACEMENT_GOLDEN
    } == REPLACEMENT_GOLDEN


def test_first_replacement_may_have_no_previous_generation() -> None:
    intent = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True, previous=None)
    )

    assert intent.mode == "replacement"
    assert intent.expected_current_generation_ref == CURRENT_GENERATION_REF
    assert intent.expected_predecessor_generation_ref == CURRENT_GENERATION_REF
    assert intent.expected_previous_generation_ref is None


def test_input_order_does_not_change_the_canonical_intent() -> None:
    raw = bundle_input(
        source_kinds=("tic", "nppes_entity_address"),
        relations=two_relation_inputs(),
    )
    reversed_input_by_field = dict(reversed(tuple(raw.items())))
    reversed_input_by_field["source_releases"] = tuple(reversed(raw["source_releases"]))
    reversed_input_by_field["relations"] = tuple(
        dict(reversed(tuple(relation.items())))
        for relation in reversed(raw["relations"])
    )

    first = contract.build_staged_bundle_publication_intent(raw)
    second = contract.build_staged_bundle_publication_intent(reversed_input_by_field)

    assert first == second
    assert first.relation_order == (
        "entity_address_evidence",
        "evidence_source_release",
    )
    assert tuple(witness.source_kind for witness in first.source_witnesses) == (
        "nppes_entity_address",
        "tic",
    )


_FENCE_FIELDS = (
    "source_fence_sha256",
    "pointer_fence_sha256",
    "oid_fence_sha256",
    "catalog_fence_sha256",
)


def _changed_fence_names(
    baseline: contract.StagedBundlePublicationIntent,
    changed: contract.StagedBundlePublicationIntent,
) -> tuple[str, ...]:
    return tuple(
        field_name
        for field_name in _FENCE_FIELDS
        if getattr(changed, field_name) != getattr(baseline, field_name)
    )


def test_source_fence_is_independent() -> None:
    baseline = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )
    source_changed = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True, source_kinds=("nppes_entity_address",))
    )

    assert _changed_fence_names(baseline, source_changed) == ("source_fence_sha256",)
    assert source_changed.contract_sha256 != baseline.contract_sha256


def test_pointer_fence_is_independent() -> None:
    baseline = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )
    pointer_changed = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True, previous=None)
    )

    assert _changed_fence_names(baseline, pointer_changed) == ("pointer_fence_sha256",)
    assert pointer_changed.contract_sha256 != baseline.contract_sha256


def test_oid_fence_is_independent() -> None:
    baseline = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )
    oid_relations = two_relation_inputs(replacement=True)
    oid_relations[0]["observed_stage_oid"] = 301
    oid_changed = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True, relations=oid_relations)
    )

    assert _changed_fence_names(baseline, oid_changed) == ("oid_fence_sha256",)
    assert oid_changed.contract_sha256 != baseline.contract_sha256


def test_catalog_fence_is_independent() -> None:
    baseline = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )
    catalog_relations = two_relation_inputs(replacement=True)
    changed_fingerprints = fingerprints(8)
    for field_name in (
        "expected_fingerprints",
        "stage_fingerprints",
        "live_fingerprints",
    ):
        catalog_relations[0][field_name] = dict(changed_fingerprints)
    catalog_changed = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True, relations=catalog_relations)
    )

    assert _changed_fence_names(baseline, catalog_changed) == ("catalog_fence_sha256",)
    assert catalog_changed.contract_sha256 != baseline.contract_sha256


def test_stage_and_retained_names_are_bounded_and_domain_separated() -> None:
    short_live = "a" * 59
    long_live = "a" * 60
    maximum_live = "a" * 62 + "b"
    same_prefix_live = "a" * 62 + "c"

    assert primitives.derive_old_relation_name(short_live) == f"{short_live}_old"
    long_old = primitives.derive_old_relation_name(long_live)
    maximum_old = primitives.derive_old_relation_name(maximum_live)
    same_prefix_old = primitives.derive_old_relation_name(same_prefix_live)
    assert all(
        len(name) <= 63 and name.endswith("_old")
        for name in (long_old, maximum_old, same_prefix_old)
    )
    assert len({long_old, maximum_old, same_prefix_old}) == 3

    identities = (
        (SCHEMA, BUILD_RUN_REF, "source_release", maximum_live),
        ("public_evidence_alt", BUILD_RUN_REF, "source_release", maximum_live),
        (
            SCHEMA,
            primitives.PUBLIC_EVIDENCE_BUILD_RUN_REF_PREFIX + "Z" * 43,
            "source_release",
            maximum_live,
        ),
        (SCHEMA, BUILD_RUN_REF, "address_evidence", maximum_live),
        (SCHEMA, BUILD_RUN_REF, "source_release", same_prefix_live),
    )
    stage_names = tuple(
        primitives.derive_stage_relation_name(*identity_parts)
        for identity_parts in identities
    )
    assert len(stage_names) == len(set(stage_names))
    assert all(len(name) <= 63 for name in stage_names)


def test_source_descriptors_are_validated_then_detached() -> None:
    raw = bundle_input()
    supplied_release = raw["source_releases"][0]
    intent = contract.build_staged_bundle_publication_intent(raw)
    original_witness = intent.source_witnesses[0]

    object.__setattr__(supplied_release, "contract_sha256", "f" * 64)

    assert intent.source_witnesses[0] == original_witness
    assert all(
        type(value) is str
        for value in (
            original_witness.source_kind,
            original_witness.source_release_ref,
            original_witness.contract_sha256,
        )
    )
    assert contract.validate_staged_bundle_publication_intent(intent) == intent


def test_revalidation_returns_an_equal_fresh_intent() -> None:
    intent = contract.build_staged_bundle_publication_intent(
        bundle_input(replacement=True)
    )
    rebuilt = contract.validate_staged_bundle_publication_intent(intent)

    assert rebuilt == intent
    assert rebuilt is not intent
    assert rebuilt.relations is not intent.relations
    assert rebuilt.source_witnesses is not intent.source_witnesses


def test_catalog_fingerprint_contract_is_explicitly_name_neutral() -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())

    assert (
        intent.catalog_fingerprint_contract == primitives.CATALOG_FINGERPRINT_CONTRACT
    )
    assert intent.catalog_fingerprint_exclusions == (
        "catalog_object_names",
        "catalog_object_oids",
    )
