# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial and dormancy tests for staged bundle publication intents."""

from __future__ import annotations

import pytest

from public_evidence import staged_bundle_publication_contract as contract
from public_evidence import staged_bundle_publication_primitives as primitives
from tests.public_evidence_staged_bundle_publication_support import (
    CURRENT_GENERATION_REF,
    GENERATION_REF,
    PREVIOUS_GENERATION_REF,
    bundle_input,
    fingerprints,
    relation_input,
    source_release,
    two_relation_inputs,
)


class EquivalentString(str):
    pass


class EqualityCompatible:
    def __eq__(self, _other: object) -> bool:
        return True

    def __ne__(self, _other: object) -> bool:
        return False


def _assert_invalid(raw: object) -> None:
    with pytest.raises(
        primitives.StagedBundlePublicationIntentError,
        match="^public_evidence_staged_bundle_intent_invalid$",
    ):
        contract.build_staged_bundle_publication_intent(raw)


@pytest.mark.parametrize(
    "field_name",
    (
        "mode",
        "source_vector_sha256",
        "source_vector_canonical",
        "publication_enabled",
        "database_io_enabled",
        "relation_order",
        "lock_order",
        "catalog_fence_sha256",
    ),
)
def test_rejects_caller_supplied_derived_or_capability_fields(field_name: str) -> None:
    raw = bundle_input()
    raw[field_name] = False
    _assert_invalid(raw)


def test_rejects_invalid_initial_and_replacement_pointer_states() -> None:
    initial_with_current = bundle_input()
    initial_with_current["expected_current_generation_ref"] = CURRENT_GENERATION_REF
    _assert_invalid(initial_with_current)

    initial_with_previous = bundle_input()
    initial_with_previous["expected_previous_generation_ref"] = PREVIOUS_GENERATION_REF
    _assert_invalid(initial_with_previous)

    replacement_without_current = bundle_input(replacement=True)
    replacement_without_current["expected_current_generation_ref"] = None
    _assert_invalid(replacement_without_current)

    replacement_same_generation = bundle_input(replacement=True)
    replacement_same_generation["generation_ref"] = CURRENT_GENERATION_REF
    _assert_invalid(replacement_same_generation)

    previous_is_current = bundle_input(replacement=True)
    previous_is_current["expected_previous_generation_ref"] = CURRENT_GENERATION_REF
    _assert_invalid(previous_is_current)

    previous_is_new = bundle_input(replacement=True)
    previous_is_new["expected_previous_generation_ref"] = GENERATION_REF
    _assert_invalid(previous_is_new)


def test_rejects_mixed_initial_and_replacement_relations() -> None:
    relations = two_relation_inputs()
    relations[0]["observed_live_oid"] = 201
    relations[0]["live_fingerprints"] = dict(relations[0]["stage_fingerprints"])

    _assert_invalid(bundle_input(relations=relations))


@pytest.mark.parametrize(
    "mutation",
    (
        "expected_catalog_mismatch",
        "stage_catalog_mismatch",
        "missing_replacement_live_catalog",
        "initial_has_live_catalog",
        "unlogged_stage",
        "old_relation_present",
        "wrong_stage_name",
        "wrong_old_name",
    ),
)
def test_rejects_catalog_persistence_and_name_mismatches(mutation: str) -> None:
    mutation_by_name = {
        "expected_catalog_mismatch": (
            "expected_fingerprints",
            fingerprints(8),
            False,
        ),
        "stage_catalog_mismatch": ("stage_fingerprints", fingerprints(8), False),
        "missing_replacement_live_catalog": ("live_fingerprints", None, True),
        "initial_has_live_catalog": ("live_fingerprints", fingerprints(), False),
        "unlogged_stage": ("stage_persistence", "unlogged", False),
        "old_relation_present": ("observed_old_oid", 301, False),
        "wrong_stage_name": ("stage_relation", "caller_chosen_stage", False),
        "wrong_old_name": ("old_relation", "caller_chosen_old", False),
    }
    field_name, invalid_value, is_replacement = mutation_by_name[mutation]
    relation = relation_input(live_oid=201 if is_replacement else None)
    relation[field_name] = invalid_value
    _assert_invalid(bundle_input(replacement=is_replacement, relations=(relation,)))


@pytest.mark.parametrize(
    "invalid_oid",
    (True, False, 0, -1, 2**32, 1.0, "101"),
)
def test_rejects_non_postgresql_oids(invalid_oid: object) -> None:
    relation = relation_input()
    relation["observed_stage_oid"] = invalid_oid
    _assert_invalid(bundle_input(relations=(relation,)))


def test_rejects_duplicate_roles_names_and_oids() -> None:
    duplicate_role = (
        relation_input(),
        relation_input(
            role="source_release",
            live_relation="entity_address_evidence",
            stage_oid=102,
            fingerprint_seed=6,
        ),
    )
    _assert_invalid(bundle_input(relations=duplicate_role))

    duplicate_live = (
        relation_input(),
        relation_input(
            role="address_evidence",
            live_relation="evidence_source_release",
            stage_oid=102,
            fingerprint_seed=6,
        ),
    )
    _assert_invalid(bundle_input(relations=duplicate_live))

    duplicate_oid = two_relation_inputs()
    duplicate_oid[1]["observed_stage_oid"] = duplicate_oid[0]["observed_stage_oid"]
    _assert_invalid(bundle_input(relations=duplicate_oid))

    cross_live_stage_oid = two_relation_inputs(replacement=True)
    cross_live_stage_oid[1]["observed_stage_oid"] = cross_live_stage_oid[0][
        "observed_live_oid"
    ]
    _assert_invalid(bundle_input(replacement=True, relations=cross_live_stage_oid))


def test_rejects_duplicate_or_tampered_source_releases() -> None:
    duplicate = source_release()
    raw = bundle_input()
    raw["source_releases"] = (duplicate, duplicate)
    _assert_invalid(raw)

    tampered = source_release()
    object.__setattr__(tampered, "contract_sha256", "f" * 64)
    raw = bundle_input()
    raw["source_releases"] = (tampered,)
    _assert_invalid(raw)


def test_rejects_non_exact_or_empty_containers() -> None:
    class DictionarySubclass(dict[str, object]):
        pass

    class TupleSubclass(tuple[object, ...]):
        pass

    _assert_invalid(DictionarySubclass(bundle_input()))

    for field_name, container_value in (
        ("source_releases", []),
        ("source_releases", ()),
        ("source_releases", TupleSubclass(bundle_input()["source_releases"])),
        ("relations", []),
        ("relations", ()),
        ("relations", TupleSubclass(bundle_input()["relations"])),
    ):
        raw = bundle_input()
        raw[field_name] = container_value
        _assert_invalid(raw)

    raw = bundle_input()
    raw["relations"] = (DictionarySubclass(raw["relations"][0]),)
    _assert_invalid(raw)

    raw = bundle_input()
    relation = raw["relations"][0]
    relation[1] = relation.pop("role")
    _assert_invalid(raw)

    raw = bundle_input()
    raw["source_releases"] = (object(),)
    _assert_invalid(raw)


def test_caps_are_checked_before_member_traversal() -> None:
    class Explosive:
        touched = False

        def __getattribute__(self, name: str) -> object:
            type(self).touched = True
            raise AssertionError(name)

    explosive = Explosive()
    raw = bundle_input()
    raw["relations"] = (explosive,) * (primitives.MAX_STAGED_BUNDLE_RELATIONS + 1)
    _assert_invalid(raw)
    assert Explosive.touched is False

    raw = bundle_input()
    raw["source_releases"] = (explosive,) * (
        primitives.MAX_STAGED_BUNDLE_SOURCE_RELEASES + 1
    )
    _assert_invalid(raw)
    assert Explosive.touched is False


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("schema", "PublicEvidence"),
        ("build_run_ref", "caller-run"),
        ("generation_ref", "caller-generation"),
        ("generation_ref", primitives.PUBLIC_EVIDENCE_GENERATION_REF_PREFIX + "A" * 42),
        ("expected_current_generation_ref", "pegen1_short"),
    ),
)
def test_rejects_noncanonical_schema_or_opaque_references(
    field_name: str,
    invalid_value: object,
) -> None:
    raw = bundle_input()
    raw[field_name] = invalid_value
    _assert_invalid(raw)


def test_allows_neutral_npi_and_ein_relation_roles() -> None:
    relations = (
        relation_input(role="npi", live_relation="npi_evidence"),
        relation_input(
            role="ein",
            live_relation="ein_evidence",
            stage_oid=102,
            fingerprint_seed=6,
        ),
    )
    intent = contract.build_staged_bundle_publication_intent(
        bundle_input(relations=relations)
    )

    assert {relation.role for relation in intent.relations} == {"npi", "ein"}


def test_rejects_malformed_fingerprint_contract_inputs() -> None:
    for invalid in (
        {"schema_sha256": "a" * 64},
        {**fingerprints(), "extra_sha256": "f" * 64},
        {**fingerprints(), "schema_sha256": "A" * 64},
        tuple(fingerprints().values()),
    ):
        relation = relation_input()
        relation["stage_fingerprints"] = invalid
        _assert_invalid(bundle_input(relations=(relation,)))


def test_rejects_same_relation_live_and_stage_oid() -> None:
    relation = relation_input(live_oid=101, stage_oid=101)
    _assert_invalid(bundle_input(replacement=True, relations=(relation,)))


def test_intent_and_nested_records_are_deeply_immutable() -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())
    values = (
        intent,
        intent.relations[0],
        intent.source_witnesses[0],
        intent.relations[0].stage_fingerprints,
    )
    for value in values:
        with pytest.raises(AttributeError):
            object.__setattr__(value, value._fields[0], "changed")
        with pytest.raises(AttributeError):
            object.__delattr__(value, value._fields[0])


def test_revalidation_rejects_forged_nested_and_fixed_state() -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())
    relation = intent.relations[0]
    witness = intent.source_witnesses[0]
    fingerprint = relation.stage_fingerprints
    forged_values = (
        intent._replace(publication_enabled=0),
        intent._replace(contract="healthporta.other.v1"),
        intent._replace(contract_sha256="f" * 64),
        intent._replace(source_vector_sha256="f" * 64),
        intent._replace(catalog_fence_sha256="f" * 64),
        intent._replace(relation_order=tuple(reversed(intent.relation_order))),
        intent._replace(
            relations=(relation._replace(live_catalog_state="verified_equal"),)
            + intent.relations[1:]
        ),
        intent._replace(source_witnesses=(witness._replace(contract_sha256="f" * 64),)),
        intent._replace(
            relations=(
                relation._replace(
                    stage_fingerprints=fingerprint._replace(schema_sha256="f" * 64)
                ),
            )
            + intent.relations[1:]
        ),
    )
    for forged in forged_values:
        with pytest.raises(primitives.StagedBundlePublicationIntentError):
            contract.validate_staged_bundle_publication_intent(forged)


def test_revalidation_rejects_wrong_nested_types_and_source_witness_shapes() -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())
    relation = intent.relations[0]
    witness = intent.source_witnesses[0]
    fingerprint = relation.stage_fingerprints
    forged_values = (
        intent._replace(relations=(object(),) + intent.relations[1:]),
        intent._replace(
            relations=(relation._replace(stage_fingerprints=tuple(fingerprint)),)
            + intent.relations[1:]
        ),
        intent._replace(source_witnesses=(object(),)),
        intent._replace(mode=EquivalentString(intent.mode)),
        intent._replace(expected_predecessor_generation_ref=EqualityCompatible()),
        intent._replace(
            catalog_fingerprint_exclusions=(
                EquivalentString("catalog_object_names"),
                EquivalentString("catalog_object_oids"),
            )
        ),
        intent._replace(
            catalog_fingerprint_exclusions=(
                EqualityCompatible(),
                EqualityCompatible(),
            )
        ),
        intent._replace(
            relations=(
                relation._replace(
                    stage_catalog_state=EquivalentString("verified_equal")
                ),
            )
            + intent.relations[1:]
        ),
        intent._replace(
            relations=(
                relation._replace(
                    live_catalog_state=EquivalentString("not_applicable_no_live")
                ),
            )
            + intent.relations[1:]
        ),
        intent._replace(
            relations=(relation._replace(old_relation_observed_absent=1),)
            + intent.relations[1:]
        ),
        intent._replace(source_witnesses=(witness._replace(source_kind="Bad"),)),
        intent._replace(
            source_witnesses=(witness._replace(source_kind="unsupported_source"),)
        ),
        intent._replace(
            source_witnesses=(witness._replace(source_release_ref="perel1_short"),)
        ),
    )
    for forged in forged_values:
        with pytest.raises(primitives.StagedBundlePublicationIntentError):
            contract.validate_staged_bundle_publication_intent(forged)


def test_revalidation_rejects_noncanonical_tuple_order() -> None:
    intent = contract.build_staged_bundle_publication_intent(
        bundle_input(source_kinds=("tic", "nppes_entity_address"))
    )
    with pytest.raises(primitives.StagedBundlePublicationIntentError):
        contract.validate_staged_bundle_publication_intent(
            intent._replace(source_witnesses=tuple(reversed(intent.source_witnesses)))
        )
    with pytest.raises(primitives.StagedBundlePublicationIntentError):
        contract.validate_staged_bundle_publication_intent(
            intent._replace(relations=tuple(reversed(intent.relations)))
        )


def test_revalidation_rejects_foreign_objects_before_property_access() -> None:
    class Hostile:
        @property
        def contract(self) -> str:
            raise AssertionError("foreign property was evaluated")

    with pytest.raises(primitives.StagedBundlePublicationIntentError):
        contract.validate_staged_bundle_publication_intent(Hostile())


def test_unexpected_builder_and_validator_errors_are_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = contract.build_staged_bundle_publication_intent(bundle_input())
    monkeypatch.setattr(contract, "_normalized_bundle", lambda _raw: 1 / 0)
    _assert_invalid(bundle_input())

    monkeypatch.setattr(contract, "_normalized_descriptor", lambda _descriptor: 1 / 0)
    with pytest.raises(primitives.StagedBundlePublicationIntentError):
        contract.validate_staged_bundle_publication_intent(intent)


def test_errors_and_reprs_do_not_echo_supplied_values() -> None:
    sensitive = "caller-supplied-sensitive-value"
    raw = bundle_input()
    raw["build_run_ref"] = sensitive
    with pytest.raises(primitives.StagedBundlePublicationIntentError) as captured:
        contract.build_staged_bundle_publication_intent(raw)
    assert sensitive not in str(captured.value)
    assert sensitive not in repr(captured.value)

    intent = contract.build_staged_bundle_publication_intent(bundle_input())
    hidden = (
        intent.build_run_ref,
        intent.generation_ref,
        intent.source_vector_sha256,
        intent.source_witnesses[0].source_release_ref,
        intent.relations[0].live_relation,
        intent.relations[0].stage_fingerprints.schema_sha256,
    )
    reprs = (
        repr(intent),
        repr(intent.relations[0]),
        repr(intent.source_witnesses[0]),
        repr(intent.relations[0].stage_fingerprints),
    )
    assert all(value not in rendered for value in hidden for rendered in reprs)
