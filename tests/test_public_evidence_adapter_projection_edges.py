# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary coverage for inventory, inclusion, and projection contracts."""

from __future__ import annotations

from types import MappingProxyType
from types import SimpleNamespace

import pytest

from public_evidence import adapter_projection_contract as projection
from public_evidence import adapter_projection_policies as policies
from public_evidence import evidence_record_contract as record
from public_evidence import source_record_inclusion_contract as inclusion
from public_evidence import source_record_inclusion_primitives as primitive
from tests.public_evidence_adapter_projection_support import (
    inclusion_witnesses_for_record,
    inventory_namespace,
    multi_member_inventory,
    valid_projection,
)
from tests.public_evidence_record_support import (
    network_input,
    source_record,
    source_release,
)


class DictSubclass(dict[str, object]):
    pass


class TupleSubclass(tuple):
    pass


@pytest.mark.parametrize(
    "candidate",
    (None, False, 0, 2**53, "1", 1.0),
)
def test_member_count_requires_positive_json_safe_exact_integer(
    candidate: object,
) -> None:
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        primitive._strict_positive_count(candidate)


@pytest.mark.parametrize("candidate", (None, False, -1, 1, "0"))
def test_member_ordinal_requires_in_range_exact_integer(candidate: object) -> None:
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        primitive._strict_ordinal(candidate, 1)


def test_audit_path_bounds_and_exact_tuple_precede_member_traversal() -> None:
    for candidate in (
        [],
        TupleSubclass(),
        ("0" * 64,) * 54,
        ("not-a-digest",),
    ):
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            primitive._bounded_audit_path(candidate)


def test_low_level_canonical_and_shape_guards_fail_uniformly() -> None:
    for callback, candidate in (
        (primitive._strict_sha256, "A" * 64),
        (primitive._strict_protocol, "bad-contract"),
        (primitive._strict_kind, "Bad Kind"),
        (primitive._canonical_json, object()),
    ):
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            callback(candidate)
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        primitive._exact_dict({1: "value"}, frozenset({"field"}))
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        primitive._exact_dict({"other": "value"}, frozenset({"field"}))
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        primitive.derive_inventory_node_sha256("0" * 64, object())


def test_inventory_input_must_be_exact_plain_dict() -> None:
    source = source_release("tic")
    source_reference = source_record(source, "tic_provider_group_occurrence")
    raw = DictSubclass(
        {
            **inventory_namespace(source_reference, 1),
            "member_root_sha256": "0" * 64,
        }
    )
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.build_source_record_inventory_descriptor(source, raw)


def test_leaf_rejects_wrong_release_kind_identity_and_ordinal() -> None:
    source = source_release("tic")
    source_reference = source_record(source, "tic_provider_group_occurrence")
    namespace = inventory_namespace(source_reference, 1)
    other_source = source_release("public_hpt")
    other_record = source_record(other_source, "hpt_hospital_record")

    for release_value, record_value, ordinal in (
        (source, other_record, 0),
        (other_source, source_reference, 0),
        (source, source_reference, 1),
    ):
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            inclusion.derive_inventory_leaf_sha256(
                release_value, namespace, record_value, ordinal
            )


def test_inventory_and_witness_validators_require_exact_public_types() -> None:
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.validate_source_record_inventory_descriptor(object())
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.validate_source_record_inclusion_witness(object())

    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion._validated_release(object())


def test_projection_requires_bounded_exact_tuple_before_traversal() -> None:
    normalized = valid_projection()
    for candidate in (
        (),
        TupleSubclass(normalized.inclusion_witnesses),
        (object(),),
        (object(),) * 17,
    ):
        with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
            projection.build_public_evidence_adapter_projection(
                normalized.record, candidate
            )

    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.build_public_evidence_adapter_projection(object(), ())


def test_projection_rejects_unknown_or_reclassified_closed_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = valid_projection()
    witnesses = normalized.inclusion_witnesses
    monkeypatch.setattr(projection, "ADAPTER_PROJECTION_RULES", MappingProxyType({}))
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.build_public_evidence_adapter_projection(
            normalized.record, witnesses
        )

    rule = next(iter(policies.ADAPTER_PROJECTION_RULES.values()))
    wrong_source_rule = rule._replace(source_kind="public_hpt")
    monkeypatch.setattr(
        projection,
        "ADAPTER_PROJECTION_RULES",
        MappingProxyType(
            {
                (
                    normalized.record.record_type,
                    normalized.record.evidence.relationship_class,
                ): wrong_source_rule
            }
        ),
    )
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.build_public_evidence_adapter_projection(
            normalized.record, witnesses
        )


def test_projection_rejects_wrong_expected_kind_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = valid_projection()
    key = (
        normalized.record.record_type,
        normalized.record.evidence.relationship_class,
    )
    rule = policies.ADAPTER_PROJECTION_RULES[key]
    monkeypatch.setattr(
        projection,
        "ADAPTER_PROJECTION_RULES",
        MappingProxyType(
            {key: rule._replace(source_record_kinds=("hpt_hospital_record",))}
        ),
    )
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.build_public_evidence_adapter_projection(
            normalized.record, normalized.inclusion_witnesses
        )


def test_internal_rule_and_release_fences_fail_closed() -> None:
    normalized = valid_projection()
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection._record_relationship(
            SimpleNamespace(evidence=SimpleNamespace(relationship_class=1))
        )

    fake_record = SimpleNamespace(
        record_type=normalized.record.record_type,
        evidence=normalized.record.evidence,
        release=SimpleNamespace(source_kind="tic", source_binding=None),
        source_records=normalized.record.source_records,
    )
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection._projection_rule(fake_record)

    rule = projection._projection_rule(normalized.record)
    witness = normalized.inclusion_witnesses[0]
    hostile_inventory = witness.inventory._replace(source_kind="public_hpt")
    hostile_witness = witness._replace(inventory=hostile_inventory)
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection._validate_projection_inputs(
            normalized.record, (hostile_witness,), rule
        )


def test_projection_validator_rejects_wrong_type_and_noncanonical_order() -> None:
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.validate_public_evidence_adapter_projection(object())

    source = source_release("public_provider_directory_fhir")
    raw = network_input(source)
    normalized_record = record.build_public_evidence_record(source, raw)
    witnesses = inclusion_witnesses_for_record(normalized_record)
    normalized = projection.build_public_evidence_adapter_projection(
        normalized_record, witnesses
    )
    hostile = normalized._replace(
        inclusion_witnesses=tuple(reversed(normalized.inclusion_witnesses))
    )
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.validate_public_evidence_adapter_projection(hostile)


def test_unexpected_builder_failures_are_wrapped_without_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = valid_projection()

    def explode(_candidate: object) -> None:
        raise ZeroDivisionError("sensitive-input")

    monkeypatch.setattr(projection, "validate_public_evidence_record", explode)
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError) as exc_info:
        projection.build_public_evidence_adapter_projection(
            normalized.record, normalized.inclusion_witnesses
        )
    assert str(exc_info.value) == "public_evidence_adapter_projection_invalid"
    assert "sensitive-input" not in str(exc_info.value)


def test_inclusion_builder_wraps_record_validation_without_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _source, _records, inventory, witnesses = multi_member_inventory()

    def explode(*_arguments: object) -> None:
        raise ZeroDivisionError("sensitive-input")

    monkeypatch.setattr(inclusion, "validate_evidence_source_record_reference", explode)
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError) as exc_info:
        inclusion.build_source_record_inclusion_witness(
            inventory,
            witnesses[0].source_record,
            witnesses[0].member_ordinal,
            witnesses[0].audit_path_sha256s,
        )
    assert str(exc_info.value) == "public_evidence_source_record_inclusion_invalid"


def test_unexpected_inventory_and_revalidation_failures_are_uniform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_release_descriptor, _records, inventory, witnesses = multi_member_inventory()
    namespace = inventory_namespace(witnesses[0].source_record, inventory.member_count)

    def explode(*_arguments: object, **_keywords: object) -> None:
        raise ZeroDivisionError("sensitive-input")

    with monkeypatch.context() as context:
        context.setattr(inclusion, "_inventory_components", explode)
        for callback in (
            lambda: inclusion.derive_inventory_leaf_sha256(
                source_release_descriptor, namespace, witnesses[0].source_record, 0
            ),
            lambda: inclusion.build_source_record_inventory_descriptor(
                source_release_descriptor,
                {**namespace, "member_root_sha256": "0" * 64},
            ),
        ):
            with pytest.raises(
                primitive.PublicEvidenceSourceRecordInclusionError
            ) as exc_info:
                callback()
            assert "sensitive-input" not in str(exc_info.value)

    with monkeypatch.context() as context:
        context.setattr(inclusion, "build_source_record_inventory_descriptor", explode)
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            inclusion.validate_source_record_inventory_descriptor(inventory)

    with monkeypatch.context() as context:
        context.setattr(inclusion, "build_source_record_inclusion_witness", explode)
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            inclusion.validate_source_record_inclusion_witness(witnesses[0])


def test_adapter_rule_descriptor_rejects_foreign_type() -> None:
    with pytest.raises(TypeError, match="adapter_projection_rule_invalid"):
        policies.adapter_projection_rule_descriptor_sha256(object())
