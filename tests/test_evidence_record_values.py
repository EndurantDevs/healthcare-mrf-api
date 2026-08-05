from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from process import evidence_record_contract as evidence
from process import evidence_record_batch as batches
from process import evidence_record_values as values

NPI = "1234567893"
ADDRESS_KEY = "ak1_00000000-0000-5000-8000-000000000001"
PREMISE_KEY = "pk1_00000000-0000-5000-8000-000000000002"


def _digest(prefix: str, character: str, *, short: bool = False) -> str:
    return prefix + character * (32 if short else 64)


def _tax_input() -> dict[str, object]:
    return {
        "identity_type": "ein",
        "token_policy_ref": _digest("tip1_", "a"),
        "token_policy_version": 1,
        "locator": _digest("til1_", "b", short=True),
        "full_hmac": _digest("tih1_", "b"),
    }


def _source_record() -> values.EvidenceSourceRecordReference:
    return values.EvidenceSourceRecordReference(
        _digest("esr1_", "d"), _digest("esp1_", "e")
    )


def _witness() -> values.OrganizationNpiWitness:
    return values.OrganizationNpiWitness(_digest("src1_", "f"), _source_record(), NPI)


def test_accepts_frozen_redacted_domain_specific_value_objects() -> None:
    tax_identity = values.OpaqueTaxIdentityReference(**_tax_input())
    source_record = _source_record()
    address = values.CanonicalAddressEvidence(
        ADDRESS_KEY, PREMISE_KEY, "nppes_practice_location"
    )
    witness = _witness()

    assert tax_identity.token_policy_version == 1
    assert not hasattr(tax_identity, "key_version")
    assert address.premise_key == PREMISE_KEY
    assert witness.source_record == source_record
    assert witness.semantic_type == "organization_type_2"
    assert witness.source_semantics == "hpt_same_record_organization_npi"
    sensitive_values = (
        tax_identity.full_hmac,
        source_record.record_hmac,
        address.address_key,
        witness.release_contract_ref,
    )
    assert all(
        value not in repr(owner)
        for value, owner in zip(
            sensitive_values, (tax_identity, source_record, address, witness)
        )
    )
    with pytest.raises(FrozenInstanceError):
        setattr(tax_identity, "token_policy_version", 2)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("identity_type", "ssn"),
        ("identity_type", True),
        ("token_policy_ref", "123456789"),
        ("token_policy_ref", _digest("tip1_", "A")),
        ("token_policy_version", True),
        ("token_policy_version", 0),
        ("token_policy_version", 2),
        ("locator", "1234567890"),
        ("locator", _digest("til1_", "a")),
        ("locator", _digest("til1_", "A", short=True)),
        ("full_hmac", "123456789"),
        ("full_hmac", _digest("other1_", "b")),
        ("full_hmac", _digest("tih1_", "c")),
    ],
)
def test_rejects_ambiguous_or_invalid_tax_identity_references(
    field_name: str, invalid_value: object
) -> None:
    raw = _tax_input()
    raw[field_name] = invalid_value

    with pytest.raises(
        values.PublicEvidenceRecordError,
        match="^public_evidence_record_invalid$",
    ):
        values.OpaqueTaxIdentityReference(**raw)


@pytest.mark.parametrize(
    ("record_hmac", "payload_digest"),
    [
        ("123456789", _digest("esp1_", "e")),
        (_digest("esr1_", "d"), "https://public.invalid/record"),
        (_digest("esr1_", "D"), _digest("esp1_", "e")),
        (_digest("esr1_", "d"), _digest("esp1_", "e")[:-1]),
    ],
)
def test_rejects_nonopaque_source_record_references(
    record_hmac: str, payload_digest: str
) -> None:
    with pytest.raises(values.PublicEvidenceRecordError):
        values.EvidenceSourceRecordReference(record_hmac, payload_digest)


@pytest.mark.parametrize(
    ("address_key", "premise_key", "purpose"),
    [
        ("00000000-0000-5000-8000-000000000001", None, "nppes_mailing"),
        ("ak1_not-a-uuid", None, "nppes_mailing"),
        ("ak1_00000000000050008000000000000001", None, "nppes_mailing"),
        ("ak1_00000000-0000-5000-8000-00000000000A", None, "nppes_mailing"),
        (ADDRESS_KEY, "123456789", "nppes_mailing"),
        (ADDRESS_KEY, "pk1_not-a-uuid", "nppes_mailing"),
        (ADDRESS_KEY, None, "exact_rate_site"),
        (ADDRESS_KEY, None, True),
    ],
)
def test_rejects_noncanonical_or_overclaiming_addresses(
    address_key: str, premise_key: str | None, purpose: object
) -> None:
    with pytest.raises(values.PublicEvidenceRecordError):
        values.CanonicalAddressEvidence(address_key, premise_key, purpose)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("release_contract_ref", "1234567890"),
        ("release_contract_ref", _digest("src1_", "F")),
        ("source_record", object()),
        ("organization_npi", "1234567890"),
        ("organization_npi", 1234567893),
    ],
)
def test_rejects_invalid_same_record_organization_witnesses(
    field_name: str, invalid_value: object
) -> None:
    witness_by_field = {
        "release_contract_ref": _digest("src1_", "f"),
        "source_record": _source_record(),
        "organization_npi": NPI,
    }
    witness_by_field[field_name] = invalid_value

    with pytest.raises(values.PublicEvidenceRecordError):
        values.OrganizationNpiWitness(**witness_by_field)


def test_witness_revalidation_rejects_fixed_state_or_nested_tampering() -> None:
    witness = _witness()
    rebuilt = values._validated_organization_witness(witness)
    assert rebuilt == witness
    assert rebuilt is not witness

    class EqualitySpoof:
        def __eq__(self, _other: object) -> bool:
            return True

    for field_name, invalid in (
        ("semantic_type", "individual_type_1"),
        ("semantic_type", EqualitySpoof()),
        ("source_semantics", 1),
        ("source_semantics", EqualitySpoof()),
    ):
        witness = _witness()
        object.__setattr__(witness, field_name, invalid)
        with pytest.raises(values.PublicEvidenceRecordError):
            values._validated_organization_witness(witness)


def test_internal_detachment_and_release_errors_are_value_free() -> None:
    address = values.CanonicalAddressEvidence(
        ADDRESS_KEY, None, "nppes_practice_location"
    )
    object.__setattr__(address, "purpose", "legal_ownership")
    with pytest.raises(values.PublicEvidenceRecordError):
        values._detached_typed(address, values.CanonicalAddressEvidence)

    source_record = _source_record()
    object.__delattr__(source_record, "payload_digest")
    with pytest.raises(values.PublicEvidenceRecordError):
        values._detached_typed(source_record, values.EvidenceSourceRecordReference)
    with pytest.raises(values.PublicEvidenceRecordError):
        values._validated_release(object())


def test_witness_revalidation_wraps_expected_and_unexpected_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    witness = _witness()

    def raise_contract_error(*_args: object) -> None:
        raise values.PublicEvidenceRecordError("public_evidence_record_invalid")

    monkeypatch.setattr(values, "_detached_typed", raise_contract_error)
    with pytest.raises(values.PublicEvidenceRecordError):
        values._validated_organization_witness(witness)
    monkeypatch.setattr(values, "_detached_typed", lambda *_args: 1 / 0)
    with pytest.raises(values.PublicEvidenceRecordError):
        values._validated_organization_witness(witness)


def test_batch_record_validation_rejects_shape_release_order_and_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(values.PublicEvidenceRecordError):
        batches._validated_batch_records(object(), [])

    release = SimpleNamespace(contract_sha256="a" * 64)
    other_release = SimpleNamespace(contract_sha256="b" * 64)
    first = SimpleNamespace(release=release, evidence_id=_digest("ev1_", "a"))
    second = SimpleNamespace(release=release, evidence_id=_digest("ev1_", "b"))
    mismatched = SimpleNamespace(
        release=other_release, evidence_id=_digest("ev1_", "c")
    )
    monkeypatch.setattr(
        evidence, "validate_public_evidence_record", lambda value: value
    )
    for records in ((mismatched,), (second, first), (first, first)):
        with pytest.raises(values.PublicEvidenceRecordError):
            batches._validated_batch_records(release, records)


def test_batch_digest_and_builder_wrap_invalid_or_unexpected_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = SimpleNamespace(contract_sha256="a" * 64)
    monkeypatch.setattr(batches, "_validated_release", lambda _value: release)
    monkeypatch.setattr(batches, "_validated_batch_records", lambda *_args: ())
    monkeypatch.setattr(batches, "_batch_digest", lambda *_args: _digest("evb1_", "a"))
    with pytest.raises(values.PublicEvidenceRecordError):
        evidence.PublicEvidenceBatch(release, (), 0, _digest("evb1_", "b"))

    monkeypatch.setattr(batches, "_batch_digest", lambda *_args: 1 / 0)
    with pytest.raises(values.PublicEvidenceRecordError):
        evidence.build_public_evidence_batch(release, ())
