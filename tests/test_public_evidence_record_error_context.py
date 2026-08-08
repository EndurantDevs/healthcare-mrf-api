# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private exception boundaries for public evidence record entrypoints."""

from __future__ import annotations

import pytest

from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_primitives as primitive
from tests.public_evidence_record_support import (
    enumeration_input,
    relationship_input,
    source_release,
)


def _valid_record() -> record.PublicEvidenceRecord:
    source = source_release("tic")
    return record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )


def _assert_context_free(error: BaseException) -> None:
    assert type(error) is primitive.PublicEvidenceRecordError
    assert str(error) == "public_evidence_record_invalid"
    assert error.__cause__ is None
    assert error.__context__ is None


def _raise_nested_public_error(*_args: object) -> None:
    try:
        raise RuntimeError("private-source-value")
    except RuntimeError:
        raise primitive.PublicEvidenceRecordError(
            "public_evidence_record_invalid"
        ) from None


def test_builder_clears_unexpected_normalizer_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_release("nppes_entity_address")

    def explode(*_args: object) -> None:
        raise ZeroDivisionError("private-normalizer-value")

    normalizer_by_record_type = dict(record._NORMALIZERS)
    normalizer_by_record_type["npi_enumeration"] = explode
    monkeypatch.setattr(record, "_NORMALIZERS", normalizer_by_record_type)

    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.build_public_evidence_record(source, enumeration_input(source))

    _assert_context_free(exc_info.value)


def test_builder_clears_nested_validation_context() -> None:
    source = source_release("nppes_entity_address")
    malformed_input = enumeration_input(source)
    malformed_input["observed_at"] = "2026-99-01T12:00:00Z"

    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.build_public_evidence_record(source, malformed_input)

    _assert_context_free(exc_info.value)


def test_revalidation_clears_unexpected_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = _valid_record()

    def explode(*_args: object) -> None:
        raise ZeroDivisionError("private-revalidation-value")

    monkeypatch.setattr(record, "build_public_evidence_record", explode)

    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.validate_public_evidence_record(normalized)

    _assert_context_free(exc_info.value)


def test_revalidation_clears_nested_public_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = _valid_record()
    monkeypatch.setattr(
        record,
        "build_public_evidence_record",
        _raise_nested_public_error,
    )

    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.validate_public_evidence_record(normalized)

    _assert_context_free(exc_info.value)
