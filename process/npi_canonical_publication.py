# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable canonical-NPI publication receipts for sealed NPPES chains."""

from __future__ import annotations

import datetime as dt
import re
from typing import NamedTuple

from public_evidence.evidence_record_primitives import (
    _canonical_sha256,
    _derived_ref,
)


NPI_CANONICAL_PUBLICATION_CONTRACT = (
    "healthporta.npi-canonical-publication.v1"
)
NPI_CANONICAL_PUBLICATION_TABLE = "npi_canonical_publication_receipt"
NPI_CANONICAL_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
)
_PURPOSE = "npi_canonical_publication"
_REF_PREFIX = "nppub1_"
_MAX_EXACT_COUNT = 2**53 - 1
_CHAIN_REF_RE = re.compile(r"penpc1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_ATTEMPT_SUFFIX_RE = re.compile(r"[0-9a-f]{32}", flags=re.ASCII)


class NpiCanonicalPublicationError(RuntimeError):
    """One value-free canonical publication contract failure."""


class NpiCanonicalPublicationReceipt(NamedTuple):
    publication_generation: int
    publication_ref: str
    contract: str
    contract_sha256: str
    run_id: str
    attempt_id: str
    attempt_started_at: str
    chain_ref: str
    import_date: str
    relation_oids: tuple[int, ...]
    row_counts: tuple[int, ...]
    publication_state: str
    evidence_serving_authority: str
    evidence_publication_enabled: bool
    created_at: str

    def __repr__(self) -> str:
        return (
            "<npi-canonical-publication-receipt "
            f"generation={self.publication_generation}>"
        )


class NpiCanonicalPublicationInput(NamedTuple):
    run_id: str
    attempt_id: str
    attempt_started_at: str
    chain_ref: str
    import_date: str
    relation_oids: tuple[int, ...]
    row_counts: tuple[int, ...]

    def __repr__(self) -> str:
        return "<npi-canonical-publication-input>"


def publication_error() -> NpiCanonicalPublicationError:
    """Return the one context-free publication error."""

    return NpiCanonicalPublicationError("npi_canonical_publication_invalid")


def _strict_text(value: object, *, maximum: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) > 0x7E for character in value)
    ):
        raise publication_error()
    return value


def _strict_attempt_started_at(value: object) -> str:
    fixed = _strict_text(value, maximum=40)
    try:
        parsed = dt.datetime.fromisoformat(fixed)
    except ValueError:
        raise publication_error() from None
    if parsed.tzinfo != dt.UTC or parsed.isoformat(timespec="microseconds") != fixed:
        raise publication_error()
    return fixed


def _strict_import_date(value: object) -> str:
    fixed = _strict_text(value, maximum=10)
    try:
        parsed = dt.datetime.strptime(fixed, "%Y-%m-%d").date()
    except ValueError:
        raise publication_error() from None
    if parsed.isoformat() != fixed:
        raise publication_error()
    return fixed


def _strict_int_vector(value: object, *, is_oid: bool) -> tuple[int, ...]:
    if type(value) is not tuple or len(value) != len(NPI_CANONICAL_TABLES):
        raise publication_error()
    fixed_values: list[int] = []
    for item in value:
        if type(item) is not int:
            raise publication_error()
        upper_bound = 2**32 - 1 if is_oid else _MAX_EXACT_COUNT
        lower_bound = 1 if is_oid else 0
        if not lower_bound <= item <= upper_bound:
            raise publication_error()
        fixed_values.append(item)
    return tuple(fixed_values)


def _publication_payload(
    run_id: str,
    attempt_id: str,
    attempt_started_at: str,
    chain_ref: str,
    import_date: str,
    relation_oids: tuple[int, ...],
    row_counts: tuple[int, ...],
) -> dict[str, object]:
    return {
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
        "chain_ref": chain_ref,
        "contract": NPI_CANONICAL_PUBLICATION_CONTRACT,
        "evidence_publication_enabled": False,
        "evidence_serving_authority": "none",
        "import_date": import_date,
        "publication_state": "canonical_api_published",
        "relation_oids": dict(zip(NPI_CANONICAL_TABLES, relation_oids, strict=True)),
        "row_counts": dict(zip(NPI_CANONICAL_TABLES, row_counts, strict=True)),
        "run_id": run_id,
    }


def _validated_publication_input(value: object) -> NpiCanonicalPublicationInput:
    if type(value) is not NpiCanonicalPublicationInput:
        raise publication_error()
    fixed_run_id = _strict_text(value.run_id, maximum=64)
    fixed_attempt_id = _strict_text(value.attempt_id, maximum=97)
    attempt_parts = fixed_attempt_id.rsplit(":", 1)
    if (
        len(attempt_parts) != 2
        or attempt_parts[0] != fixed_run_id
        or _ATTEMPT_SUFFIX_RE.fullmatch(attempt_parts[1]) is None
    ):
        raise publication_error()
    fixed_attempt_started_at = _strict_attempt_started_at(value.attempt_started_at)
    fixed_chain_ref = _strict_text(value.chain_ref, maximum=50)
    if _CHAIN_REF_RE.fullmatch(fixed_chain_ref) is None:
        raise publication_error()
    fixed_import_date = _strict_import_date(value.import_date)
    fixed_oids = _strict_int_vector(value.relation_oids, is_oid=True)
    fixed_counts = _strict_int_vector(value.row_counts, is_oid=False)
    return NpiCanonicalPublicationInput(
        fixed_run_id,
        fixed_attempt_id,
        fixed_attempt_started_at,
        fixed_chain_ref,
        fixed_import_date,
        fixed_oids,
        fixed_counts,
    )


def build_npi_canonical_publication_receipt(
    publication_input: object,
    *,
    publication_generation: object,
    created_at: object,
) -> NpiCanonicalPublicationReceipt:
    """Build one exact immutable receipt from a completed table rotation."""

    if type(publication_generation) is not int or publication_generation < 1:
        raise publication_error()
    fixed_input = _validated_publication_input(publication_input)
    fixed_created_at = _strict_attempt_started_at(created_at)
    publication_payload = _publication_payload(
        *fixed_input,
    )
    contract_sha256 = _canonical_sha256(_PURPOSE, publication_payload)
    publication_ref = _derived_ref(_REF_PREFIX, _PURPOSE, publication_payload)
    return NpiCanonicalPublicationReceipt(
        publication_generation,
        publication_ref,
        NPI_CANONICAL_PUBLICATION_CONTRACT,
        contract_sha256,
        *fixed_input,
        "canonical_api_published",
        "none",
        False,
        fixed_created_at,
    )


def receipt_insert_values(
    receipt: object,
) -> tuple[object, ...]:
    """Encode one validated receipt in exact PostgreSQL column order."""

    fixed = validate_npi_canonical_publication_receipt(receipt)
    return (
        fixed.publication_ref,
        fixed.contract,
        bytes.fromhex(fixed.contract_sha256),
        fixed.run_id,
        fixed.attempt_id,
        dt.datetime.fromisoformat(fixed.attempt_started_at),
        fixed.chain_ref,
        dt.date.fromisoformat(fixed.import_date),
        *fixed.relation_oids,
        *fixed.row_counts,
        fixed.publication_state,
        fixed.evidence_serving_authority,
        fixed.evidence_publication_enabled,
    )


def validate_npi_canonical_publication_receipt(
    value: object,
) -> NpiCanonicalPublicationReceipt:
    """Rebuild and constant-shape compare one publication receipt."""

    if type(value) is not NpiCanonicalPublicationReceipt:
        raise publication_error()
    rebuilt = build_npi_canonical_publication_receipt(
        NpiCanonicalPublicationInput(
            value.run_id,
            value.attempt_id,
            value.attempt_started_at,
            value.chain_ref,
            value.import_date,
            value.relation_oids,
            value.row_counts,
        ),
        publication_generation=value.publication_generation,
        created_at=value.created_at,
    )
    if rebuilt != value:
        raise publication_error()
    return rebuilt


def receipt_metrics(receipt: object) -> dict[str, object]:
    """Return the bounded public control metrics for one receipt."""

    fixed = validate_npi_canonical_publication_receipt(receipt)
    return {
        "publication_generation": fixed.publication_generation,
        "publication_ref": fixed.publication_ref,
        "chain_ref": fixed.chain_ref,
        "row_counts": dict(
            zip(NPI_CANONICAL_TABLES, fixed.row_counts, strict=True)
        ),
    }


__all__ = (
    "NPI_CANONICAL_PUBLICATION_CONTRACT",
    "NPI_CANONICAL_PUBLICATION_TABLE",
    "NPI_CANONICAL_TABLES",
    "NpiCanonicalPublicationError",
    "NpiCanonicalPublicationInput",
    "NpiCanonicalPublicationReceipt",
    "build_npi_canonical_publication_receipt",
    "publication_error",
    "receipt_insert_values",
    "receipt_metrics",
    "validate_npi_canonical_publication_receipt",
)
