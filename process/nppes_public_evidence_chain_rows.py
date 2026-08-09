# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact database rows for one durable NPPES listing-chain admission."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, NamedTuple

from public_evidence.evidence_record_primitives import _canonical_sha256
from process.nppes_public_evidence_archive import archive_error
from process.nppes_public_evidence_chain import (
    NppesPublicEvidenceChainReceipt,
    validate_nppes_public_evidence_chain_receipt,
)


class NppesChainAdmissionRow(NamedTuple):
    """One immutable, publication-disabled listing-chain admission."""

    chain_ref: str
    contract: str
    contract_sha256: str
    listing_sha256: str
    listing_byte_count: int
    listing_candidate_names: tuple[str, ...]
    archive_count: int
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    admission_state: Literal["verified_complete_disabled"]
    serving_authority: Literal["none"]
    publication_enabled: Literal[False]
    row_sha256: str

    def __repr__(self) -> str:
        return "<nppes-chain-admission-row>"

    __str__ = __repr__


class NppesChainArchiveRow(NamedTuple):
    """One selected archive occurrence in a durable listing-chain admission."""

    chain_ref: str
    archive_ordinal: int
    archive_count: int
    archive_name: str
    snapshot_at: str
    admission_ref: str
    source_release_ref: str
    artifact_sha256: str
    manifest_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    row_sha256: str

    def __repr__(self) -> str:
        return "<nppes-chain-archive-row>"

    __str__ = __repr__


CHAIN_ADMISSION_COLUMNS = tuple(NppesChainAdmissionRow._fields)
CHAIN_ARCHIVE_COLUMNS = tuple(NppesChainArchiveRow._fields)
_DIGEST_FIELDS = frozenset(
    {
        "artifact_sha256",
        "contract_sha256",
        "listing_sha256",
        "manifest_sha256",
        "row_sha256",
    }
)


def _finished_row(row: object, purpose: str):
    payload = dict(row._asdict())
    payload.pop("row_sha256")
    return row._replace(
        row_sha256=_canonical_sha256(purpose, payload),
    )


def build_nppes_chain_storage_rows(
    receipt: object,
) -> tuple[NppesChainAdmissionRow, tuple[NppesChainArchiveRow, ...]]:
    """Build the exact parent and ordered child rows for one chain receipt."""

    try:
        fixed = validate_nppes_public_evidence_chain_receipt(receipt)
        archive_count = len(fixed.archives)
        parent = _finished_row(
            NppesChainAdmissionRow(
                chain_ref=fixed.chain_ref,
                contract=fixed.contract,
                contract_sha256=fixed.contract_sha256,
                listing_sha256=fixed.listing_sha256,
                listing_byte_count=fixed.listing_byte_count,
                listing_candidate_names=fixed.listing_candidate_names,
                archive_count=archive_count,
                source_record_count=fixed.source_record_count,
                projected_record_count=fixed.projected_record_count,
                excluded_record_count=fixed.excluded_record_count,
                admission_state="verified_complete_disabled",
                serving_authority="none",
                publication_enabled=False,
                row_sha256="",
            ),
            "nppes_chain_admission_row",
        )
        child_rows = tuple(
            _finished_row(
                NppesChainArchiveRow(
                    chain_ref=fixed.chain_ref,
                    archive_ordinal=archive_ordinal,
                    archive_count=archive_count,
                    archive_name=archive.archive_name,
                    snapshot_at=archive.snapshot_at,
                    admission_ref=archive.admission_ref,
                    source_release_ref=archive.source_release_ref,
                    artifact_sha256=archive.artifact_sha256,
                    manifest_sha256=archive.manifest_sha256,
                    source_record_count=archive.source_record_count,
                    projected_record_count=archive.projected_record_count,
                    excluded_record_count=archive.excluded_record_count,
                    row_sha256="",
                ),
                "nppes_chain_archive_row",
            )
            for archive_ordinal, archive in enumerate(fixed.archives)
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return parent, child_rows
    raise normalized_error


def _database_value(field_name: str, value: object) -> object:
    if field_name in _DIGEST_FIELDS:
        return bytes.fromhex(value)
    if field_name == "snapshot_at":
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    if field_name == "listing_candidate_names":
        return list(value)
    return value


def chain_admission_values(row: NppesChainAdmissionRow) -> tuple[object, ...]:
    """Encode the exact chain parent row for asyncpg."""

    return tuple(
        _database_value(field_name, getattr(row, field_name))
        for field_name in CHAIN_ADMISSION_COLUMNS
    )


def chain_archive_values(row: NppesChainArchiveRow) -> tuple[object, ...]:
    """Encode one exact selected-archive child row for asyncpg."""

    return tuple(
        _database_value(field_name, getattr(row, field_name))
        for field_name in CHAIN_ARCHIVE_COLUMNS
    )


__all__ = (
    "CHAIN_ADMISSION_COLUMNS",
    "CHAIN_ARCHIVE_COLUMNS",
    "NppesChainAdmissionRow",
    "NppesChainArchiveRow",
    "build_nppes_chain_storage_rows",
    "chain_admission_values",
    "chain_archive_values",
)
