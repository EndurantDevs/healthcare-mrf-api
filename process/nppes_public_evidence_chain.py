# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value-safe deterministic receipts for one selected NPPES release chain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re

from public_evidence.evidence_record_primitives import _canonical_sha256, _derived_ref
from process.nppes_public_evidence_archive import (
    _candidate_from_url,
    archive_error,
    select_nppes_release_chain,
)


_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_ADMISSION_REF_RE = re.compile(r"penpa1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_RELEASE_REF_RE = re.compile(r"perel1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_CHAIN_REF_RE = re.compile(r"penpc1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_CHAIN_CONTRACT = "healthporta.nppes-public-evidence-import-chain.v1"
_CHAIN_REF_PREFIX = "penpc1_"
_MAX_RECORD_COUNT = 2**53 - 1
_MAX_ARCHIVE_COUNT = 4096


@dataclass(frozen=True, slots=True, repr=False)
class NppesPublicEvidenceArchiveReceipt:
    """Value-safe outcome for one archive in the ordered import chain."""

    archive_name: str
    snapshot_at: str
    admission_ref: str
    source_release_ref: str
    artifact_sha256: str
    manifest_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    write_state: str

    def __repr__(self) -> str:
        return "<nppes-public-evidence-archive-receipt>"


@dataclass(frozen=True, slots=True, repr=False)
class NppesPublicEvidenceChainReceipt:
    """Complete ordered-chain receipt required before canonical NPI publish."""

    chain_ref: str
    contract: str
    listing_sha256: str
    listing_byte_count: int
    listing_candidate_names: tuple[str, ...]
    archives: tuple[NppesPublicEvidenceArchiveReceipt, ...]
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    contract_sha256: str

    def __repr__(self) -> str:
        return "<nppes-public-evidence-chain-receipt>"


def _chain_payload(
    listing_sha256: str,
    listing_byte_count: int,
    listing_candidate_names: tuple[str, ...],
    archives: tuple[NppesPublicEvidenceArchiveReceipt, ...],
) -> dict[str, object]:
    return {
        "contract": _CHAIN_CONTRACT,
        "listing_sha256": listing_sha256,
        "listing_byte_count": listing_byte_count,
        "listing_candidate_names": list(listing_candidate_names),
        "archives": [
            {
                "archive_name": archive.archive_name,
                "snapshot_at": archive.snapshot_at,
                "admission_ref": archive.admission_ref,
                "source_release_ref": archive.source_release_ref,
                "artifact_sha256": archive.artifact_sha256,
                "manifest_sha256": archive.manifest_sha256,
                "source_record_count": archive.source_record_count,
                "projected_record_count": archive.projected_record_count,
                "excluded_record_count": archive.excluded_record_count,
            }
            for archive in archives
        ],
    }


def _finished_chain_receipt(
    listing_sha256: str,
    listing_byte_count: int,
    listing_candidate_names: tuple[str, ...],
    archives: tuple[NppesPublicEvidenceArchiveReceipt, ...],
) -> NppesPublicEvidenceChainReceipt:
    if not archives:
        raise archive_error()
    chain_payload = _chain_payload(
        listing_sha256,
        listing_byte_count,
        listing_candidate_names,
        archives,
    )
    return NppesPublicEvidenceChainReceipt(
        chain_ref=_derived_ref(
            _CHAIN_REF_PREFIX,
            "nppes_public_evidence_import_chain",
            chain_payload,
        ),
        contract=_CHAIN_CONTRACT,
        listing_sha256=listing_sha256,
        listing_byte_count=listing_byte_count,
        listing_candidate_names=listing_candidate_names,
        archives=archives,
        source_record_count=sum(
            archive_receipt.source_record_count for archive_receipt in archives
        ),
        projected_record_count=sum(
            archive_receipt.projected_record_count for archive_receipt in archives
        ),
        excluded_record_count=sum(
            archive_receipt.excluded_record_count for archive_receipt in archives
        ),
        contract_sha256=_canonical_sha256(
            "nppes_public_evidence_import_chain",
            chain_payload,
        ),
    )


def _archive_snapshot(
    archive: NppesPublicEvidenceArchiveReceipt,
):
    if type(archive.archive_name) is not str:
        raise archive_error()
    archive_candidate = _candidate_from_url(
        f"https://download.cms.gov/nppes/{archive.archive_name}"
    )
    if archive_candidate is None or type(archive.snapshot_at) is not str:
        raise archive_error()
    try:
        snapshot = datetime.strptime(
            archive.snapshot_at,
            "%Y-%m-%dT%H:%M:%SZ",
        )
    except ValueError:
        raise archive_error() from None
    if archive.snapshot_at != snapshot.strftime("%Y-%m-%dT%H:%M:%SZ"):
        raise archive_error()
    snapshot_date = snapshot.date()
    if archive_candidate.archive_kind == "monthly":
        if (
            snapshot_date.year != archive_candidate.period_start.year
            or snapshot_date.month != archive_candidate.period_start.month
            or snapshot_date < archive_candidate.period_start
        ):
            raise archive_error()
    elif snapshot_date != archive_candidate.period_end:
        raise archive_error()
    return archive_candidate, snapshot_date


def _validated_archive_receipt(
    archive: object,
) -> tuple[NppesPublicEvidenceArchiveReceipt, object, object]:
    if type(archive) is not NppesPublicEvidenceArchiveReceipt:
        raise archive_error()
    archive_candidate, snapshot_date = _archive_snapshot(archive)
    if (
        type(archive.admission_ref) is not str
        or _ADMISSION_REF_RE.fullmatch(archive.admission_ref) is None
        or type(archive.source_release_ref) is not str
        or _RELEASE_REF_RE.fullmatch(archive.source_release_ref) is None
        or type(archive.write_state) is not str
        or archive.write_state not in {"inserted", "already_present"}
        or any(
            type(digest) is not str or _SHA256_RE.fullmatch(digest) is None
            for digest in (archive.artifact_sha256, archive.manifest_sha256)
        )
        or type(archive.source_record_count) is not int
        or not 1 <= archive.source_record_count <= _MAX_RECORD_COUNT
        or type(archive.projected_record_count) is not int
        or not 0 <= archive.projected_record_count <= _MAX_RECORD_COUNT
        or type(archive.excluded_record_count) is not int
        or not 0 <= archive.excluded_record_count <= _MAX_RECORD_COUNT
        or archive.source_record_count
        != archive.projected_record_count + archive.excluded_record_count
    ):
        raise archive_error()
    rebuilt = NppesPublicEvidenceArchiveReceipt(
        archive_name=archive.archive_name,
        snapshot_at=archive.snapshot_at,
        admission_ref=archive.admission_ref,
        source_release_ref=archive.source_release_ref,
        artifact_sha256=archive.artifact_sha256,
        manifest_sha256=archive.manifest_sha256,
        source_record_count=archive.source_record_count,
        projected_record_count=archive.projected_record_count,
        excluded_record_count=archive.excluded_record_count,
        write_state=archive.write_state,
    )
    return rebuilt, archive_candidate, snapshot_date


def _listing_candidates(candidate_names: object):
    if type(candidate_names) is not tuple or not candidate_names:
        raise archive_error()
    candidates = []
    for archive_name in candidate_names:
        if type(archive_name) is not str:
            raise archive_error()
        candidate = _candidate_from_url(
            f"https://download.cms.gov/nppes/{archive_name}"
        )
        if candidate is None:
            raise archive_error()
        candidates.append(candidate)
    ordered_candidates = tuple(
        sorted(
            candidates,
            key=lambda archive_candidate: (
                0 if archive_candidate.archive_kind == "monthly" else 1,
                archive_candidate.period_start,
                archive_candidate.period_end or archive_candidate.period_start,
                archive_candidate.archive_name,
            ),
        )
    )
    if (
        tuple(
            archive_candidate.archive_name
            for archive_candidate in ordered_candidates
        )
        != candidate_names
        or len(set(candidate_names)) != len(candidate_names)
    ):
        raise archive_error()
    return ordered_candidates


def _validated_chain_archives(
    archives: object,
    listing_candidates: tuple[object, ...],
) -> tuple[NppesPublicEvidenceArchiveReceipt, ...]:
    if (
        type(archives) is not tuple
        or not 1 <= len(archives) <= _MAX_ARCHIVE_COUNT
    ):
        raise archive_error()
    validated_archives = tuple(
        _validated_archive_receipt(archive_receipt)
        for archive_receipt in archives
    )
    rebuilt_archives = tuple(
        validated_archive[0] for validated_archive in validated_archives
    )
    archive_candidates = tuple(
        validated_archive[1] for validated_archive in validated_archives
    )
    expected_candidates = select_nppes_release_chain(
        listing_candidates,
        validated_archives[0][2],
    )
    if archive_candidates != expected_candidates:
        raise archive_error()
    unique_vectors = (
        tuple(archive.admission_ref for archive in rebuilt_archives),
        tuple(archive.source_release_ref for archive in rebuilt_archives),
        tuple(archive.artifact_sha256 for archive in rebuilt_archives),
        tuple(archive.manifest_sha256 for archive in rebuilt_archives),
    )
    if any(len(set(vector)) != len(vector) for vector in unique_vectors):
        raise archive_error()
    return rebuilt_archives


def validate_nppes_public_evidence_chain_receipt(
    candidate: object,
) -> NppesPublicEvidenceChainReceipt:
    """Rebuild an exact complete-chain receipt with no paths or source values."""

    try:
        if (
            type(candidate) is not NppesPublicEvidenceChainReceipt
            or type(candidate.chain_ref) is not str
            or _CHAIN_REF_RE.fullmatch(candidate.chain_ref) is None
            or type(candidate.contract) is not str
            or candidate.contract != _CHAIN_CONTRACT
            or type(candidate.listing_sha256) is not str
            or _SHA256_RE.fullmatch(candidate.listing_sha256) is None
            or type(candidate.listing_byte_count) is not int
            or not 1 <= candidate.listing_byte_count <= 4 * 1024 * 1024
            or type(candidate.source_record_count) is not int
            or not 1 <= candidate.source_record_count <= _MAX_RECORD_COUNT
            or type(candidate.projected_record_count) is not int
            or not 0 <= candidate.projected_record_count <= _MAX_RECORD_COUNT
            or type(candidate.excluded_record_count) is not int
            or not 0 <= candidate.excluded_record_count <= _MAX_RECORD_COUNT
            or candidate.source_record_count
            != candidate.projected_record_count + candidate.excluded_record_count
            or type(candidate.contract_sha256) is not str
            or _SHA256_RE.fullmatch(candidate.contract_sha256) is None
        ):
            raise archive_error()
        listing_candidates = _listing_candidates(candidate.listing_candidate_names)
        archives = _validated_chain_archives(
            candidate.archives,
            listing_candidates,
        )
        rebuilt = _finished_chain_receipt(
            candidate.listing_sha256,
            candidate.listing_byte_count,
            candidate.listing_candidate_names,
            archives,
        )
        if candidate != rebuilt:
            raise archive_error()
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


__all__ = (
    "NppesPublicEvidenceArchiveReceipt",
    "NppesPublicEvidenceChainReceipt",
    "validate_nppes_public_evidence_chain_receipt",
)
