# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sealed listing and archive vector for one NPPES release chain."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import stat

from process.nppes_public_evidence_acquisition import (
    NppesListingSnapshot,
    validate_nppes_listing_snapshot,
)
from process.nppes_public_evidence_archive import (
    PreparedNppesArchive,
    archive_error,
    select_nppes_release_chain,
    validate_prepared_nppes_archive,
)


_PREPARED_CHAIN_SEAL = object()


@dataclass(frozen=True, slots=True, repr=False, init=False)
class PreparedNppesReleaseChain:
    """One sealed listing plus its exact selected retained archive vector."""

    listing: NppesListingSnapshot
    archives: tuple[PreparedNppesArchive, ...]
    _listing_file_identity: tuple[int, int, int, int, int, int, int]
    _seal: object

    def __repr__(self) -> str:
        return "<prepared-nppes-release-chain>"


def _listing_file_identity(
    path: Path,
) -> tuple[int, int, int, int, int, int, int]:
    if path.is_symlink():
        raise archive_error()
    file_stat = path.stat()
    if not stat.S_ISREG(file_stat.st_mode):
        raise archive_error()
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
        file_stat.st_uid,
        stat.S_IMODE(file_stat.st_mode),
    )


def _selected_archives(
    listing: NppesListingSnapshot,
    archives: object,
) -> tuple[PreparedNppesArchive, ...]:
    if type(archives) is not tuple or not archives:
        raise archive_error()
    fixed_archives = tuple(
        validate_prepared_nppes_archive(archive)
        for archive in archives
    )
    expected_candidates = select_nppes_release_chain(
        listing.candidates,
        fixed_archives[0].layout.primary_snapshot_date,
    )
    if (
        tuple(archive.retained.candidate for archive in fixed_archives)
        != expected_candidates
        or any(
            archive.retained.listing_sha256 != listing.listing_sha256
            for archive in fixed_archives
        )
        or len(
            {archive.retained.artifact_sha256 for archive in fixed_archives}
        )
        != len(fixed_archives)
    ):
        raise archive_error()
    return fixed_archives


def build_prepared_nppes_release_chain(
    listing: object,
    archives: object,
) -> PreparedNppesReleaseChain:
    """Seal one reverified listing and its exact deterministic release chain."""

    try:
        fixed_listing = validate_nppes_listing_snapshot(listing)
        fixed_archives = _selected_archives(fixed_listing, archives)
        prepared_chain = object.__new__(PreparedNppesReleaseChain)
        object.__setattr__(prepared_chain, "listing", fixed_listing)
        object.__setattr__(prepared_chain, "archives", fixed_archives)
        object.__setattr__(
            prepared_chain,
            "_listing_file_identity",
            _listing_file_identity(fixed_listing.path),
        )
        object.__setattr__(prepared_chain, "_seal", _PREPARED_CHAIN_SEAL)
    except Exception:
        normalized_error = archive_error()
    else:
        return prepared_chain
    raise normalized_error


def validate_prepared_nppes_release_chain(
    candidate: object,
) -> PreparedNppesReleaseChain:
    """Require one sealed chain with unchanged listing and archive identities."""

    try:
        if (
            type(candidate) is not PreparedNppesReleaseChain
            or candidate._seal is not _PREPARED_CHAIN_SEAL
            or type(candidate.listing) is not NppesListingSnapshot
            or type(candidate.archives) is not tuple
            or candidate._listing_file_identity
            != _listing_file_identity(candidate.listing.path)
        ):
            raise archive_error()
        fixed_listing = validate_nppes_listing_snapshot(candidate.listing)
        _selected_archives(fixed_listing, candidate.archives)
    except Exception:
        normalized_error = archive_error()
    else:
        return candidate
    raise normalized_error


__all__ = (
    "PreparedNppesReleaseChain",
    "build_prepared_nppes_release_chain",
    "validate_prepared_nppes_release_chain",
)
