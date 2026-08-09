# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Runtime binding to the reviewed NPPES public-access evidence artifact."""

from __future__ import annotations

import hashlib
from pathlib import Path

from process.nppes_public_evidence_archive import archive_error


NPPES_RIGHTS_PROOF_SHA256 = (
    "6bbb296fe4edb6764563ef01ccb6f264c795df594fe33dc5b7a6bcb74ac0eb40"
)
_RIGHTS_PROOF_PATH = (
    Path(__file__).resolve().parents[1]
    / "specs"
    / "nppes-public-access-retention-review-v1.json"
)


def verified_nppes_rights_proof_sha256() -> str:
    """Hash the shipped bounded review and require its frozen identity."""

    if (
        _RIGHTS_PROOF_PATH.is_symlink()
        or not _RIGHTS_PROOF_PATH.is_file()
        or not 1 <= _RIGHTS_PROOF_PATH.stat().st_size <= 64 * 1024
    ):
        raise archive_error()
    try:
        proof_bytes = _RIGHTS_PROOF_PATH.read_bytes()
    except OSError:
        raise archive_error() from None
    digest = hashlib.sha256(proof_bytes).hexdigest()
    if digest != NPPES_RIGHTS_PROOF_SHA256:
        raise archive_error()
    return digest


__all__ = (
    "NPPES_RIGHTS_PROOF_SHA256",
    "verified_nppes_rights_proof_sha256",
)
