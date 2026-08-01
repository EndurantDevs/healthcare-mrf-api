# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable identity for every component that can omit a UHC source fact."""

from __future__ import annotations

from functools import lru_cache
import hashlib
import json
from pathlib import Path


UHC_SEMANTIC_VERIFIER_IDENTITY_CONTRACT_ID = (
    "healthporta.uhc.semantic-verifier-identity.v1"
)
_DEPENDENCY_NAMES = (
    "uhc_provider_file_source_identity.py",
    "uhc_provider_quarantine_contract.py",
    "uhc_provider_quarantine_record.py",
    "uhc_provider_quarantine_raw_verifier.py",
    "uhc_retained_range_manifest.py",
    "uhc_retained_types.py",
    "uhc_semantic_build_store.py",
    "uhc_semantic_evidence.py",
    "uhc_semantic_stage_verifier.py",
    "uhc_semantic_verifier_identity.py",
)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@lru_cache(maxsize=1)
def semantic_verifier_identity_sha256() -> str:
    """Hash the exact executable quarantine verification dependency set."""

    root = Path(__file__).resolve().parent
    dependency_proofs = [
        [dependency_name, _file_sha256(root / dependency_name)]
        for dependency_name in _DEPENDENCY_NAMES
    ]
    encoded = json.dumps(
        [UHC_SEMANTIC_VERIFIER_IDENTITY_CONTRACT_ID, dependency_proofs],
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
