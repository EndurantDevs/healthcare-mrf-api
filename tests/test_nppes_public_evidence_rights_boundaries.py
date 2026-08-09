# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure-boundary proof for the shipped NPPES rights artifact."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process import nppes_public_evidence_rights as rights


def test_rights_proof_rejects_a_symlink(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "review.json"
    target.write_bytes(b"{}")
    symlink = tmp_path / "review-link.json"
    symlink.symlink_to(target)
    monkeypatch.setattr(rights, "_RIGHTS_PROOF_PATH", symlink)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        rights.verified_nppes_rights_proof_sha256()


def test_rights_proof_normalizes_read_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _UnreadableProof:
        def is_symlink(self) -> bool:
            return False

        def is_file(self) -> bool:
            return True

        def stat(self):
            return SimpleNamespace(st_size=1)

        def read_bytes(self) -> bytes:
            raise OSError("PRIVATE-RIGHTS-PATH")

    monkeypatch.setattr(rights, "_RIGHTS_PROOF_PATH", _UnreadableProof())
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        rights.verified_nppes_rights_proof_sha256()
    assert "PRIVATE" not in repr(caught.value)
