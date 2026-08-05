from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import _ptg2_tax_identity_shadow_files as files
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from tests.ptg2_tax_identity_shadow_admission_support import POLICY_ID, sidecar_bytes


def test_open_scratch_root_rejects_inode_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir(mode=0o700)
    original_node_identity = files._node_identity
    observed_identities: list[tuple[int, int, int, int]] = []

    def drifting_node_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
        identity = original_node_identity(metadata)
        observed_identities.append(identity)
        if len(observed_identities) == 2:
            return (identity[0], identity[1] + 1, *identity[2:])
        return identity

    monkeypatch.setattr(files, "_node_identity", drifting_node_identity)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        with files._open_scratch_root(scratch_root):
            pytest.fail("identity-drifted scratch root was admitted")
    assert len(observed_identities) == 2


def test_recheck_root_rejects_unavailable_open_descriptor_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir(mode=0o700)

    def unavailable_fstat(_descriptor: int) -> os.stat_result:
        raise OSError("synthetic metadata failure")

    with files._open_scratch_root(scratch_root) as root:
        monkeypatch.setattr(files.os, "fstat", unavailable_fstat)
        with pytest.raises(
            admission.TaxIdentityShadowAdmissionError,
            match="ptg2_tax_identity_shadow_artifact_changed",
        ):
            files._recheck_root(root)


def test_authentication_rejects_declared_bytes_smaller_than_header() -> None:
    data = sidecar_bytes(1)
    artifact = SimpleNamespace(
        sidecar_version=1,
        record_bytes=65,
        token_policy_id=POLICY_ID,
        byte_count=13,
        row_count=6,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    held = SimpleNamespace(
        preflight=SimpleNamespace(artifact=artifact),
        stream=io.BytesIO(data),
    )

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        files._authenticate_held_artifact(held)


def test_authentication_rejects_truncated_payload_chunk() -> None:
    data = sidecar_bytes(1)
    authenticated_header_bytes = 13 + len(POLICY_ID)
    artifact = SimpleNamespace(
        sidecar_version=1,
        record_bytes=65,
        token_policy_id=POLICY_ID,
        byte_count=len(data),
        row_count=6,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    held = SimpleNamespace(
        preflight=SimpleNamespace(artifact=artifact),
        stream=io.BytesIO(data[:authenticated_header_bytes]),
    )

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        files._authenticate_held_artifact(held)
