from __future__ import annotations

import hashlib
import io
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest

from process.ptg_parts import _ptg2_tax_identity_shadow_files as files
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from tests.ptg2_tax_identity_shadow_admission_support import (
    POLICY_ID,
    descriptor_for,
    make_sidecar_pair,
    refresh_descriptor,
    sidecar_bytes,
)


def _admit(
    scratch_root: Path,
    v1: dict[str, object],
    v2: dict[str, object],
) -> admission.TaxIdentityShadowBundleDescriptor:
    return admission.admit_tax_identity_shadow_bundle(
        scratch_root=scratch_root,
        v1_scanner_descriptor=v1,
        v2_scanner_descriptor=v2,
    )


@pytest.mark.parametrize(
    "header_part",
    ["magic", "version", "record", "policy", "nonascii_policy"],
)
def test_rejects_header_drift_even_with_refreshed_digest(
    tmp_path: Path,
    header_part: str,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    path = Path(v1["path"])
    data = bytearray(path.read_bytes())
    if header_part == "magic":
        data[0] ^= 1
    elif header_part == "version":
        data[8:10] = (2).to_bytes(2, "little")
    elif header_part == "record":
        data[10:12] = (64).to_bytes(2, "little")
    elif header_part == "policy":
        data[13] ^= 1
    else:
        data[13] = 0xFF
    path.write_bytes(data)
    refresh_descriptor(v1)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_same_size_payload_change_against_descriptor_digest(
    tmp_path: Path,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    path = Path(v2["path"])
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_artifact_outside_authenticated_scratch_root(tmp_path: Path) -> None:
    scratch_root, _v1, v2 = make_sidecar_pair(tmp_path)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(sidecar_bytes(1))
    v1 = descriptor_for(outside, 1)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        _admit(scratch_root, v1, v2)


@pytest.mark.parametrize("file_shape", ["symlink", "hardlink"])
def test_rejects_linked_artifacts(tmp_path: Path, file_shape: str) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    path = Path(v1["path"])
    if file_shape == "symlink":
        real_path = scratch_root / "real-v1.bin"
        path.rename(real_path)
        path.symlink_to(real_path.name)
    else:
        os.link(path, scratch_root / "v1-alias.bin")

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_non_private_or_symlinked_scratch_root(tmp_path: Path) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    scratch_root.chmod(0o750)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        _admit(scratch_root, v1, v2)

    scratch_root.chmod(0o700)
    link = tmp_path / "scratch-link"
    link.symlink_to(scratch_root.name)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        _admit(link, v1, v2)


@pytest.mark.parametrize("root_shape", ["relative", "missing"])
def test_rejects_unresolved_scratch_roots(tmp_path: Path, root_shape: str) -> None:
    _scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    invalid_root = (
        Path("relative-shadow") if root_shape == "relative" else tmp_path / "missing"
    )

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        _admit(invalid_root, v1, v2)


@pytest.mark.parametrize("required_flag", ["O_NOFOLLOW", "O_NONBLOCK"])
def test_missing_required_open_flags_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    required_flag: str,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    monkeypatch.delattr(files.os, required_flag)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_scratch_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_missing_artifact_fails_closed_during_preflight(tmp_path: Path) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    Path(v1["path"]).unlink()

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_fifo_replacement_race_is_opened_nonblocking_and_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    v1_path = Path(v1["path"])
    original_preflight = files._preflight_artifact
    original_open = files.os.open
    counts_by_phase = {"preflight": 0}

    def racing_preflight(*args: object, **kwargs: object) -> object:
        result = original_preflight(*args, **kwargs)
        counts_by_phase["preflight"] += 1
        if counts_by_phase["preflight"] == 2:
            v1_path.unlink()
            os.mkfifo(v1_path, mode=0o600)
        return result

    def guarded_open(
        path: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if path == v1_path.name:
            assert flags & os.O_NONBLOCK
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(files, "_preflight_artifact", racing_preflight)
    monkeypatch.setattr(files.os, "open", guarded_open)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_changed",
    ):
        _admit(scratch_root, v1, v2)


def test_unlinked_artifact_between_preflight_and_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    v1_path = Path(v1["path"])
    original_preflight = files._preflight_artifact
    counts_by_phase = {"preflight": 0}

    def unlink_after_preflight(*args: object, **kwargs: object) -> object:
        result = original_preflight(*args, **kwargs)
        counts_by_phase["preflight"] += 1
        if counts_by_phase["preflight"] == 2:
            v1_path.unlink()
        return result

    monkeypatch.setattr(files, "_preflight_artifact", unlink_after_preflight)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_changed",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_synthetic_same_held_inode_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    inode_checks: list[bool] = []

    def is_synthetic_inode_pair_distinct(_v1_held: object, _v2_held: object) -> bool:
        inode_checks.append(True)
        return False

    monkeypatch.setattr(
        files,
        "_is_held_artifact_pair_distinct",
        is_synthetic_inode_pair_distinct,
    )
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_pair_invalid",
    ):
        _admit(scratch_root, v1, v2)
    assert len(inode_checks) == 1


def test_rejects_name_replacement_after_held_fd_authentication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    v1_path = Path(v1["path"])
    original_authenticate = files._authenticate_held_artifact
    counts_by_phase = {"authentication": 0}

    def authenticate_then_replace(held: object) -> None:
        original_authenticate(held)
        counts_by_phase["authentication"] += 1
        if counts_by_phase["authentication"] == 2:
            replacement = scratch_root / "replacement.bin"
            replacement.write_bytes(v1_path.read_bytes())
            os.replace(replacement, v1_path)

    monkeypatch.setattr(files, "_authenticate_held_artifact", authenticate_then_replace)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_changed",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_name_removal_after_held_fd_authentication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    v1_path = Path(v1["path"])
    original_authenticate = files._authenticate_held_artifact
    counts_by_phase = {"authentication": 0}

    def authenticate_then_unlink(held: object) -> None:
        original_authenticate(held)
        counts_by_phase["authentication"] += 1
        if counts_by_phase["authentication"] == 2:
            v1_path.unlink()

    monkeypatch.setattr(files, "_authenticate_held_artifact", authenticate_then_unlink)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_changed",
    ):
        _admit(scratch_root, v1, v2)


def test_rejects_scratch_root_replacement_after_authentication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    original_authenticate = files._authenticate_held_artifact
    counts_by_phase = {"authentication": 0}

    def authenticate_then_replace_root(held: object) -> None:
        original_authenticate(held)
        counts_by_phase["authentication"] += 1
        if counts_by_phase["authentication"] == 2:
            scratch_root.rename(tmp_path / "held-root")
            scratch_root.mkdir(mode=0o700)

    monkeypatch.setattr(
        files,
        "_authenticate_held_artifact",
        authenticate_then_replace_root,
    )
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_changed",
    ):
        _admit(scratch_root, v1, v2)


def test_admission_closes_held_fds_before_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    original_open_artifact = files._open_artifact
    held_descriptors: list[int] = []

    @contextmanager
    def recording_open_artifact(*args: object, **kwargs: object) -> Iterator[object]:
        with original_open_artifact(*args, **kwargs) as held:
            held_descriptors.append(held.stream.fileno())
            yield held

    monkeypatch.setattr(files, "_open_artifact", recording_open_artifact)
    bundle = _admit(scratch_root, v1, v2)

    assert bundle.publication_enabled is False
    assert len(held_descriptors) == 2
    for descriptor in held_descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


class _RecordingStream(io.BytesIO):
    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.request_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.request_sizes.append(size)
        return super().read(size)


def test_artifact_authentication_streams_with_bounded_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = sidecar_bytes(1)
    stream = _RecordingStream(data)
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
        stream=stream,
    )
    monkeypatch.setattr(files, "_HASH_CHUNK_BYTES", 17)

    files._authenticate_held_artifact(held)

    assert -1 not in stream.request_sizes
    assert max(stream.request_sizes) <= max(13, len(POLICY_ID), 17)


def test_authentication_budget_stops_appended_bytes_after_one_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declared_bytes = sidecar_bytes(1)
    stream = _RecordingStream(declared_bytes + b"x" * 10_000)
    artifact = SimpleNamespace(
        sidecar_version=1,
        record_bytes=65,
        token_policy_id=POLICY_ID,
        byte_count=len(declared_bytes),
        row_count=6,
        sha256=hashlib.sha256(declared_bytes).hexdigest(),
    )
    held = SimpleNamespace(
        preflight=SimpleNamespace(artifact=artifact),
        stream=stream,
    )
    monkeypatch.setattr(files, "_HASH_CHUNK_BYTES", 17)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        files._authenticate_held_artifact(held)

    assert stream.tell() == len(declared_bytes) + 1
    assert -1 not in stream.request_sizes
    assert max(stream.request_sizes) <= max(13, len(POLICY_ID), 17)


def test_exact_read_rejects_truncation_without_unbounded_retry() -> None:
    digest = hashlib.sha256()

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        files._read_exact(io.BytesIO(b""), 1, digest)
