# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scratch admission tests for required NPPES legacy materialization."""

from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process import nppes_public_evidence_scratch as scratch
from tests.test_process_npi_import import _build_nppes_zip, _prepared_chain


@pytest.mark.parametrize("configured", (None, "relative/path", "/", "/tmp/nppes"))
def test_scratch_root_fails_closed_for_unsafe_configuration(
    configured: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if configured is None:
        monkeypatch.delenv(
            "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT",
            raising=False,
        )
    else:
        monkeypatch.setenv(
            "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT",
            configured,
        )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch.resolve_nppes_scratch_root()


def test_scratch_root_is_explicit_private_and_owner_controlled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_root = tmp_path.resolve() / "scratch"
    monkeypatch.setattr(scratch.tempfile, "gettempdir", lambda: "/private/tmp")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT",
        str(configured_root),
    )

    resolved_root = scratch.resolve_nppes_scratch_root()

    assert resolved_root == configured_root
    assert resolved_root.stat().st_uid == os.geteuid()
    assert resolved_root.stat().st_mode & 0o077 == 0


def test_scratch_root_rejects_wrong_type_temporary_child_or_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch._validated_scratch_root(object())

    temporary_root = tmp_path / "temporary"
    temporary_root.mkdir(mode=0o700)
    configured_root = temporary_root / "nppes"
    monkeypatch.setattr(scratch.tempfile, "gettempdir", lambda: str(temporary_root))
    monkeypatch.setenv(
        "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT",
        str(configured_root),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch.resolve_nppes_scratch_root()

    durable_parent = tmp_path / "durable"
    durable_parent.mkdir(mode=0o700)
    symlink_target = durable_parent / "target"
    symlink_target.mkdir(mode=0o700)
    symlink = durable_parent / "scratch-link"
    symlink.symlink_to(symlink_target, target_is_directory=True)
    monkeypatch.setattr(scratch.tempfile, "gettempdir", lambda: "/private/tmp")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT",
        str(symlink),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch.resolve_nppes_scratch_root()


def test_scratch_capacity_binds_four_members_and_twenty_percent_reserve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared_chain = _prepared_chain(tmp_path, _build_nppes_zip(tmp_path))
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir(mode=0o700)
    required_bytes = scratch.required_nppes_scratch_bytes(prepared_chain)
    prepared_archive = prepared_chain.archives[0]
    size_by_name = {
        member.name: member.uncompressed_size
        for member in prepared_archive.layout.members
    }
    expected_bytes = sum(
        size_by_name[member_name]
        for _member_kind, member_name in prepared_archive.layout.legacy_member_names
    )
    minimum_free_bytes = (required_bytes * 6 + 4) // 5
    assert required_bytes == expected_bytes

    monkeypatch.setattr(
        scratch.shutil,
        "disk_usage",
        lambda _root: SimpleNamespace(free=minimum_free_bytes - 1),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch.assert_nppes_scratch_capacity(prepared_chain, scratch_root)

    monkeypatch.setattr(
        scratch.shutil,
        "disk_usage",
        lambda _root: SimpleNamespace(free=minimum_free_bytes),
    )
    scratch.assert_nppes_scratch_capacity(prepared_chain, scratch_root)


def test_materialization_size_rejects_wrong_incomplete_or_empty_archive(
    tmp_path: Path,
) -> None:
    prepared_chain = _prepared_chain(tmp_path, _build_nppes_zip(tmp_path))
    prepared_archive = prepared_chain.archives[0]

    def with_layout(layout):
        forged_archive = object.__new__(type(prepared_archive))
        object.__setattr__(forged_archive, "retained", prepared_archive.retained)
        object.__setattr__(forged_archive, "layout", layout)
        object.__setattr__(
            forged_archive,
            "_file_identity",
            prepared_archive._file_identity,
        )
        object.__setattr__(forged_archive, "_seal", prepared_archive._seal)
        return forged_archive

    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch._archive_materialization_bytes(object())

    incomplete_layout = replace(
        prepared_archive.layout,
        legacy_member_names=prepared_archive.layout.legacy_member_names[:-1],
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch._archive_materialization_bytes(with_layout(incomplete_layout))

    zero_members = tuple(
        replace(member, uncompressed_size=0)
        for member in prepared_archive.layout.members
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch._archive_materialization_bytes(
            with_layout(replace(prepared_archive.layout, members=zero_members)),
        )

    with pytest.raises(NppesPublicEvidenceArchiveError):
        scratch.required_nppes_scratch_bytes(object())
