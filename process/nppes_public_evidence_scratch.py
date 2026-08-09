# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed scratch-space admission for full NPPES materialization."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import tempfile

from process.nppes_public_evidence_archive import (
    PreparedNppesArchive,
    archive_error,
)
from process.nppes_public_evidence_prepared_chain import PreparedNppesReleaseChain


_SCRATCH_ENV = "HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT"
_RESERVE_NUMERATOR = 6
_RESERVE_DENOMINATOR = 5


def _validated_scratch_root(candidate: object) -> Path:
    if not isinstance(candidate, Path) or candidate.is_symlink():
        raise archive_error()
    resolved = candidate.resolve()
    root_stat = resolved.stat()
    if (
        resolved != candidate
        or not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
        or stat.S_IMODE(root_stat.st_mode) & 0o077
    ):
        raise archive_error()
    return resolved


def resolve_nppes_scratch_root() -> Path:
    """Resolve one explicit private non-temporary materialization root."""

    try:
        configured = os.getenv(_SCRATCH_ENV, "")
        if not configured or "\x00" in configured:
            raise archive_error()
        root = Path(configured)
        if not root.is_absolute() or root == Path(root.anchor):
            raise archive_error()
        temporary_root = Path(tempfile.gettempdir()).resolve()
        resolved_parent = root.parent.resolve()
        if resolved_parent == temporary_root or temporary_root in resolved_parent.parents:
            raise archive_error()
        if root.exists() and root.is_symlink():
            raise archive_error()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        fixed_root = _validated_scratch_root(root)
    except Exception:
        normalized_error = archive_error()
    else:
        return fixed_root
    raise normalized_error


def _archive_materialization_bytes(prepared_archive: object) -> int:
    if type(prepared_archive) is not PreparedNppesArchive:
        raise archive_error()
    legacy_name_by_kind = dict(prepared_archive.layout.legacy_member_names)
    size_by_name = {
        member.name: member.uncompressed_size
        for member in prepared_archive.layout.members
    }
    if (
        len(legacy_name_by_kind) != 4
        or not set(legacy_name_by_kind.values()).issubset(size_by_name)
    ):
        raise archive_error()
    materialization_bytes = sum(
        size_by_name[member_name]
        for member_name in legacy_name_by_kind.values()
    )
    if materialization_bytes <= 0:
        raise archive_error()
    return materialization_bytes


def required_nppes_scratch_bytes(prepared_chain: object) -> int:
    """Return the largest exact four-member footprint in one prepared chain."""

    try:
        if (
            type(prepared_chain) is not PreparedNppesReleaseChain
            or type(prepared_chain.archives) is not tuple
            or not prepared_chain.archives
        ):
            raise archive_error()
        required_bytes = max(
            _archive_materialization_bytes(prepared_archive)
            for prepared_archive in prepared_chain.archives
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return required_bytes
    raise normalized_error


def assert_nppes_scratch_capacity(
    prepared_chain: object,
    scratch_root: object,
) -> None:
    """Require the largest materialization footprint plus 20 percent reserve."""

    try:
        fixed_root = _validated_scratch_root(scratch_root)
        required_bytes = required_nppes_scratch_bytes(prepared_chain)
        minimum_free_bytes = (
            required_bytes * _RESERVE_NUMERATOR + _RESERVE_DENOMINATOR - 1
        ) // _RESERVE_DENOMINATOR
        if shutil.disk_usage(fixed_root).free < minimum_free_bytes:
            raise archive_error()
    except Exception:
        normalized_error = archive_error()
    else:
        return
    raise normalized_error


__all__ = (
    "assert_nppes_scratch_capacity",
    "required_nppes_scratch_bytes",
    "resolve_nppes_scratch_root",
)
