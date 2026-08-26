# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Symlink-safe transient hospital source cleanup."""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path
from typing import Any


HOSPITAL_SOURCE_TMP_PREFIX = "hospital-mrf-source-"
_SAFE_DIR_FD_UNLINK = (
    hasattr(os, "O_DIRECTORY")
    and hasattr(os, "O_NOFOLLOW")
    and os.open in os.supports_dir_fd
    and os.stat in os.supports_dir_fd
    and os.stat in os.supports_follow_symlinks
    and os.unlink in os.supports_dir_fd
)


def owned_tmp_root(store: Any) -> Path:
    """Return one non-symlink artifact tmp directory on the store filesystem."""

    store_root = Path(store.root).resolve(strict=True)
    tmp_path = Path(store.tmp_dir)
    try:
        tmp_stat = tmp_path.lstat()
        tmp_root = tmp_path.resolve(strict=True)
        root_stat = store_root.stat()
    except OSError as exc:
        raise RuntimeError("hospital artifact tmp directory is unsafe") from exc
    if (
        stat.S_ISLNK(tmp_stat.st_mode)
        or not stat.S_ISDIR(tmp_stat.st_mode)
        or tmp_root not in {store_root, store_root / "tmp"}
        or tmp_stat.st_dev != root_stat.st_dev
    ):
        raise RuntimeError("hospital artifact tmp directory is unsafe")
    return tmp_root


def _transient_relative_path(store: Any, raw: Any) -> Path:
    source_path = Path(raw.raw_path)
    try:
        logical_relative = Path(os.path.abspath(source_path)).relative_to(
            Path(os.path.abspath(store.tmp_dir))
        )
        relative_path = source_path.resolve(strict=False).relative_to(
            owned_tmp_root(store)
        )
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "hospital source artifact is outside hospital source scratch"
        ) from exc
    if logical_relative != relative_path:
        raise RuntimeError(
            "hospital source artifact is not a regular non-symlink file"
        )
    if (
        len(relative_path.parts) < 3
        or not relative_path.parts[0].startswith(HOSPITAL_SOURCE_TMP_PREFIX)
        or relative_path.parts[1] != "raw"
    ):
        raise RuntimeError(
            "hospital source artifact is outside task-owned raw scratch"
        )
    return relative_path


def unlink_transient_source(store: Any, raw: Any) -> None:
    """Delete one exact regular source file from hospital-only scratch."""

    if not _SAFE_DIR_FD_UNLINK:
        raise RuntimeError("hospital source cleanup requires anchored directory unlink")
    relative_path = _transient_relative_path(store, raw)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_fds: list[int] = []
    try:
        directory_fd = os.open(
            Path(store.tmp_dir).resolve(strict=True), directory_flags
        )
        directory_fds.append(directory_fd)
        for component in relative_path.parts[:-1]:
            directory_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            directory_fds.append(directory_fd)
        try:
            source_stat = os.stat(
                relative_path.name, dir_fd=directory_fd, follow_symlinks=False
            )
        except FileNotFoundError:
            return
        if not stat.S_ISREG(source_stat.st_mode):
            raise RuntimeError(
                "hospital source artifact is not a regular non-symlink file"
            )
        os.unlink(relative_path.name, dir_fd=directory_fd)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise RuntimeError("hospital source artifact cleanup was unsafe") from exc
    finally:
        for directory_fd in reversed(directory_fds):
            os.close(directory_fd)


def sweep_transient_source_roots(store: Any) -> None:
    """Remove stale hospital-only source roots left by a terminated run."""

    if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
        raise RuntimeError(
            "hospital source scratch requires symlink-resistant removal"
        )
    tmp_root = owned_tmp_root(store)
    stale_roots: list[Path] = []
    for candidate in sorted(tmp_root.iterdir(), key=lambda path: path.name):
        if not candidate.name.startswith(HOSPITAL_SOURCE_TMP_PREFIX):
            continue
        candidate_stat = candidate.lstat()
        if not stat.S_ISDIR(candidate_stat.st_mode) or candidate.is_symlink():
            raise RuntimeError(
                "hospital source scratch entry is not a regular directory"
            )
        if candidate.resolve(strict=True).parent != tmp_root:
            raise RuntimeError("hospital source scratch entry is not task-owned")
        stale_roots.append(candidate)
    for stale_root in stale_roots:
        shutil.rmtree(stale_root)
