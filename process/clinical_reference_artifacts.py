# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic artifact and provenance publication for clinical references."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
from pathlib import Path
from typing import Any


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_stream:
        for artifact_chunk in iter(lambda: source_stream.read(1024 * 1024), b""):
            digest.update(artifact_chunk)
    return digest.hexdigest()


def _discard_partial_download(temporary_path: Path) -> None:
    try:
        temporary_path.unlink(missing_ok=True)
    except OSError:
        return


def _write_manifest_temporary(
    path: Path,
    manifest_map: dict[str, Any],
) -> Path:
    temporary_manifest_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.manifest.",
            suffix=".tmp",
            delete=False,
        ) as manifest_stream:
            temporary_manifest_path = Path(manifest_stream.name)
            json.dump(manifest_map, manifest_stream, indent=2)
        return temporary_manifest_path
    except BaseException:
        if temporary_manifest_path is not None:
            _discard_partial_download(temporary_manifest_path)
        raise


def _manifest_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".manifest.json")


def _is_manifest_current(path: Path) -> bool:
    try:
        manifest_map = json.loads(_manifest_path(path).read_text(encoding="utf-8"))
        return (
            path.is_file()
            and manifest_map.get("byte_count") == path.stat().st_size
            and manifest_map.get("sha256") == _sha256_file(path)
        )
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def _publication_rollback_path(path: Path, state: str) -> Path:
    return path.with_name(
        f".{path.name}.rollback-{state}.{secrets.token_hex(8)}.tmp"
    )


def _create_publication_rollback(path: Path) -> tuple[Path, bool]:
    had_artifact = path.exists()
    rollback_state = "existing" if had_artifact else "empty"
    rollback_path = _publication_rollback_path(path, rollback_state)
    if had_artifact:
        os.link(path, rollback_path)
    else:
        rollback_path.touch(mode=0o600, exist_ok=False)
    return rollback_path, had_artifact


def _restore_publication(
    path: Path,
    rollback_path: Path,
    had_artifact: bool,
) -> None:
    if had_artifact:
        os.replace(rollback_path, path)
        return
    path.unlink(missing_ok=True)
    _discard_partial_download(rollback_path)


def _recover_interrupted_publication(path: Path) -> None:
    rollback_paths = sorted(
        path.parent.glob(f".{path.name}.rollback-*.*.tmp")
    )
    if not rollback_paths:
        return
    if _is_manifest_current(path):
        for rollback_path in rollback_paths:
            _discard_partial_download(rollback_path)
        return
    existing_rollback = next(
        (
            rollback_path
            for rollback_path in rollback_paths
            if ".rollback-existing." in rollback_path.name
        ),
        None,
    )
    if existing_rollback is not None:
        os.replace(existing_rollback, path)
    else:
        path.unlink(missing_ok=True)
    for rollback_path in rollback_paths:
        _discard_partial_download(rollback_path)
