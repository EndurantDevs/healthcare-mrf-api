# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Constrain source-sidecar file identity during projection."""

from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import BinaryIO

def is_source_file_unchanged(
    source_file: BinaryIO,
    source_path: Path,
    expected_metadata: os.stat_result,
) -> bool:
    """Return whether one open source still matches its authenticated path."""

    try:
        opened_metadata = os.fstat(source_file.fileno())
        current_metadata = os.lstat(source_path)
        return (
            stat.S_ISREG(opened_metadata.st_mode)
            and stat.S_ISREG(current_metadata.st_mode)
            and opened_metadata.st_dev
            == expected_metadata.st_dev
            == current_metadata.st_dev
            and opened_metadata.st_ino
            == expected_metadata.st_ino
            == current_metadata.st_ino
            and opened_metadata.st_size
            == expected_metadata.st_size
            == current_metadata.st_size
            and opened_metadata.st_mtime_ns
            == expected_metadata.st_mtime_ns
            == current_metadata.st_mtime_ns
        )
    except Exception:
        return False


__all__ = ["is_source_file_unchanged"]
