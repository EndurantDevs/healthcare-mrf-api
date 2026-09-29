# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The only production modules staged into the pure preflight wheel."""

from __future__ import annotations

from pathlib import Path
from shutil import copy2

CANONICAL_MODULES = (
    "process/custom_import/_source_text.py",
    "process/custom_import/definition.py",
    "process/custom_import/family.py",
    "process/custom_import/capture_limits.py",
    "process/custom_import/snowflake.py",
    "process/custom_import/snowflake_bundle.py",
    "process/custom_import/snowflake_binding.py",
    "process/custom_import/snowflake_preflight.py",
    "process/custom_import/snowflake_preflight_schema.py",
)
WHEEL_INITIALIZERS = (
    "process/__init__.py",
    "process/custom_import/__init__.py",
)


def stage_modules(source_root: Path, stage_root: Path) -> None:
    """Stage only the import-safe module allowlist under empty package roots."""

    for relative_path in WHEEL_INITIALIZERS:
        target = stage_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")
    for relative_path in CANONICAL_MODULES:
        target = stage_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_root / relative_path, target)
