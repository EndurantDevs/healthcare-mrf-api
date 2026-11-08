# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The only production modules staged into the pure preflight wheel."""

from __future__ import annotations

from pathlib import Path

CANONICAL_MODULES = (
    "process/custom_import/_source_text.py",
    "process/custom_import/definition.py",
    "process/custom_import/derived_query.py",
    "process/custom_import/family.py",
    "process/custom_import/capture_limits.py",
    "process/custom_import/segmented_capture_policy.py",
    "process/custom_import/processing_policy.py",
    "process/custom_import/snowflake.py",
    "process/custom_import/snowflake_bundle.py",
    "process/custom_import/snowflake_bundle_scope.py",
    "process/custom_import/snowflake_binding.py",
    "process/custom_import/snowflake_preflight.py",
    "process/custom_import/snowflake_preflight_schema.py",
    "process/custom_import/snowflake_inspection.py",
)
WHEEL_INITIALIZERS = ("custom_import_preflight/__init__.py",)


def stage_modules(source_root: Path, stage_root: Path) -> None:
    """Stage the pure contracts without shadowing the native process package."""

    for relative_path in WHEEL_INITIALIZERS:
        target = stage_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")
    for relative_path in CANONICAL_MODULES:
        target = stage_root / "custom_import_preflight" / Path(relative_path).name
        source = (source_root / relative_path).read_text(encoding="utf-8")
        target.write_text(
            source.replace("from process.custom_import", "from custom_import_preflight"), encoding="utf-8"
        )
