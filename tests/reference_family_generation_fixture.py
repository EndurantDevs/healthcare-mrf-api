# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared final-schema DDL for reference-family archive fixtures."""

import importlib.util
from pathlib import Path

_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260929000000_cms_doctor_group_site.py"
_SPEC = importlib.util.spec_from_file_location("reference_family_generation_fixture_migration", _MIGRATION_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MIGRATION = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MIGRATION)


def generation_shape_check() -> str:
    previous, counts = _MIGRATION._shape_support()
    return previous._shape({**counts, "cms-doctors": 3})
