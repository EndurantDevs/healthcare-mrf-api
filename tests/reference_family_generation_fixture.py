# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared final-schema DDL for reference-family archive fixtures."""

import importlib.util
from pathlib import Path


_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260914130000_mrf_result_generation.py"
_SPEC = importlib.util.spec_from_file_location("reference_family_generation_fixture_migration", _MIGRATION_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MIGRATION = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MIGRATION)


def generation_shape_check() -> str:
    return _MIGRATION._shape_check({"mrf": _MIGRATION._MRF_CARDINALITY, **_MIGRATION._REFERENCE_CARDINALITY})
