# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the nullable-plan packed parser under unchanged header constraints."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260907220000_hospital_price_missing_plan"
down_revision = "20260907193000_hospital_price_csv_v4_v2"
branch_labels = None
depends_on = None

_PREVIOUS = "d2725216821ac8aa9b9405f2a95e50e3899c524eb23b0663bdce15279498ad39"
_CURRENT = "d8edd2cf698f67adb02fc222313f189429f3167d94010068be6c26be54f84a89"


def _upgrade_statements() -> tuple[str, str]:
    path = Path(__file__).with_name("20260907193000_hospital_price_csv_v4_v2.py")
    spec = importlib.util.spec_from_file_location("_hospital_missing_plan_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("hospital price shape predecessor unavailable")
    predecessor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(predecessor)
    drop, shape = predecessor._upgrade_statements()
    old_list, old_scalar = f"'{_PREVIOUS}')", f"parser_contract_sha256 = '{_PREVIOUS}'"
    if shape.count(old_list) != 1 or shape.count(old_scalar) != 1:
        raise RuntimeError("hospital price packed parser profile changed")
    shape = shape.replace(old_list, f"'{_PREVIOUS}', '{_CURRENT}')", 1)
    shape = shape.replace(old_scalar, f"parser_contract_sha256 IN ('{_PREVIOUS}', '{_CURRENT}')", 1)
    return drop, shape


def upgrade() -> None:
    """Admit the new packed codec without relaxing any header invariant."""

    drop, add = _upgrade_statements()
    op.execute(drop)
    op.execute(add)


def downgrade() -> None:
    """Preserve immutable accepted rows rather than make them invalid."""

    return None
