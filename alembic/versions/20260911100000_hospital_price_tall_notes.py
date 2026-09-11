# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit separate tall payer notes without changing the packed row schema."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20260911100000_hospital_price_tall_notes"
down_revision = "20260909120000_fhir_request_failure_budget"
branch_labels = None
depends_on = None

_PREVIOUS = "d8edd2cf698f67adb02fc222313f189429f3167d94010068be6c26be54f84a89"
_CURRENT = "bb8b427a158d00c4de9b7dca2cb97a25b5097b859404e4abe943483f47a1b184"


def _upgrade_statements() -> tuple[str, str]:
    path = Path(__file__).with_name("20260907220000_hospital_price_missing_plan.py")
    spec = importlib.util.spec_from_file_location("_hospital_tall_notes_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("hospital price shape predecessor unavailable")
    predecessor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(predecessor)
    drop, shape = predecessor._upgrade_statements()
    previous = f"'{_PREVIOUS}')"
    if shape.count(previous) != 2:
        raise RuntimeError("hospital price packed parser profile changed")
    return drop, shape.replace(previous, f"'{_PREVIOUS}', '{_CURRENT}')")


def upgrade() -> None:
    """Append the producer contract while preserving every header invariant."""

    drop, add = _upgrade_statements()
    op.execute(drop)
    op.execute(add)


def downgrade() -> None:
    """Preserve immutable accepted rows rather than make them invalid."""

    return None
