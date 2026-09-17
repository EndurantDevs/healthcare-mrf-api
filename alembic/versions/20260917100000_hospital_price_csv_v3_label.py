# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the exact producer label 3 for strictly detected V3 CSV files."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20260917100000_hospital_price_csv_v3_label"
down_revision = "20260914120000_custom_import_v1_schema"
branch_labels = None
depends_on = None

_CURRENT = "bb8b427a158d00c4de9b7dca2cb97a25b5097b859404e4abe943483f47a1b184"
_V3_PRODUCER_LABELS = "(template_version IN ('3.0.1', '4.0.0') AND npi_count > 0 AND attester_name IS NOT NULL)"


def _upgrade_statements() -> tuple[str, str]:
    path = Path(__file__).with_name("20260911100000_hospital_price_tall_notes.py")
    spec = importlib.util.spec_from_file_location("_hospital_csv_v3_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("hospital price shape predecessor unavailable")
    predecessor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(predecessor)
    drop, shape = predecessor._upgrade_statements()
    if shape.count(_V3_PRODUCER_LABELS) != 1:
        raise RuntimeError("hospital price V3 producer profile changed")
    v3_alias = (
        f"{_V3_PRODUCER_LABELS} OR (parser_contract_sha256 = '{_CURRENT}' "
        "AND template_version = '3' AND npi_count > 0 "
        "AND attester_name IS NOT NULL)"
    )
    return drop, shape.replace(_V3_PRODUCER_LABELS, v3_alias)


def upgrade() -> None:
    """Admit only source-faithful V3 CSV rows produced by the current parser."""

    drop, add = _upgrade_statements()
    op.execute(drop)
    op.execute(add)


def downgrade() -> None:
    """Preserve immutable accepted rows rather than make them invalid."""

    return None
