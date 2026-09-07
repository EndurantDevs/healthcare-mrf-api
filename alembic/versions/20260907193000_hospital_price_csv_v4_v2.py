# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit validated V2 CSV metadata while retaining producer label 4.0.0."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260907193000_hospital_price_csv_v4_v2"
down_revision = "20260904223000_provider_directory_michigan_generation_retirement"
branch_labels = None
depends_on = None


def _upgrade_statements() -> tuple[str, str]:
    """Extend only the predecessor's current-parser CSV profile clause."""

    path = Path(__file__).with_name("20260905130000_hospital_price_csv_3_0_1.py")
    spec = importlib.util.spec_from_file_location("_hospital_csv_v4_v2_previous", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("hospital price shape predecessor unavailable")
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    drop, add = previous._upgrade_statements()
    anchor = "(template_version IN ('3.0.1', '4.0.0') AND npi_count > 0 "
    v2 = (
        "(template_version = '4.0.0' AND attestation_text = "
        "'To the best of its knowledge and belief, the hospital has included all "
        "applicable standard charge information in accordance with the requirements "
        "of 45 CFR 180.50, and the information encoded is true, accurate, and complete "
        "as of the date indicated.' AND npi_count >= 0 "
        "AND (attester_name IS NULL OR btrim(attester_name) <> '')) OR "
    )
    if add.count(anchor) != 1:
        raise RuntimeError("hospital price V3 producer profile changed")
    return drop, add.replace(anchor, v2 + anchor, 1)


def upgrade() -> None:
    """Preserve all old rows and add the exact V2 CSV producer case."""

    drop, add = _upgrade_statements()
    op.execute(drop)
    op.execute(add)


def downgrade() -> None:
    """Retain accepted source-faithful rows rather than invalidate them."""

    return None
