# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A changed note projection gets a new version without invalidating old rows."""

import hashlib
from pathlib import Path
import runpy

from db.models.hospital_price_header import HospitalPriceVersion
from process.hospital_price_native import hospital_price_version_id
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_SCHEMA_REVISION,
    HOSPITAL_MRF_SUMMARY_CONTRACT,
)


def test_tall_notes_contract_changes_version_not_codec():
    assert HOSPITAL_MRF_SCHEMA_REVISION == "hospital-mrf-packed-blocks-v5"
    assert HOSPITAL_MRF_SUMMARY_CONTRACT == "hospital-mrf-copy-v2-v3-packed-v8"
    assert HOSPITAL_MRF_PARSER_CONTRACT_SHA256 == (
        "bb8b427a158d00c4de9b7dca2cb97a25b5097b859404e4abe943483f47a1b184"
    )
    content = "a" * 64
    old = hashlib.sha256(
        f"hospital-price-version-v1\0{content}\0{HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256}".encode("ascii")
    ).hexdigest()
    assert hospital_price_version_id(content) != old


def test_tall_notes_admission_only_appends_current_contract():
    versions = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    previous = runpy.run_path(str(versions / "20260907220000_hospital_price_missing_plan.py"))
    migration = runpy.run_path(str(versions / "20260908160000_hospital_price_tall_notes.py"))
    assert migration["down_revision"] == previous["revision"]
    old_drop, old_shape = previous["_upgrade_statements"]()
    drop, statement = migration["_upgrade_statements"]()
    shape = next(str(constraint.sqltext) for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check")
    assert statement.endswith(f"CHECK ({shape});")
    assert drop == old_drop
    assert statement.replace(f", '{HOSPITAL_MRF_PARSER_CONTRACT_SHA256}'", "") == old_shape
    assert statement.count(HOSPITAL_MRF_PARSER_CONTRACT_SHA256) == 2
    assert statement.count(HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256) == 2
    assert migration["downgrade"]() is None
