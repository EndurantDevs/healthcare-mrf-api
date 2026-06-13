# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import importlib
import types
import asyncio
from pathlib import Path

from sqlalchemy.dialects import postgresql

module = importlib.import_module("process.partd_formulary_network")


def test_extract_dispensing_fee_fields_maps_known_headers():
    values = module._row_index(  # pylint: disable=protected-access
        {
            "Brand Dispensing Fee 30 Days Supply": "1.25",
            "Brand Dispensing Fee 60 Days Supply": "2.50",
            "Brand Dispensing Fee 90 Days Supply": "3.75",
            "Generic Dispensing Fee 30 Days Supply": "0.25",
            "Generic Dispensing Fee 60 Days Supply": "0.50",
            "Generic Dispensing Fee 90 Days Supply": "0.75",
            "Selected Drug Dispensing Fee 30 Days Supply": "4.25",
            "Selected Drug Dispensing Fee 60 Days Supply": "4.50",
            "Selected Drug Dispensing Fee 90 Days Supply": "4.75",
        }
    )
    fees = module._extract_dispensing_fee_fields(values)  # pylint: disable=protected-access
    assert fees["dispensing_fee_brand_30"] == 1.25
    assert fees["dispensing_fee_generic_60"] == 0.5
    assert fees["dispensing_fee_selected_drug_90"] == 4.75


def test_activity_row_from_source_includes_dispensing_fee_fields():
    row = {
        "NPI": "1518379601",
        "Contract ID": "S1234",
        "Plan ID": "001",
        "Segment ID": "000",
        "Pharmacy Name": "Test Pharmacy",
        "Pharmacy Retail": "Y",
        "Mail Order": "N",
        "Brand Dispensing Fee 30 Days Supply": "1.10",
        "Generic Dispensing Fee 90 Days Supply": "0.30",
        "Selected Drug Dispensing Fee 60 Days Supply": "2.20",
    }
    activity = module._activity_row_from_source(  # pylint: disable=protected-access
        row,
        snapshot_id="quarterly:20260101:test",
        source_type="quarterly",
        default_date=datetime.date(2026, 1, 1),
    )
    assert activity is not None
    assert activity["dispensing_fee_brand_30"] == 1.1
    assert activity["dispensing_fee_generic_90"] == 0.3
    assert activity["dispensing_fee_selected_drug_60"] == 2.2


def test_match_cost_fields_includes_fee_token():
    entries = module._match_cost_fields(  # pylint: disable=protected-access
        {
            "Brand Dispensing Fee 30 Days Supply": "1.23",
            "Some Amount": "9.99",
        }
    )
    keys = {key for key, _ in entries}
    assert "branddispensingfee30dayssupply" in keys
    assert "someamount" in keys


def test_entry_kind_accepts_singular_pharmacy_network_file():
    assert module._entry_kind("2026_Q1/Pharmacy Network File.csv") == "activity"  # pylint: disable=protected-access


def test_explicit_artifacts_accepts_source_urls_and_metadata():
    artifacts = module._explicit_artifacts(  # pylint: disable=protected-access
        {
            "artifacts": [
                {
                    "url": "/tmp/partd-smoke.zip",
                    "source_type": "monthly",
                    "release_date": "2026-05-15",
                    "cutoff_month": "2026-05",
                }
            ]
        }
    )

    assert len(artifacts) == 1
    assert artifacts[0].url == "/tmp/partd-smoke.zip"
    assert artifacts[0].source_type == "monthly"
    assert artifacts[0].release_date == datetime.date(2026, 5, 15)
    assert artifacts[0].cutoff_month == datetime.date(2026, 5, 1)


def test_stage_artifact_file_copies_local_path(tmp_path):
    source = tmp_path / "source.zip"
    target = tmp_path / "target.zip"
    source.write_bytes(b"zip-bytes")

    asyncio.run(module._stage_artifact_file(str(source), str(target)))  # pylint: disable=protected-access

    assert target.read_bytes() == b"zip-bytes"


def test_test_mode_skips_full_table_index_maintenance():
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "if not test_mode and PARTD_DEFER_ADDITIONAL_INDEXES" in source
    assert "if not test_mode:\n            await _drop_legacy_partd_tables(schema)" in source


def test_materialize_pricing_snapshot_analyzes_stage_and_uses_single_aggregate(monkeypatch):
    executed: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._materialize_pricing_snapshot("mrf", "monthly:20260520:test"))  # pylint: disable=protected-access

    assert executed[0] == "ANALYZE mrf.partd_medication_cost_stage_v2;"
    insert_sql = next(stmt for stmt in executed if "INSERT INTO mrf.partd_medication_cost_v2" in stmt)
    assert "array_agg(DISTINCT plan_id ORDER BY plan_id) AS plan_ids" in insert_sql
    assert "dedup AS" not in insert_sql
    assert "SELECT DISTINCT" not in insert_sql


def test_flush_batches_dedupes_exact_pricing_rows_before_copy(monkeypatch):
    captured: list[tuple[type, list[dict]]] = []

    async def fake_push_objects(rows, model, **_kwargs):
        captured.append((model, rows))

    monkeypatch.setattr(module, "push_objects", fake_push_objects)
    row = {
        "snapshot_id": "monthly:20260520:test",
        "plan_id": "S1234001000",
        "year": 2026,
        "code_system": "ndc",
        "code": "00000000000",
        "normalized_code": "00000000000",
        "rxnorm_id": None,
        "ndc11": "00000000000",
        "days_supply": 30,
        "drug_name": "Drug",
        "tier": "1",
        "pharmacy_type": "retail",
        "mail_order": False,
        "cost_type": "preferred_days_30",
        "cost_amount": 1.23,
        "effective_from": datetime.date(2026, 4, 1),
        "effective_to": None,
        "source_type": "monthly",
    }
    other_plan = dict(row, plan_id="S1234002000")

    asyncio.run(module._flush_batches([], [dict(row), dict(row), other_plan]))  # pylint: disable=protected-access

    assert len(captured) == 1
    model, rows = captured[0]
    assert model is module.PartDMedicationCostStage
    assert rows == [row, other_plan]


def test_ensure_columns_adds_missing_columns(monkeypatch):
    table = module.PartDPharmacyActivityStage.__table__  # pylint: disable=protected-access
    existing = {column.name for column in table.columns}
    existing.remove("dispensing_fee_brand_30")
    executed: list[str] = []

    class FakeDB:
        def __init__(self):
            self.engine = types.SimpleNamespace(dialect=postgresql.dialect())

        async def all(self, *_args, **_kwargs):
            return [(name,) for name in sorted(existing)]

        async def status(self, stmt, **_kwargs):
            executed.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())
    asyncio.run(module._ensure_columns(module.PartDPharmacyActivityStage, "mrf"))  # pylint: disable=protected-access
    assert any("ADD COLUMN IF NOT EXISTS \"dispensing_fee_brand_30\"" in stmt for stmt in executed)


def test_fill_activity_state_from_zip_uses_geo_lookup(monkeypatch):
    executed: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._fill_activity_state_from_zip("mrf", "partd_pharmacy_activity_stage_v2"))  # pylint: disable=protected-access

    assert executed
    stmt = executed[0]
    assert "UPDATE mrf.partd_pharmacy_activity_stage_v2 AS activity" in stmt
    assert "FROM mrf.geo_zip_lookup AS geo" in stmt
    assert "NULLIF(activity.state, '') IS NULL" in stmt
    assert "regexp_replace(COALESCE(activity.zip_code, ''), '[^0-9]', '', 'g')" in stmt


def test_fill_activity_address_from_npi_uses_primary_address(monkeypatch):
    executed: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._fill_activity_address_from_npi("mrf", "partd_pharmacy_activity_stage_v2"))  # pylint: disable=protected-access

    assert executed
    stmt = executed[0]
    assert "to_regclass('mrf.npi_address')" in stmt
    assert "UPDATE mrf.partd_pharmacy_activity_stage_v2 AS activity" in stmt
    assert "FROM mrf.npi_address" in stmt
    assert "type = 'primary'" in stmt
    assert "activity.npi = npi_addr.npi" in stmt
