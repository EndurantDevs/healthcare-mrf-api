# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import importlib
import types
import asyncio

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
