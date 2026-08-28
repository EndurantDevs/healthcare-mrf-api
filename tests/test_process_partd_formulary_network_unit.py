# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import importlib
import types
import asyncio
import zipfile
from pathlib import Path

from sqlalchemy.dialects import postgresql

module = importlib.import_module("process.partd_formulary_network")


def test_partd_pure_parser_coverage_paydown(tmp_path):
    expected_kind_by_name = {
        "readme.pdf": "skip",
        "Plan Information File.csv": "plan_info",
        "Basic Drugs Formulary File.txt": "formulary_map",
        "Pharmacy Networks File.csv": "activity",
        "Pricing File.csv": "pricing",
        "other pharmacy listing.csv": "activity",
        "network notes.txt": "activity",
        "ordinary.csv": "unknown",
    }
    assert {
        name: module._entry_kind(name) for name in expected_kind_by_name
    } == expected_kind_by_name

    nested = tmp_path / "nested.zip"
    with zipfile.ZipFile(nested, "w") as archive:
        archive.writestr("Pharmacy Network File.txt", "NPI\n1234567890\n")
        archive.writestr("unrelated.csv", "ignored\n")

    outer = tmp_path / "outer.zip"
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("folder/", "")
        archive.writestr("random.csv", "ignored\n")
        archive.writestr("Plan Information File.csv", "contract\nS1234\n")
        archive.writestr("Pricing File.csv", "cost\n1.00\n")
        archive.writestr("Basic Drugs Formulary File.txt", "ndc\n123\n")
        archive.writestr("Pharmacy Notes.json", "{}")
        archive.write(nested, "Pharmacy Network bundle.zip")

    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    extracted = module._extract_data_files(outer, extract_dir)
    assert {module._entry_kind(name) for _path, name in extracted} == {
        "plan_info",
        "pricing",
        "formulary_map",
        "activity",
    }


def test_partd_plan_and_formulary_map_coverage_paydown(tmp_path):
    plan_map_path = tmp_path / "Plan Information File.csv"
    plan_map_path.write_text(
        "Contract ID|Plan ID|Segment ID|Formulary ID\n"
        "S1234|001|000|\n"
        "S1234|001|000|FORM1\n",
        encoding="utf-8",
    )
    plan_map = module._load_plan_formulary_map(plan_map_path)
    assert plan_map == {("S1234", "001", "000"): "FORM1"}
    formulary_path = tmp_path / "Basic Drugs Formulary File.csv"
    formulary_path.write_text(
        "Formulary ID,NDC11,RXCUI\n"
        ",00000000001,11\n"
        "FORM1,00000000001,11\n"
        "FORM1,00000000001,22\n",
        encoding="utf-8",
    )
    ndc_map = module._load_formulary_ndc_map(formulary_path)
    assert ndc_map[("FORM1", "00000000001")] == "11"
    assert ndc_map[("*", "00000000001")] == "11"


def test_partd_pricing_row_coverage_paydown():
    common_arguments_by_name = {
        "snapshot_id": "monthly:test",
        "source_type": "monthly",
        "default_date": datetime.date(2026, 1, 1),
        "plan_to_formulary": {("S1234", "001", "000"): "FORM1"},
        "formulary_ndc_to_rxnorm": {
            ("FORM1", "00000000001"): "11",
            ("*", "00000000001"): "11",
        },
    }
    ndc_rows = module._pricing_rows_from_source(
        {
            "Contract ID": "S1234",
            "Plan ID": "001",
            "Segment ID": "000",
            "NDC11": "00000000001",
            "Days Supply": "30",
            "Copay Amount": "5.50",
            "Coinsurance Cost": "2.25",
        },
        **common_arguments_by_name,
    )
    assert len(ndc_rows) == 2
    assert ndc_rows[0]["rxnorm_id"] == "11"
    assert ndc_rows[0]["cost_type"].endswith("_days_30")

    rxnorm_rows = module._pricing_rows_from_source(
        {"RXCUI": "22", "Unit Cost": "1.25"},
        **common_arguments_by_name,
    )
    assert rxnorm_rows[0]["code_system"] == "RXNORM"
    assert (
        module._pricing_rows_from_source(
            {"Unit Cost": "1.25"},
            **common_arguments_by_name,
        )
        == []
    )
    assert (
        module._pricing_rows_from_source(
            {"RXCUI": "22", "Name": "No cost"},
            **common_arguments_by_name,
        )
        == []
    )


def test_extract_dispensing_fee_fields_maps_known_headers():
    values = module._row_index(
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
    fees = module._extract_dispensing_fee_fields(values)
    assert fees["dispensing_fee_brand_30"] == 1.25
    assert fees["dispensing_fee_generic_60"] == 0.5
    assert fees["dispensing_fee_selected_drug_90"] == 4.75


def test_activity_row_from_source_includes_dispensing_fee_fields():
    source_row_by_field = {
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
    activity = module._activity_row_from_source(
        source_row_by_field,
        snapshot_id="quarterly:20260101:test",
        source_type="quarterly",
        default_date=datetime.date(2026, 1, 1),
    )
    assert activity is not None
    assert activity["dispensing_fee_brand_30"] == 1.1
    assert activity["dispensing_fee_generic_90"] == 0.3
    assert activity["dispensing_fee_selected_drug_60"] == 2.2
    assert activity["address_observed_in_source"] is False


def test_activity_row_tracks_address_observed_in_partd_source():
    activity = module._activity_row_from_source(
        {
            "NPI": "1518379601",
            "Contract ID": "S1234",
            "Plan ID": "001",
            "Segment ID": "000",
            "Address 1": "10 E 20 N",
            "City": "Example City",
            "State": "TX",
            "ZIP": "75001",
        },
        snapshot_id="quarterly:20260101:test",
        source_type="quarterly",
        default_date=datetime.date(2026, 1, 1),
    )

    assert activity is not None
    assert activity["address_observed_in_source"] is True


def test_match_cost_fields_includes_fee_token():
    entries = module._match_cost_fields(
        {
            "Brand Dispensing Fee 30 Days Supply": "1.23",
            "Some Amount": "9.99",
        }
    )
    keys = {key for key, _ in entries}
    assert "branddispensingfee30dayssupply" in keys
    assert "someamount" in keys


def test_entry_kind_accepts_singular_pharmacy_network_file():
    assert module._entry_kind("2026_Q1/Pharmacy Network File.csv") == "activity"


def test_explicit_artifacts_accepts_source_urls_and_metadata():
    artifacts = module._explicit_artifacts(
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

    asyncio.run(module._stage_artifact_file(str(source), str(target)))

    assert target.read_bytes() == b"zip-bytes"


def test_wait_for_activity_chunks_emits_live_progress_and_fallback(monkeypatch):
    events: list[dict] = []

    class FakeRedis:
        async def get(self, key):
            if key.endswith(":activity_total"):
                return b"2"
            if key.endswith(":activity_rows"):
                return b"0"
            if key.endswith(":activity_bytes_total"):
                return b"1000"
            return None

        async def scard(self, key):
            if key.endswith(":activity_started"):
                return 1
            return 0

        async def smembers(self, _key):
            return set()

        async def hgetall(self, key):
            if key.endswith(":activity_bytes_progress"):
                return {b"chunk-1": b"250"}
            if key.endswith(":activity_rows_progress"):
                return {b"chunk-1": b"10"}
            return {}

    monkeypatch.setattr(module, "PARTD_CHUNK_STALL_SECONDS", 0)
    monkeypatch.setattr(module, "enqueue_live_progress", lambda **payload: events.append(payload))

    processed_row_count, done_chunks = asyncio.run(
        module._wait_for_activity_chunks(
            FakeRedis(),
            "run_partd",
            "quarterly:20260428:test",
            2,
        )
    )

    assert processed_row_count == 0
    assert done_chunks == set()
    assert [event["phase"] for event in events] == [
        "partd activity chunks running",
        "partd activity chunk fallback",
    ]
    assert events[0]["unit"] == "chunks"
    assert events[0]["done"] == 0
    assert events[0]["total"] == 2
    assert events[0]["pct"] == 25
    assert "started=1" in events[0]["message"]
    assert "rows=10" in events[0]["message"]
    assert "processing remaining 2 chunk(s) locally" in events[1]["message"]


def test_process_activity_file_emits_byte_progress(tmp_path, monkeypatch):
    activity_path = tmp_path / "activity.csv"
    activity_path.write_text(
        "NPI|Contract ID|Plan ID|Segment ID|Pharmacy Name|Pharmacy Retail\n"
        "1518379601|S1234|001|000|Test Pharmacy|Y\n"
        "1518379602|S1234|001|000|Other Pharmacy|Y\n",
        encoding="utf-8",
    )
    progress: list[tuple[int, int, int, int]] = []

    async def fake_push_objects(*_args, **_kwargs):
        return None

    async def capture_progress(processed_rows, accepted_rows, processed_bytes, total_bytes):
        progress.append((processed_rows, accepted_rows, processed_bytes, total_bytes))

    monkeypatch.setattr(module, "push_objects", fake_push_objects)
    monkeypatch.setattr(module, "PARTD_PROGRESS_INTERVAL_ROWS", 1)

    accepted = asyncio.run(
        module._process_activity_file(
            activity_path,
            snapshot_id="quarterly:20260428:test",
            source_type="quarterly",
            default_date=datetime.date(2026, 4, 1),
            test_mode=False,
            progress_callback=capture_progress,
        )
    )

    assert accepted == 2
    assert progress
    assert progress[-1][0] == 2
    assert progress[-1][1] == 2
    assert progress[-1][2] == activity_path.stat().st_size
    assert progress[-1][3] == activity_path.stat().st_size


def test_test_mode_skips_full_table_index_maintenance():
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "if not test_mode and PARTD_DEFER_ADDITIONAL_INDEXES" in source
    assert "if not test_mode:\n            await _drop_legacy_partd_tables(schema)" in source


def test_materialize_pricing_snapshot_analyzes_stage_and_uses_single_aggregate(monkeypatch):
    executed_statements: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed_statements.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._materialize_pricing_snapshot("mrf", "monthly:20260520:test"))

    assert executed_statements[0] == "ANALYZE mrf.partd_medication_cost_stage_v2;"
    insert_sql = next(stmt for stmt in executed_statements if "INSERT INTO mrf.partd_medication_cost_v2" in stmt)
    assert "array_agg(DISTINCT plan_id ORDER BY plan_id) AS plan_ids" in insert_sql
    assert "dedup AS" not in insert_sql
    assert "SELECT DISTINCT" not in insert_sql


def test_flush_batches_dedupes_exact_pricing_rows_before_copy(monkeypatch):
    captured_batches: list[tuple[type, list[dict]]] = []

    async def fake_push_objects(pricing_rows, model, **_kwargs):
        captured_batches.append((model, pricing_rows))

    monkeypatch.setattr(module, "push_objects", fake_push_objects)
    pricing_row_by_field = {
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
    other_plan_by_field = dict(pricing_row_by_field, plan_id="S1234002000")

    asyncio.run(
        module._flush_batches(
            [],
            [dict(pricing_row_by_field), dict(pricing_row_by_field), other_plan_by_field],
        )
    )

    assert len(captured_batches) == 1
    model, published_rows = captured_batches[0]
    assert model is module.PartDMedicationCostStage
    assert published_rows == [pricing_row_by_field, other_plan_by_field]


def test_flush_batches_aggregates_activity_plans_and_address_evidence(monkeypatch):
    captured_batches: list[tuple[type, list[dict]]] = []

    async def fake_push_objects(activity_rows, model, **_kwargs):
        captured_batches.append((model, activity_rows))

    monkeypatch.setattr(module, "push_objects", fake_push_objects)
    activity_row_by_field = {
        "snapshot_id": "monthly:20260520:test",
        "npi": 1234567890,
        "year": 2026,
        "plan_id": "S1234002000",
        "pharmacy_name": "Example Pharmacy",
        "address_observed_in_source": False,
        "source_type": "monthly",
    }
    corroborated_row_by_field = {
        **activity_row_by_field,
        "plan_id": "S1234001000",
        "address_observed_in_source": True,
    }

    asyncio.run(
        module._flush_batches(
            [activity_row_by_field, corroborated_row_by_field],
            [],
        )
    )

    assert len(captured_batches) == 1
    model, published_rows = captured_batches[0]
    assert model is module.PartDPharmacyActivityStage
    assert len(published_rows) == 1
    assert published_rows[0]["plan_ids"] == ["S1234001000", "S1234002000"]
    assert published_rows[0]["address_observed_in_source"] is True
    assert "_plans" not in published_rows[0]


def test_ensure_columns_adds_missing_columns(monkeypatch):
    table = module.PartDPharmacyActivityStage.__table__
    existing_columns = {column.name for column in table.columns}
    existing_columns.remove("dispensing_fee_brand_30")
    executed_statements: list[str] = []

    class FakeDB:
        def __init__(self):
            self.engine = types.SimpleNamespace(dialect=postgresql.dialect())

        async def all(self, *_args, **_kwargs):
            return [(name,) for name in sorted(existing_columns)]

        async def status(self, stmt, **_kwargs):
            executed_statements.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())
    asyncio.run(module._ensure_columns(module.PartDPharmacyActivityStage, "mrf"))
    assert any("ADD COLUMN IF NOT EXISTS \"dispensing_fee_brand_30\"" in stmt for stmt in executed_statements)


def test_fill_activity_state_from_zip_uses_geo_lookup(monkeypatch):
    executed_statements: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed_statements.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._fill_activity_state_from_zip("mrf", "partd_pharmacy_activity_stage_v2"))

    assert executed_statements
    stmt = executed_statements[0]
    assert "UPDATE mrf.partd_pharmacy_activity_stage_v2 AS activity" in stmt
    assert "FROM mrf.geo_zip_lookup AS geo" in stmt
    assert "NULLIF(activity.state, '') IS NULL" in stmt
    assert "regexp_replace(COALESCE(activity.zip_code, ''), '[^0-9]', '', 'g')" in stmt


def test_fill_activity_address_from_npi_uses_primary_address(monkeypatch):
    executed_statements: list[str] = []

    class FakeDB:
        async def status(self, stmt, **_kwargs):
            executed_statements.append(stmt)

    monkeypatch.setattr(module, "db", FakeDB())

    asyncio.run(module._fill_activity_address_from_npi("mrf", "partd_pharmacy_activity_stage_v2"))

    assert executed_statements
    stmt = executed_statements[0]
    assert "to_regclass('mrf.npi_address')" in stmt
    assert "UPDATE mrf.partd_pharmacy_activity_stage_v2 AS activity" in stmt
    assert "FROM mrf.npi_address" in stmt
    assert "type = 'primary'" in stmt
    assert "activity.npi = npi_addr.npi" in stmt
