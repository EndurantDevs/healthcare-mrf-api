# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior contracts for PLACES importer helper boundaries."""
from __future__ import annotations
import csv
import importlib
import io
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
places = importlib.import_module("process.places_zcta")
class _Transaction:
    async def __aenter__(self): return self
    async def __aexit__(self, *_args): return False
def _places_csv(row_list):
    field_name_list=["Year","LocationID","MeasureId","Measure","Data_Value","Low_Confidence_Limit","High_Confidence_Limit","Data_Value_Type","DataSource"]
    buffer=io.StringIO(); writer=csv.DictWriter(buffer,fieldnames=field_name_list); writer.writeheader(); writer.writerows(row_list); return buffer.getvalue()

@pytest.mark.asyncio
async def test_places_reader_flushes_deduplicated_rows_and_stops_at_selected_limit(monkeypatch, tmp_path):
    """Only valid current-year records count toward the bounded test selection."""

    csv_path = tmp_path / "places.csv"
    csv_path.write_text(
        _places_csv([
            {"Year": "2025", "LocationID": "USZCTA5 60654", "MeasureId": "A", "Measure": "old"},
            {"Year": "2026", "LocationID": "USZCTA5 60654", "MeasureId": "A", "Measure": "first"},
            {"Year": "2026", "LocationID": "USZCTA5 60654", "MeasureId": "A", "Measure": "replacement"},
            {"Year": "2026", "LocationID": "bad", "MeasureId": "B", "Measure": "invalid"},
            {"Year": "2026", "LocationID": "60655", "MeasureId": "C", "Measure": "second"},
        ]),
        encoding="utf-8",
    )
    pushed_batch_list = []

    async def push(place_record_list, target, **push_kwargs):
        pushed_batch_list.append((list(place_record_list), target, push_kwargs))

    monkeypatch.setattr(places, "push_objects", push)
    monkeypatch.setattr(places, "raise_if_cancelled", AsyncMock())
    processed, accepted = await places._read_places_rows(
        str(csv_path), latest_year=2026, target_cls="stage", batch_size=2,
        test_mode=True, test_row_limit=3, ctx={}, task={},
    )

    assert processed == 5
    assert accepted == 2
    staged_by_measure = {staged_record["measure_id"]: staged_record for batch, _, _ in pushed_batch_list for staged_record in batch}
    assert staged_by_measure["A"]["measure_name"] == "replacement"
    assert staged_by_measure["C"]["zcta"] == "60655"


@pytest.mark.asyncio
async def test_places_detect_and_validation_fail_closed_for_empty_or_sparse_stages(monkeypatch, tmp_path):
    """No-year input and a sparse non-test stage are both non-publishable."""

    no_year = tmp_path / "empty.csv"
    no_year.write_text("Year,LocationID\nnot-a-year,60654\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="no valid Year"):
        await places._detect_latest_year(str(no_year))

    stage = SimpleNamespace(__tablename__="places_stage")
    monkeypatch.setattr(places, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(places.db, "scalar", AsyncMock(return_value=2))
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_MIN_ROWS", "3")
    with pytest.raises(RuntimeError, match="below minimum"):
        await places._validated_places_stage_rows(stage, "mrf", {"test_mode": False})

    monkeypatch.setattr(places, "_is_table_available", AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="is missing"):
        await places._validated_places_stage_rows(stage, "mrf", {"test_mode": True})

    monkeypatch.setattr(places, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(places.db, "scalar", AsyncMock(return_value=2))
    assert await places._validated_places_stage_rows(stage, "mrf", {"test_mode": True}) == 2



def test_places_scalar_normalizers_and_identifier_helpers_are_fail_closed(monkeypatch):
    """Loose source fields cannot manufacture valid PLACES values or unsafe limits."""

    monkeypatch.setenv("PLACES_LIMIT", "0")
    assert places._env_positive_int("PLACES_LIMIT", 3) == 3
    monkeypatch.setenv("PLACES_LIMIT", "not-a-number")
    assert places._env_positive_int("PLACES_LIMIT", 3) == 3
    monkeypatch.setenv("PLACES_LIMIT", "7")
    assert places._env_positive_int("PLACES_LIMIT", 3) == 7
    assert places._safe_text("  text ") == "text"
    assert places._safe_text(" ") is None
    assert places._safe_int("1,024") == 1024
    assert places._safe_int("nan") is None


def test_places_empty_identifiers_and_incomplete_records_are_rejected():
    """A generated date identifier never turns empty source fields into a usable record."""

    assert len(places._normalize_import_id(None)) == 8
    assert len(places._normalize_import_id("!!!")) == 8
    assert places._safe_int(None) is None
    assert places._build_places_record(
        {"Year": "2026", "LocationID": "60654"},
        latest_year=2026,
    ) is None
    assert places._safe_float("1,024.5") == 1024.5
    assert places._safe_float({}) is None
    assert places._normalize_zcta("zip 60654 then 60655") == "60655"
    assert places._normalize_zcta("only 1234") is None
    assert places._normalize_import_id("run-01 / alpha") == "run01alpha"
    assert places._archived_identifier("x" * 80).endswith("_old")
    assert places._archived_identifier("short") == "short_old"


@pytest.mark.asyncio
async def test_places_validation_allows_production_stage_at_minimum(monkeypatch):
    """The production admission threshold is inclusive, preventing an off-by-one rejection."""

    stage = SimpleNamespace(__tablename__="places_stage")
    monkeypatch.setattr(places, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(places.db, "scalar", AsyncMock(return_value=3))
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_MIN_ROWS", "3")
    assert await places._validated_places_stage_rows(stage, "mrf", {"test_mode": False}) == 3


@pytest.mark.asyncio
async def test_places_index_builder_and_publish_swap_preserve_declared_indexes(monkeypatch):
    """Publication retains every declared secondary index across the table swap."""

    status = AsyncMock()
    stage = SimpleNamespace(__main_table__="pricing_places_zcta", __tablename__="places_stage")
    monkeypatch.setattr(places, "ensure_database", AsyncMock())
    monkeypatch.setattr(places, "make_class", lambda *_args: stage)
    monkeypatch.setattr(places, "_validated_places_stage_rows", AsyncMock(return_value=4))
    monkeypatch.setattr(places, "_create_places_stage_indexes", AsyncMock())
    monkeypatch.setattr(places.db, "execute_ddl", AsyncMock())
    monkeypatch.setattr(places.db, "status", status)
    monkeypatch.setattr(places.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(places, "print_time_info", lambda _start: None)
    monkeypatch.setattr(
        places.PricingPlacesZcta,
        "__my_additional_indexes__",
        ({"name": "metric", "index_elements": ("measure_id",), "using": "gin"},),
    )

    await places.publish_places_zcta_generation(
        {"import_date": "run", "context": {"run": 1, "test_mode": True, "start": "start"}}
    )

    sql_statement_list = [call.args[0] for call in status.await_args_list]
    assert any("pricing_places_zcta_old" in statement for statement in sql_statement_list)
    assert any("places_stage_idx_metric" in statement for statement in sql_statement_list)


@pytest.mark.asyncio
async def test_places_index_builder_uses_declared_method_predicate_and_noops_when_absent(monkeypatch):
    """Staging indexes are driven solely by the schema declaration before cutover."""

    status = AsyncMock()
    monkeypatch.setattr(places.db, "status", status)
    monkeypatch.setattr(places.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(
        places.PricingPlacesZcta,
        "__my_additional_indexes__",
        ({"name": "metric", "index_elements": ("measure_id",), "using": "gin", "where": "data_value IS NOT NULL"},),
    )
    await places._create_places_stage_indexes(SimpleNamespace(__tablename__="stage"), "mrf")
    statement = status.await_args.args[0]
    assert "USING gin" in statement
    assert "WHERE data_value IS NOT NULL" in statement

    status.reset_mock()
    monkeypatch.setattr(places.PricingPlacesZcta, "__my_additional_indexes__", ())
    await places._create_places_stage_indexes(SimpleNamespace(__tablename__="stage"), "mrf")
    status.assert_not_awaited()
