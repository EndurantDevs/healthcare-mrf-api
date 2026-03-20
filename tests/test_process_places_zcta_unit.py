# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import importlib
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")


@pytest.fixture
def places_module():
    return importlib.import_module("process.places_zcta")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("60654", "60654"),
        ("USZCTA5 60654", "60654"),
        ("060654", "60654"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_zcta(value, expected, places_module):
    assert places_module._normalize_zcta(value) == expected


def test_build_places_record_filters_latest_year(places_module):
    row = {
        "Year": "2025",
        "LocationID": "USZCTA5 60654",
        "MeasureId": "CSMOKING",
        "Measure": "Current smoking among adults aged >=18 years",
        "Data_Value": "11.2",
        "Low_Confidence_Limit": "9.5",
        "High_Confidence_Limit": "13.1",
        "Data_Value_Type": "Crude prevalence",
        "DataSource": "CDC PLACES",
    }
    record = places_module._build_places_record(row, latest_year=2025)
    assert record is not None
    assert record["zcta"] == "60654"
    assert record["year"] == 2025
    assert record["measure_id"] == "CSMOKING"
    assert record["data_value"] == 11.2
    assert places_module._build_places_record(row, latest_year=2024) is None


@pytest.mark.asyncio
async def test_process_data_dedupes_rows_latest_year(monkeypatch, places_module, tmp_path):
    csv_path = tmp_path / "places.csv"
    csv_path.write_text(
        "Year,LocationID,MeasureId,Measure,Data_Value,Low_Confidence_Limit,High_Confidence_Limit,Data_Value_Type,DataSource\n"
        "2025,60654,CSMOKING,Smoking A,10,8,12,Crude prevalence,CDC PLACES\n"
        "2025,60654,CSMOKING,Smoking B,11,9,13,Crude prevalence,CDC PLACES\n"
        "2025,60654,BPHIGH,High blood pressure,20,18,22,Crude prevalence,CDC PLACES\n"
        "2024,60654,OLD,Old measure,1,1,1,Crude prevalence,CDC PLACES\n",
        encoding="utf-8",
    )

    async def _fake_download(_url, target_path, **_kwargs):
        Path(target_path).write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    captured = {}

    async def _fake_push(rows, cls, **kwargs):
        captured["rows"] = list(rows)
        captured["cls"] = cls
        captured["kwargs"] = kwargs

    monkeypatch.setattr(places_module, "download_it_and_save", _fake_download)
    monkeypatch.setattr(places_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(places_module, "make_class", lambda _cls, suffix: SimpleNamespace(__tablename__=f"pricing_places_zcta_{suffix}"))
    monkeypatch.setattr(places_module, "push_objects", _fake_push)

    ctx = {"import_date": "20260319", "context": {}}
    await places_module.process_data(ctx, {"test_mode": False})

    assert captured["cls"].__tablename__ == "pricing_places_zcta_20260319"
    assert captured["kwargs"] == {"rewrite": True, "use_copy": False}
    assert len(captured["rows"]) == 2
    by_measure = {row["measure_id"]: row for row in captured["rows"]}
    assert by_measure["CSMOKING"]["measure_name"] == "Smoking B"
    assert by_measure["CSMOKING"]["data_value"] == 11.0
    assert by_measure["BPHIGH"]["data_value"] == 20.0
    assert ctx["context"]["audit"]["latest_year"] == 2025


@pytest.mark.asyncio
async def test_process_data_honors_test_row_limit(monkeypatch, places_module, tmp_path):
    csv_path = tmp_path / "places_limit.csv"
    csv_path.write_text(
        "Year,LocationID,MeasureId,Measure,Data_Value,Low_Confidence_Limit,High_Confidence_Limit,Data_Value_Type,DataSource\n"
        "2025,60654,M1,Measure 1,1,1,1,Crude prevalence,CDC PLACES\n"
        "2025,60654,M2,Measure 2,2,1,3,Crude prevalence,CDC PLACES\n"
        "2025,60654,M3,Measure 3,3,2,4,Crude prevalence,CDC PLACES\n",
        encoding="utf-8",
    )

    async def _fake_download(_url, target_path, **_kwargs):
        Path(target_path).write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    pushed_rows = []

    async def _fake_push(rows, _cls, **_kwargs):
        pushed_rows.extend(rows)

    monkeypatch.setattr(places_module, "download_it_and_save", _fake_download)
    monkeypatch.setattr(places_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(places_module, "make_class", lambda _cls, _suffix: SimpleNamespace(__tablename__="pricing_places_zcta_test"))
    monkeypatch.setattr(places_module, "push_objects", _fake_push)
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_TEST_ROWS", "2")

    ctx = {"import_date": "20260319", "context": {}}
    await places_module.process_data(ctx, {"test_mode": True})

    assert len(pushed_rows) == 2
    assert {row["measure_id"] for row in pushed_rows} == {"M1", "M2"}


@pytest.mark.asyncio
async def test_startup_creates_stage_table(monkeypatch, places_module):
    create_calls = []
    status_calls = []

    monkeypatch.setattr(places_module, "my_init_db", AsyncMock())
    monkeypatch.setattr(places_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        places_module,
        "make_class",
        lambda cls, suffix: SimpleNamespace(
            __main_table__=cls.__tablename__,
            __tablename__=f"{cls.__tablename__}_{suffix}",
            __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
            __my_index_elements__=["zcta", "year", "measure_id"],
        ),
    )
    monkeypatch.setattr(
        places_module.db,
        "create_table",
        AsyncMock(side_effect=lambda table, **_kw: create_calls.append(table.name)),
    )
    monkeypatch.setattr(
        places_module.db,
        "status",
        AsyncMock(side_effect=lambda stmt: status_calls.append(stmt)),
    )
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "2026-03-19")

    ctx = {}
    await places_module.startup(ctx)

    assert ctx["import_date"] == "20260319"
    assert create_calls == ["pricing_places_zcta_20260319"]
    assert any("CREATE UNIQUE INDEX" in stmt for stmt in status_calls)


@pytest.mark.asyncio
async def test_shutdown_aborts_when_stage_rows_below_min(monkeypatch, places_module):
    monkeypatch.setattr(places_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        places_module,
        "make_class",
        lambda cls, suffix: SimpleNamespace(
            __main_table__=cls.__tablename__,
            __tablename__=f"{cls.__tablename__}_{suffix}",
        ),
    )
    monkeypatch.setattr(places_module, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(places_module.db, "scalar", AsyncMock(return_value=12))
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_MIN_ROWS", "500")

    ctx = {
        "import_date": "20260319",
        "context": {
            "run": 1,
            "start": datetime.datetime.utcnow(),
            "test_mode": False,
        },
    }

    with pytest.raises(RuntimeError, match="below minimum"):
        await places_module.shutdown(ctx)


@pytest.mark.asyncio
async def test_main_enqueues_process_data_job(monkeypatch, places_module):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(places_module, "create_pool", AsyncMock(return_value=fake_pool))
    monkeypatch.setattr(places_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await places_module.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": True},
        _queue_name=places_module.PLACES_ZCTA_QUEUE_NAME,
    )
