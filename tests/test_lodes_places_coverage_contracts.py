# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Behavior contracts for LODES and PLACES importer helper boundaries."""

from __future__ import annotations

import csv
import gzip
import importlib
import io
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


lodes = importlib.import_module("process.lodes")
places = importlib.import_module("process.places_zcta")


class _AsyncResponse:
    def __init__(self, *, status=200, body=b"", json_payload=None):
        self.status = status
        self._body = body
        self._json_payload = json_payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def read(self):
        return self._body

    async def json(self, *, content_type=None):
        assert content_type is None
        return self._json_payload


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


def _places_csv(row_list):
    field_name_list = [
        "Year", "LocationID", "MeasureId", "Measure", "Data_Value",
        "Low_Confidence_Limit", "High_Confidence_Limit", "Data_Value_Type",
        "DataSource",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=field_name_list)
    writer.writeheader()
    writer.writerows(row_list)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_lodes_crosswalk_falls_back_from_short_local_file_to_census(monkeypatch, tmp_path):
    """An incomplete local file is discarded before the public relationship fallback."""

    local_path = tmp_path / "short.csv"
    local_path.write_text("TRACT,ZIP\n17031010100,60654\n", encoding="utf-8")
    census_text = (
        "GEOID_TRACT_20|GEOID_ZCTA5_20|AREALAND_PART\n"
        "17031010100|60654|1\n17031010200|60655|2\n"
    ).encode()
    monkeypatch.setattr(lodes, "MIN_TRACT_CROSSWALK_ROWS", 2)
    monkeypatch.setenv("HLTHPRT_LODES_CROSSWALK_FILE", str(local_path))
    monkeypatch.delenv("HLTHPRT_HUD_API_TOKEN", raising=False)

    class CensusClient:
        def get(self, url, **_kwargs):
            assert url == lodes.CENSUS_TRACT_ZCTA_REL_URL
            return _AsyncResponse(body=census_text)

    assert await lodes._load_tract_to_zip_crosswalk(CensusClient()) == {
        "17031010100": "60654", "17031010200": "60655"
    }


@pytest.mark.asyncio
async def test_lodes_hud_requires_the_documented_object_payload_and_rejects_non_success(monkeypatch):
    """HUD accepts its object payload and fails closed for malformed responses."""

    monkeypatch.setattr(lodes, "MIN_TRACT_CROSSWALK_ROWS", 1)

    class HudClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(
                json_payload={"data": {"results": [{"geoid": "17031010100", "zip": "60654"}]}}
            )

    zip_by_tract_geoid = {}
    assert await lodes._has_loaded_hud_tract_crosswalk(HudClient(), "token", zip_by_tract_geoid)
    assert zip_by_tract_geoid == {"17031010100": "60654"}

    class MalformedClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(json_payload=[{"geoid": "17031010100", "zip": "60654"}])

    assert not await lodes._has_loaded_hud_tract_crosswalk(MalformedClient(), "token", {})

    class DownClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(status=503)

    assert not await lodes._has_loaded_hud_tract_crosswalk(DownClient(), "token", {})


@pytest.mark.asyncio
async def test_lodes_resolver_uses_range_get_after_head_errors(monkeypatch):
    """A transient HEAD error does not hide an available historical WAC year."""

    class ProbeClient:
        def __init__(self):
            self.head_calls = 0
            self.get_calls = 0

        def head(self, *_args, **_kwargs):
            self.head_calls += 1
            raise OSError("head unavailable")

        def get(self, *_args, **_kwargs):
            self.get_calls += 1
            return _AsyncResponse(status=206)

    client = ProbeClient()
    assert await lodes._resolve_state_year(client, "il", 2021, 2020) == 2021
    assert (client.head_calls, client.get_calls) == (1, 1)


@pytest.mark.asyncio
async def test_lodes_resolver_accepts_head_success_and_census_handles_http_failure():
    """Cheap HEAD success wins; an unavailable Census response cannot create a partial crosswalk."""

    class HeadClient:
        def head(self, *_args, **_kwargs):
            return _AsyncResponse(status=200)

    assert await lodes._resolve_state_year(HeadClient(), "il", 2021, 2020) == 2021

    class CensusDown:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(status=503)

    assert not await lodes._has_loaded_census_tract_crosswalk(CensusDown(), {})


def test_lodes_identifier_helper_uses_date_only_when_no_alphanumeric_input_exists():
    """Empty or punctuation-only external IDs cannot become stable import identifiers."""

    assert lodes._normalize_import_id("run-123") == "run123"
    assert len(lodes._normalize_import_id("!!!")) == 8
    assert len(lodes._normalize_import_id(None)) == 8


@pytest.mark.asyncio
async def test_lodes_census_keeps_first_equal_area_and_rejects_unavailable_year(monkeypatch):
    """Equal-area rows retain deterministic first order; unavailable artifacts resolve to none."""

    monkeypatch.setattr(lodes, "MIN_TRACT_CROSSWALK_ROWS", 1)
    census_text = (
        "GEOID_TRACT_20|GEOID_ZCTA5_20|AREALAND_PART\n"
        "17031010100|60654|5\n17031010100|60655|5\n"
    ).encode()

    class CensusClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(body=census_text)

    zip_by_tract_geoid = {}
    assert await lodes._has_loaded_census_tract_crosswalk(CensusClient(), zip_by_tract_geoid)
    assert zip_by_tract_geoid == {"17031010100": "60654"}

    class MissingClient:
        def head(self, *_args, **_kwargs):
            return _AsyncResponse(status=404)

        def get(self, *_args, **_kwargs):
            return _AsyncResponse(status=404)

    assert await lodes._resolve_state_year(MissingClient(), "il", 2021, 2021) is None

    class UnavailableClient:
        def head(self, *_args, **_kwargs):
            return _AsyncResponse(status=500)

        def get(self, *_args, **_kwargs):
            return _AsyncResponse(status=404)

    assert await lodes._resolve_state_year(UnavailableClient(), "il", 2021, 2021) is None


@pytest.mark.asyncio
async def test_lodes_state_aggregates_valid_workers_in_bounded_batches(monkeypatch):
    """Invalid counts and unmapped blocks never enter the staged aggregate."""

    payload = gzip.compress(
        b"w_geocode,C000\n170310101001234,2\n170310101001235,3\nbad,9\n170310102001234,nope\n"
    )
    pushed_batch_list = []

    class Client:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(body=payload)

    async def push(rows, stage):
        pushed_batch_list.append((list(rows), stage))

    monkeypatch.setattr(lodes, "push_objects", push)
    total = await lodes._process_lodes_state(
        Client(), "il", 2021, {"17031010100": "60654"}, "stage", 1
    )
    assert total == 1
    assert pushed_batch_list[0][0][0]["zcta_code"] == "60654"
    assert pushed_batch_list[0][0][0]["total_workers"] == 5


@pytest.mark.asyncio
async def test_lodes_state_rejects_http_failures_and_parser_errors_without_staging(monkeypatch):
    """A bad state artifact leaves no partial rows to publish."""

    pushed = AsyncMock()
    monkeypatch.setattr(lodes, "push_objects", pushed)

    class NotFoundClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(status=404)

    assert await lodes._process_lodes_state(NotFoundClient(), "il", 2021, {}, "stage", 2) == 0

    class BadGzipClient:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(body=b"not-gzip")

    assert await lodes._process_lodes_state(BadGzipClient(), "il", 2021, {}, "stage", 2) == 0
    pushed.assert_not_awaited()


@pytest.mark.asyncio
async def test_lodes_state_flushes_residual_batch_and_preserves_largest_census_overlap(monkeypatch):
    """Residual aggregate rows are staged after the source stream ends, using the strongest tract match."""

    payload = gzip.compress(
        b"w_geocode,C000\n170310101001234,2\n170310102001234,3\n"
    )
    pushed_batch_list = []

    class Client:
        def get(self, *_args, **_kwargs):
            return _AsyncResponse(body=payload)

    async def push(rows, _stage):
        pushed_batch_list.append(list(rows))

    monkeypatch.setattr(lodes, "push_objects", push)
    assert await lodes._process_lodes_state(
        Client(), "il", 2021,
        {"17031010100": "60654", "17031010200": "60655"}, "stage", 3,
    ) == 2
    assert {staged_record["zcta_code"] for staged_record in pushed_batch_list[0]} == {"60654", "60655"}


@pytest.mark.asyncio
async def test_lodes_publish_error_paths_mark_control_failure_without_swapping(monkeypatch):
    """Missing geo validation and swap exceptions both produce a terminal failed control record."""

    stage = SimpleNamespace(__tablename__="lodes_stage")
    marked = AsyncMock()
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage)
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, False]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[10, 9]))
    monkeypatch.setattr(lodes, "mark_control_run", marked)

    with pytest.raises(RuntimeError, match="geo_zip_lookup"):
        await lodes.publish_lodes_generation(
            {"import_date": "run", "context": {"run": 1, "control_run_id": "control"}}
        )
    assert marked.await_args.kwargs["status"] == "failed"

    marked.reset_mock()
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, True]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[6000, 5500, 5000]))
    monkeypatch.setattr(lodes.db, "transaction", lambda: (_ for _ in ()).throw(RuntimeError("swap failed")))
    with pytest.raises(RuntimeError, match="swap failed"):
        await lodes.publish_lodes_generation(
            {"import_date": "run", "context": {"run": 1, "control_run_id": "control"}}
        )
    assert marked.await_args.kwargs["error"]["code"] == "lodes_publish_failed"


@pytest.mark.asyncio
async def test_lodes_publish_skips_idle_worker_without_database_access(monkeypatch):
    """An idle worker does not probe or alter the active generation during shutdown."""

    ensure = AsyncMock()
    monkeypatch.setattr(lodes, "ensure_database", ensure)
    await lodes.publish_lodes_generation({"context": {"run": 0}})
    ensure.assert_not_awaited()


@pytest.mark.asyncio
async def test_lodes_publish_missing_stage_modes_and_test_geo(monkeypatch):
    """Test workers record inert output; production workers reject missing publication prerequisites."""

    stage = SimpleNamespace(__tablename__="lodes_stage")
    marked_run_list = []

    async def mark_run(*_args, **mark_kwargs):
        marked_run_list.append(mark_kwargs)

    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage)
    monkeypatch.setattr(lodes, "mark_control_run", mark_run)
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=False))
    await lodes.publish_lodes_generation(
        {"import_date": "run", "context": {"run": 1, "test_mode": True, "control_run_id": "test"}}
    )
    assert marked_run_list[-1]["metrics"]["stage_rows"] == 0

    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="stage table"):
        await lodes.publish_lodes_generation(
            {"import_date": "run", "context": {"run": 1, "control_run_id": "production"}}
        )
    assert marked_run_list[-1]["status"] == "failed"

    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, False]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[3, 2]))
    monkeypatch.setattr(lodes.db, "status", AsyncMock())
    monkeypatch.setattr(lodes.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(lodes, "print_time_info", lambda _start: None)
    await lodes.publish_lodes_generation(
        {"import_date": "run", "context": {"run": 1, "test_mode": True, "control_run_id": "test"}}
    )
    assert marked_run_list[-1]["metrics"]["geo_match_ratio"] == 0.0


@pytest.mark.asyncio
async def test_lodes_production_thresholds_each_fail_closed(monkeypatch):
    """Rows, distinct ZCTAs, and geo ratio are independent production admission gates."""

    stage = SimpleNamespace(__tablename__="lodes_stage")
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage)
    monkeypatch.setattr(lodes, "mark_control_run", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, True]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[1, 1, 1]))
    with pytest.raises(RuntimeError, match="row count"):
        await lodes.publish_lodes_generation({"import_date": "run", "context": {"run": 1}})

    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, True]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[lodes.DEFAULT_MIN_ROWS, 1, 1]))
    with pytest.raises(RuntimeError, match="distinct ZCTA"):
        await lodes.publish_lodes_generation({"import_date": "run", "context": {"run": 1}})

    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(side_effect=[True, True]))
    monkeypatch.setattr(lodes.db, "scalar", AsyncMock(side_effect=[lodes.DEFAULT_MIN_ROWS, lodes.DEFAULT_MIN_DISTINCT_ZCTAS, 0]))
    with pytest.raises(RuntimeError, match="geo match ratio"):
        await lodes.publish_lodes_generation({"import_date": "run", "context": {"run": 1}})


@pytest.mark.asyncio
async def test_lodes_worker_creates_unindexed_stage_and_hud_success_short_circuits_fallback(monkeypatch):
    """A new stage is created without imaginary indexes, and a viable HUD map avoids Census."""

    client = SimpleNamespace(close=AsyncMock())
    monkeypatch.setitem(__import__("sys").modules, "aiohttp", SimpleNamespace(ClientSession=lambda: client))
    stage = SimpleNamespace(__tablename__="lodes_stage", __table__="table")
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(lodes.db, "create_table", AsyncMock())
    status = AsyncMock()
    monkeypatch.setattr(lodes.db, "status", status)
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage)
    monkeypatch.setattr(lodes, "_load_tract_to_zip_crosswalk", AsyncMock(return_value={"17031010100": "60654"}))
    monkeypatch.setattr(lodes, "_resolve_state_year", AsyncMock(return_value=None))
    monkeypatch.setattr(lodes, "ALL_STATES", ["il"])
    await lodes.process_lodes_data({"import_date": "run", "context": {}}, {})
    assert status.await_count == 0
    assert client.close.await_count == 1

    indexed_stage = SimpleNamespace(
        __tablename__="lodes_stage",
        __table__="table",
        __my_index_elements__=["zcta_code", "year"],
    )
    monkeypatch.setattr(lodes, "make_class", lambda *_args: indexed_stage)
    monkeypatch.setattr(lodes, "TEST_STATES", ["il"])
    await lodes.process_lodes_data(
        {"import_date": "run", "context": {}},
        {"test_mode": True},
    )
    assert "CREATE UNIQUE INDEX" in status.await_args.args[0]

    importlib.reload(lodes)
    monkeypatch.delenv("HLTHPRT_LODES_CROSSWALK_FILE", raising=False)
    monkeypatch.setenv("HLTHPRT_HUD_API_TOKEN", "token")
    monkeypatch.setattr(lodes, "_has_loaded_hud_tract_crosswalk", AsyncMock(return_value=True))
    monkeypatch.setattr(lodes, "_has_loaded_census_tract_crosswalk", AsyncMock())
    assert await lodes._load_tract_to_zip_crosswalk(object()) == {}
    lodes._has_loaded_census_tract_crosswalk.assert_not_awaited()
