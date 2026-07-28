# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Behavior contracts for importer helpers split out during readability work.

These cases exercise the actual streaming boundaries: invalid source rows never
reach staging, batches remain bounded, and a generation is only published after
its stage passes the configured admission checks.
"""

from __future__ import annotations

import csv
import datetime
import gzip
import importlib
import io
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


cms_doctors = importlib.import_module("process.cms_doctors")
doctor_rows = importlib.import_module("process.cms_doctors_rows")
lodes = importlib.import_module("process.lodes")
places = importlib.import_module("process.places_zcta")
attributes = importlib.import_module("process.attributes")


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


class _AsyncFile:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _AsyncRows:
    def __init__(self, rows):
        self._rows = iter(rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._rows)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def _doctor_row(**overrides):
    source_row_by_field = {
        "NPI": "1234567890",
        "Line 1 Street Address": "10 Main St",
        "Line 2 Street Address": "Suite 4",
        "City": "Springfield",
        "State": "IL",
        "Zip Code": "62704-1234",
        "Primary specialty": "Internal Medicine",
    }
    source_row_by_field.update(overrides)
    return source_row_by_field


def _places_csv(rows):
    fields = [
        "Year", "LocationID", "MeasureId", "Measure", "Data_Value",
        "Low_Confidence_Limit", "High_Confidence_Limit", "Data_Value_Type",
        "DataSource",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _benefit_row(**overrides):
    benefit_by_field = {
        "BenefitName": "Office visit", "CopayInnTier1": "$1", "CopayInnTier2": "$2",
        "CopayOutofNet": "$3", "CoinsInnTier1": "0.1", "CoinsInnTier2": "0.2",
        "CoinsOutofNet": "0.3", "LimitUnit": "visit", "Exclusions": "none",
        "Explanation": "standard", "EHBVarReason": "", "LimitQty": "3.5",
        "BusinessYear": "2026", "IsEHB": "yes", "IsCovered": "covered",
        "QuantLimitOnSvc": "no", "IsExclFromInnMOOP": "n", "IsExclFromOonMOOP": "y",
    }
    benefit_by_field.update(overrides)
    return benefit_by_field


def test_doctor_row_normalizes_aliases_and_rejects_unstageable_sources(monkeypatch):
    """The row boundary rejects bad NPIs/addresses before checksum deduplication."""

    fixed_now = datetime.datetime(2026, 7, 28, 12, 0, 0)
    checksum = object()
    monkeypatch.setattr(doctor_rows, "return_checksum", lambda values: checksum)

    assert doctor_rows.doctor_address_row({}, fixed_now) is None
    assert doctor_rows.doctor_address_row(_doctor_row(NPI="not-an-npi"), fixed_now) is None
    assert doctor_rows.doctor_address_row(_doctor_row(**{"Line 1 Street Address": ""}), fixed_now) is None
    assert doctor_rows.doctor_address_row(_doctor_row(**{"Zip Code": "123"}), fixed_now) is None

    alias_record = doctor_rows.doctor_address_row(
        {
            "npi": "9876543210",
            "adr_ln_1": "20 Lake St",
            "adr_ln_2": "",
            "citytown": "Chicago",
            "state": "IL",
            "zip_code": "60601-8899",
            "pri_spec": "Cardiology",
        },
        fixed_now,
    )

    assert alias_record == {
        "npi": 9876543210,
        "address_checksum": checksum,
        "address_line1": "20 Lake St",
        "address_line2": "",
        "city": "Chicago",
        "state": "IL",
        "zip_code": "60601",
        "provider_type": "Cardiology",
        "updated_at": fixed_now,
    }


def test_attribute_normalizers_preserve_only_meaningful_rows_and_fail_bad_years():
    """Wide source rows are normalized before any queue payload is constructed."""

    objects = attributes._attribute_objects_from_row(
        {"StandardComponentId": None, "Plan Name": " Gold ", "Empty": " ", "Accenté": 7},
        plan_id="plan", full_plan_id="plan-01", year=2026,
        attribute_name_by_label={"StandardComponentId": "id", "Plan Name": "name", "Empty": "empty", "Accenté": "accenté"},
    )
    assert objects == [
        {"plan_id": "plan", "full_plan_id": "plan-01", "year": 2026, "attr_name": "name", "attr_value": "Gold"},
        {"plan_id": "plan", "full_plan_id": "plan-01", "year": 2026, "attr_name": "accent", "attr_value": "7"},
    ]

    normalized = attributes._benefit_object_from_row(_benefit_row(), "plan", "plan-01")
    assert normalized["year"] == 2026
    assert normalized["limit_qty"] == 3.5
    assert normalized["is_ehb"] is True
    assert normalized["is_covered"] is True
    assert normalized["quant_limit_on_svc"] is False
    assert normalized["is_excl_from_oon_mo"] is True
    assert attributes._benefit_object_from_row(_benefit_row(BusinessYear="bad"), "plan", "plan-01") is None


@pytest.mark.asyncio
async def test_attribute_batch_owns_payload_until_enqueue_completes_and_startup_sets_context(monkeypatch):
    """A typed batch carries explicit test context and releases the transient list only after enqueue."""

    captured_payloads = []

    async def enqueue(_name, payload, **_kwargs):
        captured_payloads.append({**payload, "attr_obj_list": list(payload["attr_obj_list"])})

    redis = SimpleNamespace(enqueue_job=AsyncMock(side_effect=enqueue))
    attribute_row_list = [{"attr_name": "name"}]
    await attributes._enqueue_attribute_batch(redis, attribute_row_list, test_mode=True, record_type="PlanBenefits")
    redis.enqueue_job.assert_awaited_once()
    assert captured_payloads == [
        {"attr_obj_list": [{"attr_name": "name"}], "context": {"test_mode": True}, "type": "PlanBenefits"}
    ]
    assert attribute_row_list == []

    initialized = AsyncMock()
    monkeypatch.setattr(attributes, "init_db", initialized)
    worker_context_by_key = {"context": {"test_mode": True}}
    await attributes.startup(worker_context_by_key)
    assert worker_context_by_key["context"]["run"] == 0
    assert worker_context_by_key["context"]["test_mode"] is True
    assert worker_context_by_key["context"]["start"].tzinfo is not None
    initialized.assert_awaited_once()


@pytest.mark.asyncio
async def test_attribute_streaming_flushes_large_batches_and_stops_test_sources(monkeypatch):
    """Large marketplace inputs flush at the threshold; test inputs stop before unbounded reads."""

    standard_attribute_by_name = {"StandardComponentId": "plan", "PlanId": "plan-01"}
    state_attribute_by_name = {"STANDARD COMPONENT ID": "plan", "PLAN ID": "plan-01"}
    monkeypatch.setattr(attributes, "_prepare_attribute_tables", AsyncMock())
    monkeypatch.setattr(attributes, "get_import_schema", lambda *_args: "mrf")
    monkeypatch.setattr(attributes, "make_class", lambda *_args, **_kwargs: "stage")
    monkeypatch.setattr(attributes, "download_it_and_save", AsyncMock())
    monkeypatch.setattr(attributes, "_safe_unzip", AsyncMock())
    monkeypatch.setattr(attributes.glob, "glob", lambda _pattern: ["/virtual/source.csv"])
    monkeypatch.setattr(attributes, "async_open", lambda *_args, **_kwargs: _AsyncFile())
    flushed_batch_list = []

    async def enqueue(_redis, attribute_row_list, **enqueue_kwargs):
        flushed_batch_list.append((len(attribute_row_list), enqueue_kwargs))
        attribute_row_list.clear()

    monkeypatch.setattr(attributes, "_enqueue_attribute_batch", enqueue)
    monkeypatch.setattr(attributes, "push_objects", AsyncMock())
    monkeypatch.setattr(attributes, "AsyncDictReader", lambda *_args, **_kwargs: _AsyncRows([dict(standard_attribute_by_name) for _ in range(5001)]))
    await attributes.process_attributes(
        {"redis": object(), "import_date": "run", "context": {}},
        {"url": "https://example.test/attributes.zip", "year": "2026", "context": {"test_mode": False}},
    )
    assert flushed_batch_list == [(10002, {"test_mode": False})]

    pushed = AsyncMock()
    monkeypatch.setattr(attributes, "push_objects", pushed)
    monkeypatch.setattr(attributes, "AsyncDictReader", lambda *_args, **_kwargs: _AsyncRows([state_attribute_by_name, state_attribute_by_name]))
    monkeypatch.setattr(attributes, "plan_attributes_labels_to_key", {"STANDARD COMPONENT ID": "component", "PLAN ID": "plan"})
    monkeypatch.setenv("HLTHPRT_ATTRIBUTES_TEST_ROW_LIMIT", "1")
    await attributes.process_state_attributes(
        {"redis": object(), "import_date": "run", "context": {}},
        {"url": "https://example.test/state.zip", "year": "2026", "context": {"test_mode": True}},
    )
    assert pushed.await_args.args[0][0]["attr_name"] == "component"


@pytest.mark.asyncio
async def test_cms_reader_deduplicates_before_bounded_push_and_honors_test_limit(monkeypatch):
    """A duplicated provider location cannot consume a second batch slot."""

    now = datetime.datetime(2026, 7, 28)
    normalized_address_row_iterator = iter(
        [
            None,
            {"address_checksum": 1, "npi": 1},
            {"address_checksum": 1, "npi": 1},
            {"address_checksum": 2, "npi": 2},
            {"address_checksum": 3, "npi": 3},
        ]
    )
    monkeypatch.setattr(cms_doctors.datetime, "datetime", SimpleNamespace(utcnow=lambda: now))
    monkeypatch.setattr(cms_doctors, "doctor_address_row", lambda _row, _now: next(normalized_address_row_iterator))
    pushed_batch_list = []

    async def push(rows, stage_cls):
        pushed_batch_list.append((list(rows), stage_cls))

    cancelled = AsyncMock()
    monkeypatch.setattr(cms_doctors, "push_objects", push)
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", cancelled)

    accepted = await cms_doctors._consume_doctors_reader(
        [{}, {}, {}, {}, {}],
        ctx={"ctx": "value"},
        task={"task": "value"},
        stage_cls="stage",
        batch_size=2,
        test_mode=True,
        test_row_limit=3,
    )

    assert accepted == 3
    assert pushed_batch_list == [
        ([{"address_checksum": 1, "npi": 1}, {"address_checksum": 2, "npi": 2}], "stage"),
        ([{"address_checksum": 3, "npi": 3}], "stage"),
    ]
    assert cancelled.await_count == 2


@pytest.mark.asyncio
async def test_cms_reader_releases_exact_batch_without_a_second_empty_write(monkeypatch):
    """A full final batch is persisted once; there is no synthetic empty tail batch."""

    monkeypatch.setattr(cms_doctors, "doctor_address_row", lambda row, _now: row)
    pushed = AsyncMock()
    monkeypatch.setattr(cms_doctors, "push_objects", pushed)
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", AsyncMock())
    accepted = await cms_doctors._consume_doctors_reader(
        [{"address_checksum": 1}], ctx={}, task={}, stage_cls="stage",
        batch_size=1, test_mode=False, test_row_limit=100,
    )
    assert accepted == 1
    pushed.assert_awaited_once()


@pytest.mark.asyncio
async def test_cms_source_opens_csv_and_zip_and_rejects_zip_without_csv(monkeypatch, tmp_path):
    """Both supported file containers feed the same streaming importer."""

    calls = []

    async def consume(reader, **kwargs):
        calls.append((list(reader), kwargs))
        return 7

    monkeypatch.setattr(cms_doctors, "_consume_doctors_reader", consume)
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("NPI\n1234567890\n", encoding="utf-8")
    zip_path = tmp_path / "source.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("nested/addresses.CSV", "NPI\n9876543210\n")
    invalid_zip = tmp_path / "invalid.zip"
    with zipfile.ZipFile(invalid_zip, "w") as archive:
        archive.writestr("readme.txt", "no rows")

    import_kwargs_by_name = {
        "ctx": {}, "task": {}, "stage_cls": "stage", "batch_size": 4,
        "test_mode": True, "test_row_limit": 5,
    }
    assert await cms_doctors._import_doctors_source(str(csv_path), **import_kwargs_by_name) == 7
    assert await cms_doctors._import_doctors_source(str(zip_path), **import_kwargs_by_name) == 7
    with pytest.raises(ValueError, match="No CSV"):
        await cms_doctors._import_doctors_source(str(invalid_zip), **import_kwargs_by_name)

    assert [parsed_row_list[0]["NPI"] for parsed_row_list, _ in calls] == ["1234567890", "9876543210"]
    assert all(call_kwargs["batch_size"] == 4 for _, call_kwargs in calls)


@pytest.mark.asyncio
async def test_cms_publish_fails_closed_below_minimum_and_skips_empty_worker(monkeypatch):
    """A production generation cannot swap a sparse stage, while idle shutdown is inert."""

    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: SimpleNamespace(__tablename__="stage"))
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=12))

    with pytest.raises(RuntimeError, match="below minimum"):
        await cms_doctors.publish_cms_doctors_generation(
            {"import_date": "run", "context": {"run": 1, "test_mode": False}}
        )

    await cms_doctors.publish_cms_doctors_generation({"context": {"run": 0}})


@pytest.mark.asyncio
async def test_cms_publish_swaps_indexes_and_records_address_resolution(monkeypatch):
    """A valid stage atomically replaces the live generation and retains metrics."""

    status = AsyncMock()
    marked = AsyncMock()
    stage = SimpleNamespace(
        __tablename__="doctor_stage",
        __my_additional_indexes__=({"name": "site", "index_elements": ("state",)},),
    )
    address_stats = SimpleNamespace(scanned=4)
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: stage)
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=4))
    monkeypatch.setattr(cms_doctors.db, "status", status)
    monkeypatch.setattr(cms_doctors.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(cms_doctors, "source_enabled", lambda _source: True)
    monkeypatch.setattr(cms_doctors, "stamp_address_keys", AsyncMock())
    monkeypatch.setattr(cms_doctors, "resolve_into_archive", AsyncMock(return_value=address_stats))
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(cms_doctors, "mark_control_run", marked)
    monkeypatch.setattr(cms_doctors, "print_time_info", lambda _value: None)

    await cms_doctors.publish_cms_doctors_generation(
        {"import_date": "run", "context": {"run": 1, "test_mode": True, "control_run_id": "control", "start": "started"}}
    )

    sql_statement_list = [call.args[0] for call in status.await_args_list]
    assert any("doctor_clinician_address_old" in statement for statement in sql_statement_list)
    assert any("doctor_stage_idx_site" in statement for statement in sql_statement_list)
    assert marked.await_args.kwargs["metrics"] == {"rows": 4, "address_resolve": {"scanned": 4}}


@pytest.mark.asyncio
async def test_cms_publish_production_stage_without_address_feature_still_swaps(monkeypatch):
    """Address enrichment is optional, but production row admission remains mandatory."""

    status = AsyncMock()
    marked = AsyncMock()
    stage = SimpleNamespace(__tablename__="doctor_stage")
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: stage)
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=cms_doctors.DEFAULT_MIN_ROWS))
    monkeypatch.setattr(cms_doctors.db, "status", status)
    monkeypatch.setattr(cms_doctors.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(cms_doctors, "source_enabled", lambda _source: False)
    monkeypatch.setattr(cms_doctors, "mark_control_run", marked)
    monkeypatch.setattr(cms_doctors, "print_time_info", lambda _value: None)

    await cms_doctors.publish_cms_doctors_generation(
        {"import_date": "run", "context": {"run": 1, "control_run_id": "control", "start": "start"}}
    )
    assert marked.await_args.kwargs["metrics"] == {"rows": cms_doctors.DEFAULT_MIN_ROWS}


@pytest.mark.asyncio
async def test_cms_publish_without_extra_stage_indexes_skips_extra_rename_work(monkeypatch):
    """A stage with no secondary indexes cannot attempt to rename undeclared index names."""

    status = AsyncMock()
    stage = SimpleNamespace(__tablename__="doctor_stage", __my_additional_indexes__=())
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: stage)
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(cms_doctors.db, "status", status)
    monkeypatch.setattr(cms_doctors.db, "transaction", lambda: _Transaction())
    monkeypatch.setattr(cms_doctors, "source_enabled", lambda _source: False)
    monkeypatch.setattr(cms_doctors, "mark_control_run", AsyncMock())
    monkeypatch.setattr(cms_doctors, "print_time_info", lambda _value: None)

    await cms_doctors.publish_cms_doctors_generation(
        {"import_date": "run", "context": {"run": 1, "test_mode": True}}
    )
    assert not any("_idx_site" in call.args[0] for call in status.await_args_list)


@pytest.mark.asyncio
async def test_cms_worker_downloads_one_source_closes_client_and_marks_run(monkeypatch, tmp_path):
    """The task has one owned temporary download and always releases its HTTP client."""

    client = SimpleNamespace(close=AsyncMock())
    monkeypatch.setitem(sys.modules, "aiohttp", SimpleNamespace(ClientSession=lambda: client))
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: "stage")
    monkeypatch.setattr(cms_doctors, "_fetch_doctors_download_url", AsyncMock(return_value="https://example.test/doctors.csv"))
    monkeypatch.setattr(cms_doctors, "_download_doctors_source", AsyncMock())
    monkeypatch.setattr(cms_doctors, "_import_doctors_source", AsyncMock(return_value=7))
    worker_context_by_key = {"import_date": "run", "context": {}}

    await cms_doctors.import_cms_doctors_data(worker_context_by_key, {"test_mode": True})

    assert worker_context_by_key["context"]["run"] == 1
    cms_doctors._import_doctors_source.assert_awaited_once()
    assert cms_doctors._import_doctors_source.await_args.kwargs["test_mode"] is True
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cms_startup_and_entrypoint_build_stage_and_enqueue_exact_task(monkeypatch):
    """Startup uses the normalized run suffix, and the public entrypoint carries test mode."""

    stage = SimpleNamespace(__tablename__="doctor_stage", __table__="table")
    status = AsyncMock()
    pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(cms_doctors, "my_init_db", AsyncMock())
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "make_class", lambda *_args: stage)
    monkeypatch.setattr(cms_doctors, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(cms_doctors, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(cms_doctors.db, "status", status)
    monkeypatch.setattr(cms_doctors.db, "create_table", AsyncMock())
    monkeypatch.setattr(cms_doctors, "create_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(cms_doctors, "build_redis_settings", lambda: "settings")
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "run-01")

    worker_context_by_key = {}
    await cms_doctors.startup(worker_context_by_key)
    await cms_doctors.main(test_mode=True)

    assert worker_context_by_key["import_date"] == "run01"
    assert worker_context_by_key["context"]["run"] == 0
    assert "DROP TABLE IF EXISTS" in status.await_args.args[0]
    pool.enqueue_job.assert_awaited_once_with(
        "process_data", {"test_mode": True}, _queue_name=cms_doctors.CMS_DOCTORS_QUEUE_NAME
    )

