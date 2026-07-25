# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import importlib
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


class _Response:
    def __init__(self, payload, *, status=200):
        self.payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, *, content_type=None):
        assert content_type is None
        return self.payload


class _Client:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.urls = []
        self.closed = False

    def get(self, url, *, timeout):
        self.urls.append((url, timeout))
        return self.responses.pop(0)

    async def close(self):
        self.closed = True


class _CountyStage:
    __tablename__ = "medicare_county_stage"
    __table__ = object()


class _ZipStage:
    __tablename__ = "medicare_zip_stage"
    __table__ = object()


def _module():
    return importlib.import_module("process.medicare_enrollment")


def _make_stage(model, module):
    if model is module.MedicareEnrollmentCountyStats:
        return _CountyStage
    return _ZipStage


def _patch_process_dependencies(monkeypatch, module, client, zip_weights):
    pushes = []

    async def record_push(rows, stage_cls):
        pushes.append((tuple(rows), stage_cls))

    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: client),
    )
    monkeypatch.setattr(module, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        module,
        "make_class",
        lambda model, _import_date: _make_stage(model, module),
    )
    monkeypatch.setattr(module, "push_objects", record_push)
    monkeypatch.setattr(
        module,
        "_load_county_zip_weights",
        AsyncMock(return_value=zip_weights),
    )
    return pushes


@pytest.mark.asyncio
async def test_stage_indexes_cover_primary_and_optional_shapes(monkeypatch):
    module = _module()

    class IndexedStage:
        __tablename__ = "stage"
        __my_index_elements__ = ("first", "second")
        __my_additional_indexes__ = (
            {
                "index_elements": ("search",),
                "using": "gin",
                "where": "search IS NOT NULL",
            },
            {"name": "named", "index_elements": ("value",)},
        )

    status = AsyncMock()
    monkeypatch.setattr(module.db, "status", status)
    await module._create_stage_indexes(IndexedStage, "scope")
    await module._create_stage_indexes(SimpleNamespace(__tablename__="empty"), "scope")

    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert "stage_idx_primary" in statements
    assert "stage_idx_search" in statements
    assert "USING gin" in statements
    assert "WHERE search IS NOT NULL" in statements
    assert "stage_idx_named" in statements


def test_identifier_and_scalar_normalizers_cover_boundary_shapes():
    module = _module()

    assert module._stage_index_name("stage", "value") == "stage_idx_value"
    assert module._normalize_import_id(" release-20_26 ") == "release2026"
    assert len(module._normalize_import_id("--")) == 8
    assert len(module._normalize_import_id(None)) == 8
    assert module._archived_identifier("short") == "short_old"
    archived = module._archived_identifier("x" * 80)
    assert archived.endswith("_old")
    assert len(archived) <= module.POSTGRES_IDENTIFIER_MAX_LENGTH
    assert module._validate_schema_name("_valid_2") == "_valid_2"
    for invalid_schema in ("", "2bad", "bad-name"):
        with pytest.raises(ValueError, match="Invalid schema name"):
            module._validate_schema_name(invalid_schema)
    assert module._to_int(None) == 0
    assert module._to_int(" 1,234.9 ") == 1234
    assert module._to_int("bad") == 0
    assert module._normalize_fips("US-00123") == "00123"
    assert module._normalize_zip("12") == ""
    assert module._normalize_zip("12345-6789") == "12345"


@pytest.mark.asyncio
async def test_schema_creation_fails_closed_only_when_schema_is_absent(monkeypatch):
    module = _module()
    monkeypatch.setattr(module.db, "status", AsyncMock())
    monkeypatch.setattr(module.db, "scalar", AsyncMock())

    await module._ensure_schema_exists("valid_scope")
    module.db.status.side_effect = RuntimeError("permission")
    module.db.scalar.return_value = True
    await module._ensure_schema_exists("valid_scope")
    module.db.scalar.return_value = False
    with pytest.raises(RuntimeError, match="permission"):
        await module._ensure_schema_exists("valid_scope")


@pytest.mark.asyncio
async def test_catalog_and_year_resolution_validate_remote_contract():
    module = _module()
    api_url = "https://example.test/data-api/v1/dataset/abc/data"
    client = _Client(
        _Response(
            {
                "dataset": [
                    {"title": "unrelated"},
                    {
                        "title": "Medicare Monthly Enrollment current",
                        "distribution": [
                            {"accessURL": "https://example.test/file.csv"},
                            {"accessURL": api_url},
                        ],
                    },
                ]
            }
        ),
        _Response([{"YEAR": "2025"}]),
    )

    assert await module._resolve_enrollment_api_url(client) == api_url
    assert await module._resolve_latest_annual_year(client, api_url) == 2025
    with pytest.raises(ValueError, match="Could not find"):
        await module._resolve_enrollment_api_url(_Client(_Response({})))
    with pytest.raises(ValueError, match="No annual"):
        await module._resolve_latest_annual_year(
            _Client(_Response([])), api_url
        )
    with pytest.raises(ValueError, match="latest annual YEAR"):
        await module._resolve_latest_annual_year(
            _Client(_Response([{"YEAR": "bad"}])), api_url
        )


def test_allocation_handles_empty_zero_and_remainder_inputs():
    module = _module()

    weighted = module._allocate_by_weights(10, [("60654", 90), ("60610", 10)])
    assert sum(weighted.values()) == 10
    assert weighted["60654"] > weighted["60610"]
    assert module._allocate_by_weights(0, [("10001", 1)]) == {}
    assert module._allocate_by_weights(3, []) == {}
    assert module._allocate_by_weights(3, [("10001", 0), ("10002", -2)]) == {
        "10001": 2,
        "10002": 1,
    }
    assert module._allocate_by_weights(1, [("10001", 1), ("10002", 1)]) == {
        "10001": 1,
        "10002": 0,
    }


@pytest.mark.asyncio
async def test_county_zip_weights_normalize_dedupe_and_bad_population(monkeypatch):
    module = _module()
    rows = (
        SimpleNamespace(county_code="1001", zip_code="12345", population=10),
        SimpleNamespace(county_code="01001", zip_code="12345", population=2),
        SimpleNamespace(county_code="01001", zip_code="54321", population="bad"),
        SimpleNamespace(county_code="bad", zip_code="54321", population=3),
        SimpleNamespace(county_code="01003", zip_code="bad", population=3),
    )
    monkeypatch.setattr(
        module, "_has_geo_zip_lookup_table", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        module, "_load_county_zip_weight_rows", AsyncMock(return_value=rows)
    )

    assert await module._load_county_zip_weights() == {
        "01001": [("12345", 10), ("54321", 0)]
    }


@pytest.mark.asyncio
async def test_county_zip_weights_reconnect_only_in_test_mode(monkeypatch):
    module = _module()
    monkeypatch.setattr(
        module, "_has_geo_zip_lookup_table", AsyncMock(return_value=False)
    )
    with pytest.raises(RuntimeError, match="requires mrf.geo_zip_lookup"):
        await module._load_county_zip_weights()

    availability = AsyncMock(side_effect=(False, False))
    monkeypatch.setattr(module, "_has_geo_zip_lookup_table", availability)
    monkeypatch.setattr(module.db, "connect", AsyncMock())
    monkeypatch.setattr(module.db, "_database_override", "saved", raising=False)
    assert await module._load_county_zip_weights(test_mode=True) == {}
    assert module.db._database_override == "saved"
    assert module.db.connect.await_count == 2


@pytest.mark.asyncio
async def test_county_zip_weights_load_after_test_reconnect(monkeypatch):
    module = _module()
    monkeypatch.setattr(
        module,
        "_has_geo_zip_lookup_table",
        AsyncMock(side_effect=(False, True)),
    )
    monkeypatch.setattr(module.db, "connect", AsyncMock())
    monkeypatch.setattr(module.db, "_database_override", None, raising=False)
    monkeypatch.setattr(
        module,
        "_load_county_zip_weight_rows",
        AsyncMock(
            return_value=(
                SimpleNamespace(
                    county_code="01001", zip_code="12345", population=7
                ),
            )
        ),
    )

    assert await module._load_county_zip_weights(test_mode=True) == {
        "01001": [("12345", 7)]
    }


@pytest.mark.asyncio
async def test_publish_stage_rotates_tables_and_all_index_shapes(monkeypatch):
    module = _module()

    class Model:
        __main_table__ = "live_table"

    class Stage:
        __tablename__ = "stage_table"
        __my_additional_indexes__ = (
            {"index_elements": ("first",)},
            {"name": "named", "index_elements": ("second",)},
        )

    status = AsyncMock()
    monkeypatch.setattr(module.db, "status", status)
    await module._publish_stage_table("scope", Model, Stage)

    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert "live_table_old" in statements
    assert "stage_table" in statements
    assert "live_table_idx_first" in statements
    assert "live_table_idx_named" in statements


@pytest.mark.asyncio
async def test_process_data_publishes_county_and_weighted_zip_rows(monkeypatch):
    module = _module()
    api_url = "https://example.test/data-api/v1/dataset/abc/data"
    source_rows = [
        {"BENE_FIPS_CD": "01001", "YEAR": "2025", "TOT_BENES": "10", "PRSCRPTN_DRUG_TOT_BENES": "4"},
        {"BENE_FIPS_CD": "01001", "YEAR": "2025", "TOT_BENES": "2", "PRSCRPTN_DRUG_TOT_BENES": "-3"},
        {"BENE_FIPS_CD": "bad", "YEAR": "2025", "TOT_BENES": "8"},
        {"BENE_FIPS_CD": "01005", "YEAR": "0", "TOT_BENES": "8"},
        {"BENE_FIPS_CD": "01003", "YEAR": "2025", "TOT_BENES": "5", "PRSCRPTN_DRUG_TOT_BENES": "2"},
    ]
    client = _Client(
        _Response({"dataset": [{"title": "Medicare Monthly Enrollment", "distribution": [{"accessURL": api_url}]}]}),
        _Response([{"YEAR": "2025"}]),
        _Response(source_rows),
        _Response([]),
    )
    pushes = _patch_process_dependencies(
        monkeypatch,
        module,
        client,
        {"01001": [("12345", 3), ("12346", 1)]},
    )
    monkeypatch.setenv("HLTHPRT_MEDICARE_ENROLLMENT_BATCH_SIZE", "2")
    worker_state_by_name = {"import_date": "20250725", "context": {}}

    await module.process_data(worker_state_by_name)

    assert client.closed is True
    assert worker_state_by_name["context"] == {
        "run": 1,
        "latest_year": 2025,
        "county_rows": 2,
        "zip_rows": 2,
        "unmatched_counties": 1,
    }
    county_rows, county_stage = pushes[0]
    zip_rows, zip_stage = pushes[1]
    assert county_stage is _CountyStage
    assert zip_stage is _ZipStage
    assert sum(zip_row["total_beneficiaries"] for zip_row in zip_rows) == 12
    assert county_rows[0]["part_d_beneficiaries"] == 4


@pytest.mark.asyncio
async def test_process_data_test_limit_and_http_failure_close_clients(monkeypatch):
    module = _module()
    api_url = "https://example.test/data-api/v1/dataset/abc/data"
    source_row_by_field = {"BENE_FIPS_CD": "01001", "YEAR": "2025", "TOT_BENES": "1"}
    limited_client = _Client(
        _Response({"dataset": [{"title": "Medicare Monthly Enrollment", "distribution": [{"accessURL": api_url}]}]}),
        _Response([{"YEAR": "2025"}]),
        _Response([source_row_by_field, dict(source_row_by_field, BENE_FIPS_CD="01003")]),
    )
    _patch_process_dependencies(
        monkeypatch, module, limited_client, {"01001": [("12345", 1)]}
    )
    monkeypatch.setenv("HLTHPRT_MEDICARE_ENROLLMENT_TEST_ROWS", "1")
    await module.process_data(
        {"import_date": "20250725", "context": {}},
        {"test_mode": True},
    )
    assert limited_client.closed is True

    failed_client = _Client(
        _Response({"dataset": [{"title": "Medicare Monthly Enrollment", "distribution": [{"accessURL": api_url}]}]}),
        _Response([{"YEAR": "2025"}]),
        _Response([], status=503),
    )
    _patch_process_dependencies(monkeypatch, module, failed_client, {})
    with pytest.raises(ValueError, match="HTTP 503"):
        await module.process_data({"import_date": "20250725", "context": {}})
    assert failed_client.closed is True


@pytest.mark.asyncio
async def test_startup_builds_both_staging_tables(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "my_init_db", AsyncMock())
    monkeypatch.setattr(module, "ensure_database", AsyncMock())
    monkeypatch.setattr(module, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(module, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(
        module,
        "make_class",
        lambda model, _import_date: _make_stage(model, module),
    )
    monkeypatch.setattr(module.db, "status", AsyncMock())
    monkeypatch.setattr(module.db, "create_table", AsyncMock())
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "release-2025")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "scope")
    startup_context_by_name = {}

    await module.startup(startup_context_by_name)

    assert startup_context_by_name["import_date"] == "release2025"
    assert startup_context_by_name["context"]["run"] == 0
    assert module.db.create_table.await_count == 2
    assert module._create_stage_indexes.await_count == 2


@asynccontextmanager
async def _transaction():
    yield


def _patch_shutdown_dependencies(monkeypatch, module, counts):
    monkeypatch.setattr(module, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        module,
        "make_class",
        lambda model, _import_date: _make_stage(model, module),
    )
    monkeypatch.setattr(module.db, "scalar", AsyncMock(side_effect=counts))
    monkeypatch.setattr(module.db, "transaction", _transaction)
    monkeypatch.setattr(module, "_publish_stage_table", AsyncMock())
    monkeypatch.setattr(module, "mark_control_run", AsyncMock())
    monkeypatch.setattr(module, "print_time_info", lambda _started_at: None)


@pytest.mark.asyncio
async def test_shutdown_skips_empty_and_publishes_test_run(monkeypatch):
    module = _module()
    await module.shutdown({"context": {}})

    _patch_shutdown_dependencies(monkeypatch, module, (2, 3))
    shutdown_context_by_name = {
        "import_date": "20250725",
        "context": {
            "run": 1,
            "test_mode": True,
            "county_rows": 2,
            "unmatched_counties": 1,
            "control_run_id": "run-1",
        },
    }
    await module.shutdown(shutdown_context_by_name)

    assert module._publish_stage_table.await_count == 2
    module.mark_control_run.assert_awaited_once()
    assert module.mark_control_run.await_args.kwargs["metrics"]["zip_rows"] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("counts", "context_values", "message"),
    (
        ((499, 6000), {}, "county stage row count"),
        ((500, 4999), {}, "ZIP stage row count"),
        ((500, 5000), {"county_rows": 10, "unmatched_counties": 2}, "unmatched county ratio"),
    ),
)
async def test_shutdown_rejects_incomplete_production_stages(
    monkeypatch, counts, context_values, message
):
    module = _module()
    _patch_shutdown_dependencies(monkeypatch, module, counts)
    shutdown_context_by_name = {"import_date": "20250725", "context": {"run": 1}}
    shutdown_context_by_name["context"].update(context_values)

    with pytest.raises(RuntimeError, match=message):
        await module.shutdown(shutdown_context_by_name)


@pytest.mark.asyncio
async def test_shutdown_publishes_complete_production_run(monkeypatch):
    module = _module()
    _patch_shutdown_dependencies(monkeypatch, module, (500, 5000))
    shutdown_context_by_name = {
        "import_date": "20250725",
        "control_run_id": "fallback-run",
        "context": {"run": 1, "county_rows": 500},
    }

    await module.shutdown(shutdown_context_by_name)

    assert module._publish_stage_table.await_count == 2
    assert module.mark_control_run.await_args.args[0] == "fallback-run"


@pytest.mark.asyncio
async def test_main_enqueues_serialized_worker_payload(monkeypatch):
    module = _module()
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(module, "create_pool", AsyncMock(return_value=redis))
    monkeypatch.setattr(module, "build_redis_settings", lambda: "settings")

    await module.main(test_mode=True)

    module.create_pool.assert_awaited_once_with(
        "settings",
        job_serializer=module.serialize_job,
        job_deserializer=module.deserialize_job,
    )
    redis.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": True},
        _queue_name=module.MEDICARE_ENROLLMENT_QUEUE_NAME,
    )
