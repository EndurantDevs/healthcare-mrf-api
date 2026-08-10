# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import importlib
import datetime
from contextlib import asynccontextmanager
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

process_attributes = importlib.import_module("process.attributes")


from tests.process_attributes_unit_support import (
    _AsyncFileContext,
    _AsyncRows,
    _EmptyIndexAttributeModel,
    _IndexedAttributeModel,
    _OtherPlainAttributeModel,
    _PlainAttributeModel,
    _benefit_row,
    _install_csv_rows,
    _install_download_pipeline,
    _install_shutdown_database_fakes,
    _install_shutdown_model_fakes,
    _price_row,
)


@pytest.mark.asyncio
async def test_plan_attributes_main_enqueues_test_context(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    create_pool_mock = AsyncMock(return_value=fake_pool)
    monkeypatch.setattr(
        process_attributes,
        "create_pool",
        create_pool_mock,
    )

    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF",
        json.dumps([{"url": "https://example.com/plan.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF",
        json.dumps([{"url": "https://example.com/state.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PRICE_PLAN_URL_PUF",
        json.dumps([{"url": "https://example.com/price.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_BENEFITS_URL_PUF",
        json.dumps([{"url": "https://example.com/benefits.json", "year": "2026"}]),
    )
    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

    await process_attributes.main(test_mode=True)

    assert create_pool_mock.await_count == 1
    _, kwargs = create_pool_mock.await_args
    assert kwargs["default_queue_name"] == process_attributes.ATTRIBUTES_QUEUE_NAME
    assert kwargs["job_serializer"] is process_attributes.serialize_job
    assert kwargs["job_deserializer"] is process_attributes.deserialize_job
    assert fake_pool.enqueue_job.await_count == 4
    for call in fake_pool.enqueue_job.await_args_list:
        enqueued_job_payload = call.args[1]
        assert enqueued_job_payload["context"]["test_mode"] is True


@pytest.mark.asyncio
async def test_plan_attributes_control_start_runs_inline_fanout(monkeypatch):
    calls = []

    monkeypatch.setattr(
        process_attributes,
        "_attribute_source_groups",
        lambda: {
            "state_attributes": [{"url": "https://example.com/state.csv.zip", "year": "2026"}],
            "attributes": [{"url": "https://example.com/attr.csv.zip", "year": "2026"}],
            "prices": [{"url": "https://example.com/price.csv.zip", "year": "2026"}],
            "benefits": [{"url": "https://example.com/benefits.csv.zip", "year": "2026"}],
        },
    )

    async def fake_process_state(_ctx, task):
        calls.append(("state", task["context"]["test_mode"]))

    async def fake_process_attributes(ctx, task):
        calls.append(("attributes", task["context"]["test_mode"]))
        await ctx["redis"].enqueue_job("save_attributes", {"attr_obj_list": [], "context": task["context"]})

    async def fake_process_prices(_ctx, task):
        calls.append(("prices", task["context"]["test_mode"]))

    async def fake_process_benefits(_ctx, task):
        calls.append(("benefits", task["context"]["test_mode"]))

    async def fake_save(_ctx, _task):
        calls.append(("save", True))

    async def fake_shutdown(_ctx):
        calls.append(("shutdown", True))

    monkeypatch.setattr(process_attributes, "process_state_attributes", fake_process_state)
    monkeypatch.setattr(process_attributes, "process_attributes", fake_process_attributes)
    monkeypatch.setattr(process_attributes, "process_prices", fake_process_prices)
    monkeypatch.setattr(process_attributes, "process_benefits", fake_process_benefits)
    monkeypatch.setattr(process_attributes, "save_attributes", fake_save)
    monkeypatch.setattr(process_attributes, "shutdown", fake_shutdown)

    control_start_summary = await process_attributes.plan_attributes_control_start({}, {"test_mode": True})

    assert control_start_summary["test_mode"] is True
    assert control_start_summary["inline_save_jobs"] == 1
    assert calls == [
        ("state", True),
        ("attributes", True),
        ("save", True),
        ("prices", True),
        ("benefits", True),
        ("shutdown", True),
    ]


@pytest.mark.asyncio
async def test_shutdown_skips_missing_import_tables(monkeypatch):
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_attributes, "get_import_schema", lambda *_args, **_kwargs: "mrf")
    monkeypatch.setattr(process_attributes.db, "scalar", AsyncMock(return_value=False))
    status_mock = AsyncMock()
    ddl_mock = AsyncMock()
    monkeypatch.setattr(process_attributes.db, "status", status_mock)
    monkeypatch.setattr(process_attributes.db, "execute_ddl", ddl_mock)

    @asynccontextmanager
    async def fake_transaction():
        yield None

    monkeypatch.setattr(process_attributes.db, "transaction", fake_transaction)

    shutdown_context_map = {
        "import_date": "20260214",
        "context": {
            "test_mode": True,
            "start": datetime.datetime.utcnow(),
        },
    }
    await process_attributes.shutdown(shutdown_context_map)

    status_statements = [
        call.args[0] for call in status_mock.await_args_list if call.args
    ]
    assert status_mock.await_count == 0
    assert all("CREATE INDEX" not in sql for sql in status_statements)
    assert ddl_mock.await_count == 0


def test_attribute_helpers_cover_strict_flags_ids_and_bounds(monkeypatch):
    assert process_attributes._parse_flag(None, ("yes",), ("no",)) is None
    assert process_attributes._parse_flag(" YES ", ("yes",), ("no",)) is True
    assert process_attributes._parse_flag("NO", ("yes",), ("no",)) is False
    assert process_attributes._parse_flag("unknown", ("yes",), ("no",)) is None

    assert process_attributes._normalize_plan_ids(" 123 ", " 123-01 ") == (
        "123",
        "123-01",
    )
    assert process_attributes._normalize_plan_ids("", "123456789012345-01") == (
        "12345678901234",
        "123456789012345-01",
    )
    assert process_attributes._normalize_plan_ids("123", "") == (None, None)
    assert process_attributes._normalize_plan_ids("", "-01") == (None, "-01")

    monkeypatch.setenv("HLTHPRT_ATTRIBUTES_TEST_FILE_LIMIT", "0")
    monkeypatch.setenv("HLTHPRT_ATTRIBUTES_TEST_ROW_LIMIT", "0")
    assert process_attributes._test_file_limit() == 1
    assert process_attributes._test_row_limit() == 1
    assert process_attributes._bounded_test_files(range(3), True) == [0]
    assert process_attributes._bounded_test_files(range(3), False) == [0, 1, 2]


@pytest.mark.asyncio
async def test_inline_attribute_redis_dispatches_and_rejects_unknown(monkeypatch):
    save_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "save_attributes", save_mock)
    inline_redis = process_attributes._InlineAttributeRedis({"run": "ctx"})

    job = await inline_redis.enqueue_job("save_attributes", {"rows": []})

    assert job.job_id == "inline_save_attributes_1"
    save_mock.assert_awaited_once_with({"run": "ctx"}, {"rows": []})
    with pytest.raises(RuntimeError, match="Unsupported inline attributes job"):
        await inline_redis.enqueue_job("unknown", {})


@pytest.mark.asyncio
async def test_safe_unzip_uses_native_and_fallback_extractors(monkeypatch):
    unzip_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "unzip", unzip_mock)
    await process_attributes._safe_unzip("good.zip", "/tmp/good")
    unzip_mock.assert_awaited_once()

    unzip_mock.reset_mock(side_effect=True)
    unzip_mock.side_effect = ValueError("invalid archive")
    archive = MagicMock()
    archive_context = MagicMock()
    archive_context.__enter__.return_value = archive
    monkeypatch.setattr(
        process_attributes.zipfile,
        "ZipFile",
        MagicMock(return_value=archive_context),
    )

    await process_attributes._safe_unzip("fallback.zip", "/tmp/fallback")

    archive.extractall.assert_called_once_with("/tmp/fallback")


@pytest.mark.asyncio
async def test_prepare_attribute_tables_is_idempotent(monkeypatch):
    monkeypatch.setitem(
        process_attributes._TABLE_STATE_BY_KEY, "is_prepared", False
    )
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    status_mock = AsyncMock()
    create_mock = AsyncMock()
    monkeypatch.setattr(process_attributes.db, "status", status_mock)
    monkeypatch.setattr(process_attributes.db, "create_table", create_mock)

    generated_models = []

    def fake_make_class(model, import_date, *, schema_override):
        generated_model = SimpleNamespace(
            __main_table__=model.__main_table__,
            __tablename__=f"{model.__main_table__}_{import_date}",
            __table__=object(),
        )
        if len(generated_models) % 2 == 0:
            generated_model.__my_index_elements__ = ("plan_id", "year")
        generated_models.append((generated_model, schema_override))
        return generated_model

    monkeypatch.setattr(process_attributes, "make_class", fake_make_class)
    preparation_context_map = {
        "import_date": "20260721",
        "context": {"test_mode": True},
    }

    await process_attributes._prepare_attribute_tables(
        preparation_context_map
    )
    first_status_count = status_mock.await_count
    await process_attributes._prepare_attribute_tables(
        preparation_context_map
    )

    assert preparation_context_map["context"]["tables_prepared"] is True
    assert create_mock.await_count == 4
    assert all(schema == "mrf_test" for _, schema in generated_models)
    assert status_mock.await_count == first_status_count
    assert any("CREATE UNIQUE INDEX" in call.args[0] for call in status_mock.await_args_list)
