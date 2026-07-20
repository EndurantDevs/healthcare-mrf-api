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


class _AsyncRows:
    def __init__(self, rows):
        self._rows = list(rows)
        self._iterator = iter(())

    def __aiter__(self):
        self._iterator = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _AsyncFileContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


def _install_csv_rows(monkeypatch, rows):
    monkeypatch.setattr(
        process_attributes,
        "async_open",
        lambda *_args, **_kwargs: _AsyncFileContext(),
    )
    monkeypatch.setattr(
        process_attributes,
        "AsyncDictReader",
        lambda *_args, **_kwargs: _AsyncRows(rows),
    )


def _install_download_pipeline(monkeypatch, rows):
    _install_csv_rows(monkeypatch, rows)
    monkeypatch.setattr(
        process_attributes,
        "_prepare_attribute_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_attributes,
        "download_it_and_save",
        AsyncMock(),
    )
    monkeypatch.setattr(process_attributes, "_safe_unzip", AsyncMock())
    monkeypatch.setattr(
        process_attributes.glob,
        "glob",
        lambda _pattern: ["/tmp/provider-directory-coverage.csv"],
    )
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    monkeypatch.setattr(
        process_attributes,
        "make_class",
        lambda model, *_args, **_kwargs: model,
    )


def _benefit_row(**overrides):
    benefit_record_map = {
        "StandardComponentId": "12345678901234",
        "PlanId": "12345678901234-01",
        "BenefitName": "Primary care",
        "CopayInnTier1": "$10",
        "CopayInnTier2": "$20",
        "CopayOutofNet": "$40",
        "CoinsInnTier1": "10%",
        "CoinsInnTier2": "20%",
        "CoinsOutofNet": "40%",
        "LimitUnit": "Visits",
        "Exclusions": "",
        "Explanation": "Covered",
        "EHBVarReason": "",
        "IsEHB": "yes",
        "IsCovered": "covered",
        "QuantLimitOnSvc": "y",
        "IsExclFromInnMOOP": "no",
        "IsExclFromOonMOOP": "n",
        "LimitQty": "12.5",
        "BusinessYear": "2026",
    }
    benefit_record_map.update(overrides)
    return benefit_record_map


def _price_row(age, **overrides):
    price_record_map = {
        "PlanId": "12345678901234-01",
        "StateCode": "tx",
        "RateEffectiveDate": "2026-01-01",
        "RateExpirationDate": "2026-12-31",
        "RatingAreaId": "RATING AREA 1",
        "Tobacco": "No Preference",
        "Age": age,
        "IndividualRate": "100.25",
        "IndividualTobaccoRate": "120.50",
        "Couple": "200.75",
        "PrimarySubscriberAndOneDependent": "220.00",
        "PrimarySubscriberAndTwoDependents": "240.00",
        "PrimarySubscriberAndThreeOrMoreDependents": "260.00",
        "CoupleAndOneDependent": "300.00",
        "CoupleAndTwoDependents": "320.00",
        "CoupleAndThreeOrMoreDependents": "340.00",
    }
    price_record_map.update(overrides)
    return price_record_map


class _IndexedAttributeModel:
    __main_table__ = "indexed"
    __my_additional_indexes__ = [
        {
            "name": "search",
            "index_elements": ["plan_id"],
            "using": "gin",
            "unique": True,
            "where": "plan_id IS NOT NULL",
        },
        {"index_elements": ["year", "plan_id"]},
    ]


class _EmptyIndexAttributeModel:
    __main_table__ = "empty_indexes"
    __my_additional_indexes__ = []


class _PlainAttributeModel:
    __main_table__ = "plain"


class _OtherPlainAttributeModel:
    __main_table__ = "other_plain"


def _install_shutdown_model_fakes(monkeypatch):
    model_by_field = {
        "PlanAttributes": _IndexedAttributeModel,
        "PlanPrices": _EmptyIndexAttributeModel,
        "PlanRatingAreas": _PlainAttributeModel,
        "PlanBenefits": _OtherPlainAttributeModel,
    }
    for field_name, model in model_by_field.items():
        monkeypatch.setattr(process_attributes, field_name, model)

    def fake_make_class(model, import_date, *, schema_override):
        generated_model = SimpleNamespace(
            __main_table__=model.__main_table__,
            __tablename__=f"{model.__main_table__}_{import_date}",
        )
        if hasattr(model, "__my_additional_indexes__"):
            generated_model.__my_additional_indexes__ = (
                model.__my_additional_indexes__
            )
        assert schema_override == "mrf_test"
        return generated_model

    monkeypatch.setattr(process_attributes, "make_class", fake_make_class)


def _install_shutdown_database_fakes(monkeypatch):
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    monkeypatch.setattr(
        process_attributes,
        "_table_exists",
        AsyncMock(return_value=True),
    )
    status_mock = AsyncMock()
    ddl_mock = AsyncMock()
    monkeypatch.setattr(process_attributes.db, "status", status_mock)
    monkeypatch.setattr(process_attributes.db, "execute_ddl", ddl_mock)

    @asynccontextmanager
    async def fake_transaction():
        yield None

    monkeypatch.setattr(process_attributes.db, "transaction", fake_transaction)
    time_mock = MagicMock()
    monkeypatch.setattr(process_attributes, "print_time_info", time_mock)
    return status_mock, ddl_mock, time_mock


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
    monkeypatch.setattr(process_attributes, "_TABLES_PREPARED", False)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_type", "expected_model", "include_context"),
    [
        (None, "attributes", False),
        ("PlanBenefits", "benefits", True),
        ("PlanPrices", "prices", True),
    ],
)
async def test_save_attributes_selects_exact_staging_model(
    monkeypatch,
    task_type,
    expected_model,
    include_context,
):
    monkeypatch.setattr(
        process_attributes,
        "_prepare_attribute_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    model_name_by_class = {
        process_attributes.PlanAttributes: "attributes",
        process_attributes.PlanBenefits: "benefits",
        process_attributes.PlanPrices: "prices",
    }
    monkeypatch.setattr(
        process_attributes,
        "make_class",
        lambda model, *_args, **_kwargs: model_name_by_class[model],
    )
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    import_task_map = {"attr_obj_list": [{"id": 1}]}
    if include_context:
        import_task_map["context"] = {"test_mode": True}
    if task_type is not None:
        import_task_map["type"] = task_type

    await process_attributes.save_attributes(
        {"import_date": "20260721", "context": {}},
        import_task_map,
    )

    push_mock.assert_awaited_once_with([{"id": 1}], expected_model)


@pytest.mark.asyncio
async def test_process_attributes_maps_rows_and_skips_missing_plan(monkeypatch):
    source_record_list = [
        {"StandardComponentId": "", "PlanId": "", "Ignored": "x"},
        {
            "StandardComponentId": None,
            "PlanId": "12345678901234-01",
            "Metal Level": "Gold",
            "Empty": "",
        },
    ]
    _install_download_pipeline(monkeypatch, source_record_list)
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    redis = SimpleNamespace(enqueue_job=AsyncMock())

    await process_attributes.process_attributes(
        {"redis": redis, "import_date": "20260721", "context": {}},
        {
            "url": "https://example.test/attributes.zip",
            "year": "2026",
            "context": {"test_mode": False},
        },
    )

    pushed_rows, pushed_model = push_mock.await_args.args
    assert pushed_model is process_attributes.PlanAttributes
    assert {
        attribute_record["attr_name"]
        for attribute_record in pushed_rows
    } == {
        "PlanId",
        "Metal Level",
    }
    assert all(
        attribute_record["plan_id"] == "12345678901234"
        for attribute_record in pushed_rows
    )
    redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_state_attributes_maps_labels(monkeypatch):
    source_record_list = [
        {"STANDARD COMPONENT ID": "", "PLAN ID": ""},
        {
            "STANDARD COMPONENT ID": "12345678901234",
            "PLAN ID": "12345678901234-01",
            "PLAN MARKETING NAME": "Example Gold",
            "EMPTY": "",
        },
    ]
    _install_download_pipeline(monkeypatch, source_record_list)
    label_key_by_name = defaultdict(lambda: "unknown")
    label_key_by_name.update(
        {
            "STANDARD COMPONENT ID": "standard_component_id",
            "PLAN ID": "plan_id",
            "PLAN MARKETING NAME": "marketing_name",
            "EMPTY": "empty",
        }
    )
    monkeypatch.setattr(
        process_attributes,
        "plan_attributes_labels_to_key",
        label_key_by_name,
    )
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    redis = SimpleNamespace(enqueue_job=AsyncMock())

    await process_attributes.process_state_attributes(
        {"redis": redis, "import_date": "20260721", "context": {}},
        {
            "url": "https://example.test/state.zip",
            "year": "2026",
            "context": {"test_mode": False},
        },
    )

    pushed_rows = push_mock.await_args.args[0]
    assert {
        state_attribute_record["attr_name"]
        for state_attribute_record in pushed_rows
    } == {
        "standard_component_id",
        "plan_id",
        "marketing_name",
    }
    redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_benefits_covers_flags_limits_and_invalid_year(monkeypatch):
    source_record_list = [
        _benefit_row(StandardComponentId="", PlanId=""),
        _benefit_row(),
        _benefit_row(
            PlanId="12345678901234-02",
            IsEHB="no",
            IsCovered="not covered",
            QuantLimitOnSvc="unknown",
            IsExclFromInnMOOP=None,
            IsExclFromOonMOOP="yes",
            LimitQty="not-a-number",
            BusinessYear="",
        ),
        _benefit_row(
            PlanId="12345678901234-04",
            LimitQty="",
            BusinessYear="",
        ),
        _benefit_row(PlanId="12345678901234-03", BusinessYear="invalid"),
    ]
    _install_download_pipeline(monkeypatch, source_record_list)
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    redis = SimpleNamespace(enqueue_job=AsyncMock())

    await process_attributes.process_benefits(
        {"redis": redis, "import_date": "20260721", "context": {}},
        {
            "url": "https://example.test/benefits.zip",
            "year": "2026",
            "context": {"test_mode": False},
        },
    )

    pushed_rows = push_mock.await_args.args[0]
    assert len(pushed_rows) == 3
    assert pushed_rows[0]["year"] == 2026
    assert pushed_rows[0]["limit_qty"] == 12.5
    assert pushed_rows[1]["year"] is None
    assert pushed_rows[1]["limit_qty"] is None
    assert pushed_rows[1]["is_covered"] is False
    assert pushed_rows[2]["limit_qty"] is None
    redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_benefits_rejects_missing_business_year(monkeypatch):
    row = _benefit_row()
    del row["BusinessYear"]
    _install_download_pipeline(monkeypatch, [row])
    monkeypatch.setattr(process_attributes, "push_objects", AsyncMock())

    with pytest.raises(SystemExit):
        await process_attributes.process_benefits(
            {
                "redis": SimpleNamespace(enqueue_job=AsyncMock()),
                "import_date": "20260721",
                "context": {},
            },
            {"url": "https://example.test/benefits.zip", "year": "2026"},
        )


@pytest.mark.asyncio
async def test_process_rating_areas_loads_rows_and_handles_empty_file(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(process_attributes, "_PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    monkeypatch.setattr(
        process_attributes,
        "make_class",
        lambda model, *_args, **_kwargs: model,
    )
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    _install_csv_rows(
        monkeypatch,
        [
            {
                "STATE CODE": "tx",
                "COUNTY": "Travis",
                "ZIP3": "787",
                "RATING AREA ID": "1",
                "MARKET": "Individual",
            }
        ],
    )

    await process_attributes.process_rating_areas(
        {"import_date": "20260721", "context": {"test_mode": True}}
    )

    assert push_mock.await_args.args[0][0]["state"] == "TX"
    push_mock.reset_mock()
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    (data_directory / "rating_areas.csv").touch()
    _install_csv_rows(monkeypatch, [])
    await process_attributes.process_rating_areas(
        {"import_date": "20260721", "context": {}}
    )
    push_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_prices_covers_age_and_optional_rate_shapes(monkeypatch):
    empty_optional_value_by_field = {
        "RateEffectiveDate": "",
        "RateExpirationDate": "",
        "IndividualRate": "",
        "IndividualTobaccoRate": "",
        "Couple": "",
        "PrimarySubscriberAndOneDependent": "",
        "PrimarySubscriberAndTwoDependents": "",
        "PrimarySubscriberAndThreeOrMoreDependents": "",
        "CoupleAndOneDependent": "",
        "CoupleAndTwoDependents": "",
        "CoupleAndThreeOrMoreDependents": "",
    }
    source_record_list = [
        _price_row("", PlanId=""),
        _price_row("34"),
        _price_row(
            "35-44",
            PlanId="12345678901234-02",
            **empty_optional_value_by_field,
        ),
        _price_row("65 and over", PlanId="12345678901234-03"),
        _price_row("Family Option", PlanId="12345678901234-04"),
    ]
    _install_download_pipeline(monkeypatch, source_record_list)
    monkeypatch.setattr(process_attributes, "process_rating_areas", AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "return_checksum",
        lambda values: len(values),
    )
    push_mock = AsyncMock()
    monkeypatch.setattr(process_attributes, "push_objects", push_mock)
    redis = SimpleNamespace(enqueue_job=AsyncMock())

    await process_attributes.process_prices(
        {"redis": redis, "import_date": "20260721", "context": {}},
        {
            "url": "https://example.test/prices.zip",
            "year": "2026",
            "context": {"test_mode": False},
        },
    )

    pushed_rows = push_mock.await_args.args[0]
    assert [
        (price_record["min_age"], price_record["max_age"])
        for price_record in pushed_rows
    ] == [
        (34, 34),
        (35, 44),
        (65, 125),
        (0, 125),
    ]
    assert pushed_rows[1]["individual_rate"] is None
    assert pushed_rows[0]["rate_effective_date"].tzinfo is not None
    redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_builds_indexes_and_swaps_complete_tables(monkeypatch):
    _install_shutdown_model_fakes(monkeypatch)
    status_mock, ddl_mock, time_mock = _install_shutdown_database_fakes(
        monkeypatch
    )
    started_at = datetime.datetime.utcnow()

    await process_attributes.shutdown(
        {
            "import_date": "20260721",
            "context": {"test_mode": True, "start": started_at},
        }
    )

    status_statement_list = [
        call.args[0] for call in status_mock.await_args_list
    ]
    assert any(
        "CREATE UNIQUE INDEX" in statement and "USING gin" in statement
        for statement in status_statement_list
    )
    assert any(
        "WHERE plan_id IS NOT NULL" in statement
        for statement in status_statement_list
    )
    assert any(
        "RENAME TO indexed_old" in statement
        for statement in status_statement_list
    )
    assert ddl_mock.await_count == 4
    time_mock.assert_called_once_with(started_at)
