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
